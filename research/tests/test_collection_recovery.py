"""Public toy Run through the ordinary collector and terminal validator."""
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import shutil
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'outer/tests'))
import test_recording_repair as recording
from outer.harness import evaluate, live_usage, profiles, run, runtime, staged_input, util
from research import collection_recovery as subject


class CollectionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.repo = self.base/'repo'; self.repo.mkdir()
        self.source = self.base/'acquisition/R'; self.source.mkdir(parents=True)
        self.destination = self.base/'recovery'
        root = self.source
        raw = recording.RecordingRepairTests().fixture(root, count=2)
        identity = dict(run_id='R', run_instance_id='1'*32)
        for name in ('started.jsonl', 'events.jsonl'):
            values = util.read_lines(raw/name)
            for value in values:
                value.update(session_id=identity['run_instance_id'], usage_complete=True,
                             send_evidence='observed_send', http_status=200)
            (raw/name).unlink()
            for value in values: util.append_line(raw/name, value)
        condition = dict(schema_version=2, task_id='toy', runtime={'model_id':'test-model'},
                         collection_policy=util.STATIC_DB_COLLECTION_POLICY)
        util.write_new_json(root/'condition.json', condition)
        (root/'profiles').mkdir(); (root/'evaluation-assets').mkdir()
        (root/'workspace/Data').mkdir(parents=True)
        (root/'workspace/Program.cs').write_bytes(b'// public toy\r\n')
        with closing(sqlite3.connect(root/'workspace/Data/store.sqlite')) as db:
            db.execute('CREATE TABLE toy (id INTEGER)'); db.commit()
        cleanup = dict(identity, network='toy-network', network_id='toy-network-id',
                       confirmed=True, status='removed')
        util.write_new_json(root/'evidence/network-cleanup/toy-result.json', cleanup)
        manifest = util.read_json(root/'manifest.json')
        manifest.update(identity, end_reason='completed', submission_fixed=False,
            network_cleanup=cleanup, collection_policy=condition['collection_policy'],
            condition_sha256=util.sha256_file(root/'condition.json'),
            prompt_sha256=util.sha256_file(root/'inputs/prompt.txt'),
            input_files=util.tree_hashes(root/'inputs'), assets_sha256={}, profile_files={})
        util.write_json_atomic(root/'manifest.json', manifest)
        util.write_new_json(root/'runtime.json', dict(identity, stop_confirmed=True,
            network=cleanup['network'], network_id=cleanup['network_id'],
            worker='s2-worker-'+'1'*16, gateway='s2-gateway-'+'1'*16))
        (root/'controller.lock').write_bytes(b'0')
        # Reproduce the late collection failure through the real collector.
        with patch.object(run, 'save_manifest', side_effect=PermissionError('synthetic denial')):
            with self.assertRaises(PermissionError): run.collect_run(root.parent, root.name)
        self.receipt = dict(identity, stop_confirmed=True, submission_fixed=False,
            collection_status='collection_fault', collection_error_type='PermissionError',
            manifest_sha256=util.sha256_file(root/'manifest.json'),
            snapshot_sha256=util.sha256_file(root/'snapshot.json'), raw=util.tree_hashes(raw))
        util.write_new_json(root/'implementation-receipt.json', self.receipt)

    def recover(self, **kwargs):
        # Only Docker resource observation is synthetic; profile/usage/session/
        # input/snapshot/terminal verification and recollection run normally.
        with patch.object(runtime, 'docker', return_value=SimpleNamespace(returncode=0, stdout='')), \
                patch.object(runtime, 'start', side_effect=AssertionError('Model dispatch forbidden')), \
                patch.object(run, 'create_run', side_effect=AssertionError('New acquisition forbidden')):
            ref = subject.recover(repo=self.repo, source=self.source, destination=self.destination, **kwargs)
        return Path(ref['path']), util.read_json(ref['path'])

    def test_same_uuid_db_and_usage_once_source_and_failure_bytes_unchanged(self):
        before = util.tree_hashes(self.source)
        path, result = self.recover()
        self.assertIsNone(result['error_type'])
        self.assertTrue(result['collection_recovered'])
        self.assertEqual(util.tree_hashes(self.source), before)
        root = path.parent/'run/R'
        self.assertEqual(profiles.validate_run(root), util.read_json(self.source/'condition.json'))
        self.assertEqual(util.read_json(root/'implementation-receipt.json'), self.receipt)
        self.assertEqual(util.tree_hashes(root/'frozen'), util.tree_hashes(self.source/'frozen'))
        self.assertEqual(util.sha256_file(root/'frozen/Data/store.sqlite'),
                         util.sha256_file(self.source/'workspace/Data/store.sqlite'))
        usage = util.read_json(root/'usage/normalized.json')
        self.assertEqual(usage['total_tokens'], 50)
        self.assertEqual(len(util.read_lines(root/'usage/events.jsonl')), 2)
        self.assertEqual(result['source_run_instance_id'], '1'*32)
        for key in ('model_calls', 'acquisition_count_increment', 'usage_count_increment'):
            self.assertEqual(result[key], 0)
        from research.acquisition_pipeline import usage_for_manifests
        self.assertEqual(usage_for_manifests([self.source/'manifest.json'])['requests'], 2)
        with self.assertRaisesRegex(ValueError, 'Duplicate Run UUID'):
            usage_for_manifests([self.source/'manifest.json', root/'manifest.json'])
        with self.assertRaises(FileExistsError): self.recover()

    def test_saved_scoring_api_receives_same_condition_and_remaining_budget(self):
        seen = []
        def score(repo, runs, rid, **kwargs):
            seen.append(kwargs)
            self.assertEqual(util.read_json(runs/rid/'condition.json'),
                             util.read_json(self.source/'condition.json'))
            self.assertEqual(rid, 'R')
            return dict(scoring_state='scored', operation_status='complete', quality=0, verdict='fail')
        with patch.object(evaluate, 'score_run', side_effect=score):
            _, result = self.recover(score=True, recovery_source_repo=self.repo, timeout=30)
        self.assertTrue(result['collection_recovered'])
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0]['recovery_source_repo'], self.repo)
        self.assertGreater(seen[0]['timeout'], 0); self.assertLess(seen[0]['timeout'], 30)
        self.assertEqual(result['scoring']['quality'], 0)
        self.assertTrue(result['recovery_complete'])

    def test_observed_control_keeps_native_inventory_and_zero_additional_proof(self):
        root = self.source; (root/'state').mkdir()
        policy = dict(suffixes=['.cs'], excluded_directories=[])
        _, _, partition = staged_input.split_sections(
            [dict(id='a', text='req'), dict(id='b', text='uest')], ['a'], ['b'])
        partition['initial_sha256'] = util.sha256_bytes(b'request')
        controller = staged_input.create(root/'_controller/staged-input', run_id='R',
            run_instance_id='1'*32, task='toy', condition='explore-observed', workspace=root/'workspace',
            worker_roots=[root/'inputs', root/'state'], initial_prompt='request', additional_prompt=None,
            budget_seconds=30, partition=partition, boundary_contract=dict(
                transport='opencode-server-response-barrier-v1', boundary_policy=staged_input.GROUP_BOUNDARY,
                observation_policy=staged_input.OBSERVATION_POLICY, snapshot_policy=policy,
                snapshot_policy_sha256=staged_input.digest(policy)))
        util.write_json_atomic(root/'context.json', dict(method='explore-observed', blocks=[]))
        manifest = util.read_json(root/'manifest.json')
        manifest.update(task_id='toy', condition_id='explore-observed', duration_seconds=1,
                        context_sha256=util.sha256_file(root/'context.json'))
        util.write_json_atomic(root/'manifest.json', manifest)
        controller.bind_run(root); controller.start(); controller.bind_session('native')
        controller.checkpoint('initial', {'native_session_id':'native'})
        part = dict(id='step', type='step-finish', messageID='assistant', sessionID='native', reason='stop')
        (root/'evidence/agent.jsonl').write_text(json.dumps(dict(type='step_finish', sessionID='native', part=part))+'\n')
        messages = [dict(info=dict(id='user', role='user', sessionID='native'), parts=[dict(
            id='input', type='text', messageID='user', sessionID='native', text='request')]),
            dict(info=dict(id='assistant', role='assistant', sessionID='native'), parts=[part])]
        inventory = controller.root/'native-messages.json'; util.write_new_json(inventory, messages)
        controller._append('native_message_inventory', dict(native_session_id='native', client_finished=True,
                                                          sha256=util.sha256_file(inventory)))
        identity = {k:controller.contract[k] for k in ('run_id', 'run_instance_id', 'delivery_id')}
        identity['contract_sha256'] = controller.contract_sha256
        raw = root/'usage/raw'
        util.append_line(raw/'staged-input.jsonl', dict(identity, kind='planned', additional_sha256=None))
        for request_id in ('0', '1'):
            util.append_line(raw/'staged-input.jsonl', dict(identity, kind='request_checked', request_id=request_id,
                request_sha256=util.sha256_file(raw/(request_id+'.request.json')), native_session_id='native',
                at='2026-10-11T00:00:00+00:00', receipt=dict(verified=False, locations=[],
                not_applicable='no_input_by_design'), rejection=None, after_resume=False, first_post_resume=False))
        controller.stop('operator_stop'); controller.checkpoint('final', {'stop_confirmed':True})
        controller.finish('operator_stop', stop_confirmed=True)
        receipt = {**self.receipt, 'manifest_sha256':util.sha256_file(root/'manifest.json'),
                   'raw':util.tree_hashes(raw)}
        util.write_json_atomic(root/'implementation-receipt.json', receipt)
        before = util.tree_hashes(root)
        path, result = self.recover()
        self.assertTrue(result['recovery_complete'], result.get('error_type'))
        usage = util.read_json(path.parent/'run/R/usage/normalized.json')
        self.assertTrue(usage['staged_input']['zero_additional_verified'])
        self.assertEqual(usage['total_tokens'], 50)
        self.assertEqual(before, util.tree_hashes(root))

    def test_ordinary_scoring_process_preserves_path_mismatch_only_in_derivative(self):
        stub = Path(recording.__file__).with_name('stub_evaluator.py')
        assets = self.source/'evaluation-assets'
        (assets/'evaluator').mkdir()
        shutil.copyfile(stub, assets/'evaluator/stub_evaluator.py')
        util.write_new_json(assets/'requirements.json', {'specVersion':'stub', 'taskId':'toy', 'requirements':[]})
        util.write_new_json(assets/'catalog.json', {})
        condition = util.read_json(self.source/'condition.json')
        condition.update(task_profile_revision='evaluators-20261006', evaluation=dict(
            assembly='stub_evaluator.py', evaluation_version='1.0.0',
            evaluator_sha256=util.sha256_file(stub), evaluator_build={},
            spec_path='evaluation-assets/requirements.json', catalog_path='evaluation-assets/catalog.json',
            spec_sha256=util.sha256_file(assets/'requirements.json')))
        util.write_json_atomic(self.source/'condition.json', condition)
        manifest = util.read_json(self.source/'manifest.json')
        manifest.update(condition_sha256=util.sha256_file(self.source/'condition.json'),
                        assets_sha256=util.tree_hashes(assets))
        util.write_json_atomic(self.source/'manifest.json', manifest)
        receipt = {**self.receipt, 'manifest_sha256':util.sha256_file(self.source/'manifest.json')}
        util.write_json_atomic(self.source/'implementation-receipt.json', receipt)
        before = util.tree_hashes(self.source)
        def command(condition, frozen, output, work, assets, version, sequence, **kwargs):
            return None, [sys.executable, str(stub), '--artifact', str(frozen), '--out', str(output),
                '--work', str(work), '--spec', str(assets/'requirements.json'),
                '--catalog', str(assets/'catalog.json'), '--evaluation-version', version,
                '--sequence', str(sequence)]
        # Substitute only the production bundle admission and Docker command.
        # The existing evaluator stub really executes; scoring/index validation runs.
        with patch.object(evaluate, 'saved_scoring_provenance', return_value={'kind':'synthetic-provenance'}), \
                patch.object(runtime, 'scoring_command', side_effect=command):
            path, result = self.recover(score=True)
        # The local stub reports a host path, not Docker's /artifact. The real
        # admission path must retain that mismatch, never relabel it as success.
        self.assertFalse(result['recovery_complete'])
        self.assertEqual(result['scoring']['scoring_state'], 'rejected_mismatch')
        self.assertEqual(result['scoring']['mismatches'][0]['check'], 'artifact_path')
        self.assertTrue((path.parent/'run/R/evaluations/index.jsonl').is_file())
        self.assertEqual(before, util.tree_hashes(self.source))

    def test_returned_scorer_fault_is_not_success_even_without_exception(self):
        with patch.object(evaluate, 'score_run', return_value={'scoring_state':'evaluator_fault'}):
            _, result = self.recover(score=True)
        self.assertFalse(result['recovery_complete'])
        self.assertEqual(result['scoring']['scoring_state'], 'evaluator_fault')

    def test_mandatory_browser_uses_source_epoch_pin_and_refuses_missing_pin(self):
        condition = util.read_json(self.source/'condition.json')
        condition['evaluation'] = {'evaluation_version':'1.6.0'}
        util.write_json_atomic(self.source/'condition.json', condition)
        manifest = util.read_json(self.source/'manifest.json')
        manifest['condition_sha256'] = util.sha256_file(self.source/'condition.json')
        def save():
            util.write_json_atomic(self.source/'manifest.json', manifest)
            util.write_json_atomic(self.source/'implementation-receipt.json', {**self.receipt,
                'manifest_sha256':util.sha256_file(self.source/'manifest.json')})
        save()
        with patch.object(evaluate, 'score_run') as score:
            _, result = self.recover(score=True)
        score.assert_not_called()
        self.assertEqual(result['error_stage'], 'scoring_environment')
        self.assertFalse(result['recovery_complete'])
        browser = self.base/'browser.json'; util.write_new_json(browser, {'toy_pin':True})
        epoch = self.base/'epoch.json'
        util.write_new_json(epoch, {'browser_pin':subject.live_pilot.reference(browser)})
        manifest['acquisition'] = {'epoch':subject.live_pilot.reference(epoch)}; save()
        self.destination = self.base/'with-pinned-browser'
        active = []
        @contextmanager
        def activated(record, repo):
            self.assertEqual(record, {'toy_pin':True}); self.assertEqual(repo, self.repo)
            active.append(True)
            try: yield
            finally: active.pop()
        def scored(*args, **kwargs):
            self.assertEqual(active, [True])
            return dict(scoring_state='scored', operation_status='complete', quality=0)
        with patch.object(subject.catalog_environment, 'activated', side_effect=activated), \
                patch.object(evaluate, 'score_run', side_effect=scored):
            _, result = self.recover(score=True)
        self.assertTrue(result['recovery_complete'])
        self.assertEqual(active, [])

    def test_prior_complete_usage_is_neither_added_nor_silently_corrected(self):
        usage = live_usage.collect(self.source)
        path, result = self.recover()
        self.assertIsNone(result['error_type'])
        self.assertEqual(util.read_json(path.parent/'run/R/usage/normalized.json')['total_tokens'],
                         usage['total_tokens'])
        usage['total_tokens'] += 1
        util.write_json_atomic(self.source/'usage/normalized.json', usage)
        self.destination = self.base/'different-recovery'
        _, failed = self.recover()
        self.assertEqual(failed['error_type'], 'ValueError')
        self.assertFalse(failed['collection_recovered'])

    def test_snapshot_or_workspace_conflict_is_retained_and_not_scored(self):
        (self.source/'workspace/Program.cs').write_text('// different public toy')
        before = util.tree_hashes(self.source)
        with patch.object(evaluate, 'score_run') as score:
            path, result = self.recover(score=True)
        score.assert_not_called()
        self.assertEqual(result['error_type'], 'ValueError')
        self.assertFalse(result['collection_recovered'])
        self.assertTrue((path.parent/'saved-frozen').is_dir())
        self.assertEqual(before, util.tree_hashes(self.source))

    def test_missing_native_or_gateway_evidence_cannot_be_recovered_as_complete(self):
        (self.source/'evidence/agent.jsonl').unlink()
        with patch.object(evaluate, 'score_run') as score:
            _, result = self.recover(score=True)
        score.assert_not_called()
        self.assertEqual(result['error_type'], 'ValueError')
        self.assertFalse(result['collection_recovered'])

    def test_preexisting_scoring_or_unstopped_source_refused_before_copy(self):
        for mode in ('score', 'running', 'wrong_fault'):
            if mode == 'score':
                util.append_line(self.source/'evaluations/index.jsonl', {'sequence':1})
            elif mode == 'running':
                (self.source/'evaluations/index.jsonl').unlink()
                manifest = util.read_json(self.source/'manifest.json'); manifest['stop_confirmed'] = False
                util.write_json_atomic(self.source/'manifest.json', manifest)
            else:
                receipt = {**self.receipt, 'collection_error_type':'OSError'}
                util.write_json_atomic(self.source/'implementation-receipt.json', receipt)
            with self.assertRaises(ValueError): self.recover()
            self.assertFalse(self.destination.exists())

    def test_destination_overlap_and_stop_do_not_write_source(self):
        before = util.tree_hashes(self.source)
        for destination in (self.source/'new', self.source.parent/'new', self.repo/'new', self.base):
            with self.assertRaises(ValueError):
                subject.recover(repo=self.repo, source=self.source, destination=destination)
        stop = self.base/'STOP'; stop.touch()
        with self.assertRaises(InterruptedError): self.recover(stop_file=stop)
        self.assertEqual(before, util.tree_hashes(self.source))
        self.assertFalse(self.destination.exists())

    def test_bound_campaign_siblings_are_not_recovery_destinations(self):
        config = self.base/'acquisition/config.json'
        util.write_new_json(config, {'root':str(config.parent)})
        manifest = util.read_json(self.source/'manifest.json')
        manifest['acquisition'] = {'campaign_config':subject.live_pilot.reference(config)}
        util.write_json_atomic(self.source/'manifest.json', manifest)
        receipt = {**self.receipt, 'manifest_sha256':util.sha256_file(self.source/'manifest.json')}
        util.write_json_atomic(self.source/'implementation-receipt.json', receipt)
        with self.assertRaises(ValueError):
            subject.recover(repo=self.repo, source=self.source,
                            destination=config.parent/'different-slot/recovery')
        path, result = self.recover()
        self.assertTrue(result['recovery_complete'])

    def test_active_source_lock_refused_without_copy(self):
        from outer.harness import ownership
        before = util.tree_hashes(self.source)
        with ownership.lease(self.source):
            with self.assertRaises(OSError): self.recover()
        self.assertFalse(self.destination.exists())
        self.assertEqual(before, util.tree_hashes(self.source))

    def test_deadline_after_copy_or_stop_after_collection_blocks_scoring(self):
        before = util.tree_hashes(self.source)
        clocks = iter([0, 0])
        with patch.object(subject.time, 'monotonic', side_effect=lambda: next(clocks, 31)), \
                patch.object(evaluate, 'score_run') as score:
            _, result = self.recover(score=True, timeout=30)
        score.assert_not_called()
        self.assertEqual(result['error_type'], 'TimeoutError')
        self.destination = self.base/'stop-after-collection'
        collect = run.collect_run
        def stop(runs, rid, **kwargs):
            result = collect(runs, rid, **kwargs)
            (runs.parent/'STOP').touch()
            return result
        with patch.object(run, 'collect_run', side_effect=stop), patch.object(evaluate, 'score_run') as score:
            _, result = self.recover(score=True)
        score.assert_not_called()
        self.assertEqual(result['error_type'], 'InterruptedError')
        self.assertEqual(before, util.tree_hashes(self.source))

    def test_scoring_crash_and_source_mutation_are_not_success(self):
        before = util.tree_hashes(self.source)
        with patch.object(evaluate, 'score_run', side_effect=RuntimeError('synthetic scorer failure')):
            _, result = self.recover(score=True)
        self.assertEqual(result['error_type'], 'RuntimeError')
        self.assertFalse(result['collection_recovered'])
        self.assertEqual(before, util.tree_hashes(self.source))
        self.destination = self.base/'different'
        def mutate(*args, **kwargs):
            (self.source/'unexpected.txt').write_text('synthetic concurrent writer')
            return {}
        with patch.object(evaluate, 'score_run', side_effect=mutate):
            _, result = self.recover(score=True)
        self.assertFalse(result['source_unchanged'])
        self.assertFalse(result['collection_recovered'])


if __name__ == '__main__': unittest.main()
