"""Unsent preparation closeout; all resource observations are synthetic fixtures."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_pipeline_owner import fixture, REPO
from outer.harness import util, runtime
from research import campaign_launcher, live_pilot, pair_execution, pipeline_guard as guard, pipeline_retry as retry


def synthetic_observer_closure(context):
    """Fixture boundary, not a claim of a real observer ACK/process acceptance."""
    rows=[]
    for number in context['pairs']:
        batch=Path(context['epoch']['batch'])/('pair-'+str(number))
        if not (batch/'pair-invocation.json').exists():continue
        directory=Path(context['epoch']['batch'])/'_observers'/('synthetic-'+str(number))
        shutdown=dict(session=directory.name,phase_sha256=util.sha256_file(batch/'phase.json'),
            generation=1,dispatched=[])
        for name in ('config.json','startup-ready.json','scope-000001.json','scope-ack-000001.json',
                     'shutdown.json','shutdown-ack.json'):
            path=directory/name
            if not path.exists():util.write_new_json(path,dict(shutdown,synthetic=True))
        rows.append(dict(directory=str(directory),shutdown=shutdown,confirmed=True,
            shutdown_ack_confirmed=True,process_exit_verified=True))
    return dict(confirmed=True,observers=rows,synthetic=True)


class PreparationRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config,self.record,self.source,_=fixture(self.tmp.name)
        self.root=Path(self.config['root']);self.config_path=self.root/'config.json'
        self.manifest=dict(run_id='R',started_at=None,ended_at=None,duration_seconds=None,model_called=False)
        util.write_json_atomic(self.source/'manifest.json',self.manifest)
        attempt=util.read_json(self.record)
        util.write_new_json(self.source.parent/'pair-invocation.json',
            dict(owner_plan=attempt['epoch'],observer_phase=attempt['phase']))
        util.write_new_json(self.root/'STOP',dict(kind='synthetic_preparation_interruption'))
        self.context=patch.object(guard,'context',return_value=self.config)
        # retry.guard dynamically loads the actual guard, so patch only context.
        self.owner=patch.object(retry,'guard',return_value=guard)
        self.observers=patch.object(campaign_launcher,'close_observers',side_effect=synthetic_observer_closure)
        self.docker=patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=0,stdout=''))
        for mock in (self.context,self.owner,self.observers,self.docker):
            mock.start();self.addCleanup(mock.stop)

    def exclude(self):
        return retry.exclude(REPO,self.config_path,self.record,'controller_interrupted','synthetic-existing-authorization')

    def test_uuid_less_manifest_seals_without_rewriting_and_guard_counts_it_once(self):
        before=util.tree_hashes(self.source);stop=(self.root/'STOP').read_bytes()
        ref=self.exclude();receipt=util.read_json(ref['path'])
        self.assertEqual(receipt['evidence']['kind'],retry.PREPARATION)
        self.assertFalse(receipt['evidence']['state']['manifest_uuid_present'])
        self.assertIsNone(receipt['evidence']['state']['measured_duration_seconds'])
        self.assertEqual(receipt['budget_usage']['dispatched_runs'],0)
        self.assertEqual(util.tree_hashes(self.source),before)
        self.assertEqual((self.root/'STOP').read_bytes(),stop)
        self.assertEqual(self.exclude(),ref)
        policy=retry.Exclusions(self.config_path)
        self.assertTrue(policy.excluded(dict(record=str(self.record))))
        # The guard remains STOP-sensitive. Archive the fixture STOP explicitly
        # outside exclusion, as the separately authorized execution-PC resume does.
        (self.root/'STOP').rename(self.root/'retained-STOP')
        snapshot=guard.budget_snapshot(self.config)
        self.assertEqual(snapshot['run_observations']['1'*32]['state'],'excluded_pre_dispatch')
        self.assertEqual(snapshot['usage']['requests'],0)
        self.assertEqual(sum(e['kind']=='reserved' for e in pair_execution.events(self.root/'events.jsonl')),1)

    def test_absent_manifest_and_absent_run_directory_are_supported(self):
        for path in list(self.source.iterdir()):path.unlink()
        self.source.rmdir()
        ref=self.exclude()
        self.assertFalse(self.source.exists())
        self.assertFalse(util.read_json(ref['path'])['evidence']['state']['manifest_present'])
        retry.Exclusions(self.config_path)

    def test_reserved_only_complete_manifest_is_supported_without_runtime(self):
        self.manifest['run_instance_id']='1'*32
        util.write_json_atomic(self.source/'manifest.json',self.manifest)
        case=guard.attempts(self.config)[0]['cases'][0]
        pair_execution.append(self.source.parent/'_control/pair-journal.jsonl',
            dict(kind='pair_reserved',assignments=[case]))
        before=util.tree_hashes(self.source);self.exclude()
        self.assertEqual(util.tree_hashes(self.source),before)

    def held(self):
        before=set((self.root/'_exclusion-holds').glob('*/*.json'))
        with self.assertRaises((ValueError,OSError)):self.exclude()
        self.assertFalse((self.root/'_exclusions'/('1'*32+'.json')).exists())
        self.assertFalse((self.source.parent/'pipeline-acquisition.json').exists())
        self.assertTrue((self.root/'STOP').exists())
        holds=set((self.root/'_exclusion-holds').glob('*/*.json'))-before
        self.assertEqual(len(holds),1)
        return util.read_json(holds.pop())

    def test_dispatch_with_missing_manifest_is_held_without_zero_claim(self):
        case=guard.attempts(self.config)[0]['cases'][0];journal=self.source.parent/'_control/pair-journal.jsonl'
        pair_execution.append(journal,dict(kind='pair_reserved',assignments=[case]))
        pair_execution.append(journal,dict(kind='dispatch',**case))
        (self.source/'manifest.json').unlink()
        value=self.held()
        self.assertTrue(value['known_usage']['totals_unknown'])
        self.assertIsNone(value['known_usage']['observed_tokens_lower_bound'])

    def test_partial_raw_usage_is_retained_in_hold_and_originals(self):
        util.append_line(self.source/'usage/raw/started.jsonl',dict(request_id='known'))
        util.append_line(self.source/'usage/raw/events.jsonl',dict(request_id='known',
            usage=dict(input_tokens=17,output_tokens=None)))
        before=util.tree_hashes(self.source);value=self.held()
        self.assertEqual(value['known_usage']['request_start_records'],1)
        self.assertEqual(value['known_usage']['observed_tokens_lower_bound'],17)
        self.assertTrue(value['known_usage']['totals_unknown'])
        self.assertEqual(util.tree_hashes(self.source),before)
        for path in (self.source/'usage/raw').iterdir():path.unlink()
        # Deleting later-observed evidence cannot turn its retained cost into 0.
        self.assertIn('Previously observed execution',self.held()['reason'])

    def assert_retained_execution_hold(self,remove):
        value=self.held()
        self.assertTrue(value['execution_evidence_seen'])
        self.assertTrue(value['known_usage']['totals_unknown'])
        remove()
        self.assertIn('Previously observed execution',self.held()['reason'])
        self.assertEqual(sum(e['kind']=='reserved' for e in pair_execution.events(self.root/'events.jsonl')),1)

    def test_native_session_only_refusal_survives_later_manifest_evidence_loss(self):
        util.write_json_atomic(self.source/'manifest.json',dict(self.manifest,native_session_id='synthetic-native'))
        self.assert_retained_execution_hold(lambda:util.write_json_atomic(self.source/'manifest.json',self.manifest))

    def test_model_called_only_refusal_survives_later_manifest_evidence_loss(self):
        util.write_json_atomic(self.source/'manifest.json',dict(self.manifest,model_called=True))
        self.assert_retained_execution_hold(lambda:util.write_json_atomic(self.source/'manifest.json',self.manifest))

    def test_staged_delivery_refusal_survives_later_ledger_loss(self):
        path=self.source/'_controller/staged-input/events.jsonl'
        util.append_line(path,dict(kind='acknowledged'))
        self.assert_retained_execution_hold(path.unlink)

    def test_snapshot_refusal_survives_later_snapshot_loss(self):
        path=self.source/'snapshots/checkpoint.json';util.write_new_json(path,dict(synthetic=True))
        self.assert_retained_execution_hold(path.unlink)

    def test_execution_decision_is_latched_before_the_hold_writer_rereads_files(self):
        util.write_json_atomic(self.source/'manifest.json',dict(self.manifest,native_session_id='synthetic-native'))
        actual=retry.record_preparation_hold
        def write(*args):
            util.write_json_atomic(self.source/'manifest.json',self.manifest)
            return actual(*args)
        with patch.object(retry,'record_preparation_hold',side_effect=write):value=self.held()
        self.assertTrue(value['execution_evidence_seen'])
        self.assertIn('Previously observed execution',self.held()['reason'])

    def test_planned_only_staged_input_remains_eligible_for_unsent_exclusion(self):
        util.append_line(self.source/'_controller/staged-input/events.jsonl',dict(kind='planned'))
        ref=self.exclude()
        self.assertEqual(util.read_json(ref['path'])['budget_usage']['requests'],0)

    def test_unreadable_manifest_is_held_with_known_components(self):
        (self.source/'manifest.json').write_text('{')
        util.append_line(self.source/'usage/raw/events.jsonl',dict(request_id='known',
            usage=dict(input_tokens=17,output_tokens=None)))
        value=self.held()
        self.assertEqual(value['known_usage']['observed_tokens_lower_bound'],17)
        self.assertEqual(value['known_usage']['observed_token_components'],[17])

    def test_runtime_or_partial_journal_is_not_unsent(self):
        util.write_new_json(self.source/'runtime.json',dict(run_id='R',run_instance_id='1'*32))
        self.held();(self.source/'runtime.json').unlink()
        journal=self.source.parent/'_control/pair-journal.jsonl';journal.write_text('{')
        self.held()

    def test_unknown_observer_docker_or_conflicting_owner_prevents_exclusion(self):
        with patch.object(campaign_launcher,'close_observers',return_value=dict(confirmed=False,observers=[])):
            self.held()
        with patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=1,stdout='')):
            self.held()
        with patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=0,stdout='owned-container')):
            self.held()
        with pair_execution.exclusive(self.root/'_owner'):self.held()

    def test_observer_remembers_dispatch_even_if_run_journal_is_absent(self):
        def sent(context):
            result=synthetic_observer_closure(context)
            result['observers'][0]['shutdown']['dispatched']=['R']
            return result
        with patch.object(campaign_launcher,'close_observers',side_effect=sent):self.held()

    def test_source_or_bound_preparation_journal_changes_after_seal_are_rejected(self):
        self.exclude()
        case=guard.attempts(self.config)[0]['cases'][0]
        pair_execution.append(self.source.parent/'_control/pair-journal.jsonl',
            dict(kind='pair_reserved',assignments=[case]))
        with self.assertRaises(ValueError):retry.Exclusions(self.config_path)
        with self.assertRaises(ValueError):guard.budget_snapshot(self.config)


if __name__=='__main__':unittest.main()
