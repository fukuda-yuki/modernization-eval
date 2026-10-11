"""Preparation race regressions and a real four-acquisition pipeline/guard composition.

No provider, Docker, private inputs or real evaluation. The composition keeps
supervise/controller/run_pipeline/PairWatch/execute_owned_pair_scope/execute_pair
and create_run real; only pinned-material/resource/model/evaluator boundaries
use synthetic fixtures. It exercises startup, not merely controller death.
"""
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from outer.harness import run, util
from research import acquisition_pipeline as pipeline, live_pilot, pair_execution, pipeline_guard as guard
from research import process_identity


class PreparationBudgetTests(unittest.TestCase):
    def setUp(self):
        from test_pipeline_owner import fixture
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.config, self.record, self.root, _ = fixture(self.tmp.name)
        self.manifest = dict(run_id='R', started_at=None, ended_at=None, duration_seconds=None, model_called=False)
        util.write_json_atomic(self.root/'manifest.json', self.manifest)
        util.write_new_json(self.record.parent/'pipeline-preparation.json',
            dict(kind='pipeline_preparation_v1', attempt=live_pilot.reference(self.record)))
        self.owner = pair_execution.exclusive(Path(self.config['root'])/'_owner')
        self.owner.__enter__(); self.addCleanup(self.owner.__exit__, None, None, None)

    def test_valid_partial_manifest_is_pending_with_null_measurement_and_baseline_usage(self):
        self.config['usage'].update(requests=2, observed_tokens=7,
            unknown_usage_requests=[dict(request_id='earlier', observed_tokens=None)])
        before = util.tree_hashes(self.root)
        with self.assertRaises(KeyError): pipeline.usage_for(self.root.parent)
        snapshot = guard.budget_snapshot(self.config)
        self.assertEqual(snapshot['usage']['requests'], 2)
        self.assertEqual(snapshot['usage']['observed_tokens'], 7)
        self.assertIsNone(snapshot['usage']['unknown_usage_requests'][0]['observed_tokens'])
        row = snapshot['run_observations']['1'*32]
        self.assertEqual(row['state'], 'preparing'); self.assertTrue(row['usage_pending'])
        self.assertIsNone(row['measured_duration_seconds'])
        self.assertEqual(util.tree_hashes(self.root), before)

    def test_incomplete_manifest_without_bound_preparation_intent_is_not_excused(self):
        (self.record.parent/'pipeline-preparation.json').unlink()
        with self.assertRaisesRegex(ValueError, 'active preparation'): guard.budget_snapshot(self.config)

    def test_foreign_manifest_uuid_is_rejected_even_before_dispatch(self):
        self.manifest['run_instance_id'] = '2'*32
        util.write_json_atomic(self.root/'manifest.json', self.manifest)
        with self.assertRaisesRegex(ValueError, 'identity'): guard.budget_snapshot(self.config)

    def test_completion_between_manifest_and_receipt_reads_is_not_a_fault(self):
        case = guard.attempts(self.config)[0]['cases'][0]; actual = util.read_json; changed = False
        def read(path):
            nonlocal changed
            value = actual(path)
            if Path(path) == self.root/'manifest.json' and not changed:
                changed = True
                util.write_json_atomic(path, dict(value, run_instance_id=case['run_instance_id']))
                util.write_new_json(self.record.parent/'pipeline-prepared'/('1'*32+'.json'),
                    dict(kind='pipeline_prepared_v1', attempt=live_pilot.reference(self.record), binding=case))
            return value
        with patch.object(util, 'read_json', side_effect=read):
            result = guard.budget_snapshot(self.config)
        self.assertTrue(result['run_observations']['1'*32]['identity_complete'])

    def test_prepared_callback_receipt_prevents_missing_manifest_being_excused(self):
        case = guard.attempts(self.config)[0]['cases'][0]
        util.write_new_json(self.record.parent/'pipeline-prepared'/('1'*32+'.json'),
            dict(kind='pipeline_prepared_v1', attempt=live_pilot.reference(self.record), binding=case))
        with self.assertRaisesRegex(ValueError, 'manifest evidence missing'): guard.budget_snapshot(self.config)

    def test_dispatch_then_manifest_deletion_is_rejected_on_first_snapshot(self):
        case = guard.attempts(self.config)[0]['cases'][0]
        path = self.record.parent/'_control/pair-journal.jsonl'
        pair_execution.append(path, dict(kind='pair_reserved', assignments=[case]))
        pair_execution.append(path, dict(kind='dispatch', **case))
        (self.root/'manifest.json').unlink()
        with self.assertRaisesRegex(ValueError, 'manifest evidence missing'): guard.budget_snapshot(self.config)

    def test_raw_usage_or_runtime_cannot_be_hidden_as_preparation(self):
        for name, value in [('usage/raw/started.jsonl', dict(request_id='a')),
                            ('runtime.json', dict(run_id='R', run_instance_id='1'*32))]:
            with self.subTest(name=name):
                util.write_new_json(self.root/name, value)
                with self.assertRaisesRegex(ValueError, 'manifest evidence missing'): guard.budget_snapshot(self.config)
                (self.root/name).unlink()

    def test_previous_complete_identity_or_usage_disappearance_is_fatal(self):
        self.manifest['run_instance_id'] = '1'*32
        util.write_json_atomic(self.root/'manifest.json', self.manifest)
        path = self.root/'usage/raw/started.jsonl'
        util.append_line(path, dict(request_id='known'))
        old = guard.budget_snapshot(self.config)
        self.assertIsNone(old['usage']['unknown_usage_requests'][0]['observed_tokens'])
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'usage evidence disappeared'): guard.budget_snapshot(self.config, old)
        util.append_line(path, dict(request_id='known'))
        del self.manifest['run_instance_id']
        util.write_json_atomic(self.root/'manifest.json', self.manifest)
        with self.assertRaisesRegex(ValueError, 'manifest evidence missing'): guard.budget_snapshot(self.config, old)


def wait_marker(path):
    deadline = time.monotonic()+15
    while not path.exists():
        if time.monotonic() >= deadline: raise TimeoutError('Synthetic composition barrier: '+path.name)
        time.sleep(.03)


def synthetic_controller(invocation):
    """Child entrypoint: real control stack, synthetic external boundaries only."""
    from research import acquisition_readiness, campaign_initialization, wave_campaign
    from outer.harness import profiles
    value = util.read_json(invocation); config = util.read_json(value['config']['path']); root = Path(config['root'])

    def phases(repo, path):
        plan = util.read_json(path)
        for assignment in plan['assignments']:
            batch = Path(plan['batch'])/('pair-'+str(assignment['pair']))
            util.write_new_json(batch/'phase.json', dict(batch=str(batch), phase_id=str(assignment['pair']),
                assignments=[assignment], runtime='synthetic', cohort=plan['cohort']))

    def create(repo, batch, task, condition, attempt, runtime_id, **kwargs):
        # The actual initial writer omits run_instance_id. Hold all four here
        # for multiple guard samples, beyond the JSON-transient retry window.
        manifest = run.create_run(repo, batch, task, condition, attempt, {},
            resolved_condition=dict(schema_version=2, task_id=task, condition_id=condition))
        if not config.get('fixture_mixed_preparation') or int(task[-1]) % 2 == 0:
            wait_marker(root/'finish-preparation')
        manifest.update(run_instance_id=kwargs['run_instance_id'], assignment=kwargs['assignment'],
            prompt_sha256='a'*64, condition_sha256='b'*64)
        run.save_manifest(batch, manifest['run_id'], manifest)
        return manifest

    class Monitor:
        def __init__(self, phase, directory, scope):
            self.scope_provider = scope; self.directory = directory/Path(phase).parent.name; self.phase_path = phase
        def start(self): pass
        def enroll_ready(self, scope): pass
        def admit(self, binding): wait_marker(root/'allow-dispatch')
        def stop(self):
            util.write_new_json(self.directory/'shutdown-ack.json', dict(fault_latched=False))
            return True

    def implementation(repo, batch, rid):
        manifest = run.load_manifest(batch, rid); manifest['started_at'] = run.now()
        run.save_manifest(batch, rid, manifest)
        util.append_line(batch/rid/'usage/raw/started.jsonl', dict(request_id='synthetic'))
        usage=dict(input_tokens=4, output_tokens=1)
        if config.get('fixture_provider_url'):
            from urllib.request import Request,urlopen
            payload=dict(run_id=rid,run_instance_id=manifest['run_instance_id'],
                         assignment=manifest['assignment'])
            with urlopen(Request(config['fixture_provider_url'],data=json.dumps(payload).encode(),
                    headers={'Content-Type':'application/json'}),timeout=5) as response:
                usage=json.load(response)['usage']
        util.append_line(batch/rid/'usage/raw/events.jsonl', dict(request_id='synthetic',
            usage=usage))
        wait_marker(root/'finish-workers')
        manifest.update(ended_at=run.now(), duration_seconds=.01, stop_confirmed=True,
            network_cleanup=dict(confirmed=True))
        run.save_manifest(batch, rid, manifest)
        util.write_new_json(batch/rid/'usage/normalized.json', dict(usage_complete=True))
        return dict(run_id=rid, run_instance_id=manifest['run_instance_id'], stop_confirmed=True)

    def evaluate(repo, record):
        attempt = util.read_json(record)
        result = dict(accepted=True, slot=attempt['slot'], attempt=attempt['attempt'], quality={})
        util.write_new_json(Path(record).parent/'pipeline-evaluation.json', result)
        return dict(result, batch=str(Path(record).parent))

    # controller() loads the current pipeline adapter separately. Patch only
    # its evaluator boundary on load; run_pipeline and PairWatch remain real.
    original_spec = guard.importlib.util.spec_from_file_location
    def spec(name, path):
        found = original_spec(name, path)
        if name == '_guard_acquisition_pipeline':
            original_exec = found.loader.exec_module
            def execute(module):
                original_exec(module); module.evaluation_process = evaluate
            found.loader.exec_module = execute
        return found

    with ExitStack() as stack:
        for target, name, replacement in [
            (guard, 'read_invocation', lambda _: (value, config)),
            (guard.importlib.util, 'spec_from_file_location', spec),
            (campaign_initialization, 'verify_config', lambda *_: None),
            (acquisition_readiness, 'create', phases),
            (live_pilot, 'verify_pins', lambda *_: None),
            (live_pilot, 'owner_plan', lambda path, _: util.read_json(path)),
            (live_pilot, 'validate_observer_phase', lambda *_: None),
            (profiles, 'create', create),
            (profiles, 'validate_run', lambda _: dict(task_profile_revision='fixture',
                runtime=dict(id='synthetic', model_id='synthetic', timeout_seconds=30, provider_timeout_seconds=10))),
            (live_pilot.next_phase_execution, 'guarded_implementation', implementation),
            (live_pilot.next_phase_execution, 'browser_postprocess', lambda _: None),
            (live_pilot, 'validate_owned_terminal', lambda root, b: dict(native_session_id=b['run_instance_id'])),
            (live_pilot, 'observer_terminal_receipt', lambda *a: dict(observer_ack_verified=True)),
            (pipeline.campaign, '_observer_snapshot', lambda _: dict(host_healthy=True, resource_healthy=True)),
            (wave_campaign, 'journal_health', lambda *a: dict(journal_healthy=True,
                provider_healthy=True, http_healthy=True, http_inflight_max=0)),
        ]: stack.enter_context(patch.object(target, name, replacement))
        stack.enter_context(patch('research.resource_supervisor.ProcessMonitor', Monitor))
        guard.controller(invocation)


class PipelineGuardCompositionTests(unittest.TestCase):
    def test_four_real_pipeline_preparations_are_observed_before_dispatch_and_completion(self):
        self.compose()

    def test_four_lanes_with_two_ready_and_two_incomplete_keep_the_same_budget(self):
        self.compose(mixed=True)

    def test_dispatched_manifest_loss_stops_guard_and_retains_last_known_usage(self):
        self.compose(fault='remove_manifest')

    def test_completed_identity_mismatch_stops_guard(self):
        self.compose(fault='change_identity')

    def test_four_interrupted_preparations_exclude_and_resume_same_slots_with_new_uuids(self):
        self.compose(mixed=True,recover=True)

    def test_excluded_preparations_do_not_refund_the_shared_150_attempt_ceiling(self):
        self.compose(mixed=True,recover=True,exhausted=True)

    def compose(self, *, mixed=False, fault=None,recover=False,exhausted=False):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)/'campaign'; directory = Path(tmp)/'supervisor'; directory.mkdir()
            browser = Path(tmp)/'browser.json'; util.write_new_json(browser, {})
            assignments = [dict(pair=n, cases=[dict(pair=n, slot=1, task='toy'+str(n),
                condition='explore', attempt=1, run_id=run.run_id_for('toy'+str(n), 'explore', 1))])
                for n in range(1, 5)]
            provider_requests=[]
            class Provider(BaseHTTPRequestHandler):
                def do_POST(self):
                    provider_requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                    body=json.dumps(dict(usage=dict(input_tokens=4,output_tokens=1))).encode()
                    self.send_response(200);self.end_headers();self.wfile.write(body)
                def log_message(self,*args):pass
            server=ThreadingHTTPServer(('127.0.0.1',0),Provider) if recover else None
            if server:
                thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
                def stop_provider():server.shutdown();server.server_close();thread.join(3)
                self.addCleanup(stop_provider)
            config = dict(root=str(root), pipeline_id='synthetic', campaign_plan={}, source_pins={},
                source_commit='a'*40, started_at=run.now(), accepted_slots=[], attempted_slots=[],
                pair_count=4, reserved_pair_attempts=(146 if exhausted else 142) if recover else 0,
                policy=dict(active_model_pairs=4),
                fixture_mixed_preparation=mixed,
                fixture_provider_url=('http://127.0.0.1:'+str(server.server_port)) if server else None,
                usage=dict(requests=2, observed_tokens=7, accumulated_run_seconds=0,
                    dispatched_runs=1, unknown_usage_requests=[dict(request_id='prior', observed_tokens=None)]),
                bounds=dict(request_count=100, observed_tokens=1000, accumulated_run_seconds=1000,
                    run_seconds=30, wall_seconds=120, max_pair_attempts=150, disk_free_min_bytes=0),
                template=dict(kind='fixture', assignments=assignments, runtime_locks={}, cases_per_assignment=1,
                    bounds=dict(max_runs=4, run_seconds=30, provider_seconds=10), thresholds={},
                    settings=dict(model_id='synthetic'), task_revision='fixture', browser_pin=live_pilot.reference(browser),
                    staged_retry=dict(retry_budget='shared_reserved_attempts')))
            util.write_new_json(root/'config.json', config)
            value = dict(repo=str(REPO), config=live_pilot.reference(root/'config.json'), guard_sources={})
            real_popen = subprocess.Popen; snapshots = []; prepare_samples = 0; fault_written = False
            interrupted=False
            actual_snapshot = guard.budget_snapshot

            def child(command, **kwargs):
                return real_popen([sys.executable, '-B', str(Path(__file__).resolve()),
                    '_controller', command[6]], **kwargs)

            def observe(config, previous=None):
                nonlocal prepare_samples, fault_written,interrupted
                snapshot = actual_snapshot(config, previous); snapshots.append(snapshot)
                rows = [r for r in snapshot['run_observations'].values() if r['state']!='excluded_pre_dispatch']
                if len(rows) == 4 and sum(r['state'] == 'preparing' for r in rows) == (2 if mixed else 4):
                    prepare_samples += 1
                    if prepare_samples >= 3:
                        if recover and not interrupted:
                            guard.latch(config,directory/'fixture-stop','synthetic_prepare_stop',guard.owned_rows(config))
                            interrupted=True
                        elif not recover:(root/'finish-preparation').touch()
                if len(rows) == 4 and all(r['prepared_receipt'] for r in rows): (root/'allow-dispatch').touch()
                if snapshot['usage']['requests'] == 6 and not fault_written:
                    if fault:
                        target = next(root.glob('epochs/*/data/pair-*/*/manifest.json'))
                        if fault == 'remove_manifest': target.unlink()
                        else:
                            value = util.read_json(target); value['run_instance_id'] = '0'*32
                            util.write_json_atomic(target, value)
                        fault_written = True
                    else: (root/'finish-workers').touch()
                return snapshot

            with patch.object(guard, 'read_invocation', return_value=(value, config)), \
                 patch.object(guard, 'identities', return_value=SimpleNamespace(
                     assert_durable_host=lambda: {'synthetic_test_host': True},
                     current_identity=process_identity.current_identity, ProcessIdentity=process_identity.ProcessIdentity)), \
                 patch.object(guard.subprocess, 'Popen', side_effect=child), \
                 patch.object(guard, 'budget_snapshot', side_effect=observe), \
                 patch.object(guard, 'close_owned', return_value={'owned_closure_confirmed':False}) as close:
                status = guard.supervise(directory/'invocation.json')
                if recover:
                    from test_pipeline_preparation_retry import synthetic_observer_closure
                    from research import pipeline_retry as retry,campaign_launcher
                    from outer.harness import runtime
                    self.assertEqual(status,1);self.assertTrue(interrupted)
                    self.assertEqual(provider_requests,[])
                    original=guard.attempts(config);self.assertEqual(len(original),4)
                    before={str(i['path']):util.tree_hashes(i['path'].parent/i['cases'][0]['run_id']) for i in original}
                    stop=(root/'STOP').read_bytes()
                    self.assertEqual(sum('run_instance_id' not in util.read_json(i['path'].parent/i['cases'][0]['run_id']/'manifest.json')
                                         for i in original),2)
                    with patch.object(retry,'guard',return_value=guard), \
                         patch.object(guard,'context',return_value=config), \
                         patch.object(campaign_launcher,'close_observers',side_effect=synthetic_observer_closure), \
                         patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=0,stdout='')):
                        for item in original:
                            ref=retry.exclude(REPO,root/'config.json',item['path'],'controller_interrupted','synthetic-authorized-retry')
                            self.assertEqual(retry.exclude(REPO,root/'config.json',item['path'],
                                'controller_interrupted','synthetic-authorized-retry'),ref)
                    self.assertEqual((root/'STOP').read_bytes(),stop)
                    self.assertEqual(len(retry.Exclusions(root/'config.json').rows),4)
                    (root/'STOP').rename(root/'retained-preparation-STOP')
                    self.assertEqual((root/'retained-preparation-STOP').read_bytes(),stop)
                    directory=Path(tmp)/'resumed-supervisor';directory.mkdir()
                    (root/'finish-preparation').touch()
                    status=guard.supervise(directory/'invocation.json')
                    if exhausted:
                        self.assertEqual(status,1)
                        self.assertIn('Attempt budget exhausted',(directory/'controller.log').read_text())
                        self.assertEqual(provider_requests,[])
                        self.assertEqual(len(guard.attempts(config)),4)
                    else:
                        self.assertEqual(status,0,(directory/'controller.log').read_text())
                        self.assertEqual(len(provider_requests),4)
                        new=[i for i in guard.attempts(config) if str(i['path']) not in before]
                        self.assertEqual(len(new),4)
                        old_by_slot={i['attempt']['slot']:i['cases'][0] for i in original}
                        for item in new:
                            old=old_by_slot[item['attempt']['slot']];fresh=item['cases'][0]
                            self.assertNotEqual(fresh['run_instance_id'],old['run_instance_id'])
                            self.assertEqual({k:v for k,v in fresh.items() if k!='run_instance_id'},
                                             {k:v for k,v in old.items() if k!='run_instance_id'})
                        self.assertEqual({r['run_instance_id'] for r in provider_requests},
                                         {i['cases'][0]['run_instance_id'] for i in new})
                    for item in original:
                        self.assertEqual(util.tree_hashes(item['path'].parent/item['cases'][0]['run_id']),before[str(item['path'])])
                    self.assertEqual(config['reserved_pair_attempts']+len(guard.attempts(config)),150)
                    self.assertEqual(util.read_json(root/'config.json'),config)
                    if exhausted:return
            result = util.read_json(directory/'result.json')
            if fault:
                self.assertEqual(status, 1); close.assert_called_once()
                self.assertEqual(result['reason'], 'independent_budget_or_scope_fault', result)
                self.assertEqual(result['last_budget']['usage']['requests'], 6)
                self.assertEqual(result['last_budget']['usage']['observed_tokens'], 27)
                self.assertIsNone(result['last_budget']['usage']['unknown_usage_requests'][0]['observed_tokens'])
                self.assertIn('manifest evidence missing' if fault == 'remove_manifest' else 'identity', result['budget_error'])
                return
            self.assertEqual(status, 0, (result, (directory/'controller.log').read_text(),
                [util.read_json(p) for p in root.glob('epochs/*/data/pair-*/pipeline-acquisition.json')]))
            if recover:self.assertEqual(close.call_count,1)
            else:close.assert_not_called()
            self.assertGreaterEqual(prepare_samples, 3)
            self.assertFalse((root/'STOP').exists())
            active=[[r for r in s['run_observations'].values() if r['state']!='excluded_pre_dispatch'] for s in snapshots]
            self.assertTrue(any(len(rows)==4 and all(r['state']=='dispatched' for r in rows) for rows in active))
            events = pair_execution.events(root/'events.jsonl')
            self.assertEqual(len([e for e in events if e['kind'] == 'reserved']), 8 if recover else 4)
            self.assertEqual(len([e for e in events if e['kind'] == 'accepted']), 4)
            self.assertEqual(events[-1]['usage']['requests'], 6)
            self.assertEqual(events[-1]['usage']['observed_tokens'], 27)
            self.assertIsNone(events[-1]['usage']['unknown_usage_requests'][0]['observed_tokens'])
            self.assertEqual(util.read_json(root/'config.json'), config)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '_controller': synthetic_controller(Path(sys.argv[2]))
    else: unittest.main()
