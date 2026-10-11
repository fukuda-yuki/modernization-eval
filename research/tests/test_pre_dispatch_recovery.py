"""Regression boundaries for failed preparation; no provider or private inputs."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock

from outer.harness import util
from research import acquisition_pipeline, live_pilot, resource_origin, resource_supervisor as supervisor


class PreDispatchRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_single_case_collector_requires_explicit_support_before_enrollment(self):
        monitor = self.root/'monitor.py'; probe = self.root/'wave_resource_probe.py'
        probe.write_text('', encoding='utf-8')
        for supported in (False, True):
            monitor.write_text('CADENCE_SECONDS=10\nSTALE_SECONDS=30\nclass ResourceMonitor:\n'
                + ('    SUPPORTED_RUN_COUNTS=(0,1,2,4,8)\n' if supported else '    pass\n'), encoding='utf-8')
            phase = dict(resource_monitor=live_pilot.reference(monitor), resource_probe=live_pilot.reference(probe))
            supervisor.load_monitor(phase)  # Historical two-case collector remains usable.
            phase['cases_per_assignment'] = 1
            if supported:
                self.assertIn(1, supervisor.load_monitor(phase).SUPPORTED_RUN_COUNTS)
            else:
                with self.assertRaisesRegex(ValueError, 'one-Run'): supervisor.load_monitor(phase)

    def test_simultaneous_pair_faults_preserve_two_distinct_stop_receipts(self):
        plan = dict(batch=str(self.root/'batch'), phase_id='synthetic-phase')
        owners = [acquisition_pipeline.PairWatch(plan, 'a'*64, None, identity) for identity in ('one', 'two')]
        barrier = threading.Barrier(2)
        def latch(owner):
            barrier.wait(2); owner.latch(owner.identity)
        with ThreadPoolExecutor(2) as pool: list(pool.map(latch, owners))
        paths = [o.stop_receipt_path() for o in owners]
        self.assertEqual(len(set(paths)), 2)
        self.assertEqual([util.read_json(p)['reason'] for p in paths], ['one', 'two'])
        self.assertTrue(all(util.read_json(p)['owned_stops'] == [] for p in paths))

    def test_shutdown_handles_last_acked_scope_before_rejected_pending_scope(self):
        value = object.__new__(supervisor.Supervisor)
        value.directory = self.root
        value.session = 'synthetic'; value.phase = {'_digest': 'a'*64}; value.generation = 1
        value.status_done = threading.Event(); value.shutdown_done = threading.Event()
        value.fault = {'reason': 'scope_or_shutdown_ValueError'}
        value.enroll = Mock(side_effect=ValueError('rejected generation 2'))
        value.owned_resolved = Mock(return_value=True)
        util.write_new_json(self.root/'shutdown.json', dict(session='synthetic',
            phase_sha256='a'*64, generation=1, dispatched=[]))
        value.command_worker()
        value.enroll.assert_not_called()
        self.assertTrue(value.shutdown_done.is_set())
        self.assertTrue(value.shutdown_receipt['fault_latched'])

    def test_unstarted_cleanup_reaps_only_its_popen_and_never_forges_ack(self):
        phase = self.root/'phase.json'; util.write_new_json(phase, {'batch':str(self.root/'batch')})
        scope = dict(pairs=[], assignments=[], dispatched=[])
        proxy = supervisor.ProcessMonitor(phase, self.root/'observers', lambda: scope)
        proxy.process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.addCleanup(lambda: proxy.process.kill() if proxy.process.poll() is None else None)
        proxy.log = (proxy.directory/'process.log').open('xb')
        scope['dispatched'] = ['ambiguous']
        self.assertFalse(proxy._terminate_unstarted_observer())
        self.assertIsNone(proxy.process.poll())
        scope['dispatched'] = []
        self.assertTrue(proxy._terminate_unstarted_observer())
        self.assertIsNotNone(proxy.process.poll())
        receipt = util.read_json(proxy.directory/'forced-process-stop.json')
        self.assertFalse(receipt['normal_shutdown_ack'])
        self.assertFalse((proxy.directory/'shutdown-ack.json').exists())

    def undispatched_origin(self):
        config = self.root/'old-config.json'
        util.write_new_json(config, dict(started_at='2026-10-01T00:00:00+00:00'))
        config_ref = live_pilot.reference(config)
        batch = self.root/'pair-1'; run_root = batch/'fixture'
        manifest = run_root/'manifest.json'
        util.write_new_json(manifest, dict(run_id='fixture', run_instance_id='synthetic-id', started_at=None,
            acquisition=dict(campaign_config=config_ref)))
        journal = batch/'_control/pair-journal.jsonl'
        util.append_line(journal, dict(kind='pair_reserved', assignments=[dict(run_instance_id='synthetic-id')]))
        acquisition = batch/'pipeline-acquisition.json'
        util.write_new_json(acquisition, dict(acquired=False, usage=dict(requests=0,dispatched_runs=0),
            runs=[dict(run_instance_id='synthetic-id', dispatch=None,
                manifest=dict(sha256_at_acquisition=util.sha256_file(manifest)))]))
        receipt = self.root/'origin.json'
        util.write_new_json(receipt, dict(kind='technical_acceptance_resources_v2',
            started_at='2026-10-01T00:00:00+00:00', runs=[], undispatched_attempts=[dict(
                manifest=live_pilot.reference(manifest), acquisition=live_pilot.reference(acquisition),
                journal=live_pilot.reference(journal))]))
        return receipt, run_root

    def test_receipt_clock_cannot_reset_an_exhausted_original_campaign_budget(self):
        receipt, _ = self.undispatched_origin()
        start, usage = resource_origin.read(live_pilot.reference(receipt))
        budget = acquisition_pipeline.Budget(dict(usage=usage, root=str(self.root), started_at=start,
            bounds=dict(request_count=100, observed_tokens=100, accumulated_run_seconds=100, wall_seconds=60)))
        with self.assertRaisesRegex(RuntimeError, 'wall budget'): budget.check(None, {})
        value = util.read_json(receipt)
        value['started_at'] = datetime.now(timezone.utc).isoformat()
        util.write_json_atomic(receipt, value)
        with self.assertRaisesRegex(ValueError, 'campaign clock'):
            resource_origin.read(live_pilot.reference(receipt))

    def test_original_campaign_clock_remains_hash_bound(self):
        receipt, _ = self.undispatched_origin()
        util.write_json_atomic(self.root/'old-config.json', dict(started_at=datetime.now(timezone.utc).isoformat()))
        with self.assertRaisesRegex(ValueError, 'Pinned pilot reference changed'):
            resource_origin.read(live_pilot.reference(receipt))

    def test_undispatched_history_keeps_clock_without_claiming_completed_runs(self):
        receipt, run_root = self.undispatched_origin()
        start, usage = resource_origin.read(live_pilot.reference(receipt))
        self.assertEqual(start, '2026-10-01T00:00:00+00:00')
        self.assertEqual(usage['dispatched_runs'], 0); self.assertEqual(usage['requests'], 0)
        self.assertEqual(usage['observed_tokens'], 0)
        util.write_new_json(run_root/'runtime.json', {'unknown':'runtime'})
        with self.assertRaisesRegex(ValueError,'possibly sent'): resource_origin.read(live_pilot.reference(receipt))

    def test_unknown_ack_or_request_refuses_recovery_even_with_refreshed_receipt_hash(self):
        receipt, run_root = self.undispatched_origin()
        util.append_line(run_root/'_controller/staged-input/events.jsonl', dict(kind='ack_unknown'))
        with self.assertRaisesRegex(ValueError,'possibly sent'): resource_origin.read(live_pilot.reference(receipt))

    def test_raw_request_refuses_recovery(self):
        receipt, run_root = self.undispatched_origin()
        util.append_line(run_root/'usage/raw/started.jsonl', dict(request_id='unknown'))
        with self.assertRaisesRegex(ValueError,'possibly sent'): resource_origin.read(live_pilot.reference(receipt))

    def test_dispatch_journal_refuses_recovery_with_refreshed_reference(self):
        receipt, run_root = self.undispatched_origin()
        # Updating an evidence reference cannot conceal a dispatch boundary.
        journal = run_root.parent/'_control/pair-journal.jsonl'
        util.append_line(journal, dict(kind='dispatch_started'))
        value = util.read_json(receipt)
        value['undispatched_attempts'][0]['journal'] = live_pilot.reference(journal)
        util.write_json_atomic(receipt,value)
        with self.assertRaisesRegex(ValueError,'possibly sent'): resource_origin.read(live_pilot.reference(receipt))


if __name__ == '__main__': unittest.main()
