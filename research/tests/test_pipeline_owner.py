"""Providerless process, ownership, closure and cumulative-accounting checks."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from outer.harness import run, util, runtime
from research import acquisition_pipeline, campaign_launcher, live_pilot, pair_execution
from research import process_identity as identity, pipeline_guard as guard

REPO=Path(__file__).resolve().parents[2]


def fixture(base):
    root=Path(base)/'campaign'
    config=dict(root=str(root),pipeline_id='fixture',source_pins={},started_at=run.now(),
        usage=dict(requests=0,observed_tokens=0,accumulated_run_seconds=0,dispatched_runs=0,unknown_usage_requests=[]),
        bounds=dict(request_count=100,observed_tokens=1000,accumulated_run_seconds=1000,run_seconds=100,wall_seconds=1000),
        template={'staged_retry':{'technical_failure_policy':'sealed_infrastructure_failure_v1','retry_budget':'shared_reserved_attempts'}},
        campaign_plan={},pair_count=1,attempted_slots=[])
    util.write_new_json(root/'config.json',config)
    case=dict(run_id='R',run_instance_id='1'*32,task='toy',condition='explore',pair=1,slot=1,attempt=1)
    epoch_path=root/'epochs/001/plan.json'; batch=epoch_path.parent/'data/pair-1'
    assignment=dict(pair=1,cases=[case])
    epoch=dict(campaign_config=live_pilot.reference(root/'config.json'),source_pins={},
               batch=str(epoch_path.parent/'data'),assignments=[assignment])
    util.write_new_json(epoch_path,epoch)
    phase=dict(batch=str(batch),phase_id='fixture-phase',assignments=[assignment])
    util.write_new_json(batch/'phase.json',phase)
    attempt=dict(slot=1,attempt='attempt-a',epoch=live_pilot.reference(epoch_path),phase=live_pilot.reference(batch/'phase.json'))
    record=batch/'pipeline-attempt.json';util.write_new_json(record,attempt)
    pair_execution.append(root/'events.jsonl',dict(kind='reserved',slot=1,attempt='attempt-a',record=str(record)))
    with pair_execution.exclusive(root/'_owner'): pass
    source=batch/'R';source.mkdir()
    (source/'controller.lock').write_bytes(b'0')
    manifest=dict(run_id='R',run_instance_id='1'*32,started_at='2026-10-11T00:00:00+00:00',
        ended_at='2026-10-11T00:00:20+00:00',duration_seconds=None,stop_confirmed=True,
        network_cleanup=dict(confirmed=True,ended_at='2026-10-11T00:00:30+00:00'))
    util.write_new_json(source/'manifest.json',manifest)
    return config,record,source,manifest


class IdentityTests(unittest.TestCase):
    def test_retained_handle_observes_real_abrupt_child_exit(self):
        process=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])
        try:
            with identity.ProcessIdentity(process.pid) as owner:
                self.assertTrue(owner.alive())
                process.kill();process.wait(5)
                self.assertFalse(owner.alive())
        finally:
            if process.poll() is None:process.kill();process.wait(5)

    def test_reused_or_foreign_start_identity_is_rejected_without_signals(self):
        record=identity.current_identity();record['started']+=1
        with self.assertRaisesRegex(ValueError,'start identity'):
            identity.ProcessIdentity(os.getpid(),record)

    @unittest.skipIf(os.name=='nt','Windows job acceptance has its own synthetic job test')
    def test_detached_child_survives_starter_process_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);result=root/'result.json'
            code=("import sys,json,time;from pathlib import Path;"
                  f"sys.path.insert(0,{str(REPO)!r});from research.process_identity import assert_durable_host;"
                  "time.sleep(.3);"
                  f"Path({str(result)!r}).write_text(json.dumps(assert_durable_host()))")
            starter=("import sys,subprocess;"
                     f"sys.path.insert(0,{str(REPO)!r});from research.process_identity import persistent_popen;"
                     f"persistent_popen([sys.executable,'-c',{code!r}],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)")
            process=subprocess.Popen([sys.executable,'-c',starter]);self.assertEqual(process.wait(5),0)
            deadline=time.monotonic()+5
            while not result.exists() and time.monotonic()<deadline:time.sleep(.03)
            self.assertEqual(json.loads(result.read_text())['platform'],'linux')

    def test_os_lease_probe_does_not_write_original(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'dispatch.lock';path.write_bytes(b'original lock bytes')
            self.assertFalse(guard.lease_held(path));self.assertEqual(path.read_bytes(),b'original lock bytes')

    def test_actual_owner_force_exit_triggers_peer_stop_without_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);ready=root/'ready'
            code=("import sys,time;from pathlib import Path;"
                  f"sys.path.insert(0,{str(REPO)!r});from research.pair_execution import exclusive;"
                  f"lock=exclusive(Path({str(root)!r}));lock.__enter__();Path({str(ready)!r}).write_text('ready');time.sleep(60)")
            process=subprocess.Popen([sys.executable,'-c',code])
            done=threading.Event();stopped=threading.Event();thread=None
            try:
                deadline=time.monotonic()+5
                while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
                self.assertTrue(ready.exists())
                with identity.ProcessIdentity(process.pid) as owner:
                    thread=threading.Thread(target=guard.watch_owner,args=(owner,root/'dispatch.lock',done,stopped.set))
                    thread.start()
                    # No heartbeat at all is required while process + lease live.
                    self.assertFalse(stopped.wait(.4))
                    process.kill();process.wait(5)
                    self.assertTrue(stopped.wait(3));thread.join(3)
            finally:
                done.set()
                if thread:thread.join(3)
                if process.poll() is None:process.kill();process.wait(5)

    def test_owner_lease_loss_is_detected_without_killing_other_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'dispatch.lock';path.write_bytes(b'0')
            with identity.ProcessIdentity(os.getpid()) as owner:
                stop=threading.Event()
                guard.watch_owner(owner,path,threading.Event(),stop.set)
                self.assertTrue(stop.is_set());self.assertTrue(owner.alive())


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)
        self.config,self.record,self.source,self.manifest=fixture(self.base)

    def test_discovered_attempt_is_exactly_phase_bound(self):
        rows=guard.owned_rows(self.config)
        self.assertEqual(rows[0]['run_instance_id'],'1'*32)
        phase=util.read_json(self.record.parent/'phase.json');phase['assignments'][0]['cases'][0]['run_instance_id']='2'*32
        util.write_json_atomic(self.record.parent/'phase.json',phase)
        with self.assertRaises(ValueError):guard.owned_rows(self.config)

    def test_unknown_duration_charged_separately_without_changing_original(self):
        before=util.tree_hashes(self.source)
        snapshot=guard.budget_snapshot(self.config)
        self.assertEqual(snapshot['usage']['accumulated_run_seconds'],30)
        self.assertEqual(snapshot['usage']['missing_duration_runs'],['1'*32])
        self.assertIsNone(snapshot['conservative_duration_charges'][0]['measured_duration_seconds'])
        self.assertEqual(util.tree_hashes(self.source),before)

    def test_missing_terminal_usage_never_zeroed_and_original_clock_enforced(self):
        util.append_line(self.source/'usage/raw/started.jsonl',{'request_id':'inflight'})
        snapshot=guard.budget_snapshot(self.config)
        self.assertEqual(snapshot['usage']['requests'],1)
        self.assertEqual(snapshot['usage']['unknown_usage_requests'][0]['status'],'missing_terminal_event')
        self.config['started_at']='2020-01-01T00:00:00+00:00'
        with self.assertRaisesRegex(RuntimeError,'wall budget'):guard.budget_snapshot(self.config)

    def test_cumulative_requests_limit_operates_without_controller(self):
        self.config['bounds']['request_count']=1
        util.append_line(self.source/'usage/raw/started.jsonl',{'request_id':'a'})
        with self.assertRaisesRegex(RuntimeError,'Cumulative budget'):guard.budget_snapshot(self.config)

    def test_stop_preserves_already_stopped_originals(self):
        before=util.tree_hashes(self.source)
        rows=guard.owned_rows(self.config)
        self.assertEqual(guard.latch(self.config,self.base/'stop','controller_lost',rows),[])
        self.assertTrue((Path(self.config['root'])/'STOP').exists())
        self.assertEqual(util.tree_hashes(self.source),before)

    def test_late_allocation_is_rescanned_even_after_first_scope_fault(self):
        row=guard.owned_rows(self.config)[0]
        with patch.object(guard,'owned_rows',side_effect=[ValueError('partial journal'),[row]]), \
             patch.object(guard,'stopped',return_value=True),patch.object(guard,'close_observers',return_value=[]):
            result=guard.close_owned(self.config,REPO,self.base/'stop','scope_unknown')
        self.assertEqual(len(result['owned_runs']),1)
        self.assertFalse(result['owned_closure_confirmed'])
        self.assertEqual(result['errors'][0]['stage'],'scope')

    def test_foreign_runtime_not_stopped(self):
        util.write_new_json(self.source/'runtime.json',dict(run_id='R',run_instance_id='other'))
        with patch.object(runtime,'request_stop') as stop:
            with self.assertRaisesRegex(ValueError,'Foreign runtime'):guard.owned_rows(self.config)
        stop.assert_not_called()

    def test_container_absence_requires_successful_physical_observation(self):
        state=dict(run_id='R',run_instance_id='1'*32,stop_confirmed=True,
                   worker='worker',gateway='gateway',network='network',network_id='network-id')
        util.write_new_json(self.source/'runtime.json',state)
        row=guard.owned_rows(self.config)[0]
        with patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=1,stdout='')):
            self.assertFalse(guard.stopped(row))
        with patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=0,stdout='')):
            self.assertTrue(guard.stopped(row))

    def test_abrupt_controller_exit_invokes_owned_closure_with_child_handle(self):
        directory=self.base/'supervisor';directory.mkdir()
        invocation=directory/'invocation.json'
        value=dict(repo=str(REPO),config={'path':str(Path(self.config['root'])/'config.json')},guard_sources={})
        real_popen=subprocess.Popen
        def abrupt(*args,**kwargs):
            return real_popen([sys.executable,'-c','import os;os._exit(17)'],**kwargs)
        with patch.object(guard,'read_invocation',return_value=(value,self.config)), \
             patch.object(guard,'identities',return_value=SimpleNamespace(
                 assert_durable_host=lambda:{'synthetic':True},current_identity=identity.current_identity)), \
             patch.object(guard.subprocess,'Popen',side_effect=abrupt), \
             patch.object(guard,'close_owned',return_value={'owned_closure_confirmed':True}) as close:
            self.assertEqual(guard.supervise(invocation),1)
        self.assertEqual(close.call_args.args[3],'controller_nonzero')
        self.assertEqual(close.call_args.args[4].poll(),17)
        self.assertEqual(util.read_json(directory/'result.json')['child_exit_code'],17)


if __name__=='__main__':unittest.main()
