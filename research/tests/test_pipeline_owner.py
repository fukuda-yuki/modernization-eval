"""Providerless process, ownership, closure and cumulative-accounting checks."""
from datetime import datetime, timezone
from contextlib import contextmanager
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

    def test_lease_probe_does_not_read_windows_locked_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'dispatch.lock';path.write_bytes(b'0');real_open=Path.open
            class LockedReadHandle:
                def __init__(self,stream):self.stream=stream
                def __enter__(self):return self
                def __exit__(self,*_):self.stream.close()
                def read(self,*_):raise PermissionError('Windows mandatory byte lock')
                def fileno(self):return self.stream.fileno()
                def seek(self,*args):return self.stream.seek(*args)
            with patch.object(Path,'open',side_effect=lambda *a,**kw:LockedReadHandle(real_open(path,'rb'))):
                self.assertFalse(guard.lease_held(path))

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

    def supervise_child(self,body):
        directory=self.base/'supervisor';directory.mkdir()
        invocation=directory/'invocation.json'
        value=dict(repo=str(REPO),config={'path':str(Path(self.config['root'])/'config.json')},guard_sources={})
        real_popen=subprocess.Popen
        def abrupt(*args,**kwargs):
            code=("import os,sys;from pathlib import Path;"
                  f"sys.path.insert(0,{str(REPO)!r});"
                  "from research import pair_execution,live_pilot,process_identity;from outer.harness import util;"
                  f"directory=Path({str(directory)!r});root=Path({self.config['root']!r});"
                  "value=util.read_json(directory/'bound-invocation.json');"
                  "record=process_identity.current_identity();util.write_new_json(directory/'controller.json',record);"
                  +body)
            return real_popen([sys.executable,'-c',code],**kwargs)
        with patch.object(guard,'read_invocation',return_value=(value,self.config)), \
             patch.object(guard,'identities',return_value=SimpleNamespace(
                 assert_durable_host=lambda:{'synthetic':True},current_identity=identity.current_identity,
                 ProcessIdentity=identity.ProcessIdentity)), \
             patch.object(guard.subprocess,'Popen',side_effect=abrupt), \
             patch.object(guard,'close_owned',return_value={'owned_closure_confirmed':True}) as close:
            self.assertEqual(guard.supervise(invocation),1)
        return directory,close

    def test_abrupt_controller_before_acquisition_cannot_stop_campaign(self):
        directory,close=self.supervise_child('os._exit(17)')
        close.assert_not_called()
        result=util.read_json(directory/'result.json')
        self.assertFalse(result['closure']['campaign_stop_requested'])
        self.assertFalse(result['closure']['ownership_acquired'])
        self.assertFalse((Path(self.config['root'])/'STOP').exists())

    def test_abrupt_controller_after_acquisition_invokes_owned_closure(self):
        body=("lock=pair_execution.exclusive(root/'_owner');lock.__enter__();"
              "util.write_new_json(directory/'controller-owned.json',dict(kind='pipeline_controller_ownership_v1',"
              "controller=record,supervisor=value['supervisor'],config=value['config'],"
              "invocation=live_pilot.reference(directory/'bound-invocation.json')));os._exit(17)")
        directory,close=self.supervise_child(body)
        self.assertEqual(close.call_args.args[3],'controller_nonzero')
        self.assertEqual(close.call_args.args[4].poll(),17)
        self.assertEqual(util.read_json(directory/'result.json')['child_exit_code'],17)

    def test_competing_direct_owner_survives_guarded_child_lock_failure(self):
        ready=self.base/'direct-ready'
        code=("import sys,time;from pathlib import Path;"
              f"sys.path.insert(0,{str(REPO)!r});from research.pair_execution import exclusive;"
              f"lock=exclusive(Path({self.config['root']!r})/'_owner');lock.__enter__();"
              f"Path({str(ready)!r}).write_text('ready');time.sleep(60)")
        direct=subprocess.Popen([sys.executable,'-c',code])
        try:
            deadline=time.monotonic()+5
            while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
            self.assertTrue(ready.exists())
            before=util.tree_hashes(self.source)
            directory,close=self.supervise_child("lock=pair_execution.exclusive(root/'_owner');lock.__enter__();os._exit(99)")
            close.assert_not_called();self.assertIsNone(direct.poll())
            result=util.read_json(directory/'result.json')
            self.assertFalse(result['closure']['ownership_acquired'])
            self.assertFalse(result['closure']['campaign_stop_requested'])
            self.assertEqual(util.tree_hashes(self.source),before)
            self.assertFalse((Path(self.config['root'])/'STOP').exists())
        finally:
            direct.terminate();direct.wait(5)

    def test_ownership_receipt_must_bind_actual_child_and_invocation(self):
        directory=self.base/'proof';directory.mkdir()
        value=dict(supervisor={'pid':1},config={'path':'fixed-config','sha256':'a'*64})
        util.write_new_json(directory/'bound-invocation.json',value)
        record=identity.current_identity();util.write_new_json(directory/'controller.json',record)
        proof=dict(kind='pipeline_controller_ownership_v1',controller=record,supervisor=value['supervisor'],
            config=value['config'],invocation=live_pilot.reference(directory/'bound-invocation.json'))
        util.write_new_json(directory/'controller-owned.json',proof)
        self.assertTrue(guard.acquired_ownership(directory,value,record))
        with self.assertRaises(ValueError):guard.acquired_ownership(directory,value,{**record,'started':record['started']+1})

    def test_past_ownership_does_not_stop_a_new_owner_after_child_exit(self):
        body=("lock=pair_execution.exclusive(root/'_owner');lock.__enter__();"
              "util.write_new_json(directory/'controller-owned.json',dict(kind='pipeline_controller_ownership_v1',"
              "controller=record,supervisor=value['supervisor'],config=value['config'],"
              "invocation=live_pilot.reference(directory/'bound-invocation.json')));os._exit(17)")
        directory,prior_close=self.supervise_child(body)
        child=prior_close.call_args.args[4]
        value=util.read_json(directory/'bound-invocation.json')
        record=util.read_json(directory/'controller.json')
        with pair_execution.exclusive(Path(self.config['root'])/'_owner'),patch.object(guard,'close_owned') as close:
            result=guard.close_registered(self.config,value,directory,'fixture',child,record)
        close.assert_not_called()
        self.assertTrue(result['ownership_acquired']);self.assertFalse(result['campaign_stop_requested'])
        self.assertEqual(result['reason'],'campaign_has_another_owner')
        self.assertFalse((Path(self.config['root'])/'STOP').exists())

    def test_owner_scope_never_enters_on_lock_conflict_and_exits_before_unlock(self):
        from research import campaign_initialization
        path=Path(self.config['root'])/'config.json';calls=[]
        @contextmanager
        def owner_scope():
            calls.append('entered')
            with self.assertRaises(OSError):
                with pair_execution.exclusive(Path(self.config['root'])/'_owner'):pass
            try:yield
            finally:
                with self.assertRaises(OSError):
                    with pair_execution.exclusive(Path(self.config['root'])/'_owner'):pass
                calls.append('exited')
        with patch.object(campaign_initialization,'verify_config'), \
             patch.object(live_pilot,'verify_pins',side_effect=RuntimeError('stop fixture before acquisition')):
            with pair_execution.exclusive(Path(self.config['root'])/'_owner'):
                with self.assertRaises(OSError):acquisition_pipeline.run_pipeline(REPO,path,owner_scope=owner_scope)
            self.assertEqual(calls,[])
            with self.assertRaisesRegex(RuntimeError,'stop fixture'):
                acquisition_pipeline.run_pipeline(REPO,path,owner_scope=owner_scope)
            self.assertEqual(calls,['entered','exited'])


if __name__=='__main__':unittest.main()
