"""Actual snapshot/stop primitives with fake processes; no Docker or model."""
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock

from outer.harness import util
from research import live_pilot as pilot
from research.resource_supervisor import ProcessMonitor


class Process:
    def __init__(self):self.code=None
    def poll(self):return self.code


class Watch(pilot.PilotWatch):
    def __init__(self,monitors):
        self.lock=threading.RLock();self.monitors=monitors;self.fault=None;self.reasons=[]
    def _check_locked(self):
        if self.fault is not None:raise RuntimeError('fault retained')
        for m in list(self.monitors):
            if m.snapshot()['resource_healthy'] is not True:raise RuntimeError('observer unhealthy')
    def latch(self,reason):
        with self.lock:
            self.reasons.append(reason)
            if self.fault is None:self.fault={'reason':reason}


class Monitor(ProcessMonitor):
    """Use real _status/snapshot/stop; only simulate the process lifecycle."""
    def __init__(self,directory):
        self.directory=directory;directory.mkdir()
        self.session=directory.name;self.digest='a'*64;self.generation=2
        self.acknowledged_generation=2
        self.process=Process();self.log=Mock();self.last_scope={'bindings':[]}
        self.requested_scope=self.last_scope;self.scope_provider=lambda:{'dispatched':[]}
        self.exit_code=0;self.on_stop=lambda:None
        self.calls=0
        util.write_new_json(directory/'snapshot.json',dict(session=self.session,phase_sha256=self.digest,
            generation=2,sampled_scope_generation=2,published_tick=time.monotonic(),
            snapshot={'host_healthy':True,'resource_healthy':True,'evidence_files':{}},diagnostics={}))
        util.write_new_json(directory/'status.json',pilot.reference(directory/'snapshot.json'))
        util.write_new_json(directory/'shutdown.json',dict(session=self.session,phase_sha256=self.digest,
            generation=2,dispatched=[]))
        util.write_new_json(directory/'shutdown-ack.json',dict(session=self.session,phase_sha256=self.digest,
            generation=2,owned_resources_resolved=True,monitor_stop_confirmed=True,fault_latched=False))
    def stop(self):
        self.calls+=1;self.on_stop();self.process.code=self.exit_code
        return super().stop()


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.music=Monitor(self.root/'music');self.education=Monitor(self.root/'education')
        self.watch=Watch([self.music,self.education])

    def retire(self,confirmed=True):
        return pilot.retire_observer(self.music,self.watch,owned_terminal_confirmed=confirmed)

    def ack(self,**changes):
        path=self.music.directory/'shutdown-ack.json'
        value=util.read_json(path);value.update(changes);util.write_json_atomic(path,value)

    def test_healthy_pair_is_removed_before_stop_and_other_pair_remains_checked(self):
        def inspect():
            self.assertNotIn(self.music,self.watch.monitors)
            self.assertIn(self.education,self.watch.monitors)
            self.watch.check()
        self.music.on_stop=inspect
        self.assertTrue(self.retire());self.assertIsNone(self.watch.fault)

    def test_normal_retirement_requires_both_owned_terminal_proofs(self):
        self.retire(confirmed=False)
        self.assertIsNotNone(self.watch.fault)

    def test_unexpected_exit_before_authorized_retirement_is_not_ignored(self):
        self.music.process.code=0
        self.retire()
        self.assertEqual(self.watch.fault,{'reason':'RuntimeError'})

    def test_existing_fault_latch_immediately_before_retirement_is_retained(self):
        util.write_new_json(self.music.directory/'fault-latch.json',{'reason':'resource_fault'})
        self.assertFalse(self.retire())
        self.assertEqual(self.watch.fault,{'reason':'RuntimeError'})

    def test_fault_between_last_check_and_exit_is_not_lost(self):
        self.music.on_stop=lambda:util.write_new_json(self.music.directory/'fault-latch.json',{'reason':'late_resource_fault'})
        self.assertFalse(self.retire())
        self.assertEqual(self.watch.fault,{'reason':'observer_fault_during_shutdown'})

    def test_faulted_ack_without_fault_file_still_latches(self):
        self.ack(fault_latched=True)
        self.assertFalse(self.retire())
        self.assertEqual(self.watch.fault,{'reason':'observer_fault_during_shutdown'})

    def test_missing_ack_latches(self):
        (self.music.directory/'shutdown-ack.json').unlink()
        self.assertFalse(self.retire())
        self.assertEqual(self.watch.fault,{'reason':'observer_shutdown_unconfirmed'})

    def test_bad_ack_generation_latches(self):
        self.ack(generation=1)
        self.assertFalse(self.retire());self.assertIsNotNone(self.watch.fault)

    def test_ack_missing_fault_disposition_is_not_normal_completion(self):
        path=self.music.directory/'shutdown-ack.json';value=util.read_json(path)
        del value['fault_latched'];util.write_json_atomic(path,value)
        self.assertFalse(self.retire());self.assertIsNotNone(self.watch.fault)

    def test_nonzero_exit_during_shutdown_latches(self):
        self.music.exit_code=2
        self.assertFalse(self.retire());self.assertIsNotNone(self.watch.fault)

    def test_existing_campaign_latch_is_never_cleared(self):
        self.watch.latch('provider_fault')
        self.assertTrue(self.retire(confirmed=False))
        self.assertEqual(self.watch.fault,{'reason':'provider_fault'})

    def test_other_pair_fault_remains_observable_while_stop_is_waiting(self):
        entered=threading.Event();release=threading.Event();results=[]
        def close():
            entered.set()
            if not release.wait(3):raise RuntimeError('fixture deadline')
        self.music.log.close=close
        thread=threading.Thread(target=lambda:results.append(self.retire()))
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            util.write_new_json(self.education.directory/'fault-latch.json',{'reason':'actual_resource_fault'})
            with self.assertRaisesRegex(RuntimeError,'observer unhealthy'):self.watch.check()
            self.watch.latch('other_pair_resource_fault')
        finally:
            release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(results,[True])
        self.assertEqual(self.watch.fault,{'reason':'other_pair_resource_fault'})

    def test_concurrent_check_list_copy_finishes_before_atomic_retirement(self):
        entered=threading.Event();release=threading.Event();started=threading.Event()
        checked=[];retired=[];original=self.music.snapshot
        first=[True]
        def snapshot():
            if first[0]:
                first[0]=False;entered.set()
                if not release.wait(3):raise RuntimeError('fixture deadline')
            return original()
        self.music.snapshot=snapshot
        check=threading.Thread(target=lambda:(self.watch.check(),checked.append(True)))
        def retire():
            started.set();retired.append(self.retire())
        finish=threading.Thread(target=retire)
        check.start()
        try:
            self.assertTrue(entered.wait(2));finish.start();self.assertTrue(started.wait(2))
            self.assertEqual(self.music.calls,0)
            self.assertIn(self.music,self.watch.monitors)
        finally:
            release.set();check.join(3);finish.join(3)
        self.assertEqual(checked,[True]);self.assertEqual(retired,[True])
        self.assertIsNone(self.watch.fault);self.watch.check()


if __name__=='__main__':unittest.main()
