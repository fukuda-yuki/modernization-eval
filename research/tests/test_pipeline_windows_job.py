"""Windows-only, providerless tests using exclusively test-created Job objects.

Never queries/modifies/terminates an unrelated Job. A positive breakaway test
under a containing tool Job is NOT acceptance of tool-lifetime independence.
"""
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from research import process_identity as identity

REPO=Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.name=='nt','Windows process/Job acceptance requires the execution PC')
class WindowsJobTests(unittest.TestCase):
    def job(self, breakaway):
        from ctypes import wintypes as w
        class Basic(ctypes.Structure):
            _fields_=[('PerProcessUserTimeLimit',ctypes.c_longlong),('PerJobUserTimeLimit',ctypes.c_longlong),
                ('LimitFlags',w.DWORD),('MinimumWorkingSetSize',ctypes.c_size_t),('MaximumWorkingSetSize',ctypes.c_size_t),
                ('ActiveProcessLimit',w.DWORD),('Affinity',ctypes.c_size_t),('PriorityClass',w.DWORD),('SchedulingClass',w.DWORD)]
        class IO(ctypes.Structure):
            _fields_=[(name,ctypes.c_ulonglong) for name in ('ReadOperationCount','WriteOperationCount','OtherOperationCount',
                                                           'ReadTransferCount','WriteTransferCount','OtherTransferCount')]
        class Extended(ctypes.Structure):
            _fields_=[('BasicLimitInformation',Basic),('IoInfo',IO),('ProcessMemoryLimit',ctypes.c_size_t),
                ('JobMemoryLimit',ctypes.c_size_t),('PeakProcessMemoryUsed',ctypes.c_size_t),('PeakJobMemoryUsed',ctypes.c_size_t)]
        k=identity._kernel()
        for name,args,result in (
            ('CreateJobObjectW',[ctypes.c_void_p,w.LPCWSTR],w.HANDLE),
            ('SetInformationJobObject',[w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD],w.BOOL),
            ('AssignProcessToJobObject',[w.HANDLE,w.HANDLE],w.BOOL),
            ('TerminateJobObject',[w.HANDLE,w.UINT],w.BOOL)):
            function=getattr(k,name);function.argtypes=args;function.restype=result
        handle=k.CreateJobObjectW(None,None)
        self.assertTrue(handle)
        limits=Extended();limits.BasicLimitInformation.LimitFlags=0x2000 | (0x800 if breakaway else 0)
        self.assertTrue(k.SetInformationJobObject(handle,9,ctypes.byref(limits),ctypes.sizeof(limits)))
        self.addCleanup(k.CloseHandle,handle)
        return k,handle

    def launch_in_test_job(self, breakaway, mode):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);root=Path(tmp.name)
        child=root/'child.py';starter=root/'starter.py'
        child.write_text("""import json,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from research.process_identity import current_identity,assert_durable_host
root=Path(sys.argv[2])
value={'identity':current_identity()}
try:value['separation']=assert_durable_host();value['accepted']=True
except Exception as exc:value.update(accepted=False,error_type=type(exc).__name__)
(root/'child.json').write_text(json.dumps(value))
deadline=time.monotonic()+20
while not (root/'stop').exists() and time.monotonic()<deadline:time.sleep(.03)
(root/'child-exited').write_text('done')
""",encoding='utf-8')
        starter.write_text("""import json,sys,time,subprocess
from pathlib import Path
sys.path.insert(0,sys.argv[1])
root=Path(sys.argv[2])
(root/'starter-ready').write_text('ready')
while not (root/'go').exists():time.sleep(.02)
try:
 command=[sys.executable,str(root/'child.py'),sys.argv[1],str(root)]
 if sys.argv[3]=='breakaway':child=subprocess.Popen(command,creationflags=0x01000000|0x8|0x200,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 else:child=subprocess.Popen(command,creationflags=0x8|0x200,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 (root/'spawn.json').write_text(json.dumps({'pid':child.pid}))
except OSError as exc:(root/'spawn.json').write_text(json.dumps({'winerror':exc.winerror}))
time.sleep(20)
""",encoding='utf-8')
        process=subprocess.Popen([sys.executable,str(starter),str(REPO),str(root),mode],
                                  stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def cleanup():
            (root/'stop').touch()
            if process.poll() is None:process.kill()
            process.wait(5)
        self.addCleanup(cleanup)
        self.await_file(root/'starter-ready')
        k,job=self.job(breakaway)
        if not k.AssignProcessToJobObject(job,int(process._handle)):
            self.skipTest('Execution host cannot assign its synthetic child to a nested test Job')
        (root/'go').touch();self.await_file(root/'spawn.json')
        return root,process,k,job

    def await_file(self,path):
        deadline=time.monotonic()+5
        while not path.exists() and time.monotonic()<deadline:time.sleep(.03)
        self.assertTrue(path.exists(),str(path))

    def test_detached_flags_alone_do_not_survive_job_termination(self):
        root,process,k,job=self.launch_in_test_job(False,'detached')
        self.await_file(root/'child.json');value=json.loads((root/'child.json').read_text())
        self.assertFalse(value['accepted'])
        with identity.ProcessIdentity(value['identity']['pid'],value['identity']) as owner:
            self.assertTrue(k.TerminateJobObject(job,91));process.wait(5)
            deadline=time.monotonic()+5
            while owner.alive() and time.monotonic()<deadline:time.sleep(.03)
            self.assertFalse(owner.alive())

    def test_breakaway_denial_does_not_fall_back(self):
        root,process,k,job=self.launch_in_test_job(False,'breakaway')
        value=json.loads((root/'spawn.json').read_text())
        self.assertEqual(value.get('winerror'),5)
        self.assertFalse((root/'child.json').exists())

    def test_allowed_breakaway_survives_only_our_job_and_reports_outer_job(self):
        root,process,k,job=self.launch_in_test_job(True,'breakaway')
        spawned=json.loads((root/'spawn.json').read_text())
        if 'winerror' in spawned:
            self.skipTest('Containing execution Job refuses breakaway; durable launch remains unaccepted')
        self.await_file(root/'child.json');value=json.loads((root/'child.json').read_text())
        with identity.ProcessIdentity(value['identity']['pid'],value['identity']) as owner:
            self.assertTrue(k.TerminateJobObject(job,91));process.wait(5)
            self.assertTrue(owner.alive())
            # The production predicate is allowed to reject a surviving child
            # still inside an outer tool Job. Never turn survival into acceptance.
            if value['accepted']:self.assertEqual(value['separation'],{'platform':'windows','in_job':False})
            else:self.assertEqual(value['error_type'],'RuntimeError')
            (root/'stop').touch();self.await_file(root/'child-exited')

    def test_production_launcher_rejects_any_containing_job(self):
        from unittest.mock import patch
        with patch.object(identity,'assert_durable_host',side_effect=RuntimeError('outer Job')), \
             patch.object(identity.subprocess,'Popen') as spawn:
            with self.assertRaisesRegex(RuntimeError,'outer Job'):
                identity.persistent_popen([sys.executable,'-c','pass'])
        spawn.assert_not_called()


if __name__=='__main__':unittest.main()
