"""One-shot detached owner. No automatic restart, model retry or budget reopening.

Windows uses Win32_Process.Create (CIM), whose children do not inherit the
launcher's Job. The owner additionally verifies it is outside every Job.
There is no fallback to a process inside the interactive execution Job.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
import uuid

from outer.harness import util,run
from outer.harness.security import child_environment
from . import lifecycle


def code_hash():
    from .campaign import digest
    return digest({p.name:util.sha256_file(p) for p in sorted(Path(__file__).parent.glob('*.py'))})


def _in_job():
    if os.name!='nt':return False
    import ctypes
    kernel=ctypes.WinDLL('kernel32',use_last_error=True);value=ctypes.c_int()
    if not kernel.IsProcessInJob(ctypes.c_void_p(-1),None,ctypes.byref(value)):raise ctypes.WinError(ctypes.get_last_error())
    return bool(value.value)


def assert_inactive(root):
    folder=root/'supervisor'
    if not (folder/'intent.json').exists():return
    owner_path=folder/'owner.json'
    if not owner_path.exists():raise ValueError('Launch registration ambiguous; recovery requires absence evidence')
    owner=util.read_json(owner_path)['identity']
    owner_alive=lifecycle.alive(owner)
    if owner['pid']!=os.getpid() and owner_alive:raise ValueError('Supervisor owner is alive')
    child_path=folder/'child.json'
    if child_path.exists() and lifecycle.alive(util.read_json(child_path)['identity']):
        raise ValueError('Controller is alive; request stop and confirm exit before recovery')


def validate_start(record,action):
    from . import controller,box
    root,settings,plan,ledger=controller.load(record)
    expected='prepared' if action=='run' else 'paused_for_confirmation_scope'
    if ledger['phase']!=expected:raise ValueError('One-shot start requires an unconsumed campaign state')
    if any((root/n).exists() for n in ('stop-request.json','confirmation-revoked.json','contradictory-setting-evidence.json')):
        raise ValueError('Campaign stopped or confirmation withdrawn')
    if ledger.get('deadline') is not None and time.time()>=ledger['deadline']:raise ValueError('Original deadline reached')
    if action=='continue':
        budget=box.budget_action(settings,'snapshot')
        if budget.get('closed_reason') or (root/'continuation.lock').exists():raise ValueError('Budget or continuation closed')
    controller.readiness(root,settings,plan)
    return {'campaign_uuid':ledger['id'],'plan_sha256':ledger['plan_sha256'],
        'settings_sha256':ledger['settings_sha256'],'runtime_sha256':plan['runtime_sha256'],
        'original_deadline':ledger.get('deadline'),'controller_code_sha256':code_hash()}


def launch(record,action='run'):
    root=Path(record).resolve()
    validate_start(root,action)  # Closed/deadline refusal must not mutate old evidence.
    with lifecycle.writer(root):
        binding=validate_start(root,action)
        return _launch(root,action,binding)


def launch_probe(record,seconds=15,outcome='normal'):
    # Entirely separate directory: no settings, provider, Docker, or campaign input.
    root=Path(record).resolve();root.mkdir(parents=True,exist_ok=False)
    return _launch(root,'probe',{'controller_code_sha256':code_hash(),'model_calls':0},
                   {'seconds':seconds,'outcome':outcome})


def _launch(root,action,binding,probe=None):
    folder=root/'supervisor';folder.mkdir(exist_ok=False)
    intent={'launch_id':uuid.uuid4().hex,'at':run.now(),'action':action,'binding':binding,'probe':probe,
            'launch_origin':lifecycle.identity(os.getpid()),'backend':'windows-cim' if os.name=='nt' else 'new-session'}
    util.write_new_json(folder/'intent.json',intent)
    try:
        command=[sys.executable,'-u','-m','outer.fineract.supervisor','_owner','--record',str(root)]
        cwd=str(Path(__file__).resolve().parents[2])
        if os.name=='nt':
            # Explicit selected backend, not a fallback after failed breakaway.
            script="""$ErrorActionPreference='Stop'
$inputObject=[Console]::In.ReadToEnd() | ConvertFrom-Json
$startup=New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ShowWindow=[uint16]0;EnvironmentVariables=[string[]]$inputObject.environment}
$result=Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=[string]$inputObject.command;CurrentDirectory=[string]$inputObject.cwd;ProcessStartupInformation=$startup}
@{return_value=$result.ReturnValue;pid=$result.ProcessId}|ConvertTo-Json -Compress
"""
            reply=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-Command',script],
                input=json.dumps({'command':subprocess.list2cmdline(command),'cwd':cwd,
                    'environment':[k+'='+v for k,v in child_environment().items()]}),
                capture_output=True,text=True,encoding='utf-8',timeout=20,check=True,
                env=child_environment(),creationflags=subprocess.CREATE_NO_WINDOW)
            created=json.loads(reply.stdout)
            if created['return_value']!=0:raise RuntimeError('Independent CIM launch refused')
            owner_pid=created['pid'];process=None
        else:
            process=subprocess.Popen(command,cwd=cwd,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,env=child_environment(),close_fds=True,start_new_session=True)
            owner_pid=process.pid
        deadline=time.monotonic()+10
        while not (folder/'owner.json').exists():
            if lifecycle.identity(owner_pid) is None:raise RuntimeError('Independent owner failed before registration')
            if time.monotonic()>=deadline:raise TimeoutError('Owner registration unconfirmed; do not retry')
            time.sleep(.05)
        registered=util.read_json(folder/'owner.json')
        if registered['identity']!=lifecycle.identity(owner_pid):raise ValueError('Independent owner identity mismatch')
        util.write_json_atomic(folder/'launch-ready.json',{'launch_id':intent['launch_id']})
        return {'record':str(root),'launch_id':intent['launch_id'],'owner':registered['identity']}
    except BaseException as error:
        util.write_new_json(folder/'launch-error.json',{'at':run.now(),'error_type':type(error).__name__,'dispatch_permitted':False})
        raise


def owner(record):
    root=Path(record).resolve();folder=root/'supervisor';intent=util.read_json(folder/'intent.json')
    util.write_new_json(folder/'owner-claim.json',{'launch_id':intent['launch_id'],'at':run.now()})
    if _in_job():raise RuntimeError('Owner remains in a Windows Job; dispatch refused')
    util.write_json_atomic(folder/'owner.json',{'launch_id':intent['launch_id'],'identity':lifecycle.identity(os.getpid()),'at':run.now(),'in_job':False})
    process=None;result={'launch_id':intent['launch_id'],'exit_code':None,'os_exit_time':None,'exit_observed_at':None,'classification':'launch_failed','recovery':None}
    try:
        deadline=time.monotonic()+10
        while not (folder/'launch-ready.json').exists():
            if time.monotonic()>=deadline:raise TimeoutError('Launcher registration did not commit')
            time.sleep(.05)
        if util.read_json(folder/'launch-ready.json')['launch_id']!=intent['launch_id']:raise ValueError('Launcher handshake mismatch')
        if intent['binding']['controller_code_sha256']!=code_hash():raise ValueError('Owner code changed')
        with (folder/'controller.stdout.log').open('xb') as out,(folder/'controller.stderr.log').open('xb') as err:
            process=subprocess.Popen([sys.executable,'-u','-m','outer.fineract.supervisor','_child','--record',str(root)],
                cwd=Path(__file__).resolve().parents[2],stdin=subprocess.DEVNULL,stdout=out,stderr=err,
                env=child_environment(),close_fds=True)
        identity=lifecycle.identity(process.pid)
        if identity is None:raise RuntimeError('Controller registration failed')
        util.write_new_json(folder/'child.json',{'launch_id':intent['launch_id'],'identity':identity,'at':run.now()})
        util.write_json_atomic(folder/'dispatch-ready.json',{'launch_id':intent['launch_id']})
        while process.poll() is None:
            campaign_deadline=None if intent['action']=='probe' else util.read_json(root/'ledger.json').get('deadline')
            if campaign_deadline is not None and not (folder/'campaign-deadline.json').exists():
                util.write_new_json(folder/'campaign-deadline.json',{'launch_id':intent['launch_id'],'deadline':campaign_deadline})
            util.write_json_atomic(folder/'heartbeat.json',{'launch_id':intent['launch_id'],'at':run.now(),
                'owner':lifecycle.identity(os.getpid()),'child':identity,'campaign_deadline':campaign_deadline})
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:pass
        result.update(exit_code=process.returncode,exit_observed_at=run.now())
        if os.name=='nt':result['os_exit_time']=lifecycle.handle_times(process._handle)['exited_at']
        terminal=(util.read_json(root/'probe-result.json').get('terminal') if intent['action']=='probe' and (root/'probe-result.json').exists()
                  else util.read_json(root/'ledger.json')['phase']=='finished' if intent['action']!='probe' else False)
        result['classification']=('normal' if process.returncode==0 and terminal else
            'controller_exception' if (folder/'controller-exception.json').exists() else
            'nonterminal_exit' if process.returncode==0 else 'terminated_or_nonzero_exit')
    except BaseException as error:
        result['error_type']=type(error).__name__
        traceback.print_exc()
        if process is not None:
            # The retained Popen handle, not a saved PID, owns this termination.
            if process.poll() is None:process.terminate()
            process.wait(timeout=15)
            result.update(exit_code=process.returncode,exit_observed_at=run.now())
            if os.name=='nt':result['os_exit_time']=lifecycle.handle_times(process._handle)['exited_at']
    finally:
        if result['classification']!='normal' and intent['action']!='probe':
            try:
                from .controller import recover
                result['recovery']=recover(root)
            except BaseException as error:result['recovery']={'recovery_complete':False,'error_type':type(error).__name__}
        result['recorded_at']=run.now()
        if (folder/'exit.json').exists():raise FileExistsError('Exit evidence already exists')
        util.write_json_atomic(folder/'exit.json',result)
    return result


def child(record):
    root=Path(record).resolve();folder=root/'supervisor';intent=util.read_json(folder/'intent.json')
    deadline=time.monotonic()+10
    while not (folder/'dispatch-ready.json').exists():
        if time.monotonic()>=deadline:raise TimeoutError('Dispatch registration absent')
        time.sleep(.05)
    registered=util.read_json(folder/'child.json')
    if util.read_json(folder/'dispatch-ready.json')['launch_id']!=intent['launch_id']:raise ValueError('Dispatch handshake mismatch')
    if registered['identity']!=lifecycle.identity(os.getpid()) or registered['launch_id']!=intent['launch_id']:
        raise ValueError('Child registration mismatch')
    if intent['binding']['controller_code_sha256']!=code_hash():raise ValueError('Controller code changed')
    if intent['action']=='probe':
        time.sleep(intent['probe']['seconds'])
        if intent['probe']['outcome']=='exception':raise RuntimeError('Deliberate non-model exception')
        util.write_new_json(root/'probe-result.json',{'terminal':intent['probe']['outcome']=='normal','model_calls':0,
            'provider_credential_in_environment':any(k in os.environ for k in ('OPENCODE_GO_API_KEY','OPENAI_API_KEY','ANTHROPIC_API_KEY'))});return
    # Parent launch's writer lock is released before entering the campaign.
    for _ in range(100):
        try:
            with lifecycle.writer(root):pass
            break
        except RuntimeError:time.sleep(.05)
    binding=validate_start(root,intent['action'])
    if binding!=intent['binding']:raise ValueError('Campaign binding changed before dispatch')
    if intent['action']=='run':
        from .controller import execute
        execute(root)
    else:
        from .continuation import continue_campaign
        continue_campaign(root)


def status(record):
    root=Path(record).resolve();folder=root/'supervisor';intent=util.read_json(folder/'intent.json')
    if (folder/'exit.json').exists():return util.read_json(folder/'exit.json')
    result={'launch_id':intent['launch_id'],'observed_at':run.now(),'exit_code':None,'os_exit_time':None}
    for role in ('owner','child'):
        path=folder/(role+'.json')
        result[role+'_alive']=lifecycle.alive(util.read_json(path)['identity']) if path.exists() else None
    result['heartbeat']=util.read_json(folder/'heartbeat.json') if (folder/'heartbeat.json').exists() else None
    if result['owner_alive'] is False:
        result['classification']='owner_lost_exit_unknown'
        util.write_new_json(folder/'observations'/(uuid.uuid4().hex+'.json'),result)
    return result


def stop(record):
    root=Path(record).resolve();folder=root/'supervisor'
    request=root/'stop-request.json'
    if not request.exists():util.write_new_json(request,{'at':run.now(),'reason':'explicit_operator_stop'})
    child_path=folder/'child.json'
    if not child_path.exists():raise ValueError('Child ownership not registered; no PID action')
    child_identity=util.read_json(child_path)['identity']
    deadline=time.monotonic()+10
    while lifecycle.alive(child_identity) and time.monotonic()<deadline:time.sleep(.2)
    if lifecycle.alive(child_identity):lifecycle.terminate(child_identity)
    return status(root)  # Owner records the handle exit and performs recovery once.


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['start','continue','status','stop','_owner','_child','probe'])
    p.add_argument('--record',type=Path,required=True);p.add_argument('--seconds',type=float,default=15)
    p.add_argument('--outcome',choices=['normal','exception','nonterminal'],default='normal');a=p.parse_args()
    if a.action=='start':result=launch(a.record)
    elif a.action=='continue':result=launch(a.record,'continue')
    elif a.action=='status':result=status(a.record)
    elif a.action=='stop':result=stop(a.record)
    elif a.action=='_owner':
        sys.stdout=(a.record/'supervisor/owner.stdout.log').open('x',encoding='utf-8',buffering=1)
        sys.stderr=(a.record/'supervisor/owner.stderr.log').open('x',encoding='utf-8',buffering=1)
        result=owner(a.record)
    elif a.action=='_child':
        try:result=child(a.record)
        except BaseException as error:
            util.write_new_json(a.record/'supervisor/controller-exception.json',{'at':run.now(),'error_type':type(error).__name__})
            raise
    else:result=launch_probe(a.record,a.seconds,a.outcome)
    print(json.dumps(result))


if __name__=='__main__':main()
