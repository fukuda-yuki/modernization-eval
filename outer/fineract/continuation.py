"""Administrative handoff of one existing campaign, without model replay or budget reset.

The original plan, approval, settings and readiness files are never rewritten.
An explicit reviewed amendment binds the small controller-policy delta separately.
Only a stopped-worker checkpoint with an OPEN original budget can continue.
"""
import argparse
import os
from pathlib import Path
import time
import uuid

from outer.harness import util,run,runtime
from .campaign import LIMITS,digest
from . import box,controller

PATCH_FILES={'campaign.py','controller.py','continuation.py'}
BLOCKERS=('stop-request.json','confirmation-revoked.json','contradictory-setting-evidence.json')

def process_alive(pid):
    if type(pid) is not int or pid<=0:raise ValueError('Exact old supervisor PID required')
    if os.name=='nt':
        import ctypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.OpenProcess.restype=ctypes.c_void_p
        handle=kernel.OpenProcess(0x1000,False,pid)
        if not handle:
            if ctypes.get_last_error() in (87,1168):return False
            raise OSError('Cannot verify old supervisor exit')
        try:
            code=ctypes.c_ulong()
            if not kernel.GetExitCodeProcess(ctypes.c_void_p(handle),ctypes.byref(code)):
                raise OSError('Cannot verify old supervisor state')
            return code.value==259
        finally:kernel.CloseHandle(ctypes.c_void_p(handle))
    try:os.kill(pid,0)
    except ProcessLookupError:return False
    return True

def evaluation_resources(settings,ledger,root):
    from .recovery import evaluation_resources as owned_resources
    found,errors=owned_resources(root,settings,ledger)
    if errors:raise ValueError('Unverified evaluation ownership; explicit review required')
    return found

def scoped_context(root,settings,plan,actual):
    scope=util.read_json(root/'confirmation-scope.json');ledger=util.read_json(root/'ledger.json')
    if any((root/name).exists() for name in BLOCKERS):raise ValueError('Campaign scope withdrawn or stopped')
    if scope.get('kind')!='explicit-user-confirmation-campaign-scope-v1':raise ValueError('Unknown scope amendment')
    base=scope['original_runtime_definition']
    if digest(base)!=plan['runtime_sha256'] or digest(actual)!=scope.get('amended_runtime_sha256'):
        raise ValueError('Administrative runtime binding mismatch')
    if {k:v for k,v in base.items() if k!='controller_files'}!={k:v for k,v in actual.items() if k!='controller_files'}:
        raise ValueError('Amendment changed scientific/runtime conditions')
    before,after=base['controller_files'],actual['controller_files']
    changed={k for k in before.keys()|after.keys() if before.get(k)!=after.get(k)}
    if not changed or not changed<=PATCH_FILES:raise ValueError('Unreviewed runtime-file change')
    for name in ('approval','readiness'):
        if scope.get(name+'_file_sha256')!=util.sha256_file(root/(name+'.json')):
            raise ValueError('Original authorization evidence changed')
    if (scope.get('settings_sha256')!=ledger['settings_sha256'] or digest(settings)!=ledger['settings_sha256']
            or scope.get('budget_volume')!=settings['budget_volume']):raise ValueError('Original configuration/budget changed')
    return {'campaign_instance':ledger['id'],'started_epoch':ledger['started_epoch'],'deadline':ledger['deadline'],'scope':scope}

def stopped_checkpoint(root,settings,plan,ledger):
    if ledger['phase'] not in ('running','paused_for_confirmation_scope') or ledger['halt_reason']:
        raise ValueError('Only an open, unhalted campaign can be handed over')
    if not ledger['attempts'] or any(a['status']!='finished' or not a['stop_confirmed'] for a in ledger['attempts']):
        raise ValueError('Every original model attempt must be stopped and collected')
    for attempt in ledger['attempts']:
        record=root/'runs'/attempt['run_uuid'];data=util.read_json(record/'box.json')
        for role in ('worker','gateway','database'):
            item=box.owned(data,role)
            if item and item['State']['Running']:raise ValueError('An owned Run container is still active')
        if util.sha256_file(record/'submission.tar')!=attempt['submission_sha256']:
            raise ValueError('Original submission changed')
    for record,name,label,ident in evaluation_resources(settings,ledger,root):
        item=runtime.inspect_container(name)
        if item and (item['Config']['Labels'].get(label)!=ident or item['State']['Running']):
            raise ValueError('An owned evaluator is active or its identity changed')
    budget=box.budget_action(settings,'snapshot')
    if (budget['closed_reason'] is not None or budget['campaign_id']!=plan['campaign_id']
            or budget['plan_sha256']!=digest(plan) or budget['deadline']!=ledger['deadline']
            or budget['max_requests']!=LIMITS['requests'] or budget['max_tokens']!=LIMITS['observed_input_output_tokens']
            or budget['inflight_requests']!=0 or budget['reserved_requests']>=LIMITS['requests']
            or budget['observed_tokens_lower_bound']>=LIMITS['observed_input_output_tokens']):
        raise ValueError('Original open budget cannot be preserved')
    if time.time()>=ledger['deadline']:raise ValueError('Original wall deadline reached')
    return budget

def prepare_scope(record,base_runtime,decision_reference,decision_sha256,supervisor_exit_receipt):
    root,settings,plan,ledger=controller.load(record)
    if (root/'confirmation-scope.json').exists():raise ValueError('Scope amendment already exists')
    exit_receipt=util.read_json(Path(supervisor_exit_receipt))
    if (exit_receipt.get('campaign_instance')!=ledger['id'] or exit_receipt.get('supervisor_exited') is not True
            or exit_receipt.get('model_workers_stopped') is not True or exit_receipt.get('evaluators_stopped') is not True):
        raise ValueError('Verified old supervisor and owned evaluator stop required')
    if process_alive(exit_receipt.get('supervisor_pid')):raise ValueError('Old supervisor is still running')
    budget=stopped_checkpoint(root,settings,plan,ledger)
    billing=util.read_json(root/'readiness.json')['billing']
    if billing.get('verification_method')!='explicit_user_confirmation':raise ValueError('Explicit user confirmation required')
    scope={'kind':'explicit-user-confirmation-campaign-scope-v1','at':run.now(),
        'campaign_instance':ledger['id'],'plan_sha256':digest(plan),
        'settings_sha256':ledger['settings_sha256'],'budget_volume':settings['budget_volume'],
        'workspace_binding_sha256':billing['workspace_binding_sha256'],
        'confirmation_record_sha256':billing['confirmation_record_sha256'],
        'confirmed_at_epoch':billing['observed_at_epoch'],'started_epoch':ledger['started_epoch'],'deadline':ledger['deadline'],
        'revoked':False,'contradictory_setting_evidence':False,'decision_reference':decision_reference,
        'decision_sha256':decision_sha256,'supervisor_exit_receipt_sha256':util.sha256_file(Path(supervisor_exit_receipt)),
        'approval_file_sha256':util.sha256_file(root/'approval.json'),'readiness_file_sha256':util.sha256_file(root/'readiness.json'),
        'original_runtime_definition':util.read_json(Path(base_runtime)),
        'amended_runtime_sha256':digest(controller.runtime_definition(settings))}
    folder=root/'handoff'/uuid.uuid4().hex;folder.mkdir(parents=True)
    # Preserve exact pre-amendment ledger and budget as immutable checkpoint evidence.
    (folder/'ledger.original.json').write_bytes((root/'ledger.json').read_bytes())
    util.write_new_json(folder/'budget.original.json',budget)
    util.write_new_json(folder/'supervisor-exit.json',exit_receipt)
    util.write_new_json(root/'confirmation-scope.json',scope)
    controller.readiness(root,settings,plan)
    ledger['phase']='paused_for_confirmation_scope'
    ledger['administrative_handoff']={'record':str(folder),'scope_sha256':util.sha256_file(root/'confirmation-scope.json'),
        'original_ledger_sha256':util.sha256_file(folder/'ledger.original.json'),'model_replay':False}
    controller.save(root,ledger,'confirmation_scope_checkpoint')
    return {'record':str(root),'checkpoint':str(folder),'budget':budget,'scope_sha256':ledger['administrative_handoff']['scope_sha256']}

def continue_campaign(record,*,pause_after_wave=False):
    from .lifecycle import writer
    if controller.load(record)[3]['phase']!='paused_for_confirmation_scope':raise ValueError('A sealed one-use handoff checkpoint is required')
    with writer(record):return _continue_campaign(record,pause_after_wave=pause_after_wave)


def _continue_campaign(record,*,pause_after_wave=False):
    root,settings,plan,ledger=controller.load(record)
    if pause_after_wave and not settings['synthetic']:raise ValueError('Test pause is synthetic only')
    if ledger['phase']!='paused_for_confirmation_scope':raise ValueError('A sealed one-use handoff checkpoint is required')
    handoff=ledger['administrative_handoff'];folder=Path(handoff['record'])
    if (util.sha256_file(folder/'ledger.original.json')!=handoff['original_ledger_sha256']
            or util.sha256_file(root/'confirmation-scope.json')!=handoff['scope_sha256']):raise ValueError('Handoff checkpoint changed')
    before=util.read_json(folder/'ledger.original.json');budget=stopped_checkpoint(root,settings,plan,ledger)
    if budget!=util.read_json(folder/'budget.original.json'):raise ValueError('Budget changed while supervisor was stopped')
    for key in ('id','started_epoch','deadline','plan_sha256','settings_sha256','attempts','assignments'):
        if ledger[key]!=before[key]:raise ValueError('Original campaign state changed: '+key)
    controller.readiness(root,settings,plan)
    # The lock is never silently removed: an interruption requires explicit recovery.
    fd=os.open(root/'continuation.lock',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as stream:stream.write(str(os.getpid()))
    ledger['phase']='running';controller.save(root,ledger,'confirmation_scope_continuation_started')
    if not settings['synthetic']:controller.score_completed(root,settings,ledger)
    for number in range(1,5):
        attempted={a['assignment_id'] for a in ledger['attempts']}
        assignments=sorted([a for a in ledger['assignments'] if a['wave']==number and a['assignment_id'] not in attempted],
                           key=lambda a:a['wave_order'])
        if not assignments or ledger['halt_reason']:continue
        controller.wave(root,settings,plan,ledger,assignments)
        if ledger['phase']=='recovery_required':return {'phase':ledger['phase']}
        if not settings['synthetic']:controller.score_completed(root,settings,ledger)
        if pause_after_wave:
            if not settings['synthetic']:raise ValueError('Test pause is synthetic only')
            return {'phase':'test_pause','attempts':len(ledger['attempts']),'budget':box.budget_action(settings,'snapshot')}
    if not settings['synthetic'] and not ledger['halt_reason']:
        retry_count=sum(a['retry'] for a in ledger['attempts'])
        candidates=[a for a in ledger['attempts'] if a['technical_failure'] and a['stop_confirmed']
            and not a['retry'] and sum(x['assignment_id']==a['assignment_id'] for x in ledger['attempts'])==1]
        ids={a['assignment_id'] for a in candidates[:LIMITS['technical_retries']-retry_count]}
        retry=[a for a in ledger['assignments'] if a['assignment_id'] in ids]
        if retry:controller.wave(root,settings,plan,ledger,retry,retry=True)
        controller.score_completed(root,settings,ledger)
    box.budget_action(settings,'close',{'reason':ledger['halt_reason'] or 'finite_allocation_complete'})
    ledger['budget']=box.budget_action(settings,'snapshot')
    for assignment in ledger['assignments']:
        found=[a for a in ledger['attempts'] if a['assignment_id']==assignment['assignment_id']]
        assignment['status']='attempted' if found else 'not_run'
        if not found:assignment['not_run_reason']=assignment.get('not_run_reason') or ledger['halt_reason'] or 'outside_finite_scope'
    ledger['phase']='finished';controller.save(root,ledger,'campaign_finished')
    return {'phase':ledger['phase'],'attempts':len(ledger['attempts']),'budget':ledger['budget']}

if __name__=='__main__':
    import json
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    q=sub.add_parser('prepare');q.add_argument('--record',type=Path,required=True)
    for key in ('base-runtime','supervisor-exit-receipt'):q.add_argument('--'+key,type=Path,required=True)
    for key in ('decision-reference','decision-sha256'):q.add_argument('--'+key,required=True)
    q=sub.add_parser('continue');q.add_argument('--record',type=Path,required=True)
    a=p.parse_args()
    if a.command=='prepare':result=prepare_scope(a.record,a.base_runtime,a.decision_reference,a.decision_sha256,a.supervisor_exit_receipt)
    else:
        from .supervisor import launch
        result=launch(a.record,'continue')
    print(json.dumps(result))
