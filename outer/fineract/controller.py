"""Finite wave supervisor with durable intents and fail-closed recovery.

The ledger is controller-owned, never a worker mount. A resumed process never
silently replays a model attempt. Recovery fences admission and reconciles owned
containers; frozen submissions can then be scored without model calls.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid

from outer.harness import runtime, util, run
from . import box
from .campaign import LIMITS, digest, make_plan, validate_readiness
from .seed import volume


def validate_settings(settings):
    for key in ('worker_image','gateway_image','api_image'):
        if not settings[key].startswith('sha256:') or runtime.image_id(settings[key])!=settings[key]:
            raise ValueError('Immutable locally available image required')
    agent=settings['agent']
    if (agent['model_id']!='deepseek-v4.1-flash' or agent['provider_timeout_seconds']!=600
            or agent['opencode_version']!='1.17.11'):
        raise ValueError('Pinned agent and provider timeout required')
    manifest=util.read_json(Path(settings['bundle'])/'manifest.json')
    util.reject_links(Path(settings['bundle']))
    return manifest


def runtime_definition(settings):
    prep=util.read_json(Path(settings['preparation'])/'result.json')
    seed=util.read_json(Path(settings['seed'])/'result.json')
    if settings.get('worker_build_policy',{}).get('kind')!='prepared_public_sdk':
        raise ValueError('Explicit public-only SDK runtime policy required')
    if not settings['worker_build_policy'].get('decision_reference'):
        raise ValueError('Explicit worker build policy decision required')
    sdk=util.read_json(Path(settings['public_sdk'])/'result.json')
    if not sdk['success'] or sdk['private_inputs'] is not False or sdk['archive_sha256']!=prep['archive_sha256']:
        raise ValueError('Verified independent public SDK required')
    return {'worker_image':settings['worker_image'],'gateway_image':settings['gateway_image'],
            'api_image':settings['api_image'],'java_image':prep['java_image_id'],
            'agent':settings['agent'],'source_commit':prep['source_commit'],
            'archive_sha256':prep['archive_sha256'],'seed_inventory_sha256':seed['inventory_sha256'],
            'public_sdk_inventory_sha256':sdk['inventory_sha256'],'public_sdk_jar_sha256':sdk['build']['jar_sha256'],
            'worker_build_policy':settings['worker_build_policy'],'worker_memory':'5g','worker_cpus':3,
            'provider_request_deadline':{'seconds':600,'clock':'monotonic',
                'scope':'dns_connect_tls_headers_body','on_expiry':'close_owned_sockets',
                'partial_usage':'incomplete'},
            'gateway_source_sha256':util.sha256_file(Path(__file__).parents[1]/'harness/gateway.py'),
            'database_image':box.POSTGRES,'database_memory':'1g','gateway_memory':'512m',
            'controller_files':{p.name:util.sha256_file(p) for p in sorted(Path(__file__).parent.iterdir()) if p.is_file()}}


def create(settings_path,plan_path,approval_path,readiness_path,output,*,synthetic=False):
    settings=util.read_json(Path(settings_path));settings['synthetic']=synthetic
    manifest=validate_settings(settings)
    plan=util.read_json(Path(plan_path))
    if plan['manifest_sha256']!=digest(manifest):raise ValueError('Exact execution-bound manifest required')
    if plan['limits']!=LIMITS:raise ValueError('Finite limits changed')
    rebuilt=make_plan(manifest,campaign_id=plan['campaign_id'],seed=plan['seed'],
        runtime_sha256=plan['runtime_sha256'],evaluator_sha256=plan['evaluator_sha256'],
        selection_seal_sha256=plan['selection_seal_sha256'])
    if rebuilt!=plan:raise ValueError('Finite assignment/order contract changed')
    approval=util.read_json(Path(approval_path)) if approval_path else {}
    readiness=util.read_json(Path(readiness_path)) if readiness_path else {}
    if not synthetic:
        if digest(runtime_definition(settings))!=plan['runtime_sha256']:
            raise ValueError('Actual runtime definition differs from approved plan')
        validate_readiness(plan,approval,**readiness,now=time.time())
        if (settings.get('runtime_sha256')!=plan['runtime_sha256']
                or settings.get('evaluator_sha256')!=plan['evaluator_sha256']):
            raise ValueError('Execution settings differ from approved plan')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=False)
    ident=uuid.uuid4().hex;settings['budget_volume']='fe-campaign-budget-'+ident
    settings['campaign_instance']=ident
    for name,value in [('settings',settings),('plan',plan),('approval',approval),('readiness',readiness)]:
        util.write_new_json(root/(name+'.json'),value)
    ledger={'kind':'fineract-finite-controller-v1','id':ident,'created_at':run.now(),
            'started_epoch':None,'deadline':None,'phase':'prepared','synthetic':synthetic,
            'plan_sha256':digest(plan),'settings_sha256':digest(settings),
            'attempts':[],'assignments':[dict(a,status='pending' if a['planned_status']=='scheduled' else 'not_run')
                                       for a in plan['assignments']],
            'model_calls':0 if synthetic else None,'halt_reason':None}
    util.write_new_json(root/'ledger.json',ledger)
    volume(settings['budget_volume'],'modernization.campaign='+ident,settings['worker_image'])
    return {'record':str(root),'synthetic':synthetic,'status':'prepared'}


def load(root):
    root=Path(root).resolve()
    settings=util.read_json(root/'settings.json');plan=util.read_json(root/'plan.json')
    ledger=util.read_json(root/'ledger.json')
    if ledger['plan_sha256']!=digest(plan) or ledger['settings_sha256']!=digest(settings):
        raise ValueError('Saved controller bindings changed')
    return root,settings,plan,ledger


def save(root,ledger,event):
    util.append_line(root/'events.jsonl',{'at':run.now(),'event':event,'phase':ledger['phase']})
    util.write_json_atomic(root/'ledger.json',ledger)


def admitted(ledger,now):
    return (not ledger['halt_reason'] and now<ledger['deadline']
            and len(ledger['attempts'])<LIMITS['max_runs']
            and (len(ledger['attempts'])+1)*LIMITS['run_seconds']<=LIMITS['total_run_seconds'])


def new_attempt(ledger,assignment,now,*,retry=False):
    if not admitted(ledger,now):raise ValueError('Campaign admission closed')
    if sum(a['status']=='running' for a in ledger['attempts'])>=LIMITS['concurrency']:
        raise ValueError('Four active Run limit')
    previous=[a for a in ledger['attempts'] if a['assignment_id']==assignment['assignment_id']]
    if retry:
        if (len(previous)!=1 or previous[0].get('technical_failure') is not True
                or previous[0].get('stop_confirmed') is not True
                or sum(a['retry'] for a in ledger['attempts'])>=LIMITS['technical_retries']):
            raise ValueError('Only two evidenced technical retries are permitted')
    elif previous:raise ValueError('Normal assignment cannot be replayed')
    row={'run_uuid':uuid.uuid4().hex,'assignment_id':assignment['assignment_id'],
         'task':assignment['task'],'condition':assignment['condition'],'wave':assignment['wave'],
         'attempt':len(previous)+1,'retry':retry,'status':'running','reserved_epoch':now,
         'launch_epoch':None,'technical_failure':False,'stop_confirmed':False}
    ledger['attempts'].append(row)
    return row


def readiness(root,settings,plan):
    if not settings['synthetic']:
        if digest(runtime_definition(settings))!=plan['runtime_sha256']:
            raise ValueError('Runtime files or frozen dependencies changed')
        validate_readiness(plan,util.read_json(root/'approval.json'),
                           **util.read_json(root/'readiness.json'),now=time.time())


def finish(root,settings,attempt,reason):
    record=root/'runs'/attempt['run_uuid']
    attempt['end_reason']=reason;attempt['ended_epoch']=time.time()
    if (record/'box.json').exists():
        stopped=box.stop(record,reason);attempt['stop_confirmed']=stopped['stop_confirmed']
        if (record/'initialized.json').exists() and stopped['stop_confirmed']:
            artifact=box.collect(record,settings);attempt['submission_sha256']=artifact['sha256']
    else:attempt['stop_confirmed']=True
    attempt['status']='finished' if attempt['stop_confirmed'] else 'stop_unconfirmed'
    attempt['duration_seconds']=None if attempt['launch_epoch'] is None else max(0,attempt['ended_epoch']-attempt['launch_epoch'])


def wave(root,settings,plan,ledger,assignments,*,retry=False,halt_after_launch=False):
    attempts=[];peers=[];processes=[]
    try:
        for assignment in assignments:
            if not admitted(ledger,time.time()):break
            attempt=new_attempt(ledger,assignment,time.time(),retry=retry);attempts.append(attempt)
            save(root,ledger,'attempt_reserved')
            try:
                data=box.allocate(root/'runs'/attempt['run_uuid'],settings,assignment,attempt['run_uuid'])
                peers.append(data)
                process=box.start_gateway(root/'runs'/attempt['run_uuid'],settings)
                if process:processes.append(process)
            except (runtime.DockerCommandError,TimeoutError) as error:
                attempt.update(technical_failure=True,technical_evidence={'phase':'allocation',
                               'type':type(error).__name__})
                finish(root,settings,attempt,'technical_allocation_failure')
                save(root,ledger,'allocation_failed')
        for attempt in attempts:
            if attempt['status']!='running':continue
            if time.time()>=ledger['deadline'] or (root/'stop-request.json').exists():
                ledger['halt_reason']='deadline_or_operator_stop';break
            readiness(root,settings,plan)
            attempt['launch_epoch']=time.time();save(root,ledger,'launch_intent')
            box.launch(root/'runs'/attempt['run_uuid'],settings,peers)
        if settings['synthetic']:
            layout=[]
            for peer in peers:
                roles={}
                for role in ('worker','gateway','database'):
                    item=box.owned(peer,role)
                    if item:
                        roles[role]={'running':item['State']['Running'],'image':item['Image'],
                            'memory':item['HostConfig']['Memory'],'user':item['Config']['User'],
                            'ports':item['HostConfig']['PortBindings'],'networks':sorted(item['NetworkSettings']['Networks']),
                            'mounts':[{'destination':m['Destination'],'type':m['Type'],'name':m.get('Name'),'writable':m['RW']} for m in item['Mounts']]}
                layout.append({'id':peer['id'],'roles':roles})
            util.write_new_json(root/'runtime-layout.json',layout)
        if halt_after_launch:
            # Explicit synthetic crash/recovery exercise. No real inference path.
            if not settings['synthetic']:raise ValueError('Crash exercise is synthetic only')
            ledger['phase']='recovery_required';save(root,ledger,'synthetic_interruption')
            return
        while any(a['status']=='running' for a in attempts):
            if time.time()>=ledger['deadline'] or (root/'stop-request.json').exists():
                ledger['halt_reason']='deadline_or_operator_stop'
            snapshot=box.budget_action(settings,'snapshot')
            if (snapshot['reserved_requests']>=LIMITS['requests']
                    or snapshot['observed_tokens_lower_bound']>=LIMITS['observed_input_output_tokens']):
                ledger['halt_reason']='shared_request_or_token_limit'
            if ledger['halt_reason']:
                box.budget_action(settings,'close',{'reason':ledger['halt_reason']})
            polled_runs={a['run_uuid']:box.poll(root/'runs'/a['run_uuid']) for a in attempts if a['status']=='running'}
            synthetic_ready=all(p['finished'] for p in polled_runs.values())
            for attempt in attempts:
                if attempt['status']!='running':continue
                polled=polled_runs[attempt['run_uuid']]
                reason=None
                if ledger['halt_reason']:reason=ledger['halt_reason']
                elif polled['finished']:
                    if settings['synthetic'] and not synthetic_ready:continue
                    attempt['worker_result']=polled
                    reason='completed' if settings['synthetic'] or polled['state']['ExitCode']==0 else 'agent_error'
                elif attempt['launch_epoch'] is not None and time.time()-attempt['launch_epoch']>=LIMITS['run_seconds']:
                    reason='run_timeout'
                if reason:
                    finish(root,settings,attempt,reason);save(root,ledger,'attempt_stopped_and_collected')
            if any(a['status']=='running' for a in attempts):time.sleep(2)
    except BaseException:
        ledger['halt_reason']='controller_exception'
        try:box.budget_action(settings,'close',{'reason':ledger['halt_reason']})
        finally:
            for attempt in attempts:
                if attempt['status']=='running':
                    try:finish(root,settings,attempt,'controller_exception')
                    except Exception:attempt['status']='stop_unconfirmed'
            ledger['phase']='recovery_required';save(root,ledger,'exception_recovery_required')
        raise
    finally:
        for process in processes:
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:pass


def execute(record,*,halt_after_launch=False):
    root,settings,plan,ledger=load(record)
    if ledger['phase']!='prepared':raise ValueError('Use recovery; never replay an existing campaign')
    readiness(root,settings,plan)
    ledger['started_epoch']=time.time();ledger['deadline']=time.time()+LIMITS['wall_seconds']
    ledger['phase']='running';save(root,ledger,'campaign_started')
    box.budget_action(settings,'create',{'campaign_id':plan['campaign_id'],
        'plan_sha256':digest(plan),'max_requests':LIMITS['requests'],
        'max_tokens':LIMITS['observed_input_output_tokens'],'deadline':ledger['deadline']})
    for wave_number in range(1,5):
        assignments=sorted([a for a in ledger['assignments'] if a['wave']==wave_number],key=lambda a:a['wave_order'])
        if not assignments or ledger['halt_reason']:continue
        wave(root,settings,plan,ledger,assignments,halt_after_launch=halt_after_launch)
        if ledger['phase']=='recovery_required':return {'record':str(root),'phase':ledger['phase']}
        if settings['synthetic']:break
        score_completed(root,settings,ledger)
    if not settings['synthetic'] and not ledger['halt_reason']:
        retry_ids=[a['assignment_id'] for a in ledger['attempts'] if a['technical_failure'] and a['stop_confirmed']][:2]
        retries=[a for a in ledger['assignments'] if a['assignment_id'] in retry_ids]
        if retries:wave(root,settings,plan,ledger,retries,retry=True)
        score_completed(root,settings,ledger)
    box.budget_action(settings,'close',{'reason':ledger['halt_reason'] or 'finite_allocation_complete'})
    ledger['budget']=box.budget_action(settings,'snapshot')
    for assignment in ledger['assignments']:
        records=[a for a in ledger['attempts'] if a['assignment_id']==assignment['assignment_id']]
        assignment['status']='attempted' if records else 'not_run'
        if not records:assignment['not_run_reason']=assignment.get('not_run_reason') or ledger['halt_reason'] or 'synthetic_probe_scope'
    ledger['phase']='finished';save(root,ledger,'campaign_finished')
    return {'record':str(root),'phase':ledger['phase'],'attempts':len(ledger['attempts']),
            'synthetic':settings['synthetic'],'budget':ledger['budget']}


def score_completed(root,settings,ledger):
    from .execution import evaluate
    for attempt in ledger['attempts']:
        if attempt.get('submission_sha256') and 'evaluation' not in attempt:
            if time.time()>=ledger['deadline']:
                ledger['halt_reason']='wall_deadline';break
            attempt['evaluation']=evaluate({**settings,'evaluation_deadline':ledger['deadline']},attempt['task'],root/'runs'/attempt['run_uuid'])
            save(root,ledger,'evaluation_saved')


def recover(record):
    root,settings,plan,ledger=load(record)
    if ledger['phase']=='prepared':raise ValueError('Campaign never started')
    try:box.budget_action(settings,'close',{'reason':'recovery'})
    except (runtime.DockerCommandError,TimeoutError) as error:
        # A crash before budget creation must not prevent per-gateway fencing.
        ledger['budget_recovery_error_type']=type(error).__name__
    for attempt in ledger['attempts']:
        if not attempt.get('stop_confirmed'):
            finish(root,settings,attempt,'recovered_interruption')
    ledger['halt_reason']='recovery';ledger['phase']='recovered'
    try:ledger['budget']=box.budget_action(settings,'snapshot')
    except (runtime.DockerCommandError,TimeoutError):ledger['budget']=None
    for assignment in ledger['assignments']:
        if not any(a['assignment_id']==assignment['assignment_id'] for a in ledger['attempts']):
            assignment.update(status='not_run',not_run_reason='recovery')
    save(root,ledger,'recovered_without_model_replay')
    return {'record':str(root),'phase':'recovered','stop_confirmed':all(a['stop_confirmed'] for a in ledger['attempts'])}


def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    for command in ('create','create-probe'):
        q=sub.add_parser(command)
        for name in ('settings','plan','output'):q.add_argument('--'+name,required=True,type=Path)
        q.add_argument('--approval',type=Path);q.add_argument('--readiness',type=Path)
    q=sub.add_parser('run');q.add_argument('--record',required=True,type=Path);q.add_argument('--halt-after-launch',action='store_true')
    for command in ('recover','score'):sub.add_parser(command).add_argument('--record',required=True,type=Path)
    a=p.parse_args()
    if a.command.startswith('create'):
        result=create(a.settings,a.plan,a.approval,a.readiness,a.output,synthetic=a.command=='create-probe')
    elif a.command=='run':result=execute(a.record,halt_after_launch=a.halt_after_launch)
    elif a.command=='recover':result=recover(a.record)
    else:
        root,settings,plan,ledger=load(a.record)
        if settings['synthetic']:raise ValueError('Synthetic probe is not a model submission')
        if ledger['phase'] not in ('finished','recovered'):raise ValueError('Stopped campaign required')
        score_completed(root,settings,ledger);result={'record':str(root),'phase':ledger['phase']}
    print(json.dumps(result))


if __name__=='__main__':
    main()
