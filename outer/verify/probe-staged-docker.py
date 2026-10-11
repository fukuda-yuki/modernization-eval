"""Opt-in real Docker/native/gateway probe with a localhost canned provider.

No model credential is read. The two explicitly replaced seams are campaign
approval (this is an artificial Run) and the gateway stdin secret (synthetic).
Everything from runtime.start through owned DockerTransport, isolation, stop,
network cleanup and live_usage collection is the production implementation.
The fixture worker has Python/bash + the unchanged native binary; no .NET SDK.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from outer.harness import gateway,live_usage,preserve,profiles,runtime,staged_input,staged_runtime,util

BASE='python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258'
BINARY_SHA='0254a429cd0e6cf0ba53fc01672cf98e4a8dc728f7fa94be88f1e4b3645e6ded'
REPO=Path(__file__).resolve().parents[2]
AUTHORIZE=staged_runtime.validate_authorization
CHECKPOINT=staged_input.Controller.checkpoint

def checkpoint_with_stop(controller,label,binding):
    receipt=CHECKPOINT(controller,label,binding)
    if label in ('before-additional','before-resume') and 'stop-before' in controller.contract['run_id']:
        # Fault injection inside the real owned pause; no message is submitted.
        util.write_new_json(Path(controller.contract['workspace']).parent/'stop-request.json',{'reason':'synthetic_operator_stop'})
        controller.stop('synthetic_operator_stop')
    return receipt

def build(directory,binary,base):
    directory.mkdir();shutil.copy2(binary,directory/'opencode')
    docker_config=directory/'docker-config';docker_config.mkdir()
    environment={**os.environ,'DOCKER_CONFIG':str(docker_config)}
    shutil.copy2(REPO/'outer/harness/gateway.py',directory/'gateway.py')
    shutil.copy2(REPO/'outer/verify/staged-fake-provider.py',directory/'provider.py')
    (directory/'Dockerfile').write_text('''ARG BASE=python:3.12-slim-bookworm
FROM ${BASE} AS worker
COPY --chmod=0755 opencode /usr/local/bin/opencode
ENV HOME=/tmp/home XDG_CONFIG_HOME=/tmp/config XDG_DATA_HOME=/state XDG_CACHE_HOME=/tmp/cache OPENCODE_DISABLE_AUTOUPDATE=true OPENCODE_DISABLE_MODELS_FETCH=true OPENCODE_DISABLE_DEFAULT_PLUGINS=true
WORKDIR /workspace
USER 1000:1000
FROM ${BASE} AS gateway
COPY --chmod=0555 gateway.py provider.py /app/
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
ENTRYPOINT ["python3","/app/provider.py"]
''')
    images={};tags={};prefix='s2-staged-fixture-'+uuid.uuid4().hex
    for target in ('worker','gateway'):
        tag=prefix+':'+target
        with (directory/(target+'.log')).open('wb') as output:
            subprocess.run(['docker','build','--network','none','--target',target,'--build-arg','BASE='+base,
                '-t',tag,str(directory)],check=True,stdout=output,stderr=subprocess.STDOUT,timeout=180,env=environment)
        images[target]=runtime.image_id(tag)
        tags[target]=tag;util.write_json_atomic(directory/'image-tags.json',tags)
    return images


def case(directory,images,scenario,grouped=False,mode=None,task=None):
    condition_id=mode or 'staged-explore'
    root=directory/((task or 'STAGED')+'-'+scenario+'-'+condition_id+'-001');root.mkdir()
    for name in ('inputs/legacy-source','workspace','state','usage/raw','evidence','profiles','evaluation-assets'):
        (root/name).mkdir(parents=True)
    initial='Synthetic initial requirement: implement stage one.\n'
    additional='Synthetic later requirement: implement stage two in the same file.\n'
    partial=initial
    if mode=='explore-observed':initial+=additional
    (root/'inputs/legacy-source/Original.cs').write_text('// synthetic original source')
    (root/'inputs/original.sqlite').write_bytes(b'synthetic original DB; never hidden')
    real_task=task in ('MS1-CONT-A','MS1-CONT-B','CU1-ENR-C','CU1-ENR-D')
    condition=(profiles.resolve(REPO,task,condition_id,
        'deepseek-music-observation-v1' if task.startswith('MS1') else 'deepseek-education-observation-v1',
        task_revision='evaluators-20261006') if real_task else profiles.resolve(REPO,'MS1-001','explore'))
    condition['runtime']['prompt_transport']='stdin';condition['runtime']['timeout_seconds']=50
    condition['runtime']['provider_timeout_seconds']=40
    condition['budget']['value']=50;condition['intervention']['method']=condition_id
    # Exercise each real profile's common wrapper/collection settings with toy
    # request text and toy source/DB. Never copy a research request into a fixture.
    condition['migration_request']=partial+additional
    context={'method':condition_id,'blocks':[]}
    if mode:initial,context=profiles.prepare_prompt({**condition,'migration_request':initial},root/'inputs/legacy-source')
    (root/'inputs/prompt.txt').write_text(initial)
    if scenario=='archive-failure':condition['collection_policy']=util.STATIC_DB_COLLECTION_POLICY
    condition['runtime_lock']={'controller_files':runtime.controller_files(REPO),'images':images,'opencode_version':'1.17.11'}
    util.write_new_json(root/'condition.json',condition)
    util.write_new_json(root/'context.json',context)
    manifest={'schema_version':2,'run_id':root.name,'run_instance_id':uuid.uuid4().hex,'task_id':task or 'STAGED-'+scenario,
        'condition_id':condition_id,'started_at':None,'stop_confirmed':False,
        'condition_sha256':util.sha256_file(root/'condition.json'),'context_sha256':util.sha256_file(root/'context.json'),
        'input_files':util.tree_hashes(root/'inputs'),'assets_sha256':{},'profile_files':{},
        'prompt_sha256':util.sha256_file(root/'inputs/prompt.txt')}
    util.write_new_json(root/'manifest.json',manifest)
    policy={'suffixes':['.cs'],'excluded_directories':['obj','bin']}
    _,_,partition=staged_input.split_sections([{'id':'a','text':partial},{'id':'b','text':additional}],['a'],['b'])
    partition['initial_sha256']=util.sha256_bytes(initial.encode())
    boundary={'transport':'opencode-server-response-barrier-v1',
        'snapshot_policy':policy,'snapshot_policy_sha256':staged_input.digest(policy)}
    if grouped:boundary['boundary_policy']=staged_input.GROUP_BOUNDARY
    if mode:boundary.update(boundary_policy=staged_input.GROUP_BOUNDARY,observation_policy=staged_input.OBSERVATION_POLICY)
    controller=staged_input.create(root/'_controller/staged-input',run_id=root.name,
        run_instance_id=manifest['run_instance_id'],task=manifest['task_id'],condition=condition_id,
        workspace=root/'workspace',worker_roots=[root/'inputs',root/'state'],initial_prompt=initial,
        additional_prompt=None if mode=='explore-observed' else additional,
        budget_seconds=50,boundary_contract=boundary,partition=partition,
        artifact_collection_policy=util.resolve_collection_policy(condition.get('collection_policy')))
    controller.bind_run(root)
    # The public runtime entrypoint must reject a partition without approval,
    # before allocating a network or reading any real provider credential.
    try:AUTHORIZE(REPO,root,manifest,controller,condition)
    except ValueError as error:
        if 'approved campaign' not in str(error):raise
    else:raise AssertionError('Unapproved staged authorization succeeded')
    result=runtime.start(REPO,directory,root.name)
    usage=util.read_json(root/'usage/normalized.json');events=controller.events()
    requests=[util.read_json(root/'usage/raw'/e['request_file']) for e in live_usage.journal(root/'usage/raw/started.jsonl')[0]]
    checks={'stop_confirmed':result['stop_confirmed'],'network_cleaned':result['network_cleanup']['confirmed'],
        'real_isolation_probe':util.read_json(root/'evidence/isolation.json')['verified'],
        'one_native_session':len(usage.get('staged_input',{}).get('native_session_ids',[]))==1,
        'correct_initial':bool(requests) and (gateway.check_additional_input(requests[0],additional)['verified']
            if mode=='explore-observed' else not gateway.check_additional_input(requests[0],additional)['locations']),
        'unauthorized_binding_refused':True}
    if mode and scenario!='stop-before':
        stage=usage['staged_input'];checkpoints=stage['checkpoints'];control=mode=='explore-observed'
        expected_calls=1 if scenario=='early' else 2 if scenario=='no-post-change' else 4 if scenario=='continue' else 3
        reached=scenario!='early';post=scenario not in ('early','no-post-change')
        actions=[e for e in events if e['kind']=='resumption_action']
        checks.update(no_extra_file=not (controller.root/'additional.txt').exists() if control else True,
            exact_sends=sum(e['kind']=='send_intent' for e in events)==(0 if control or not reached else 1),
            explicit_action=(len(actions)==1 and actions[0]['action']['action']==('no_input_by_design' if control else 'additional_input_acknowledged')) if reached else not actions,
            complete_stage_evidence=usage['input_reached'] and not usage['inventory_issues'],
            all_usage=usage['observed_tokens']==12*expected_calls,
            request_count=len(requests)==expected_calls,
            first_post_resume=bool(stage['first_post_resume_request'])==reached,
            first_additional_absent=stage['first_observed_request'] is None if control else bool(stage['first_observed_request'])==reached,
            exact_native_user_count=bool(stage.get('native_input_inventory')) and len(stage['native_input_inventory']['external_user_messages'])==(1 if control or not reached else 2),
            before_checkpoint=(checkpoints['before-resume']['archive'] is None
                and bool(checkpoints['before-resume']['reason'])) if scenario=='archive-failure'
                else bool(checkpoints['before-resume']['archive'])==reached,
            after_checkpoint=bool(checkpoints['after-resume-first-change']['archive'])==post,
            final_checkpoint=bool(checkpoints['final']['archive']),
            neutral_labels=set(checkpoints)=={'initial','before-resume','after-resume-first-change','final'},
            pause_resume_pairs=sum(e['kind']=='worker_paused' for e in events)==sum(e['kind']=='worker_resumed' for e in events)==(2 if post else 1 if reached else 0))
        if post:
            receipt=checkpoints['after-resume-first-change']['archive']
            preserve.restore(controller.root/'checkpoint-archive',receipt,root/'restore-post')
            checks['first_post_change_restored']=(root/'restore-post/workspace/Program.cs').read_text()=='// synthetic stage 2\n'
        if scenario=='continue':
            checks['model_continued_after_observation']=(root/'workspace/Program.cs').read_text()=='// synthetic stage 3\n'
        checks['not_applicable_delivery']=stage['additional_input_reached'] is None if control else True
    elif scenario=='stop-before':
        checks.update(no_additional_send=not any(e['kind']=='send_intent' for e in events),
            initial_request_only=len(requests)==1,stopped_reason=result['end_reason']=='operator_stop',
            no_false_delivery=not usage['staged_input']['additional_input_reached'],
            pause_released=any(e['kind']=='worker_resumed' and not e['paused'] for e in events))
    elif scenario=='write' or grouped:
        checkpoints=usage['staged_input']['checkpoints']
        for label in ('before-additional','after-additional'):
            if checkpoints[label]['archive']:
                preserve.restore(controller.root/'checkpoint-archive',checkpoints[label]['archive'],root/('restore-'+label))
        restore_before=(root/'restore-before-additional/workspace/Program.cs').read_text() if scenario!='archive-failure' else None
        restore_after=(root/'restore-after-additional/workspace/Program.cs').read_text()
        checks.update(additional_in_next=len(requests)==3 and gateway.check_additional_input(requests[1],additional)['verified'],
            later_file_change=(root/'workspace/Program.cs').is_file() and (root/'workspace/Program.cs').read_text()==('// first' if scenario=='post-child' else '// synthetic stage 2\n'),
            one_send=sum(e['kind']=='send_intent' for e in events)==1,
            complete_stage_evidence=usage['input_reached'] and not usage['inventory_issues'],all_usage=usage['observed_tokens']==36,
            checkpoint_before_restored=(checkpoints['before-additional']['archive'] is None and bool(checkpoints['before-additional']['reason'])
                if scenario=='archive-failure' else restore_before==('// first' if scenario=='child' else '// synthetic stage 1\n')),
            checkpoint_after_restored=restore_after==('// first' if scenario=='post-child' else '// synthetic stage 2\n'))
        if grouped:
            checks['pause_resume_pairs']=sum(e['kind']=='worker_paused' for e in events)==sum(e['kind']=='worker_resumed' for e in events)==2
            checks['no_boundary_unknown']=not any(e['kind']=='boundary_unknown' for e in events)
    else:
        post=scenario.startswith('post-')
        reason='parallel_mutation' if 'parallel' in scenario else 'live_children_at_tool_terminal'
        checkpoints=usage['staged_input']['checkpoints']
        checks.update(no_resend=sum(e['kind']=='send_intent' for e in events)==int(post),
            stopped_at_ambiguous_response=len(requests)==1+int(post),
            explicit_unknown=any(e['kind']=='boundary_unknown' and e['reason']==reason for e in events),
            retained_usage=usage['observed_tokens']==12*(1+int(post)),
            post_checkpoint_null=checkpoints['after-additional']['archive'] is None,
            final_not_used_for_post=checkpoints['final']['archive'] is not None)
        if post:checks['null_reason']=checkpoints['after-additional']['reason']=='boundary_unknown:'+reason
    state=util.read_json(root/'runtime.json')
    for role in ('worker','gateway'):
        item=runtime._owned_container(state,role)
        if item: runtime.docker('rm',state[role])
    return {'scenario':scenario,'condition':condition_id,'task':manifest['task_id'],'checks':checks,'passed':all(checks.values()),'end_reason':result['end_reason'],
        'started_at':result['started_at'],'ended_at':result['ended_at'],
        'request_count':len(requests),'observed_tokens':usage['observed_tokens'],'inventory_issues':usage['inventory_issues']}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--opencode',required=True,type=Path)
    p.add_argument('--base-image',default=BASE);p.add_argument('--output',required=True,type=Path)
    p.add_argument('--scenario',choices=['write','parallel','child','post-parallel','post-child','archive-failure','stop-before','all'],default='all')
    p.add_argument('--group-boundary',action='store_true')
    p.add_argument('--presentation-matrix',action='store_true',help='All four synthetic task bindings in both observed presentation modes, plus early/no-change/STOP cases')
    p.add_argument('--concurrency',type=int,choices=[1,4],default=1)
    a=p.parse_args();binary=a.opencode.resolve(strict=True)
    if util.sha256_file(binary)!=BINARY_SHA:raise ValueError('Exact already-acquired binary required')
    # Keep synthetic artifacts in the explicitly chosen private output directory
    # for failed-case investigation; the caller may remove them after inspection.
    a.output.mkdir(parents=True,exist_ok=False)
    images=build(a.output/'build',binary,a.base_image)
    rows=[]
    try:
        with patch.dict(os.environ,{'DOCKER_CONFIG':str(a.output/'build/docker-config')}),\
             patch.object(staged_runtime,'validate_authorization'),\
             patch.object(staged_input.Controller,'checkpoint',checkpoint_with_stop),\
             patch.object(runtime,'_gateway_credential',return_value='synthetic-canary'):
            # Patch shared process globals once, not from overlapping threads.
            # Each case separately checks the real authorization validator.
            scenarios=('write','parallel','child','post-parallel','post-child') if a.scenario=='all' else (a.scenario,)
            if a.scenario=='all' and a.group_boundary:scenarios+=('archive-failure','stop-before')
            if a.presentation_matrix:
                jobs=[('continue',mode,task) for task in ('MS1-CONT-A','MS1-CONT-B','CU1-ENR-C','CU1-ENR-D')
                      for mode in ('staged-explore','explore-observed')]
                jobs += [(scenario,mode,'TOY') for scenario in ('early','no-post-change','stop-before','archive-failure')
                         for mode in ('staged-explore','explore-observed')]
                with ThreadPoolExecutor(a.concurrency) as pool:
                    rows=list(pool.map(lambda job:case(a.output,images,job[0],True,job[1],job[2]),jobs))
            elif a.concurrency == 1:
                for scenario in scenarios: rows.append(case(a.output,images,scenario,a.group_boundary))
            else:
                with ThreadPoolExecutor(a.concurrency) as pool:
                    rows=list(pool.map(lambda scenario:case(a.output,images,scenario,a.group_boundary),scenarios))
        active=peak=0
        for _,delta in sorted([(r['started_at'],1) for r in rows]+[(r['ended_at'],-1) for r in rows]):
            active+=delta;peak=max(peak,active)
        result={'synthetic':True,'external_model_called':False,'images':images,'base_image':a.base_image,
            'binary_sha256':BINARY_SHA,'cases':rows,'passed':all(row['passed'] for row in rows),
            'production_dotnet_worker_tested':False,'live_acceptance':False,
            'concurrency':a.concurrency,'peak_overlapping_runs':peak,'group_boundary':a.group_boundary or a.presentation_matrix,
            'presentation_matrix':a.presentation_matrix}
        if a.concurrency==4 and len(rows)>=4:result['passed'] &= peak==4
        util.write_new_json(a.output/'result.json',result);print(json.dumps(result,indent=2))
        return 0 if result['passed'] else 1
    finally:
        for path in a.output.glob('*/runtime.json'):
            state=util.read_json(path)
            runtime.stop_owned(path.parent)
            runtime.cleanup_network(path.parent)
            for role in ('worker','gateway'):
                if runtime._owned_container(state,role): runtime.docker('rm',state[role])
        # Tags are unique to this invocation; image layers can be shared by a
        # concurrent fixture. Never force-remove a shared ID or use prune.
        for tag in util.read_json(a.output/'build/image-tags.json').values():
            runtime.docker('image','rm',tag,check=False)
if __name__=='__main__':raise SystemExit(main())
