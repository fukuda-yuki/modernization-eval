"""Bind and execute frozen private specs; no private assets enter a worker."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import uuid

from outer.harness import runtime,util,run
from outer.harness.security import child_environment
from . import api_check,evaluation,java_api,service
from .campaign import TASKS
from .seed import volume


def release_specs(release_path):
    release_path=Path(release_path).resolve();release=util.read_json(release_path)
    specs={}
    for task in TASKS:
        relative='execution-spec-candidate-v1/'+task+'.json'
        path=release_path.parent/relative
        if util.sha256_file(path)!=release['private_files'][relative]['sha256']:
            raise ValueError('Private execution spec changed')
        spec=util.read_json(path)
        if spec['task']!=task or spec['kind']!='execution-spec':raise ValueError('Task spec mismatch')
        specs[task]=(path,spec)
    return release_path,release,specs


def bind(preparation,release_path,output):
    """Generate new public inputs with only evaluator hashes changed.

    Linux-native output preserves the accepted executable-bit inventory. Exported
    host files contain only public routing metadata and initial messages; the full
    public corpus remains in a retained native volume and a recovery tar archive.
    """
    release_path,release,specs=release_specs(release_path)
    prep_root=Path(preparation).resolve();prep=util.read_json(prep_root/'result.json')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=False)
    repo=Path(__file__).resolve().parents[2]
    if runtime.command(['git','diff','HEAD','--','tools/fineract_corpus','docs/fineract-corpus/public'],cwd=repo).stdout:
        raise ValueError('Commit public producer/material changes before binding')
    source=subprocess.run(['git','archive','--format=tar','HEAD','tools/fineract_corpus',
                           'docs/fineract-corpus/public'],cwd=repo,env=child_environment(),capture_output=True,check=True)
    (root/'producer.tar').write_bytes(source.stdout)
    task_dir=root/'tasks';task_dir.mkdir()
    with tarfile.open(root/'producer.tar') as archive:
        tasks=json.loads(archive.extractfile('docs/fineract-corpus/public/tasks.json').read())
        for row in tasks['tasks']:
            row['evaluation_spec_kind']='execution-spec'
            row['evaluation_spec_sha256']=util.sha256_file(specs[row['id']][0])
            (task_dir/row['request_file']).write_bytes(archive.extractfile('docs/fineract-corpus/public/'+row['request_file']).read())
    util.write_new_json(task_dir/'tasks.json',tasks)
    ident=uuid.uuid4().hex;data='fe-corpus-'+ident
    receipt={'id':ident,'at':run.now(),'volume':data,'preparation':str(prep_root),
             'evaluator_sha256':util.sha256_file(release_path),'release':str(release_path),
             'producer_archive_sha256':util.sha256_file(root/'producer.tar')}
    util.write_new_json(root/'intent.json',receipt)
    volume(data,'modernization.corpus='+ident,prep['java_image_id'])
    script="""import json,shutil,tarfile,sys
from pathlib import Path
with tarfile.open('/producer.tar') as archive:
 if any(m.issym() or m.islnk() or m.isdev() for m in archive.getmembers()):raise ValueError('Producer links forbidden')
 archive.extractall('/tmp/producer',filter='data')
sys.path.insert(0,'/tmp/producer')
from tools.fineract_corpus.producer import generate
result=generate(Path('/archive.tar.gz'),Path('/tasks/tasks.json'),Path('/out/bundle'))
for name in ('manifest.json','conditions.md','conditions','tasks','navigation'):
 source=Path('/out/bundle')/name;dest=Path('/export/bundle')/name;dest.parent.mkdir(parents=True,exist_ok=True)
 if source.is_dir():shutil.copytree(source,dest)
 else:shutil.copyfile(source,dest)
with tarfile.open('/export/public-corpus.tar','x') as archive:archive.add('/out/bundle',arcname='bundle')
print(json.dumps(result))
"""
    result=runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=2g','--cpus=2',
        '--tmpfs','/tmp:rw,size=512m',
        '--mount',f'type=bind,source={root / "producer.tar"},target=/producer.tar,readonly',
        '--mount',f'type=bind,source={task_dir},target=/tasks,readonly',
        '--mount',f'type=bind,source={prep_root.parent / "source.tar.gz"},target=/archive.tar.gz,readonly',
        '--mount',f'type=volume,source={data},target=/out',
        '--mount',f'type=bind,source={root},target=/export',prep['java_image_id'],
        'python3','-c',script,timeout=600)
    manifest=util.read_json(root/'bundle/manifest.json')
    receipt.update(bundle=str(root/'bundle'),bundle_volume=data,
        bundle_file_sha256=util.sha256_file(root/'bundle/manifest.json'),
        recovery_archive_sha256=util.sha256_file(root/'public-corpus.tar'),
        semantic_corpus_sha256=manifest['semantic_corpus_sha256'],generation=json.loads(result.stdout))
    util.write_new_json(root/'result.json',receipt)
    return receipt


def wait_for(collect,record,seconds):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=collect(record)
        if not result.get('running'):return result
        time.sleep(3)
    request=util.read_json(Path(record)/'started.json')
    item=runtime.inspect_container(request['container'])
    if item:
        labels=item['Config'].get('Labels') or {}
        if not any(labels.get(k)==request['id'] for k in (
                'modernization.evaluation','modernization.java_api','modernization.api_check')):
            raise ValueError('Evaluator timeout ownership mismatch')
        runtime.docker('stop','--time','10',request['container'],timeout=40)
        collect(record)
    raise TimeoutError('Owned evaluator stage deadline exceeded')


def restore_bundle(binding,output):
    """Restore a frozen public bundle into a new volume without touching its source."""
    binding=Path(binding).resolve();saved=util.read_json(binding/'result.json')
    archive=binding/'public-corpus.tar'
    if util.sha256_file(archive)!=saved['recovery_archive_sha256']:raise ValueError('Recovery archive changed')
    prep=util.read_json(Path(saved['preparation'])/'result.json')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=False)
    ident=uuid.uuid4().hex;data='fe-corpus-recovered-'+ident
    volume(data,'modernization.corpus='+ident,prep['java_image_id'])
    script=r'''import hashlib,json,tarfile
from pathlib import Path
with tarfile.open('/archive.tar') as archive:
 for member in archive:
  if not (member.isfile() or member.isdir()) or member.name.startswith('/') or '..' in Path(member.name).parts:
   raise ValueError('Unsafe public recovery archive')
  archive.extract(member,'/out',filter='data')
root=Path('/out/bundle');manifest=json.loads((root/'manifest.json').read_text())
expected={e['path']:e for e in manifest['output_inventory']}
actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='manifest.json'}
if actual!=set(expected):raise ValueError('Recovered inventory differs')
for name,entry in expected.items():
 if hashlib.sha256((root/name).read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Recovered bytes differ')
print(json.dumps({'manifest_sha256':hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest(),'files_verified':len(expected)}))
'''
    observed=json.loads(runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=1g','--cpus=2',
        '--mount',f'type=bind,source={archive},target=/archive.tar,readonly',
        '--mount',f'type=volume,source={data},target=/out',prep['java_image_id'],
        'python3','-c',script,timeout=300).stdout)
    if observed['manifest_sha256']!=saved['bundle_file_sha256']:raise ValueError('Recovered manifest differs')
    result={'at':run.now(),'source_binding':str(binding),'source_volume':saved['bundle_volume'],
        'volume':data,'archive_sha256':saved['recovery_archive_sha256'],'verified':True,
        'model_calls':0,**observed}
    util.write_new_json(root/'result.json',result);return result


def case_status(cases,expected):
    """Exact private identities are required; implementation-specific units are separate."""
    ids=[c['id'] for c in cases]
    if len(ids)!=len(set(ids)) or set(ids)!=set(expected) or not expected:return 'unknown'
    if any(c['status'] not in ('passed','failed') for c in cases):return 'unknown'
    return 'failed' if any(c['status']=='failed' for c in cases) else 'passed'


def quality(build,api,database,stop_confirmed):
    if not stop_confirmed:return 'unknown'
    if build!='passed':return build
    if 'failed' in (api,database):return 'failed'
    if api==database=='passed':return 'passed'
    return 'unknown'


def completed_successfully(observed):
    state=observed.get('state',{})
    return (state.get('ExitCode')==0 and state.get('Running') is False
            and state.get('OOMKilled') is False and not observed.get('environment_failure',False))


def db_input(spec,java_record,output):
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    descriptor=spec['database'];script=Path(descriptor['script'])
    if util.sha256_file(script)!=descriptor['script_sha256']:raise ValueError('DB evaluator changed')
    result=util.read_json(Path(java_record)/'result.json')
    directory=Path(result['evidence_directory'])
    if os.name=='nt':directory=Path('\\\\?\\'+str(directory))
    source=directory/'api-fixtures.jsonl'
    rows=[] if not source.is_file() else [json.loads(line) for line in source.read_text(encoding='utf-8').splitlines()]
    expected=descriptor['expected_fixture_counts']
    rows=[row for row in rows if row['kind'] in expected]
    shutil.copyfile(script,output/'check.py')
    util.write_new_json(output/'fixture-contract.json',{'expected_counts':expected})
    (output/'fixtures.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows),encoding='utf-8')
    util.write_new_json(output/'provenance.json',{'api_result_sha256':util.sha256_file(Path(java_record)/'result.json'),
        'api_fixture_sha256':util.sha256_file(source) if source.is_file() else None,'task':spec['task']})
    return output


def evaluate(settings,task,run_record):
    deadline=settings.get('evaluation_deadline',time.time()+3600)
    def remaining(limit):return max(0,min(limit,deadline-time.time()))
    run_record=Path(run_record).resolve();release_path,release,specs=release_specs(settings['evaluator_release'])
    if util.sha256_file(release_path)!=settings['evaluator_sha256']:raise ValueError('Evaluator release changed')
    spec_path,spec=specs[task]
    submission=run_record/'submission.tar';artifact=util.read_json(run_record/'artifact.json')
    if util.sha256_file(submission)!=artifact['sha256']:raise ValueError('Saved submission changed')
    root=run_record/'evaluations'/uuid.uuid4().hex;root.mkdir(parents=True)
    request={'at':run.now(),'task':task,'submission_sha256':artifact['sha256'],
             'execution_spec_sha256':util.sha256_file(spec_path),'evaluator_sha256':settings['evaluator_sha256'],
             'model_calls':0}
    util.write_new_json(root/'request.json',request)
    result={**request,'build':'unknown','api':'unknown','database':'unknown',
            'quality':'unknown','service_stop_confirmed':True,'records':{}}
    service_record=None
    try:
        private=release_path.parent/'private'
        for rel,entry in release['private_files'].items():
            if rel.startswith('private/') and util.sha256_file(release_path.parent/rel)!=entry['sha256']:
                raise ValueError('Diagnostic Java bundle changed')
        start=evaluation.begin(settings['preparation'],settings['seed'],private,submission)
        build=Path(start['record']);result['records']['build']=str(build)
        wait_for(evaluation.collect,build,remaining(1900))
        built=util.read_json(build/'result.json')
        result['diagnostic_units']={k:built['junit'][k] for k in ('passed','failed','skipped')}
        if built['environment_failure']:result['build']='unknown';return persist(root,result)
        if not any(g['id']=='bootJar' and g['exit_code']==0 for g in (built.get('stage') or {}).get('groups',[])):
            result['build']='build_failed';return persist(root,result)
        result['build']='passed'
        start=service.start_evaluation(build);service_record=Path(start['record'])
        result['records']['service']=str(service_record)
        health_deadline=time.monotonic()+remaining(240)
        while service.inspect(service_record)['health'].get('http_status')!=200:
            if time.monotonic()>=health_deadline:raise TimeoutError('Application readiness deadline')
            time.sleep(3)
        if 'java_api' in spec:
            api=spec['java_api'];harness=Path(api['harness_record'])
            if util.sha256_file(harness/'result.json')!=api['harness_result_sha256']:raise ValueError('API harness changed')
            started=java_api.begin(harness,service_record,task)
            record=Path(started['record']);result['records']['api']=str(record)
            wait_for(java_api.collect,record,remaining(1300))
            observed=util.read_json(record/'result.json')
            expected=[(c['suite'],c['case']) for c in api['expected_cases']]
            cases=[{'id':(c['suite'],c['case']),'status':c['status']} for c in observed['junit']['cases']]
            result['api']=case_status(cases,expected)
            if observed['environment_failure'] or (result['api']=='passed' and not completed_successfully(observed)):
                result['api']='unknown'
            inputs=db_input(spec,record,root/'database-input')
            started=api_check.begin(service_record,inputs,settings['api_image'],'verify')
            db_record=Path(started['record']);result['records']['database']=str(db_record)
            db=wait_for(api_check.collect,db_record,remaining(360));result['database']=db['status']
            if not completed_successfully(db):result['database']='unknown'
            result['database_counts']=db['counts']
        else:
            api=spec['http_database'];private=Path(api['private_bundle'])
            if util.tree_hashes(private)!=api['files']:raise ValueError('HTTP/DB evaluator bundle changed')
            started=api_check.begin(service_record,private,api['image_id'],'verify')
            record=Path(started['record']);result['records']['api_database']=str(record)
            collected=wait_for(api_check.collect,record,remaining(360))
            if collected['status']!='unknown' and completed_successfully(collected):
                body=util.read_json(record/'fresh-evidence/result.json')
                result['api']=case_status(body['checks'],api['expected_cases'])
                db=[c for c in body['checks'] if c['id']=='C11-db-nonmutation']
                result['database']=case_status(db,['C11-db-nonmutation'])
            result['api_counts']=collected['counts']
    except (runtime.DockerCommandError,TimeoutError) as error:
        result['environment_error_type']=type(error).__name__
    finally:
        if service_record is not None:
            try:result['service_stop_confirmed']=service.stop(service_record)['stop_confirmed']
            except Exception as error:
                result['service_stop_confirmed']=False;result['stop_error_type']=type(error).__name__
    return persist(root,result)


def persist(root,result):
    result['quality']=quality(result['build'],result['api'],result['database'],result['service_stop_confirmed'])
    result['ended_at']=run.now();util.write_new_json(root/'result.json',result)
    return {'record':str(root),'result_sha256':util.sha256_file(root/'result.json'),
            'quality':result['quality'],'task':result['task'],'model_calls':0}


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    q=sub.add_parser('bind')
    for name in ('preparation','release','output'):q.add_argument('--'+name,required=True,type=Path)
    q=sub.add_parser('evaluate')
    for name in ('settings','run'):q.add_argument('--'+name,required=True,type=Path)
    q.add_argument('--task',required=True,choices=TASKS)
    q=sub.add_parser('restore-bundle');q.add_argument('--binding',required=True,type=Path);q.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    result=bind(a.preparation,a.release,a.output) if a.command=='bind' else restore_bundle(a.binding,a.output) if a.command=='restore-bundle' else evaluate(util.read_json(a.settings),a.task,a.run)
    print(json.dumps(result))
