"""Frozen canonical integration-test harness, isolated from submitted code.

The preparation snapshot may contain compiled public SDK/fixtures. Each run
copies it into a new volume, creates fresh reports and talks only to its owned
service. This snapshot is an evaluator dependency, never a worker cache seed.
"""
import argparse
import json
from pathlib import Path
import shutil
import uuid
import os
import xml.etree.ElementTree as ET

from outer.harness import runtime, util, run
from .artifacts import junit_result, relative_path
from .seed import volume
from .service import inspect


COPY = r'''import os,shutil
from pathlib import Path
for name in ('source','gradle'):
 shutil.copytree(Path('/baseline')/name,Path('/prep')/name,
   ignore=shutil.ignore_patterns('test-results','reports','daemon','*.lock','*.lck'))
for root,dirs,files in os.walk('/prep'):
 os.chown(root,1000,1000)
 for name in files:os.chown(Path(root)/name,1000,1000)
'''


def copy_volume(source, target, image):
    runtime.docker('run','--rm','--network','none','--user','0:0',
        '--security-opt=no-new-privileges','--memory=1g','--cpus=2',
        '--mount',f'type=volume,source={source},target=/baseline,readonly',
        '--mount',f'type=volume,source={target},target=/prep',
        image,'python3','-c',COPY,timeout=600)


def prepare(preparation, private):
    preparation=Path(preparation).resolve();private=Path(private).resolve()
    prep=util.read_json(preparation/'result.json');util.reject_links(private)
    if not prep['success']:raise ValueError('Successful pinned preparation required')
    spec=util.read_json(private/'spec.json')
    for item in spec['tests']:
        relative_path(item['file'])
        target=relative_path(item['target'])
        if not str(target).startswith('integration-tests/src/test/java/'):
            raise ValueError('Canonical integration-test target required')
    ident=uuid.uuid4().hex;root=preparation/'java-harnesses'/ident;root.mkdir(parents=True)
    shutil.copytree(private,root/'private')
    name='fe-java-harness-'+ident;data='fe-java-harness-data-'+ident
    receipt={'kind':'fineract-frozen-java-api-v1','id':ident,'at':run.now(),
        'preparation_id':prep['id'],'container':name,'volume':data,
        'java_image_id':prep['java_image_id'],'private_files':util.tree_hashes(root/'private'),
        'expected_suites':spec['expected_suites'],'expected_cases':spec.get('expected_cases'),
        'selectors':spec['selectors'],'tasks':spec.get('tasks',{}),'model_calls':0}
    util.write_new_json(root/'request.json',receipt)
    volume(data,'modernization.java_harness='+ident,prep['java_image_id'])
    copy_volume(prep['volume'],data,prep['java_image_id'])
    script=r'''import hashlib,json,shutil,subprocess,re
from pathlib import Path
private=Path('/private');spec=json.loads((private/'spec.json').read_text())
for item in spec['tests']:
 dest=Path('/prep/source')/item['target']
 if dest.exists():raise ValueError('Private test collides with canonical source')
 dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(private/item['file'],dest)
properties=Path('gradle.properties').read_text()
jvm=re.search(r'^org\.gradle\.jvmargs=(.*)$',properties,re.M).group(1)
jvm=re.sub(r'-Xmx\S+','-Xmx8g',jvm)
status=subprocess.call(['fineract-gradle','--offline','-PcargoDisabled',
 '-Dorg.gradle.jvmargs='+jvm,':integration-tests:compileTestJava'])
Path('/prep/compile-result.json').write_text(json.dumps({'exit_code':status}))
raise SystemExit(status)
'''
    runtime.docker('run','-d','--name',name,'--label','modernization.java_harness='+ident,
        '--network','none','--read-only','--user','1000:1000','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--memory=12g','--cpus=4','--pids-limit=512',
        '--tmpfs','/tmp:rw,exec,size=1g','-e','HOME=/tmp','-e','GRADLE_USER_HOME=/prep/gradle',
        '--mount',f'type=volume,source={data},target=/prep',
        '--mount',f'type=bind,source={root / "private"},target=/private,readonly',
        '-w','/prep/source',prep['java_image_id'],'timeout','1200','python3','-c',script)
    util.write_new_json(root/'started.json',receipt)
    return {'record':str(root),'container':name,'model_calls':0}


def collect_preparation(record):
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    if (root/'result.json').exists():return util.read_json(root/'result.json')
    item=json.loads(runtime.docker('inspect',data['container']).stdout)[0]
    if item['Config']['Labels'].get('modernization.java_harness')!=data['id']:
        raise ValueError('Harness ownership mismatch')
    if item['State']['Running']:return {'running':True,'record':str(root)}
    logs=runtime.docker('logs',data['container'],check=False)
    (root/'stdout.log').write_text(logs.stdout,encoding='utf-8')
    (root/'stderr.log').write_text(logs.stderr,encoding='utf-8')
    runtime.docker('cp',data['container']+':/prep/compile-result.json',root/'compile-result.json',check=False)
    compile_result=util.read_json(root/'compile-result.json') if (root/'compile-result.json').exists() else None
    result={**data,'state':item['State'],'running':False,'compile_result':compile_result,
        'success':bool(compile_result and compile_result['exit_code']==0
                       and item['State']['ExitCode']==0 and not item['State']['OOMKilled'])}
    if result['success']:
        inventory=snapshot(data['volume'],data['java_image_id'])
        util.write_new_json(root/'snapshot.json',inventory)
        result['snapshot_sha256']=util.sha256_file(root/'snapshot.json')
    util.write_new_json(root/'result.json',result)
    return result


def snapshot(data,image):
    script=r'''import hashlib,json
from pathlib import Path
inventory={}
for name in ('source','gradle'):
 for p in sorted((Path('/prep')/name).rglob('*')):
  rel=p.relative_to('/prep')
  if set(rel.parts)&{'test-results','reports','daemon'} or p.suffix in ('.lock','.lck'):continue
  if p.is_symlink():raise ValueError('Evaluator snapshot link forbidden')
  if not p.is_file():continue
  h=hashlib.sha256()
  with p.open('rb') as stream:
   while block:=stream.read(1024*1024):h.update(block)
  inventory[rel.as_posix()]={'sha256':h.hexdigest(),'bytes':p.stat().st_size}
print(json.dumps(inventory,sort_keys=True,separators=(',',':')))
'''
    return json.loads(runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=512m',
        '--mount',f'type=volume,source={data},target=/prep,readonly',
        image,'python3','-c',script,timeout=300).stdout)


def begin(harness, service, task=None):
    harness=Path(harness).resolve();service=Path(service).resolve()
    fixed=util.read_json(harness/'result.json');server=util.read_json(service/'started.json')
    if not fixed['success']:raise ValueError('Successful frozen API harness required')
    if fixed.get('snapshot_sha256')!=util.sha256_file(harness/'snapshot.json'):
        raise ValueError('Frozen evaluator dependency binding changed')
    selected=fixed if task is None else fixed.get('tasks',{}).get(task)
    if selected is None:raise ValueError('Task is absent from frozen evaluator binding')
    if inspect(service)['health'].get('http_status')!=200:raise ValueError('Healthy owned service required')
    ident=uuid.uuid4().hex;root=service/'java-api-checks'/ident;root.mkdir(parents=True)
    name='fe-java-api-'+ident;data='fe-java-api-data-'+ident
    receipt={'kind':'fineract-private-java-api-run-v1','id':ident,'at':run.now(),
        'harness_record':str(harness),'harness_result_sha256':util.sha256_file(harness/'result.json'),
        'service_record':str(service),'service_id':server['id'],'container':name,'volume':data,
        'java_image_id':fixed['java_image_id'],'task':task,'expected_suites':selected['expected_suites'],
        'expected_cases':selected.get('expected_cases'),'model_calls':0}
    util.write_new_json(root/'request.json',receipt)
    volume(data,'modernization.java_api='+ident,fixed['java_image_id'])
    copy_volume(fixed['volume'],data,fixed['java_image_id'])
    if snapshot(data,fixed['java_image_id'])!=util.read_json(harness/'snapshot.json'):
        raise ValueError('Fresh evaluator copy differs from frozen snapshot')
    args=[]
    for selector in selected['selectors']:args+=['--tests',selector]
    runtime.docker('run','-d','--name',name,'--label','modernization.java_api='+ident,
        '--network','container:'+server['names']['app'],'--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=6g','--cpus=4','--pids-limit=512',
        '--tmpfs','/tmp:rw,exec,size=1g','-e','HOME=/tmp','-e','GRADLE_USER_HOME=/prep/gradle',
        '-e','BACKEND_HOST=localhost','-e','FINERACT_TEST_EXTERNAL_HOST=localhost',
        '--mount',f'type=volume,source={data},target=/prep','-w','/prep/source',
        fixed['java_image_id'],'timeout','1200','fineract-gradle','--offline','-PcargoDisabled',
        ':integration-tests:test',*args)
    util.write_new_json(root/'started.json',receipt)
    return {'record':str(root),'container':name,'model_calls':0}


def collect(record):
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    if (root/'result.json').exists():return summary(root,util.read_json(root/'result.json'))
    item=json.loads(runtime.docker('inspect',data['container']).stdout)[0]
    if item['Config']['Labels'].get('modernization.java_api')!=data['id']:raise ValueError('Ownership mismatch')
    if item['State']['Running']:return {'running':True,'record':str(root)}
    logs=runtime.docker('logs',data['container'],check=False)
    (root/'stdout.log').write_text(logs.stdout,encoding='utf-8')
    (root/'stderr.log').write_text(logs.stderr,encoding='utf-8')
    output=root/'fresh-evidence'
    if output.exists():output=root/('recovery-'+uuid.uuid4().hex)
    # Extended local paths avoid changing the host's Windows long-path policy.
    readable=Path('\\\\?\\'+str(output)) if os.name=='nt' else output
    readable.mkdir()
    runtime.docker('cp',data['container']+':/prep/source/integration-tests/build/test-results/test/.',output,check=False)
    runtime.docker('cp',data['container']+':/prep/api-fixtures.jsonl',output/'api-fixtures.jsonl',check=False)
    collection_error=None
    try:junit=junit_result(readable,data['expected_suites'],data.get('expected_cases'))
    except (ValueError,ET.ParseError) as error:
        collection_error=type(error).__name__
        junit={'status':'incomplete','cases':[],'executed':None,'passed':None,'failed':None,'skipped':None}
    result={'at':run.now(),'state':item['State'],'junit':junit,'evidence_directory':str(output),
        'evidence_files':util.tree_hashes(readable),
        'collection_error':collection_error,
        'environment_failure':item['State']['OOMKilled'] or not junit['executed'],'model_calls':0}
    util.write_new_json(root/'result.json',result)
    return summary(root,result)


def summary(root,result):
    return {'record':str(root),'running':False,'state':result['state'],
        **{k:result['junit'][k] for k in ('status','executed','passed','failed','skipped')},
        'environment_failure':result['environment_failure'],'model_calls':0}


def seal_cases(harness, passing_run, task_map):
    """Bind the observed complete case inventory without replacing old evidence.

    This is a calibration metadata operation, not independent acceptance. The
    caller's final execution spec still binds this new receipt and review.
    """
    harness=Path(harness).resolve();passing_run=Path(passing_run).resolve()
    base=util.read_json(harness/'result.json')
    request=util.read_json(passing_run/'started.json');result=util.read_json(passing_run/'result.json')
    if (request['harness_result_sha256']!=util.sha256_file(harness/'result.json')
            or result['junit']['status']!='passed' or result['environment_failure']
            or result['state']['ExitCode']!=0):
        raise ValueError('Complete passing run of this exact harness required')
    cases=[{k:c[k] for k in ('suite','case')} for c in result['junit']['cases']]
    if not cases or len({(c['suite'],c['case']) for c in cases})!=len(cases):
        raise ValueError('Unique nonempty case inventory required')
    mapping=util.read_json(task_map);tasks={};seen=set()
    for task,item in mapping.items():
        suites=set(item['expected_suites'])
        if not suites or seen&suites or not suites.issubset(base['expected_suites']) or not item['selectors']:
            raise ValueError('Task suites must partition the complete harness')
        seen.update(suites)
        tasks[task]={**item,'expected_cases':[c for c in cases if c['suite'] in suites]}
    if seen!=set(base['expected_suites']):raise ValueError('Unassigned evaluator suite')
    ident=uuid.uuid4().hex;root=harness.parent/ident;root.mkdir()
    shutil.copytree(harness/'private',root/'private')
    shutil.copyfile(harness/'snapshot.json',root/'snapshot.json')
    sealed={**base,'id':ident,'at':run.now(),'expected_cases':cases,'tasks':tasks,
        'parent_harness_record':str(harness),'parent_harness_sha256':util.sha256_file(harness/'result.json'),
        'passing_calibration_record':str(passing_run),
        'passing_calibration_sha256':util.sha256_file(passing_run/'result.json'),
        'independent_review':'pending'}
    util.write_new_json(root/'result.json',sealed)
    return {'record':str(root),'expected_cases':len(cases),'tasks':sorted(tasks),'independent_review':'pending'}


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    s=sub.add_parser('prepare');s.add_argument('--preparation',required=True,type=Path);s.add_argument('--private',required=True,type=Path)
    s=sub.add_parser('begin');s.add_argument('--harness',required=True,type=Path);s.add_argument('--service',required=True,type=Path);s.add_argument('--task')
    for command in ('collect-preparation','collect'):sub.add_parser(command).add_argument('--record',required=True,type=Path)
    s=sub.add_parser('seal-cases')
    for name in ('harness','passing-run','task-map'):s.add_argument('--'+name,required=True,type=Path)
    a=p.parse_args()
    result=prepare(a.preparation,a.private) if a.command=='prepare' else begin(a.harness,a.service,a.task) if a.command=='begin' else seal_cases(a.harness,a.passing_run,a.task_map) if a.command=='seal-cases' else {
        'collect-preparation':collect_preparation,'collect':collect}[a.command](a.record)
    print(json.dumps(result))
