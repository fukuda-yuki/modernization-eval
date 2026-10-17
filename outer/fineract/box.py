"""Owned Fineract Run boxes. Keys enter only a live gateway's stdin.

Allocation and stopped artifacts survive crashes. No resource is deleted here.
Synthetic transport has no outbound network and never reads a provider key.
"""
import json
from pathlib import Path
import subprocess
import tarfile
import time

from outer.harness import runtime, util, run
from outer.harness.security import child_environment
from .network import create_internal
from .prepare import POSTGRES
from .seed import volume


INITIALIZE=r'''import hashlib,json,shutil,re
from pathlib import Path
root=Path('/bundle');manifest=json.loads((root/'manifest.json').read_text())
request=json.loads(Path('/control/box.json').read_text())
if hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest()!=request['bundle_file_sha256']:raise ValueError('Bundle changed')
entries={x['path']:x for x in manifest['output_inventory']}
bundle_files=len(entries)
if any(p.is_symlink() for p in root.rglob('*')):raise ValueError('Bundle links forbidden')
actual={p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() and p.name!='manifest.json'}
if actual!=set(entries):raise ValueError('Bundle file inventory differs')
for rel,entry in entries.items():
 p=root/rel
 if p.is_symlink() or hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Bundle content differs')
workspace=Path('/owned/workspace');workspace.mkdir()
shutil.copytree(root/'corpus',workspace,dirs_exist_ok=True)
for entry in manifest['semantic_inventory']:
 (workspace/entry['path']).chmod(0o755 if entry['executable'] else 0o644)
shutil.copyfile(root/'tasks'/request['assignment']['task']/'request.md',workspace/'request.md')
if request['assignment']['material_binding']['navigation_sha256'] is not None and request['assignment']['condition'].endswith('-indexed'):
 shutil.copytree(root/'navigation',workspace/'navigation')
state=Path('/owned/state');state.mkdir();(state/'home').mkdir()
shutil.copyfile('/control/initial.md',state/'initial.md')
shutil.copyfile('/control/opencode.json',state/'opencode.json')
inventory=json.loads(Path('/seed/inventory.json').read_text())
if hashlib.sha256(Path('/seed/inventory.json').read_bytes()).hexdigest()!=request['seed_inventory_sha256']:raise ValueError('Seed changed')
for rel,entry in inventory.items():
 p=Path('/seed')/rel
 if hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Seed content changed')
 dest=state/'gradle'/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest)
properties=(workspace/'source/gradle.properties').read_text()
jvm=re.search(r'^org\.gradle\.jvmargs=(.*)$',properties,re.M).group(1)
jvm=re.sub(r'-Xmx\S+','-Xmx2048m',jvm)
(state/'gradle/gradle.properties').write_text('org.gradle.jvmargs='+jvm+'\norg.gradle.workers.max=1\norg.gradle.parallel=false\norg.gradle.daemon=false\norg.gradle.vfs.watch=false\n')
init=state/'gradle/init.d';init.mkdir(parents=True,exist_ok=True)
shutil.copyfile('/opt/fineract/limits.gradle',init/'00-bounded-tests.gradle')
if request.get('sdk_inventory_sha256'):
 source=Path('/public-sdk');raw=(source/'inventory.json').read_bytes()
 if hashlib.sha256(raw).hexdigest()!=request['sdk_inventory_sha256']:raise ValueError('Public SDK inventory changed')
 entries=json.loads(raw)
 if {p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()}!=set(entries)|{'inventory.json'}:raise ValueError('Public SDK extra/missing files')
 for rel,entry in entries.items():
  p=source/rel
  if p.is_symlink() or hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Public SDK changed')
 shutil.copytree(source,state/'public-sdk')
 shutil.copyfile('/opt/fineract/public-sdk.gradle',init/'01-public-sdk.gradle')
print(json.dumps({'files_verified':bundle_files,'seed_files':len(inventory),'build_outputs_present':any(p.name=='build' for p in (workspace/'source').rglob('*') if p.is_dir())}))
'''


PROBE=r'''import json,os,socket,subprocess,time,urllib.request
from pathlib import Path
def blocked(host,port):
 try:
  with socket.create_connection((host,port),timeout=2):return False
 except OSError:return True
ident=os.environ['FINERACT_RUN_UUID']
env={k:v for line in Path('/workspace/source/config/docker/env/postgresql.env').read_text().splitlines() if line and not line.startswith('#') and '=' in line for k,v in [line.split('=',1)]}
java='''+repr('''import java.sql.*;
public class Probe {
 public static void main(String[] args) throws Exception {
  try(var c=DriverManager.getConnection("jdbc:postgresql://db:5432/postgres",System.getenv("PROBE_DB_USER"),System.getenv("PROBE_DB_PASSWORD"));var s=c.createStatement()) {
   s.execute("CREATE TABLE acceptance_marker (id text PRIMARY KEY)");
   try(var p=c.prepareStatement("INSERT INTO acceptance_marker VALUES (?)")){p.setString(1,System.getenv("FINERACT_RUN_UUID"));p.executeUpdate();}
   try(var r=s.executeQuery("SELECT id FROM acceptance_marker")){if(!r.next() || !r.getString(1).equals(System.getenv("FINERACT_RUN_UUID")) || r.next())throw new IllegalStateException("DB marker mismatch");}
  }
 }
}''')+r'''
Path('/state/Probe.java').write_text(java)
jars=list(Path('/state/gradle/caches/modules-2/files-2.1/org.postgresql/postgresql').rglob('*.jar'))
if len(jars)!=1:raise ValueError('One pinned public JDBC dependency required')
db=subprocess.run(['java','--class-path',str(jars[0]),'/state/Probe.java'],env={**os.environ,'PROBE_DB_USER':env['POSTGRES_USER'],'PROBE_DB_PASSWORD':env['POSTGRES_PASSWORD']},capture_output=True,timeout=60)
body={'model':'deepseek-v4.1-flash','stream':True,'messages':[{'role':'user','content':Path('/state/initial.md').read_text()}]}
req=urllib.request.Request('http://gateway:8080/v1/chat/completions',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
with urllib.request.urlopen(req,timeout=30) as response:raw=response.read()
result={'id':ident,'non_root':os.getuid()!=0,'database_marker_verified':db.returncode==0,
 'gateway_synthetic_response':b'synthetic probe' in raw and b'[DONE]' in raw,
 'internet_blocked':blocked('1.1.1.1',443),'provider_blocked':blocked('opencode.ai',443),
 'credential_absent':'OPENCODE_GO_API_KEY' not in os.environ,
 'management_absent':all(not Path(p).exists() for p in ['/records','/budget','/private','/var/run/docker.sock']),
 'other_database_blocked':all(blocked(ip,5432) for ip in json.loads(os.environ['PROBE_OTHER_DATABASES'])),
 'other_gateway_blocked':all(blocked(ip,8080) for ip in json.loads(os.environ['PROBE_OTHER_GATEWAYS']))}
result['verified']=all(v is True for k,v in result.items() if k!='id')
Path('/state/probe.json').write_text(json.dumps(result))
while not Path('/state/probe-release').exists():time.sleep(1)
'''


def owned(data,role):
    item=runtime.inspect_container(data['names'][role])
    if item and item['Config']['Labels'].get('modernization.run')!=data['id']:
        raise ValueError('Run resource ownership mismatch')
    return item


def budget_action(settings,action,arguments=None):
    script="""import json,sys
from campaign_budget import Budget
action=sys.argv[1];args=json.loads(sys.argv[2]);path='/budget/admission.sqlite'
if action=='create': Budget.create(path,**args)
elif action=='close': Budget(path).close(args['reason'])
print(json.dumps(Budget(path).snapshot()))
"""
    return json.loads(runtime.docker('run','--rm','--network','none','--read-only',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--entrypoint','python3','-e','PYTHONPATH=/app',
        '--mount',f'type=volume,source={settings["budget_volume"]},target=/budget',
        settings['gateway_image'],'-c',script,action,json.dumps(arguments or {})).stdout)


def allocate(root,settings,assignment,ident):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=False)
    prep=util.read_json(Path(settings['preparation'])/'result.json')
    seed=util.read_json(Path(settings['seed'])/'result.json')
    bundle=Path(settings['bundle']);manifest=util.read_json(bundle/'manifest.json')
    if seed['preparation_id']!=prep['id']:raise ValueError('Seed/preparation mismatch')
    row=next(r for r in manifest['conditions'] if (r['task'],r['condition'])==(assignment['task'],assignment['condition']))
    if any(row[k]!=v for k,v in assignment['material_binding'].items()):raise ValueError('Assignment changed')
    initial=(bundle/row['initial_path']).read_bytes()
    if util.sha256_bytes(initial)!=row['initial_sha256']:raise ValueError('Initial input changed')
    names={k:'fe-run-'+k+'-'+ident for k in ('worker','gateway','database','network','data','records','dbdata')}
    data={'id':ident,'assignment':assignment,'names':names,'allocated_at':run.now(),
          'bundle_file_sha256':util.sha256_file(bundle/'manifest.json'),
          'seed_inventory_sha256':seed['inventory_sha256'],'synthetic':settings['synthetic'],
          'worker_image':settings['worker_image'],'gateway_image':settings['gateway_image']}
    sdk=util.read_json(Path(settings['public_sdk'])/'result.json') if settings.get('public_sdk') else None
    if sdk and (not sdk['success'] or sdk['private_inputs'] is not False or sdk['archive_sha256']!=prep['archive_sha256']):
        raise ValueError('Successful independent public-only SDK required')
    if sdk:data['sdk_inventory_sha256']=sdk['inventory_sha256']
    util.write_new_json(root/'box.json',data)
    (root/'initial.md').write_bytes(initial)
    util.write_new_json(root/'opencode.json',runtime.opencode_config({'runtime':settings['agent']}))
    labels=['--label','modernization.run='+ident]
    create_internal(names['network'],labels)
    for key in ('data','records'):
        volume(names[key],'modernization.run='+ident,settings['worker_image'])
    bundle_mount=(f'type=volume,source={settings["bundle_volume"]},target=/bundle,volume-subpath=bundle,readonly'
                  if settings.get('bundle_volume') else f'type=bind,source={bundle.resolve()},target=/bundle,readonly')
    sdk_mounts=['--mount',f'type=volume,source={sdk["volume"]},target=/public-sdk,volume-subpath=export,readonly'] if sdk else []
    initialized=runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=1g','--cpus=2',
        '--mount',f'type=volume,source={names["data"]},target=/owned',
        '--mount',f'type=volume,source={seed["volume"]},target=/seed,readonly',
        '--mount',bundle_mount,
        '--mount',f'type=bind,source={root},target=/control,readonly',
        *sdk_mounts,settings['worker_image'],'python3','-c',INITIALIZE,timeout=600)
    util.write_new_json(root/'initialized.json',json.loads(initialized.stdout))
    archive=Path(settings['preparation']).parent/'source.tar.gz'
    if util.sha256_file(archive)!=prep['archive_sha256']:raise ValueError('Archive changed')
    with tarfile.open(archive) as tar:
        raw=tar.extractfile('fineract-'+prep['source_commit']+'/config/docker/env/postgresql.env').read()
        app={}
        for name in ('fineract-common.env','fineract.env','fineract-postgresql.env'):
            text=tar.extractfile('fineract-'+prep['source_commit']+'/config/docker/env/'+name).read().decode()
            app.update({k:v for line in text.splitlines() if line and not line.startswith('#') and '=' in line
                        for k,v in [line.split('=',1)]})
        app.pop('JAVA_TOOL_OPTIONS',None)
    (root/'database.env').write_bytes(raw)
    (root/'application.env').write_text(''.join(k+'='+v+'\n' for k,v in app.items()),encoding='utf-8')
    runtime.docker('volume','create',*labels,names['dbdata'])
    runtime.docker('run','-d','--name',names['database'],*labels,'--network',names['network'],
        '--network-alias','db','--memory=1g','--cpus=1','--pids-limit=256',
        '--security-opt=no-new-privileges','--env-file',root/'database.env',
        '--mount',f'type=volume,source={names["dbdata"]},target=/var/lib/postgresql',
        '--mount',f'type=volume,source={prep["volume"]},target=/docker-entrypoint-initdb.d,volume-subpath=source/config/docker/postgresql/docker-entrypoint-initdb.d,readonly',POSTGRES)
    args=['create','-i','--name',names['gateway'],*labels,'--network',names['network'],
          '--network-alias','gateway','--read-only','--cap-drop=ALL','--security-opt=no-new-privileges',
          '--memory=512m','--cpus=1','--pids-limit=128',
          '--mount',f'type=volume,source={names["records"]},target=/records',
          '--mount',f'type=volume,source={settings["budget_volume"]},target=/budget',
          '--mount',f'type=bind,source={root / "initial.md"},target=/contract/initial.md,readonly']
    if settings['synthetic']:
        args+=['--entrypoint','python3',settings['gateway_image'],'/app/probe_gateway.py']
    else:args+=[settings['gateway_image'],'--model','deepseek-v4.1-flash',
               '--upstream-timeout','600','--upstream-deadline','600']
    args+=['--run-id',ident,'--campaign-budget','/budget/admission.sqlite','--expected-prompt','/contract/initial.md']
    runtime.docker(*args)
    util.write_new_json(root/'allocated.json',data)
    return data


def start_gateway(root,settings):
    root=Path(root);data=util.read_json(root/'box.json');owned(data,'gateway')
    if settings['synthetic']:
        runtime.docker('start',data['names']['gateway'])
        process=None
    else:
        runtime.docker('network','connect','bridge',data['names']['gateway'])
        log=(root/'gateway-attach.log').open('xb')
        process=subprocess.Popen(['docker','start','-ai',data['names']['gateway']],
            env=child_environment(),stdin=subprocess.PIPE,stdout=log,stderr=subprocess.STDOUT)
        try:
            process.stdin.write((runtime._gateway_credential()+'\n').encode())
            process.stdin.flush();process.stdin.close()
        finally:log.close()
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        result=runtime.docker('exec',data['names']['gateway'],'python3','-c',
            "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=2).read()",check=False,timeout=5)
        db=runtime.docker('exec',data['names']['database'],'pg_isready',check=False,timeout=5)
        if result.returncode==0 and db.returncode==0:return process
        time.sleep(1)
    raise TimeoutError('Owned gateway/database startup failed')


def launch(root,settings,peers=(),*,public_build_probe=None):
    root=Path(root);data=util.read_json(root/'box.json');ident=data['id'];names=data['names']
    peer_ips={'database':[],'gateway':[]}
    for peer in peers:
        if peer['id']==ident:continue
        for role in peer_ips:
            item=owned(peer,role)
            peer_ips[role].extend(n['IPAddress'] for n in item['NetworkSettings']['Networks'].values())
    args=['run','-d','--name',names['worker'],'--label','modernization.run='+ident,
          '--network',names['network'],'--read-only','--cap-drop=ALL','--security-opt=no-new-privileges',
          '--memory=5g','--cpus=3','--pids-limit=512','--tmpfs','/tmp:rw,exec,size=1g',
          '--env-file',root/'application.env',
          '--mount',f'type=volume,source={names["data"]},target=/workspace,volume-subpath=workspace',
          '--mount',f'type=volume,source={names["data"]},target=/state,volume-subpath=state',
          '-w','/workspace','-e','OPENCODE_CONFIG=/state/opencode.json','-e','FINERACT_RUN_UUID='+ident]
    if settings['synthetic']:
        args+=['-e','PROBE_OTHER_DATABASES='+json.dumps(peer_ips['database']),
               '-e','PROBE_OTHER_GATEWAYS='+json.dumps(peer_ips['gateway']),settings['worker_image'],
               'timeout','1800' if public_build_probe else '300','python3','-c',public_build_probe or PROBE]
    else:
        script="import os; p=open('/state/initial.md','rb');os.dup2(p.fileno(),0);os.execvp('opencode',['opencode','run','--pure','--format','json','--title',os.environ['FINERACT_RUN_UUID'],'--model','sample2/deepseek-v4.1-flash'])"
        args+=[settings['worker_image'],'timeout','1800','python3','-c',script]
    util.write_new_json(root/'launch-intent.json',{'at':run.now(),'epoch':time.time(),'id':ident})
    runtime.docker(*args)
    util.write_new_json(root/'launched.json',{'at':run.now(),'epoch':time.time(),'id':ident})


def poll(root):
    root=Path(root);data=util.read_json(root/'box.json');item=owned(data,'worker')
    if not item:return {'finished':False,'missing':True}
    result={'finished':not item['State']['Running'],'state':item['State']}
    if data['synthetic'] and item['State']['Running']:
        probe=runtime.docker('exec',data['names']['worker'],'cat','/state/probe.json',check=False)
        if probe.returncode==0:result.update(finished=True,probe=json.loads(probe.stdout))
    return result


def stop(root,reason,*,evidence=None):
    root=Path(root);data=util.read_json(root/'box.json');evidence=Path(evidence) if evidence is not None else root/'stop'
    evidence.mkdir(parents=True,exist_ok=True)
    if (evidence/'result.json').exists():return util.read_json(evidence/'result.json')
    gateway=owned(data,'gateway');fenced=False
    if gateway and gateway['State']['Running']:
        body=json.dumps({'run_id':data['id'],'session_id':data['id']})
        script="import urllib.request,sys;print(urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8080/control/stop-admission',data=sys.argv[1].encode(),headers={'Content-Type':'application/json'}),timeout=5).read().decode())"
        ack=runtime.docker('exec',data['names']['gateway'],'python3','-c',script,body,check=False,timeout=10)
        if ack.returncode==0:
            util.write_new_json(evidence/'gateway-fence.json',json.loads(ack.stdout));fenced=True
    roles=('worker','gateway','database') if fenced else ('gateway','worker','database')
    states={}
    for role in roles:
        item=owned(data,role)
        if item:
            if item['State']['Running']:runtime.docker('stop','--time','10',data['names'][role],timeout=40)
            item=owned(data,role);states[role]=item['State']
            logs=runtime.docker('logs',data['names'][role],check=False)
            (evidence/(role+'.stdout.log')).write_text(logs.stdout,encoding='utf-8')
            (evidence/(role+'.stderr.log')).write_text(logs.stderr,encoding='utf-8')
        else:states[role]={'Running':False,'absent':True}
    result={'reason':reason,'at':run.now(),'states':states,'admission_ack':fenced,
            'stop_confirmed':all(not x['Running'] for x in states.values())}
    util.write_new_json(evidence/'result.json',result)
    return result


def collect(root,settings,*,stop_record=None):
    root=Path(root);data=util.read_json(root/'box.json')
    evidence=Path(stop_record) if stop_record is not None else root/'stop'
    if not util.read_json(evidence/'result.json')['stop_confirmed']:raise ValueError('Confirmed stop required')
    if (root/'artifact.json').exists():return util.read_json(root/'artifact.json')
    script="""import json,shutil
from pathlib import Path
from artifacts import pack_source
result=pack_source('/owned/workspace/source','/out/submission.tar')
Path('/out/artifact.json').write_text(json.dumps(result,sort_keys=True))
shutil.copytree('/records','/out/usage')
if Path('/owned/state/probe.json').exists():shutil.copyfile('/owned/state/probe.json','/out/probe.json')
"""
    runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=1g','--cpus=2',
        '-e','PYTHONPATH=/adapter','--mount',f'type=volume,source={data["names"]["data"]},target=/owned,readonly',
        '--mount',f'type=volume,source={data["names"]["records"]},target=/records,readonly',
        '--mount',f'type=bind,source={Path(__file__).parent},target=/adapter,readonly',
        '--mount',f'type=bind,source={root},target=/out',settings['worker_image'],'python3','-c',script,timeout=300)
    return util.read_json(root/'artifact.json')
