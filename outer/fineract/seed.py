"""Retained dependency-only seed and four independent offline Java probes.

No model or credential entry point exists here. Generated Gradle build outputs,
daemon state and build caches never enter the seed.
"""
import argparse
import json
from pathlib import Path
import uuid

from outer.harness import runtime, util, run
from .network import create_internal


def volume(name, label, image):
    runtime.docker('volume','create','--label',label,name)
    runtime.docker('run','--rm','--network','none','--user','0:0','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--mount',f'type=volume,source={name},target=/owned',
        image,'chmod','0777','/owned')


def create(preparation):
    prep_root=Path(preparation).resolve();prep=util.read_json(prep_root/'result.json')
    if not prep['success']:raise ValueError('Successful baseline build required')
    ident=uuid.uuid4().hex;root=prep_root/'seeds'/ident;root.mkdir(parents=True)
    name='fe-seed-'+ident;image=prep['java_image_id']
    receipt={'kind':'fineract-dependency-seed-v1','id':ident,'at':run.now(),
        'preparation_id':prep['id'],'volume':name,'java_image_id':image,
        'includes':['wrapper/dists','caches/modules-2'],
        'excludes':['build-cache','project outputs','transforms','daemon state','lock files'],
        'model_calls':0}
    util.write_new_json(root/'intent.json',receipt)
    volume(name,'modernization.seed='+ident,image)
    script=r'''import hashlib,json,shutil
from pathlib import Path
src=Path('/baseline/gradle');dest=Path('/seed');inventory={}
for section in ('wrapper/dists','caches/modules-2'):
 for p in sorted((src/section).rglob('*')):
  if p.is_symlink():raise ValueError('Dependency symlink forbidden')
  if not p.is_file() or p.suffix in ('.lock','.lck','.part'):continue
  rel=p.relative_to(src);target=dest/rel;target.parent.mkdir(parents=True,exist_ok=True)
  if target.exists():raise ValueError('Fresh seed required')
  shutil.copy2(p,target)
  target.chmod(0o755 if p.stat().st_mode&0o111 else 0o644)
  inventory[rel.as_posix()]={'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest()}
raw=json.dumps(inventory,sort_keys=True,separators=(',',':')).encode()
(dest/'inventory.json').write_bytes(raw)
print(json.dumps({'inventory_sha256':hashlib.sha256(raw).hexdigest(),'files':len(inventory),
 'bytes':sum(i['bytes'] for i in inventory.values())}))
'''
    output=runtime.docker('run','--rm','--network','none','--user','0:0',
        '--cap-drop=ALL','--security-opt=no-new-privileges',
        '--mount',f'type=volume,source={prep["volume"]},target=/baseline,readonly',
        '--mount',f'type=volume,source={name},target=/seed',image,'python3','-c',script,timeout=300)
    receipt.update(json.loads(output.stdout))
    util.write_new_json(root/'result.json',receipt)
    return {'record':str(root),**receipt}


def probe_four(preparation, seed, worker_image):
    prep_root=Path(preparation).resolve();prep=util.read_json(prep_root/'result.json')
    seed=util.read_json(Path(seed)/'result.json');image=runtime.image_id(worker_image)
    if seed['preparation_id']!=prep['id']:raise ValueError('Seed/preparation binding mismatch')
    ident=uuid.uuid4().hex;root=prep_root/'isolation'/ident;root.mkdir(parents=True)
    receipt={'kind':'fineract-four-worker-probe-v1','id':ident,'at':run.now(),
        'worker_image_id':image,'seed':seed,'workers':[],'model_calls':0}
    util.write_new_json(root/'intent.json',receipt)
    # Use verified archive, not the baseline build's generated source tree.
    archive=prep_root.parent/'source.tar.gz'
    if util.sha256_file(archive)!=prep['archive_sha256']:raise ValueError('Archive changed')
    initialize=r'''import hashlib,json,shutil,tarfile,os
from pathlib import Path
seed=Path('/seed');inventory=json.loads((seed/'inventory.json').read_text())
for rel,entry in inventory.items():
 p=seed/rel
 if hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Seed changed')
 dst=Path('/owned/state/gradle')/rel;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dst)
dest=Path('/owned/workspace/source');dest.mkdir(parents=True)
with tarfile.open('/input/source.tar.gz') as archive:
 for m in archive:
  if m.issym() or m.islnk() or m.isdev():raise ValueError('Archive links forbidden')
  parts=Path(m.name).parts
  if '..' in parts:raise ValueError('Archive path invalid')
  if len(parts)<2:continue
  m.name=Path(*parts[1:]).as_posix();archive.extract(m,dest,filter='data')
Path('/owned/state/home').mkdir(parents=True)
print(json.dumps({'seed_verified':True,'git_absent':not (dest/'.git').exists(),
 'build_outputs_absent':not any(p.name=='build' for p in dest.rglob('*') if p.is_dir())}))
'''
    probe=r'''import json,os,socket,subprocess
from pathlib import Path
def blocked(host):
 try:
  with socket.create_connection((host,443),timeout=2):return False
 except OSError:return True
result={'non_root':os.getuid()!=0,'credential_absent':'OPENCODE_GO_API_KEY' not in os.environ,
 'internet_blocked':blocked('1.1.1.1'),'provider_blocked':blocked('opencode.ai'),
 'management_absent':all(not Path(p).exists() for p in ['/evaluator','/records','/var/run/docker.sock','/input']),
 'source_present':Path('gradlew').is_file()}
Path('/state/unique-marker').write_text(os.environ['PROBE_ID'])
result['gradle_exit']=subprocess.call(['timeout','600','fineract-gradle','--offline','--no-build-cache','help'])
result['verified']=all(v is True for k,v in result.items() if k!='gradle_exit') and result['gradle_exit']==0
Path('/state/probe.json').write_text(json.dumps(result))
print(json.dumps(result));raise SystemExit(0 if result['verified'] else 1)
'''
    for i in range(4):
        names={k:f'fe-probe-{k}-{ident}-{i}' for k in ('data','network','worker')}
        label='modernization.probe='+ident
        volume(names['data'],label,image)
        create_internal(names['network'],['--label',label])
        output=runtime.docker('run','--rm','--network','none','--user','1000:1000',
            '--cap-drop=ALL','--security-opt=no-new-privileges',
            '--mount',f'type=volume,source={seed["volume"]},target=/seed,readonly',
            '--mount',f'type=volume,source={names["data"]},target=/owned',
            '--mount',f'type=bind,source={archive},target=/input/source.tar.gz,readonly',
            image,'python3','-c',initialize,timeout=300)
        receipt['workers'].append({'index':i,'names':names,'initialized':json.loads(output.stdout)})
    # All four copies exist before any Gradle starts. Each container has a unique
    # network, volume, HOME and cache; no seed mount remains during execution.
    for row in receipt['workers']:
        names=row['names']
        runtime.docker('run','-d','--name',names['worker'],'--label','modernization.probe='+ident,
            '--network',names['network'],'--read-only','--cap-drop=ALL',
            '--security-opt=no-new-privileges','--memory=5g','--cpus=3','--pids-limit=512',
            '--tmpfs','/tmp:rw,exec,size=1g','-e','PROBE_ID='+str(row['index']),
            '--mount',f'type=volume,source={names["data"]},target=/workspace,volume-subpath=workspace',
            '--mount',f'type=volume,source={names["data"]},target=/state,volume-subpath=state',
            '-w','/workspace/source',image,'python3','-c',probe)
    util.write_new_json(root/'started.json',receipt)
    return {'record':str(root),'containers':[x['names']['worker'] for x in receipt['workers']]}


def collect(record):
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    if (root/'result.json').exists():return util.read_json(root/'result.json')
    rows=[]
    for row in data['workers']:
        name=row['names']['worker'];item=json.loads(runtime.docker('inspect',name).stdout)[0]
        if item['Config']['Labels'].get('modernization.probe')!=data['id']:raise ValueError('Ownership mismatch')
        rows.append({**row,'state':item['State']})
    if any(x['state']['Running'] for x in rows):return {'running':True,'workers':rows}
    for row in rows:
        logs=runtime.docker('logs',row['names']['worker'],check=False)
        (root/(str(row['index'])+'.stdout.log')).write_text(logs.stdout,encoding='utf-8')
        (root/(str(row['index'])+'.stderr.log')).write_text(logs.stderr,encoding='utf-8')
        try:row['probe']=json.loads(logs.stdout.splitlines()[-1])
        except (ValueError,IndexError):row['probe']={'verified':False,'missing_result':True}
    result={**data,'workers':rows,'running':False,'ended_at':run.now(),
        'success':all(r['probe']['verified'] and r['state']['ExitCode']==0 and not r['state']['OOMKilled'] for r in rows)}
    util.write_new_json(root/'result.json',result)
    return result


def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    s=sub.add_parser('create');s.add_argument('--preparation',required=True,type=Path)
    s=sub.add_parser('probe-four');s.add_argument('--preparation',required=True,type=Path)
    s.add_argument('--seed',required=True,type=Path);s.add_argument('--worker-image',required=True)
    sub.add_parser('collect').add_argument('--record',required=True,type=Path)
    a=p.parse_args()
    result=create(a.preparation) if a.command=='create' else collect(a.record) if a.command=='collect' else probe_four(a.preparation,a.seed,a.worker_image)
    print(json.dumps(result))


if __name__=='__main__':main()
