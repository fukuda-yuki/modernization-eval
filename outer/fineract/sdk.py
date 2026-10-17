"""Serial public-only SDK preparation. Never clones the private evaluator volume."""
import argparse
import json
from pathlib import Path
import uuid

from outer.harness import runtime,util,run
from .seed import volume


def begin(preparation,seed):
    preparation=Path(preparation).resolve();prep=util.read_json(preparation/'result.json')
    seed=util.read_json(Path(seed).resolve()/'result.json')
    if seed['preparation_id']!=prep['id']:raise ValueError('Public seed mismatch')
    ident=uuid.uuid4().hex;root=preparation/'public-sdk'/ident;root.mkdir(parents=True)
    name='fe-public-sdk-'+ident;data='fe-public-sdk-data-'+ident
    archive=preparation.parent/'source.tar.gz'
    if util.sha256_file(archive)!=prep['archive_sha256']:raise ValueError('Canonical public archive changed')
    receipt={'kind':'fineract-public-sdk-v1','id':ident,'at':run.now(),'container':name,'volume':data,
             'source_commit':prep['source_commit'],'archive_sha256':prep['archive_sha256'],
             'java_image_id':prep['java_image_id'],'seed_inventory_sha256':seed['inventory_sha256'],
             'private_inputs':False,'model_calls':0}
    util.write_new_json(root/'intent.json',receipt);volume(data,'modernization.public_sdk='+ident,prep['java_image_id'])
    script=r'''import hashlib,json,shutil,subprocess,tarfile,re,zipfile
from pathlib import Path
root=Path('/sdk');source=root/'source';source.mkdir()
request=json.loads(Path('/control/intent.json').read_text())
if hashlib.sha256(Path('/archive.tar.gz').read_bytes()).hexdigest()!=request['archive_sha256']:raise ValueError('Archive changed')
with tarfile.open('/archive.tar.gz') as archive:
 for m in archive:
  if not m.isfile() and not m.isdir():raise ValueError('Unexpected source member')
  parts=Path(m.name).parts
  if '..' in parts or m.name.startswith('/'):raise ValueError('Unsafe source member')
  if len(parts)<2:continue
  m.name=Path(*parts[1:]).as_posix();archive.extract(m,source,filter='data')
inventory=json.loads(Path('/seed/inventory.json').read_text())
if hashlib.sha256(Path('/seed/inventory.json').read_bytes()).hexdigest()!=request['seed_inventory_sha256']:raise ValueError('Dependency seed changed')
for rel,entry in inventory.items():
 p=Path('/seed')/rel
 if hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Dependency content changed')
 dest=root/'gradle'/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest)
properties=(source/'gradle.properties').read_text();jvm=re.search(r'^org\.gradle\.jvmargs=(.*)$',properties,re.M).group(1)
jvm=re.sub(r'-Xmx\S+','-Xmx8g',jvm)
args=['fineract-gradle','--offline','--no-build-cache','-Dorg.gradle.jvmargs='+jvm,':fineract-client:jar',':fineract-client-feign:jar']
status=subprocess.call(args,cwd=source)
result={'exit_code':status,'command':args,'source_commit':request['source_commit'],'private_inputs':False}
if status==0:
 export=root/'export';export.mkdir();jar_hashes={}
 for module,relative in [('fineract-client','.'),('fineract-client-feign','feign')]:
  jars=[p for p in (source/module/'build/libs').glob('*.jar') if not any(x in p.name for x in ('sources','javadoc','tests'))]
  if len(jars)!=1:raise ValueError('Exactly one public SDK JAR per client required')
  destination=export/relative;destination.mkdir(exist_ok=True);shutil.copyfile(jars[0],destination/'client.jar')
  jar_hashes[module]=hashlib.sha256(jars[0].read_bytes()).hexdigest()
  with zipfile.ZipFile(jars[0]) as archive:
   for entry in archive.infolist():
    if entry.is_dir():continue
    parts=Path(entry.filename).parts
    if '..' in parts or entry.filename.startswith('/'):raise ValueError('Unsafe SDK entry')
    category='classes' if entry.filename.endswith('.class') else 'resources'
    target=destination/category/entry.filename;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.read(entry))
 inventory={p.relative_to(export).as_posix():{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size} for p in sorted(export.rglob('*')) if p.is_file()}
 (export/'inventory.json').write_text(json.dumps(inventory,sort_keys=True,separators=(',',':')))
 result.update(inventory_sha256=hashlib.sha256((export/'inventory.json').read_bytes()).hexdigest(),files=len(inventory),bytes=sum(x['bytes'] for x in inventory.values()),jar_sha256=jar_hashes['fineract-client'],jar_sha256_by_module=jar_hashes,configuration={name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in ['fineract-client/build.gradle','fineract-client/dependencies.gradle','fineract-client-feign/build.gradle','fineract-client-feign/dependencies.gradle','gradle.properties','build.gradle']})
Path('/sdk/build-result.json').write_text(json.dumps(result))
raise SystemExit(status)
'''
    runtime.docker('run','-d','--name',name,'--label','modernization.public_sdk='+ident,
        '--network','none','--read-only','--user','1000:1000','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--memory=12g','--cpus=4','--pids-limit=512',
        '--tmpfs','/tmp:rw,exec,size=1g','-e','HOME=/tmp','-e','GRADLE_USER_HOME=/sdk/gradle',
        '--mount',f'type=volume,source={data},target=/sdk',
        '--mount',f'type=volume,source={seed["volume"]},target=/seed,readonly',
        '--mount',f'type=bind,source={archive},target=/archive.tar.gz,readonly',
        '--mount',f'type=bind,source={root},target=/control,readonly',prep['java_image_id'],
        'timeout','2400','python3','-c',script)
    util.write_new_json(root/'started.json',receipt)
    return {'record':str(root),'container':name,'private_inputs':False,'model_calls':0}


def collect(record):
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    if (root/'result.json').exists():return util.read_json(root/'result.json')
    item=runtime.inspect_container(data['container'])
    if item['Config']['Labels'].get('modernization.public_sdk')!=data['id']:raise ValueError('SDK ownership mismatch')
    if item['State']['Running']:return {'running':True,'record':str(root)}
    logs=runtime.docker('logs',data['container'],check=False)
    (root/'stdout.log').write_text(logs.stdout,encoding='utf-8');(root/'stderr.log').write_text(logs.stderr,encoding='utf-8')
    runtime.docker('cp',data['container']+':/sdk/build-result.json',root/'build-result.json',check=False)
    build=util.read_json(root/'build-result.json') if (root/'build-result.json').exists() else None
    result={**data,'state':item['State'],'running':False,'build':build,
            'success':bool(build and build['exit_code']==0 and item['State']['ExitCode']==0 and not item['State']['OOMKilled'])}
    if result['success']:
        runtime.docker('cp',data['container']+':/sdk/export/inventory.json',root/'inventory.json')
        result['inventory_sha256']=util.sha256_file(root/'inventory.json')
        if result['inventory_sha256']!=build['inventory_sha256']:raise ValueError('SDK export mismatch')
    util.write_new_json(root/'result.json',result)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    q=sub.add_parser('begin');q.add_argument('--preparation',required=True,type=Path);q.add_argument('--seed',required=True,type=Path)
    sub.add_parser('collect').add_argument('--record',required=True,type=Path);a=p.parse_args()
    print(json.dumps(begin(a.preparation,a.seed) if a.command=='begin' else collect(a.record)))
