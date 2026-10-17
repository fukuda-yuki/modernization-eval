"""Run a frozen private HTTP/DB test client outside the submitted application."""
import argparse
import json
from pathlib import Path
import shutil
import uuid

from outer.harness import runtime,util,run
from .seed import volume
from .service import inspect


def begin(service,private,image,mode):
    service=Path(service).resolve();data=util.read_json(service/'started.json')
    health=inspect(service)
    if health['health'].get('http_status')!=200:raise ValueError('Running healthy service required')
    if mode=='capture' and data.get('build_basis')!='pinned_baseline':
        raise ValueError('Reference may be captured only from pinned baseline')
    private=Path(private).resolve();util.reject_links(private)
    if not (private/'check.py').is_file():raise ValueError('Private real API check required')
    ident=uuid.uuid4().hex;root=service/'api-checks'/ident;root.mkdir(parents=True)
    shutil.copytree(private,root/'private',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    name='fe-api-check-'+ident;output='fe-api-result-'+ident;image=runtime.image_id(image)
    receipt={'kind':'fineract-private-api-db-v1','id':ident,'at':run.now(),'service_id':data['id'],
        'service_record':str(service),'image_id':image,'mode':mode,'container':name,'volume':output,
        'private_files':util.tree_hashes(root/'private'),'model_calls':0}
    util.write_new_json(root/'request.json',receipt)
    volume(output,'modernization.api_check='+ident,image)
    runtime.docker('run','-d','--name',name,'--label','modernization.api_check='+ident,
        '--network',data['names']['network'],'--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','--memory=512m','--cpus=1','--pids-limit=128',
        '--tmpfs','/tmp:rw,size=64m',
        '--mount',f'type=bind,source={root / "private"},target=/private,readonly',
        '--mount',f'type=volume,source={output},target=/result',
        '--mount',f'type=volume,source={data["source_volume"]},target=/baseline,readonly',
        image,'timeout','300','python3','/private/check.py','--mode',mode)
    util.write_new_json(root/'started.json',receipt)
    return {'record':str(root),'container':name,'model_calls':0}


def collect(record):
    root=Path(record).resolve();data=util.read_json(root/('started.json' if (root/'started.json').exists() else 'request.json'))
    if (root/'result.json').exists():return util.read_json(root/'result.json')
    item=json.loads(runtime.docker('inspect',data['container']).stdout)[0]
    if item['Config']['Labels'].get('modernization.api_check')!=data['id']:raise ValueError('Ownership mismatch')
    if item['State']['Running']:return {'running':True}
    logs=runtime.docker('logs',data['container'],check=False)
    (root/'stdout.log').write_text(logs.stdout,encoding='utf-8');(root/'stderr.log').write_text(logs.stderr,encoding='utf-8')
    output=root/'fresh-evidence';output.mkdir()
    runtime.docker('cp',data['container']+':/result/.',output)
    report=util.read_json(output/'result.json') if (output/'result.json').is_file() else None
    result={'at':run.now(),'state':item['State'],'record':str(root),'running':False,
        'status':'unknown' if report is None or item['State']['OOMKilled'] or item['State']['ExitCode']!=0 else report['status'],
        'counts':None if report is None else report['counts'],'evidence_files':util.tree_hashes(output),
        'model_calls':0}
    util.write_new_json(root/'result.json',result)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    s=sub.add_parser('begin');s.add_argument('--service',required=True,type=Path)
    s.add_argument('--private',required=True,type=Path);s.add_argument('--image',required=True)
    s.add_argument('--mode',choices=('capture','verify'),required=True)
    sub.add_parser('collect').add_argument('--record',required=True,type=Path);a=p.parse_args()
    print(json.dumps(begin(a.service,a.private,a.image,a.mode) if a.command=='begin' else collect(a.record)))
