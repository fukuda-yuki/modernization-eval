"""Prepare and collect an owned canonical Java build stage, preserving originals."""
import argparse
import json
from pathlib import Path
import shutil
import uuid

from outer.harness import runtime, util, run
from .artifacts import junit_result
from .seed import volume


def begin(preparation, seed, private, submission=None):
    prep_root=Path(preparation).resolve();prep=util.read_json(prep_root/'result.json')
    seed=util.read_json(Path(seed).resolve()/'result.json');private=Path(private).resolve()
    spec=util.read_json(private/'spec.json')
    if not prep['success'] or seed['preparation_id']!=prep['id']:raise ValueError('Prepared baseline/seed mismatch')
    if not spec.get('expected_suites') or not spec.get('groups') or not spec.get('tests'):
        raise ValueError('Actual Java tests, canonical commands and expected suites required')
    ident=uuid.uuid4().hex;root=prep_root.parent.parent/'fineract-evaluations'/ident;root.mkdir(parents=True)
    name='fe-evaluate-'+ident;data='fe-evaluation-data-'+ident
    archive=prep_root.parent/'source.tar.gz'
    if util.sha256_file(archive)!=prep['archive_sha256']:raise ValueError('Canonical source changed')
    submission=Path(submission).resolve() if submission else None
    adapter=Path(__file__).parent
    frozen_adapter=root/'adapter';frozen_adapter.mkdir()
    for path in adapter.iterdir():
        if path.is_file():shutil.copyfile(path,frozen_adapter/path.name)
    util.reject_links(private)
    frozen_private=root/'private'
    shutil.copytree(private,frozen_private)
    request={'kind':'fineract-canonical-build-v1','id':ident,'at':run.now(),
        'preparation_id':prep['id'],'source_commit':prep['source_commit'],
        'archive_sha256':prep['archive_sha256'],'java_image_id':prep['java_image_id'],
        'seed_inventory_sha256':seed['inventory_sha256'],'seed_volume':seed['volume'],
        'submission_sha256':util.sha256_file(submission) if submission else None,
        'basis':'frozen_submission' if submission else 'pinned_baseline_calibration',
        'adapter_files':util.tree_hashes(frozen_adapter),'private_files':util.tree_hashes(frozen_private),
        'expected_suites':spec['expected_suites'],'container':name,'volume':data,
        'model_calls':0,'business_verdict':'not_assigned'}
    util.write_new_json(root/'request.json',request)
    volume(data,'modernization.evaluation='+ident,prep['java_image_id'])
    mounts=['--mount',f'type=volume,source={data},target=/work',
        '--mount',f'type=volume,source={seed["volume"]},target=/seed,readonly',
        '--mount',f'type=bind,source={archive},target=/input/source.tar.gz,readonly',
        '--mount',f'type=bind,source={frozen_private},target=/private,readonly',
        '--mount',f'type=bind,source={root},target=/control,readonly',
        '--mount',f'type=bind,source={frozen_adapter},target=/adapter,readonly']
    if submission:mounts+=['--mount',f'type=bind,source={submission},target=/input/submission.tar,readonly']
    runtime.docker('run','-d','--name',name,'--label','modernization.evaluation='+ident,
        '--network','none','--read-only','--user','1000:1000','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--memory=6g','--cpus=4','--pids-limit=512',
        '--tmpfs','/tmp:rw,exec,size=1g','-e','GRADLE_USER_HOME=/work/gradle',
        '-e','HOME=/work/home',*mounts,prep['java_image_id'],
        'timeout','1800','python3','/adapter/evaluation_worker.py')
    util.write_new_json(root/'started.json',request)
    return {'record':str(root),'container':name,'model_calls':0}


def collect(record):
    root=Path(record).resolve();data=util.read_json(root/'started.json')
    if (root/'result.json').exists():return summary(root,util.read_json(root/'result.json'))
    item=json.loads(runtime.docker('inspect',data['container']).stdout)[0]
    if item['Config']['Labels'].get('modernization.evaluation')!=data['id']:raise ValueError('Evaluation ownership mismatch')
    if item['State']['Running']:return {'running':True,'record':str(root)}
    logs=runtime.docker('logs',data['container'],check=False)
    (root/'container.stdout.log').write_text(logs.stdout,encoding='utf-8')
    (root/'container.stderr.log').write_text(logs.stderr,encoding='utf-8')
    output=root/'fresh-evidence';output.mkdir()
    # Copy only newly generated reports/logs using an owned helper. No submitted
    # report paths survive canonical reconstruction.
    extract=r'''import json,shutil
from pathlib import Path
work=Path('/work');out=Path('/out')
for p in list(work.glob('*.json'))+list(work.glob('*.log')):
 shutil.copyfile(p,out/p.name)
for p in (work/'source').glob('*/build/test-results/test/TEST-*.xml'):
 target=out/'junit'/p.parent.parent.parent.parent.name/p.name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target)
'''
    runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges',
        '--mount',f'type=volume,source={data["volume"]},target=/work,readonly',
        '--mount',f'type=bind,source={output},target=/out',data['java_image_id'],
        'python3','-c',extract)
    junit=junit_result(output/'junit',data['expected_suites'])
    stage=util.read_json(output/'build-stage.json') if (output/'build-stage.json').exists() else None
    result={'id':data['id'],'at':run.now(),'state':item['State'],'junit':junit,'stage':stage,
        'business_verdict':'not_assigned','api_db_cob_complete':False,
        'canonical_build_complete':bool(stage and stage.get('completed')),
        'environment_failure':item['State']['OOMKilled'] or not stage,
        'evidence_files':util.tree_hashes(output)}
    util.write_new_json(root/'result.json',result)
    # Hidden names and messages remain in the local private result, not stdout.
    return summary(root,result)


def summary(root,result):
    junit=result['junit']
    return {'record':str(root),'running':False,'state':result['state'],
        'unit_status':junit['status'],'executed':junit['executed'],
        'passed':junit['passed'],'failed':junit['failed'],'skipped':junit['skipped'],
        'canonical_build_complete':result['canonical_build_complete'],
        'business_verdict':'not_assigned','environment_failure':result['environment_failure']}


def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    s=sub.add_parser('begin')
    for key in ('preparation','seed','private'):s.add_argument('--'+key,required=True,type=Path)
    s.add_argument('--submission',type=Path)
    sub.add_parser('collect').add_argument('--record',required=True,type=Path)
    a=p.parse_args()
    result=begin(a.preparation,a.seed,a.private,a.submission) if a.command=='begin' else collect(a.record)
    print(json.dumps(result))


if __name__=='__main__':main()
