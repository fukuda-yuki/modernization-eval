"""Retained, non-model preparation in a new Docker volume; never prune old data."""
import argparse
import json
from pathlib import Path
import subprocess
import uuid

from outer.harness import runtime, util, run
from outer.harness.security import child_environment

SOURCE = 'd5636847ac556c30b437254c353f05526d172b97'
POSTGRES = 'postgres:18.3@sha256:7e32e9833a6fb1c92c32552794cb6ed569d51b445a54907d35fc112ef39684db'


def begin(repo, archive):
    repo, archive = Path(repo).resolve(), Path(archive).resolve()
    lock = util.read_json(repo/'tools/fineract_corpus/source.lock.json')
    if util.sha256_file(archive) != lock['archive_sha256']:
        raise ValueError('Source archive mismatch')
    ident = uuid.uuid4().hex
    root = repo/'artifacts/fineract-preparation'/ident
    root.mkdir(parents=True)
    volume, container, tag = 'fe-prep-'+ident, 'fe-build-'+ident, 'fineract-java:'+ident
    receipt = {'kind':'fineract-preparation-v1', 'id':ident, 'started_at':run.now(),
               'source_commit':SOURCE, 'archive_sha256':lock['archive_sha256'],
               'volume':volume, 'container':container, 'tag':tag,
               'model_requests':0, 'recipe_files':util.tree_hashes(repo/'outer/fineract'),
               'resources':{'memory_bytes':6*1024**3, 'cpus':4, 'gradle_heap_mib':2048,
                            'gradle_workers':1, 'test_heap_mib':768},
               'postgres_image':POSTGRES, 'git_history_present':False}
    util.write_new_json(root/'request.json',receipt)
    with (root/'image-build.log').open('xb') as log:
        result = subprocess.run(['docker','build','--progress=plain','--target','java',
            '-f','outer/fineract/Dockerfile','-t',tag,'.'], cwd=repo,
            env=child_environment(),stdout=log,stderr=subprocess.STDOUT,timeout=1800)
    if result.returncode:
        raise RuntimeError('Java image build failed; see retained log at '+str(root))
    receipt['java_image_id'] = runtime.image_id(tag)
    receipt['java_version'] = runtime.docker('run','--rm','--network','none',tag,'java','-version').stderr.strip()
    receipt['docker_resources'] = json.loads(runtime.docker('info','--format',
        '{"memory_bytes":{{.MemTotal}},"cpus":{{.NCPU}}}').stdout)
    runtime.docker('volume','create','--label','modernization.preparation='+ident,volume)
    # Linux volume preserves executable bits used in the sealed source inventory;
    # Windows bind mounts are unsuitable for that provenance calculation.
    extract = ('from pathlib import Path; from fineract_corpus.producer import unpack, read_json; '
        "unpack(Path('/input/source.tar.gz'),Path('/prep/source'),read_json(Path('/producer/fineract_corpus/source.lock.json')))")
    runtime.docker('run','--rm','--network','none','--cap-drop=ALL',
        '--security-opt=no-new-privileges','-e','PYTHONPATH=/producer',
        '--mount',f'type=bind,source={repo / "tools"},target=/producer,readonly',
        '--mount',f'type=bind,source={archive},target=/input/source.tar.gz,readonly',
        '--mount',f'type=volume,source={volume},target=/prep',tag,'python3','-c',extract,timeout=300)
    tasks = [':fineract-provider:bootJar', ':fineract-core:test',
        '--tests','*SavingsHelperAnniversaryPostingTest', ':fineract-provider:test',
        '--tests','*DelinquencyEffectivePauseHelperTest']
    receipt['gradle_tasks'] = tasks
    runtime.docker('run','-d','--name',container,'--label','modernization.preparation='+ident,
        '--memory=6g','--cpus=4','--pids-limit=512','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--mount',f'type=volume,source={volume},target=/prep',
        '-e','GRADLE_USER_HOME=/prep/gradle','-e','HOME=/prep/home','-w','/prep/source',
        tag,'timeout','1800','fineract-gradle',*tasks)
    receipt['build_started_at']=run.now()
    util.write_new_json(root/'started.json',receipt)
    return {'record':str(root),'container':container,'volume':volume}


def collect(record):
    root=Path(record).resolve()
    if (root/'result.json').exists():
        result=util.read_json(root/'result.json')
        return {'running':False,'success':result['success'],'state':result['state'],'record':str(root)}
    data=util.read_json(root/'started.json')
    item=json.loads(runtime.docker('inspect',data['container']).stdout)[0]
    if item['Config']['Labels'].get('modernization.preparation') != data['id']:
        raise ValueError('Preparation ownership mismatch')
    state=item['State']
    if state['Running']:
        stats=runtime.docker('stats','--no-stream','--format','{{json .}}',data['container']).stdout
        util.append_line(root/'resources.jsonl',{'at':run.now(),'docker':json.loads(stats)})
        return {'running':True,'stats':json.loads(stats)}
    logs=runtime.docker('logs',data['container'],check=False)
    (root/'gradle.stdout.log').write_text(logs.stdout,encoding='utf-8')
    (root/'gradle.stderr.log').write_text(logs.stderr,encoding='utf-8')
    result={**data,'ended_at':run.now(),'state':state,
            'success':state['ExitCode']==0 and not state['OOMKilled']}
    util.write_new_json(root/'result.json',result)
    return {'running':False,'success':result['success'],'state':state,'record':str(root)}


def main():
    p=argparse.ArgumentParser()
    sub=p.add_subparsers(dest='command',required=True)
    b=sub.add_parser('begin');b.add_argument('--repo',type=Path,default=Path.cwd())
    b.add_argument('--archive',type=Path,required=True)
    c=sub.add_parser('collect');c.add_argument('--record',type=Path,required=True)
    args=p.parse_args()
    result=begin(args.repo,args.archive) if args.command=='begin' else collect(args.record)
    print(json.dumps(result))


if __name__=='__main__':main()
