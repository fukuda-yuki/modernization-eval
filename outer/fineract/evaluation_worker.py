"""Trusted fresh-build stage. Runs only in an owned offline evaluation container.

This stage is not the complete business evaluator: API/DB/COB checks must also
complete before any overall pass is assigned. Hidden test bundles stay private.
"""
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile

from artifacts import extract_artifact


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def safe(name):
    p=PurePosixPath(name)
    if p.is_absolute() or '\\' in name or ':' in name or any(x in ('..','.') for x in name.split('/')):
        raise ValueError('Unsafe relative path')
    return p


def main():
    work=Path('/work');source=work/'source';source.mkdir()
    spec=json.loads(Path('/private/spec.json').read_text())
    request=json.loads(Path('/control/request.json').read_text())
    if {p.relative_to('/adapter').as_posix():{'sha256':digest(p),'bytes':p.stat().st_size}
            for p in Path('/adapter').rglob('*') if p.is_file()}!=request['adapter_files']:
        raise ValueError('Frozen evaluator adapter changed')
    if digest(Path('/input/source.tar.gz'))!=request['archive_sha256']:raise ValueError('Canonical source changed')
    if {p.relative_to('/private').as_posix():{'sha256':digest(p),'bytes':p.stat().st_size}
            for p in Path('/private').rglob('*') if p.is_file()}!=request['private_files']:
        raise ValueError('Private evaluator bundle changed')
    submitted=None
    if request['submission_sha256'] is not None:
        artifact=Path('/input/submission.tar')
        if digest(artifact)!=request['submission_sha256']:raise ValueError('Frozen submission changed')
        submitted=work/'submitted';extract_artifact(artifact,submitted)
    with tarfile.open('/input/source.tar.gz') as archive:
        for m in archive:
            parts=PurePosixPath(m.name).parts
            if m.issym() or m.islnk() or m.isdev() or '..' in parts:raise ValueError('Unexpected canonical archive member')
            if len(parts)<2:continue
            rel=PurePosixPath(*parts[1:])
            # Submission production directories replace their canonical contents,
            # including deletions. Build scripts, wrapper and tests stay canonical.
            if submitted is not None and '/src/main/' in '/'+rel.as_posix()+'/':continue
            m.name=rel.as_posix();archive.extract(m,source,filter='data')
    if submitted is not None:
        for module in source.iterdir():
            if not module.is_dir():continue
            original=submitted/module.name/'src/main'
            if original.is_dir():shutil.copytree(original,module/'src/main',dirs_exist_ok=False)
    for test in spec['tests']:
        origin=Path('/private').joinpath(*safe(test['file']).parts)
        rel=safe(test['target'])
        if '/src/test/java/' not in '/'+rel.as_posix() or rel.suffix!='.java':raise ValueError('Invalid private Java target')
        target=source.joinpath(*rel.parts)
        if target.exists():raise ValueError('Private test collides with canonical public test')
        target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(origin,target)
    seed=Path('/seed');inventory=json.loads((seed/'inventory.json').read_text())
    if digest(seed/'inventory.json')!=request['seed_inventory_sha256']:raise ValueError('Dependency seed changed')
    for rel,entry in inventory.items():
        original=seed.joinpath(*safe(rel).parts)
        if digest(original)!=entry['sha256']:raise ValueError('Dependency content changed')
        target=work/'gradle'/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(original,target)
    outcomes=[]
    # Run each group explicitly. --continue cannot hide a failed compile or
    # skip later tests behind a different failed Gradle task.
    for group in spec['groups']:
        args=['timeout','1200','fineract-gradle','--offline','--no-build-cache',*group['gradle_args']]
        with (work/(group['id']+'.log')).open('xb') as log:
            result=subprocess.run(args,cwd=source,stdout=log,stderr=subprocess.STDOUT,timeout=1260)
        outcomes.append({'id':group['id'],'exit_code':result.returncode})
        (work/'groups.json').write_text(json.dumps(outcomes))
    (work/'build-stage.json').write_text(json.dumps({'completed':True,'groups':outcomes,
        'overall_business_verdict':'not_assigned','api_db_cob_complete':False}))
    raise SystemExit(0)


if __name__=='__main__':main()
