"""Fineract artifact and JUnit evidence primitives. No host execution of submissions."""
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import tarfile
import xml.etree.ElementTree as ET

EXCLUDED = {'.git','.agents','.codex','.gradle','build','node_modules','__pycache__'}
MAX_BYTES = 1024**3
MAX_FILES = 30000


def relative_path(name):
    p=PurePosixPath(name)
    if (not name or p.is_absolute() or '\\' in name or ':' in name
            or any(x in ('..','.') for x in name.split('/'))):
        raise ValueError('Unsafe artifact path')
    return p


def sha(data):return hashlib.sha256(data).hexdigest()


def files(root):
    root=Path(root)
    total=0; inventory={}
    for path in sorted(root.rglob('*')):
        rel=path.relative_to(root)
        if set(rel.parts)&EXCLUDED:continue
        if path.is_symlink():raise ValueError('Artifact symlinks forbidden')
        if not path.is_file():continue
        size=path.stat().st_size; total+=size
        if total>MAX_BYTES or len(inventory)>=MAX_FILES:raise ValueError('Artifact size limit')
        inventory[rel.as_posix()]={'sha256':sha(path.read_bytes()),'bytes':size}
    return inventory


def pack_source(source, archive):
    source, archive=Path(source).resolve(),Path(archive).resolve()
    if archive.is_relative_to(source):raise ValueError('Archive must be outside submission')
    inventory=files(source)
    # Exclusive output prevents a re-evaluation from replacing the original.
    with archive.open('xb') as stream,tarfile.open(fileobj=stream,mode='w') as tar:
        for rel,entry in inventory.items():
            path=source/rel
            data=path.read_bytes()
            if sha(data)!=entry['sha256']:raise ValueError('Artifact changed during collection')
            info=tar.gettarinfo(str(path),arcname=rel)
            info.uid=info.gid=0;info.uname=info.gname='';info.mtime=0
            info.mode=0o755 if path.stat().st_mode&0o111 else 0o644
            info.size=len(data)
            tar.addfile(info,io.BytesIO(data))
    return {'sha256':sha(archive.read_bytes()),'files':inventory,
            'inventory_sha256':sha(json.dumps(inventory,sort_keys=True,separators=(',',':')).encode()),
            'excluded_directories':sorted(EXCLUDED),'normalization':'none; exact submitted bytes'}


def extract_artifact(archive, destination):
    destination=Path(destination)
    if destination.exists():raise ValueError('Fresh evaluation destination required')
    destination.mkdir(parents=True)
    seen=set();total=0
    with tarfile.open(archive,'r:*') as tar:
        for member in tar:
            p=relative_path(member.name)
            if not member.isfile() or set(p.parts)&EXCLUDED or member.name in seen:
                raise ValueError('Artifact links, directories, duplicates or forbidden paths')
            seen.add(member.name);total+=member.size
            if total>MAX_BYTES or len(seen)>MAX_FILES:raise ValueError('Artifact size limit')
            dest=destination.joinpath(*p.parts);dest.parent.mkdir(parents=True,exist_ok=True)
            with tar.extractfile(member) as data,dest.open('xb') as output:
                while block:=data.read(1024*1024):output.write(block)
            dest.chmod(0o755 if member.mode&0o111 else 0o644)
    return files(destination)


def junit_result(report_dir, expected_suites):
    """Zero, skipped, missing, duplicate and malformed tests can never pass.

    Caller supplies fresh reports produced by its canonical launcher, never an
    archive's reports. Public output maps this private case inventory to requirement
    IDs; test names and failure text remain private.
    """
    if not expected_suites:raise ValueError('Expected real test suites are required')
    cases=[];seen=set()
    for path in sorted(Path(report_dir).rglob('TEST-*.xml')):
        root=ET.parse(path).getroot()
        for case in root.iter('testcase'):
            ident=(case.get('classname'),case.get('name'))
            if not all(ident) or ident in seen:raise ValueError('Missing/duplicate JUnit case identity')
            seen.add(ident)
            status='failed' if case.find('failure') is not None or case.find('error') is not None else (
                'skipped' if case.find('skipped') is not None else 'passed')
            cases.append({'suite':ident[0],'case':ident[1],'status':status})
    missing=sorted(set(expected_suites)-{c['suite'] for c in cases})
    return {'status':'incomplete' if not cases or missing or any(c['status']=='skipped' for c in cases)
            else 'failed' if any(c['status']=='failed' for c in cases) else 'passed',
            'cases':cases,'missing_suites':missing,'executed':sum(c['status']!='skipped' for c in cases),
            'passed':sum(c['status']=='passed' for c in cases),'failed':sum(c['status']=='failed' for c in cases),
            'skipped':sum(c['status']=='skipped' for c in cases)}
