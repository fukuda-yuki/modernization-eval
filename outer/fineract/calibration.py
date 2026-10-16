"""Create retained, exact-byte private calibration submissions from pinned source."""
import argparse
import json
from pathlib import Path
import uuid

from outer.harness import runtime,util,run
from .seed import volume


def snapshot(preparation, overlay):
    prep_root=Path(preparation).resolve();prep=util.read_json(prep_root/'result.json')
    overlay=Path(overlay).resolve();util.reject_links(overlay)
    ident=uuid.uuid4().hex;root=prep_root.parent.parent/'fineract-calibration'/ident;root.mkdir(parents=True)
    archive=prep_root.parent/'source.tar.gz'
    if util.sha256_file(archive)!=prep['archive_sha256']:raise ValueError('Canonical archive changed')
    name='fe-calibration-'+ident
    receipt={'kind':'private-calibration-submission-v1','id':ident,'at':run.now(),
        'volume':name,'archive_sha256':prep['archive_sha256'],'overlay_files':util.tree_hashes(overlay),
        'quality':'unvalidated','model_calls':0}
    util.write_new_json(root/'request.json',receipt)
    volume(name,'modernization.calibration='+ident,prep['java_image_id'])
    script=r'''import hashlib,json,shutil,tarfile
from pathlib import Path
from artifacts import pack_source,relative_path
source=Path('/owned/source');source.mkdir()
with tarfile.open('/input/source.tar.gz') as archive:
 for m in archive:
  if not m.isfile() and not m.isdir():raise ValueError('Unexpected archive member')
  parts=Path(m.name).parts
  if len(parts)<2:continue
  m.name=Path(*parts[1:]).as_posix();relative_path(m.name);archive.extract(m,source,filter='data')
overlay=json.loads(Path('/overlay/overlay.json').read_text())
for rel,entry in overlay.items():
 p=Path('/overlay').joinpath(*relative_path(entry['file']).parts)
 if hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('Overlay changed')
 target=source.joinpath(*relative_path(rel).parts)
 if not target.is_file():raise ValueError('Calibration overlay must identify an existing source file')
 shutil.copyfile(p,target)
result=pack_source(source,Path('/out/submission.tar'))
Path('/out/artifact.json').write_text(json.dumps(result,sort_keys=True))
print(json.dumps({'sha256':result['sha256'],'inventory_sha256':result['inventory_sha256'],'files':len(result['files'])}))
'''
    output=runtime.docker('run','--rm','--network','none','--read-only','--user','1000:1000',
        '--cap-drop=ALL','--security-opt=no-new-privileges','-e','PYTHONPATH=/adapter',
        '--mount',f'type=volume,source={name},target=/owned',
        '--mount',f'type=bind,source={archive},target=/input/source.tar.gz,readonly',
        '--mount',f'type=bind,source={overlay},target=/overlay,readonly',
        '--mount',f'type=bind,source={Path(__file__).parent},target=/adapter,readonly',
        '--mount',f'type=bind,source={root},target=/out',
        prep['java_image_id'],'python3','-c',script,timeout=300)
    result={**receipt,**json.loads(output.stdout),'record':str(root)}
    util.write_new_json(root/'result.json',result)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--preparation',required=True,type=Path)
    p.add_argument('--overlay',required=True,type=Path);a=p.parse_args()
    print(json.dumps(snapshot(a.preparation,a.overlay)))
