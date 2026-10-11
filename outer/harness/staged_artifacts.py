"""Private, reversible Run checkpoints; never final collection or evaluation."""
from pathlib import Path
import time
from . import preserve, staged_input, util

LABELS=('initial','before-additional','after-additional','final')


def capture(controller,label,binding):
    labels=staged_input.checkpoint_labels(controller.contract)
    if label not in labels:raise ValueError('Unknown staged checkpoint')
    directory=controller.root/'checkpoints'/label
    directory.mkdir(parents=True,exist_ok=False)
    started=time.monotonic();at=controller.clock()
    receipt={'label':label,'run_instance_id':controller.contract['run_instance_id'],
        'contract_sha256':controller.contract_sha256,'binding':binding,'started_at_unix':at,
        'started_at_monotonic':started,'archive':None,'reason':None}
    try:
        workspace=Path(controller.contract['workspace'])
        policy=controller.contract.get('artifact_collection_policy')
        before=util.collection_hash(workspace,policy=policy)
        # Reuse byte selection / sealed static-DB rules. Do not write to the
        # workspace, normalize bytes, set submission_fixed, or invoke a scorer.
        copied=util.collect(workspace,directory/'workspace',normalize=False,policy=policy)
        if (util.collection_hash(workspace,policy=policy)!=before
                or util.collection_hash(directory/'workspace',policy=policy)!=before):
            raise ValueError('Workspace changed during private checkpoint')
        previous=None
        for name in reversed(labels[:labels.index(label)]):
            candidate=controller.root/'checkpoints'/name/'tree.json'
            if candidate.exists():previous=(name,util.read_json(candidate));break
        files=copied['frozen'];old=previous[1]['files'] if previous else {}
        delta={'basis':previous[0] if previous else None,'added':sorted(files.keys()-old.keys()),
            'removed':sorted(old.keys()-files.keys()),
            'changed':sorted(k for k in files.keys() & old.keys() if files[k]!=old[k])}
        tree={'files':files,'collection_sha256':before,'collection_contract':copied['collection_contract'],
            'excluded_files':copied['excluded_files'],'excluded_file_evidence':copied['excluded_file_evidence'],
            'difference':delta,'binding':binding,'label':label,'contract_sha256':controller.contract_sha256}
        util.write_new_json(directory/'tree.json',tree)
        sources={'workspace':directory/'workspace','tree.json':directory/'tree.json'}
        if label=='initial':
            util.write_new_json(directory/'inputs-manifest.json',util.tree_hashes(workspace.parent/'inputs'))
            sources['inputs-manifest.json']=directory/'inputs-manifest.json'
        receipt['archive']=preserve.pack(controller.root/'checkpoint-archive',label,sources,
            metadata={'kind':'private_staged_checkpoint','label':label,'binding':binding,
                'run_instance_id':controller.contract['run_instance_id'],'contract_sha256':controller.contract_sha256})
        receipt['tree_sha256']=util.sha256_file(directory/'tree.json')
        receipt['bytes']=sum(v['bytes'] for v in files.values())
    except Exception as error:
        receipt['reason']='checkpoint_failed:'+type(error).__name__
        receipt['error_detail']=str(error)
    receipt.update(ended_at_unix=controller.clock(),duration_seconds=time.monotonic()-started)
    util.write_new_json(directory/'receipt.json',receipt)
    return receipt


def summary(controller,terminal_reason):
    result={};ledger=controller.events()
    labels=staged_input.checkpoint_labels(controller.contract)
    for label in labels:
        path=controller.root/'checkpoints'/label/'receipt.json'
        if path.exists():
            receipt=util.read_json(path)
            events=[e for e in controller.events() if e['kind']=='checkpoint' and e['label']==label]
            if len(events)!=1 or events[0]['receipt_sha256']!=util.sha256_file(path):
                raise ValueError('Staged checkpoint receipt changed')
            if receipt['archive']:
                package=preserve.verify(controller.root/'checkpoint-archive',**{
                    'package_id':receipt['archive']['package_id'],'expected_hash':receipt['archive']['sha256']})
                if (package['metadata']['contract_sha256']!=controller.contract_sha256
                        or util.sha256_file(path.parent/'tree.json')!=receipt['tree_sha256']):
                    raise ValueError('Staged checkpoint identity changed')
            result[label]=receipt
        else:
            reason=(('no_post_resume_implementation_change_observed' if staged_input.observed(controller.contract['boundary_contract'])
                     else 'no_post_input_implementation_change_observed') if label==labels[2]
                    else 'boundary_never_reached' if label==labels[1] else 'checkpoint_not_taken')
            phase={labels[1]:'before_resume' if staged_input.observed(controller.contract['boundary_contract']) else 'before_additional',
                   labels[2]:'after_resume' if staged_input.observed(controller.contract['boundary_contract']) else 'after_additional'}.get(label)
            unknown=[e for e in ledger if e['kind']=='boundary_unknown' and phase and e.get('phase')==phase]
            if unknown:reason='boundary_unknown:'+unknown[-1]['reason']
            elif label==labels[2] and not any(e['kind'] in ('acknowledged','resumption_action') for e in ledger):
                reason='resume_not_reached' if staged_input.observed(controller.contract['boundary_contract']) else 'additional_input_not_acknowledged'
            result[label]={'archive':None,'reason':reason,'terminal_reason':terminal_reason}
    return result
