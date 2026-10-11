"""Explicit non-adoption of a stopped technical attempt, with all costs retained.

No collection, scoring, derivative import, model call, STOP removal, deletion or
automatic authorization. A later guarded run uses ordinary fresh UUID dispatch.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime
import importlib.util
import json
from pathlib import Path

KIND='pipeline_technical_exclusion_v1'


def source_inventory(root):
    """Hash acquisition evidence, excluding only the root's OS ownership lock.

    Windows denies a second handle's read of a byte locked by this process.
    The lock is still held throughout exclusion; its payload is not Run evidence.
    """
    from outer.harness import util
    root=Path(root);util.reject_links(root);entries={}
    for path in sorted(root.rglob('*'),key=lambda p:str(p.relative_to(root))):
        relative=path.relative_to(root).as_posix()
        if path.is_file() and relative!='controller.lock':
            entries[relative]=dict(sha256=util.sha256_file(path),bytes=path.stat().st_size)
    return entries


def guard():
    spec=importlib.util.spec_from_file_location('_retry_guard',Path(__file__).with_name('pipeline_guard.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def budget_usage(source):
    from research import acquisition_pipeline as pipeline
    from outer.harness import util
    observed=pipeline.usage_for_manifests([source/'manifest.json'])
    charged=json.loads(json.dumps(observed));charge=None
    manifest=util.read_json(source/'manifest.json')
    if manifest['run_instance_id'] in charged['missing_duration_runs']:
        end=(manifest.get('network_cleanup') or {}).get('ended_at') or manifest.get('ended_at')
        start,end=(datetime.fromisoformat(v) for v in (manifest['started_at'],end))
        if start.tzinfo is None or end.tzinfo is None or end<start:raise ValueError('Unbound recovery duration')
        seconds=(end-start).total_seconds();charged['accumulated_run_seconds']+=seconds
        charge=dict(measured_duration_seconds=None,budget_only_seconds=seconds,
                    basis='start_to_confirmed_cleanup_upper_bound')
    return observed,charged,charge


def technical_reason(item,source,reason):
    """Quality values/verdicts never participate in this decision."""
    from research import technical_retry
    from outer.harness import util
    manifest=util.read_json(source/'manifest.json')
    saved=source.parent/'pipeline-acquisition.json'
    if reason=='controller_interrupted':
        if manifest.get('end_reason')!='operator_stop':
            raise ValueError('Controller interruption requires a retained stopped attempt')
        if saved.exists() and util.read_json(saved).get('reason')!='controller_interrupted':
            raise ValueError('Existing completed attempt is not a controller interruption')
        return dict(kind='controller_interrupted',cause='unknown',exit_code_inferred=False)
    if reason!='technical_failure':raise ValueError('Explicit technical exclusion reason required')
    classified=technical_retry.classify(source.parent)
    if classified:return dict(kind='existing_infrastructure_classifier',evidence=classified)
    path=source/'implementation-receipt.json'
    if path.exists():
        receipt=util.read_json(path)
        if (receipt.get('collection_status')=='collection_fault'
                and isinstance(receipt.get('collection_error_type'),str) and receipt['collection_error_type']
                and all(receipt.get(k)==manifest[k] for k in ('run_id','run_instance_id'))
                and receipt.get('stop_confirmed') is True
                and receipt.get('raw')==util.tree_hashes(source/'usage/raw')
                and receipt.get('snapshot_sha256')==util.sha256_file(source/'snapshot.json')):
            from research import live_pilot
            return dict(kind='recorded_collection_fault',receipt=live_pilot.reference(path),
                        error_type=receipt['collection_error_type'])
    raise ValueError('No supported technical failure evidence; product/quality failure is not retry authority')


def _item(config,record):
    from research import pair_execution
    record=Path(record).resolve()
    reservations=[e for e in pair_execution.events(Path(config['root'])/'events.jsonl')
                  if e['kind']=='reserved' and Path(e['record'])==record]
    found=[item for item in guard().attempts(config) if item['path']==record]
    if len(reservations)!=1 or len(found)!=1 or len(found[0]['cases'])!=1:
        raise ValueError('One original reserved single-condition attempt required')
    item=found[0]
    if reservations[0]['slot']!=item['attempt']['slot']:raise ValueError('Reservation slot mismatch')
    return item


def exclude(repo,config_path,record,reason,authorization_reference):
    """Append a factual exclusion under the existing owner locks; never resend."""
    if not isinstance(authorization_reference,str) or not authorization_reference.strip():
        raise ValueError('Existing user instruction reference required; never synthesize approval')
    owner=guard();config=owner.context(repo,config_path)
    from research import live_pilot as live,pair_execution,acquisition_pipeline as pipeline
    from research.campaign_reassessment import _existing_lock
    from outer.harness import profiles,util,run
    if config['template'].get('staged_retry',{}).get('retry_budget')!='shared_reserved_attempts':
        raise ValueError('This narrow exclusion path requires the existing shared-attempt budget')
    root=Path(config['root']);item=_item(config,record);case=item['cases'][0];source=item['path'].parent/case['run_id']
    with ExitStack() as stack:
        for path in (root/'_pipeline-supervisor/dispatch.lock',root/'_owner/dispatch.lock',source/'controller.lock'):
            if path.exists():stack.enter_context(_existing_lock(path))
            elif path.name=='controller.lock':raise ValueError('Original Run ownership lock missing')
        profiles.validate_run(source)
        manifest=util.read_json(source/'manifest.json')
        if any(manifest.get(k)!=case[k] for k in ('run_id','run_instance_id')):raise ValueError('Run identity mismatch')
        row=dict(root=str(source),allocated=True,**{k:case[k] for k in ('run_id','run_instance_id')})
        if not owner.stopped(row):raise ValueError('Physical stop and network cleanup required')
        events=pair_execution.events(root/'events.jsonl')
        if any(e['kind']=='accepted' and e['slot']==item['attempt']['slot'] for e in events):
            raise ValueError('An adopted slot cannot be replaced')
        path=root/'_exclusions'/(case['run_instance_id']+'.json')
        before=source_inventory(source)
        if path.exists():
            receipt=util.read_json(path)
            if (receipt['source_inventory']!=before or receipt['attempt']!=live.reference(item['path'])
                    or receipt['reason']!=reason or receipt['authorization_reference']!=authorization_reference):
                raise ValueError('Existing exclusion changed; reconcile without dispatch')
        else:
            evidence=technical_reason(item,source,reason)
            observed,charged,charge=budget_usage(source)
            receipt=dict(kind=KIND,config=live.reference(config_path),attempt=live.reference(item['path']),
                slot=item['attempt']['slot'],run_id=case['run_id'],run_instance_id=case['run_instance_id'],
                source_inventory=before,inventory_excluded_paths=['controller.lock'],reason=reason,evidence=evidence,
                authorization_reference=authorization_reference,adopted=False,
                observed_usage=observed,budget_usage=charged,conservative_duration_charge=charge,
                retry_requires_new_uuid=True,model_calls=0,acquisition_count_increment=0,at=run.now())
            util.write_new_json(path,receipt)
        acquisition=source.parent/'pipeline-acquisition.json'
        if not acquisition.exists():
            util.write_new_json(acquisition,dict(acquired=False,slot=item['attempt']['slot'],attempt=item['attempt']['attempt'],
                record=str(item['path']),usage=receipt['budget_usage'],runs=pipeline.attempt_runs(source.parent,item['attempt']),
                reason=reason,fault=None,error=None,at=receipt['at'],exclusion=live.reference(path)))
        if source_inventory(source)!=before:raise ValueError('Original Run changed during exclusion')
        ref=live.reference(path)
        prior=[e for e in events if e['kind']=='attempt_excluded' and e.get('record')==str(item['path'])]
        if prior:
            if len(prior)!=1 or prior[0]['exclusion']!=ref:raise ValueError('Conflicting exclusion event')
        else:pair_execution.append(root/'events.jsonl',dict(kind='attempt_excluded',slot=receipt['slot'],
            record=str(item['path']),exclusion=ref,acquisition_count_increment=0,model_calls=0))
        return ref


class Exclusions:
    """Read-only adapter policy. Every original reservation still consumes budget."""
    def __init__(self,config_path):
        from research import live_pilot as live,pair_execution
        from outer.harness import util
        config_path=Path(config_path);config=util.read_json(config_path);self.rows={}
        events=pair_execution.events(Path(config['root'])/'events.jsonl')
        accepted={e['slot'] for e in events if e['kind']=='accepted'}
        for event in events:
            if event['kind']!='attempt_excluded':continue
            path=live.checked(event['exclusion']);value=util.read_json(path)
            record=live.checked(value['attempt']);item=_item(config,record)
            case=item['cases'][0];source=record.parent/case['run_id']
            if (value.get('kind')!=KIND or value.get('config')!=live.reference(config_path)
                    or path!=Path(config['root'])/'_exclusions'/(case['run_instance_id']+'.json')
                    or value.get('run_instance_id')!=case['run_instance_id'] or value.get('run_id')!=case['run_id']
                    or value.get('slot')!=item['attempt']['slot'] or event['slot']!=value['slot']
                    or event.get('record')!=str(record) or str(record) in self.rows
                    or not value.get('authorization_reference') or value.get('adopted') is not False
                    or value.get('retry_requires_new_uuid') is not True
                    or value.get('model_calls')!=0 or value.get('acquisition_count_increment')!=0
                    or value.get('inventory_excluded_paths')!=['controller.lock']
                    or value['source_inventory']!=source_inventory(source)):
                raise ValueError('Unbound or changed technical exclusion')
            # A subsequent accepted replacement is allowed; never exclude it.
            if value['slot'] in accepted:
                chosen=[e for e in events if e['kind']=='accepted' and e['slot']==value['slot']]
                for selected in chosen:
                    ref=selected.get('record',{})
                    if Path(ref.get('path','')).parent==record.parent:raise ValueError('Excluded attempt was adopted')
            observed,charged,charge=budget_usage(source)
            if (value['observed_usage']!=observed or value['budget_usage']!=charged
                    or value['conservative_duration_charge']!=charge):raise ValueError('Excluded usage changed')
            if technical_reason(item,source,value['reason'])!=value['evidence']:raise ValueError('Technical evidence changed')
            self.rows[str(record)]=value

    def excluded(self,attempt):return str(Path(attempt['record'])) in self.rows

    def usage(self,attempt,saved):
        return self.rows[str(Path(attempt['record']))]['budget_usage'] if self.excluded(attempt) else saved['usage']


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--record',type=Path,required=True)
    parser.add_argument('--reason',choices=['controller_interrupted','technical_failure'],required=True)
    parser.add_argument('--authorization-reference',required=True)
    args=parser.parse_args();guard().dependencies(args.repo)
    print(json.dumps(exclude(args.repo,args.config,args.record,args.reason,args.authorization_reference)))


if __name__=='__main__':main()
