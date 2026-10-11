"""Explicit non-adoption of a stopped technical attempt, with all costs retained.

No collection, scoring, derivative import, model call, STOP removal, deletion or
automatic authorization. A later guarded run uses ordinary fresh UUID dispatch.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime
import importlib.util
import json
import math
from pathlib import Path
import re
import uuid

KIND='pipeline_technical_exclusion_v1'
PREPARATION='pre_dispatch_preparation_interrupted_v1'
EVALUATION_ADJUDICATION='evaluation_infrastructure_exclusion_adjudication_v1'
INFRASTRUCTURE_ADJUDICATION='infrastructure_exclusion_adjudication_v1'
EXECUTION_FILES=('runtime.json','implementation-receipt.json','snapshot.json',
                 'usage/normalized.json','usage/provenance.json','evidence/agent.jsonl')


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


def _product_failure(value):
    """Refuse retained product failures, including partial HTTP/browser results."""
    if isinstance(value,list):return any(_product_failure(v) for v in value)
    if not isinstance(value,dict):return False
    return (any(value.get(k) in ('fail','fail_critical') for k in ('verdict','judgement'))
        or any(value.get(k)=='not_run_product_prerequisite' for k in
               ('browserCartCoverage','browserReviewCoverage','browser_cart_coverage','browser_review_coverage'))
        or any(type(value.get(k)) is int and value[k]>0 for k in ('failedCount','failed_count'))
        or any(_product_failure(v) for v in value.values()))


def _ordinary_timeout(value):
    if isinstance(value,list):return any(_ordinary_timeout(v) for v in value)
    if not isinstance(value,dict):return False
    return (any(value.get(k) is True for k in ('evaluator_timed_out','timed_out'))
        or any(value.get(k) in ('timeout','budget_exceeded','budget_stop') for k in ('end_reason','reason'))
        or any(value.get(k) in ('TimeoutError','TimeoutExpired') for k in ('type','error_type','collection_error_type'))
        or any(_ordinary_timeout(v) for v in value.values()))


def _ambiguous_stop(value):
    """RuntimeError also carries ordinary watcher ceilings and explicit STOP."""
    if isinstance(value,list):return any(_ambiguous_stop(v) for v in value)
    if not isinstance(value,dict):return False
    return (any(value.get(k)=='RuntimeError' for k in ('reason','type','error_type','collection_error_type'))
        or any(_ambiguous_stop(v) for v in value.values()))


def _run_ceiling_clear(item,source,manifest,acquisition,journal):
    """Veto ceilings from original bounds/times; never infer a fault from a stop."""
    from research import live_pilot as live
    from outer.harness import util
    config=util.read_json(live.checked(item['epoch']['campaign_config']))
    limits=[config['bounds']['run_seconds'],item['epoch']['bounds']['run_seconds']]
    condition=util.read_json(source/'condition.json')
    if 'budget' in condition:
        budget=condition['budget']
        if budget.get('kind')!='wall_clock_seconds' or budget.get('scope')!='run':
            raise ValueError('Unknown original Run ceiling')
        limits.append(budget['value'])
    if any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in limits):
        raise ValueError('Missing or invalid original Run ceiling')
    start=datetime.fromisoformat(manifest['started_at'])
    if start.tzinfo is None:raise ValueError('Unbound original Run start')
    times=[manifest['ended_at']]
    fault=acquisition.get('fault')
    if fault:times.append(fault['at'])
    stop_path=source/'stop-request.json';stop=None
    if stop_path.exists():
        stop=util.read_json(stop_path)
        if any(k in stop and stop[k]!=manifest[k] for k in ('run_id','run_instance_id')):
            raise ValueError('Foreign original Run stop')
        times.append(stop['requested_at'])
    elif manifest.get('end_reason')=='operator_stop':
        raise ValueError('Missing original Run stop evidence; retain and reconcile')
    elapsed=[]
    for value in times:
        end=datetime.fromisoformat(value)
        if end.tzinfo is None or end<start:raise ValueError('Unbound original Run stop time')
        elapsed.append((end-start).total_seconds())
    duration=manifest.get('duration_seconds')
    if duration is not None:
        if type(duration) not in (int,float) or not math.isfinite(duration) or duration<0:
            raise ValueError('Invalid original Run duration')
        elapsed.append(duration)
    if max(elapsed)>=min(limits):
        raise ValueError('Original Run ceiling reached; not infrastructure retry authority')
    if _ordinary_timeout(stop) or _ambiguous_stop([acquisition,journal,stop]):
        raise ValueError('Ambiguous or ordinary original stop; retain and reconcile')


def adjudicated_fault(item,source,adjudication,authorization_reference):
    """Validate an explicit private ruling; neither error prose nor a code is authority."""
    from research import live_pilot as live,saved_reassessment,pair_execution,acquisition_pipeline
    from outer.harness import util,evaluate,browser_cleanup
    path=live.checked(adjudication);value=util.read_json(path)
    if path.is_relative_to(source):raise ValueError('Adjudication must remain outside the original Run')
    case=item['cases'][0]
    decision=value.get('decision',{})
    legacy=value.get('kind')==EVALUATION_ADJUDICATION
    if (value.get('kind') not in (EVALUATION_ADJUDICATION,INFRASTRUCTURE_ADJUDICATION)
            or value.get('authorization_reference')!=authorization_reference
            or not authorization_reference
            or any(value.get(k)!=case[k] for k in ('run_id','run_instance_id'))
            or decision.get('classification')!=('evaluation_infrastructure_fault' if legacy else 'infrastructure_fault')
            or decision.get('low_level_cause')!='unknown'
            or any(decision.get(k) is not False for k in ('product_defect','quality_selection','ordinary_timeout'))
            or decision.get('retry_requires_new_uuid') is not True
            or not isinstance(decision.get('basis'),str) or not decision['basis'].strip()):
        raise ValueError('Explicit infrastructure adjudication required')
    expected=dict(config=Path(item['epoch']['campaign_config']['path']),attempt=item['path'],
        acquisition=source.parent/'pipeline-acquisition.json',manifest=source/'manifest.json')
    optional=dict(snapshot=source/'snapshot.json',evaluation_index=source/'evaluations/index.jsonl')
    if legacy:expected.update(optional)
    else:
        expected.update(runtime=source/'runtime.json',pair_journal=source.parent/'_control/pair-journal.jsonl')
        for key,target in optional.items():
            if key not in value or value[key]!=(live.reference(target) if target.exists() else None):
                raise ValueError('Original optional evidence changed: '+key)
    for key,target in expected.items():
        if live.checked(value[key])!=target:raise ValueError('Foreign adjudication evidence: '+key)
    if (value['config']!=item['epoch']['campaign_config'] or value['attempt']!=live.reference(item['path'])):
        raise ValueError('Adjudication differs from its fixed campaign/attempt')
    acquisition=util.read_json(expected['acquisition']);manifest=util.read_json(expected['manifest'])
    if (type(acquisition.get('acquired')) is not bool or acquisition.get('attempt')!=item['attempt']['attempt']
            or acquisition.get('slot')!=item['attempt']['slot']):
        raise ValueError('Bound original acquisition receipt required')
    if not legacy:
        runtime=util.read_json(expected['runtime']);journal=pair_execution.events(expected['pair_journal'])
        current=pair_execution.state(expected['pair_journal'])
        dispatch=current['dispatch'].get(case['run_id'],{})
        if (set(current['dispatch'])!={case['run_id']} or any(dispatch.get(k)!=v for k,v in case.items())
                or any(runtime.get(k)!=case[k] for k in ('run_id','run_instance_id'))):
            raise ValueError('Original dispatch/runtime identity differs')
        if any((e.get('row',{}).get('scoring') or {}).get('state')=='scored' for e in current['results'].values()):
            raise ValueError('Recorded scored result cannot become an infrastructure exclusion')
        if _ordinary_timeout([manifest,acquisition,runtime,journal]) or _product_failure([manifest,acquisition,runtime,journal]):
            raise ValueError('Ordinary timeout/budget stop or product failure is not infrastructure retry authority')
        _run_ceiling_clear(item,source,manifest,acquisition,journal)
        observed=acquisition_pipeline.usage_for_manifests([source/'manifest.json'])
        if observed['dispatched_runs']!=1 or acquisition.get('usage')!=observed:
            raise ValueError('Original acquisition usage differs; retain and reconcile')
    pipeline_evaluation=source.parent/'pipeline-evaluation.json'
    if 'pipeline_evaluation' not in value or value['pipeline_evaluation']!=(
            live.reference(pipeline_evaluation) if pipeline_evaluation.exists() else None):
        raise ValueError('Original pipeline evaluation receipt changed')
    if pipeline_evaluation.exists():
        outcome=util.read_json(pipeline_evaluation)
        if outcome.get('accepted') is not False or _product_failure(outcome):
            raise ValueError('Accepted or ambiguous evaluation cannot be excluded')
    index=evaluate.read_index(source)
    if any(r.get('adopted') is True or r.get('scoring_state')=='scored' or _product_failure(r) for r in index):
        raise ValueError('Retained scored/product result')
    # The same protection applies before evaluation and after an incomplete score.
    for name in ('record.json','evaluation.json','results.jsonl'):
        for saved in (source/'evaluations').rglob(name):
            data=util.read_json(saved) if name.endswith('.json') else util.read_lines(saved)
            if _product_failure(data) or (not legacy and _ordinary_timeout(data)):
                raise ValueError('Retained product failure/timeout prevents infrastructure exclusion')
    faults=value.get('fault_evidence')
    if not isinstance(faults,list) or not faults:raise ValueError('Fixed original fault evidence required')
    fault_paths={live.checked(ref) for ref in faults}
    if any(not path.stat().st_size for path in fault_paths):raise ValueError('Empty original fault evidence')
    evidence=dict(kind='adjudicated_evaluation_infrastructure_fault' if legacy else 'adjudicated_infrastructure_fault',
        adjudication=adjudication,low_level_cause='unknown')
    if not index:
        if legacy or 'evaluation_record' not in value or value['evaluation_record'] is not None:
            raise ValueError('Missing or unbound original evaluation record')
        if any((source/'evaluations').rglob('*')):
            raise ValueError('Unindexed evaluation evidence must remain held')
        # A ruling supplies attribution; existing typed failure records supply the
        # occurrence. No exception-class or OS error-number allowlist is used.
        failure_paths=set()
        error,fault=acquisition.get('error'),acquisition.get('fault')
        if (isinstance(error,dict) and isinstance(error.get('type'),str) and error['type']
                or isinstance(fault,dict) and isinstance(fault.get('reason'),str) and fault['reason']):
            failure_paths.add(expected['acquisition'])
        for event in journal:
            receipt=event.get('receipt',{})
            if (isinstance(event.get('error_type'),str) and event['error_type']
                    or isinstance(receipt,dict) and any(isinstance(receipt.get(k),str) and receipt[k]
                        for k in ('error_type','collection_error_type'))):
                failure_paths.add(expected['pair_journal'])
                if event.get('receipt_path'):failure_paths.add(Path(event['receipt_path']))
        if not fault_paths & failure_paths:
            raise ValueError('Fixed original acquisition/persistence failure record required')
        allowed={expected['acquisition'],expected['pair_journal'],source.parent/'pipeline-evaluation.log'}|failure_paths
        if any(path not in allowed and not path.is_relative_to(source/'evidence')
                and not path.is_relative_to(source/'_controller') for path in fault_paths):
            raise ValueError('Foreign original infrastructure fault evidence')
        return evidence
    if (acquisition.get('acquired') is not True or manifest.get('submission_fixed') is not True):
        raise ValueError('Completed original acquisition with fixed artifact required')
    last=index[-1];record_path=live.checked(value['evaluation_record'])
    if 'directory' in last:
        directory=live.safe_path(source/last['directory'])
    else:
        # The original missing-output writer omits directory in both record and
        # index. Bind its existing, adjudicated record without repairing either.
        directory=record_path.parent;sequence=last.get('sequence')
        if (type(sequence) is not int or sequence<1 or directory.parent!=source/'evaluations'
                or not re.fullmatch(r'fault-'+f'{sequence:03d}'+r'-[0-9a-f]{32}',directory.name)):
            raise ValueError('Unbound directory-less evaluation fault')
        matches=[]
        for candidate in (source/'evaluations').glob(f'fault-{sequence:03d}-*/record.json'):
            saved=util.read_json(candidate)
            if all(saved.get(k)==v for k,v in last.items()):matches.append(candidate)
        if matches!=[record_path]:raise ValueError('Ambiguous directory-less evaluation fault')
    if not directory.is_relative_to(source/'evaluations'):
        raise ValueError('Evaluation evidence escaped original Run')
    if record_path!=directory/'record.json':raise ValueError('Adjudication must bind the latest original evaluation')
    record=util.read_json(record_path)
    if 'directory' not in last and ('directory' in record or record.get('evaluation_sha256') is not None):
        raise ValueError('Directory-less record must be the original missing-output fault')
    if (any(record.get(k)!=v for k,v in last.items()) or record.get('run_id')!=case['run_id']
            or record.get('scoring_state')!='evaluator_fault' or record.get('adopted') is not False
            or record.get('evaluator_timed_out') is not False or record.get('evaluator_exit_code')!=2
            or record.get('mismatches')!=[] or record.get('quality') is not None
            or record.get('verdict') not in (None,'error')):
        raise ValueError('Bound non-timeout evaluator fault required; quality/product failure is not authority')
    condition=util.read_json(source/'condition.json');evaluation=condition['evaluation']
    artifact=util.artifact_hash(source/'frozen');snapshot=util.read_json(optional['snapshot'])
    if (snapshot.get('run_id')!=case['run_id'] or snapshot.get('artifact_sha256')!=artifact
            or record.get('artifact_sha256_outer')!=artifact
            or record.get('artifact_sha256_frozen')!=artifact
            or any(record.get(k)!=evaluation[k] for k in ('evaluation_version','evaluator_sha256','spec_sha256'))):
        raise ValueError('Evaluation artifact/spec/evaluator binding differs')
    output=directory/'evaluation.json'
    if output.exists():
        result=util.read_json(output)
        if (record.get('evaluation_sha256')!=util.sha256_file(output)
                or evaluate.check_mismatches(result,condition,evaluation['evaluation_version'],source/'frozen',artifact,
                    source/evaluation['spec_path'],evaluation['spec_sha256'])
                or evaluate._reported_evaluator_sha256(directory)!=evaluation['evaluator_sha256']
                or result.get('reviewRunInstanceId') not in (None,case['run_instance_id'])
                or result.get('quality') is not None or result.get('verdict')!='error'):
            raise ValueError('Unbound or non-error original evaluation output')
    elif record.get('evaluation_sha256') is not None:
        raise ValueError('Original evaluation output missing')
    allowed={source/'evidence'/f'scoring-{record["sequence"]:03d}-{stream}.log' for stream in ('stdout','stderr')}
    allowed.add(source.parent/'pipeline-evaluation.log')
    for fault in fault_paths:
        if not fault.is_relative_to(directory) and fault not in allowed:
            raise ValueError('Foreign or empty original fault evidence')
    # Reuse recorded evaluator closure; no resource creation, cleanup or scoring.
    closure=browser_cleanup.latest(directory)
    if closure.get('confirmed') is not True:
        raise ValueError('Original evaluation closure is unconfirmed')
    if not (directory/'browser-resources.json').exists() and (
            closure.get('status')!='no_browser_resources_created'
            or closure.get('run_instance_id')!=case['run_instance_id']
            or closure.get('artifact_sha256')!=artifact
            or closure.get('spec_sha256')!=evaluation['spec_sha256']
            or closure.get('evaluator_container_removed') is not True):
        raise ValueError('Bound original no-resource evaluation closure required')
    for ownership in (source/'evaluations').rglob('browser-resources.json'):
        if not saved_reassessment._saved_cleanup_bound(ownership.parent,case['run_instance_id']):
            raise ValueError('Original evaluator resource closure is unconfirmed')
    return evidence


def technical_reason(item,source,reason,adjudication=None,authorization_reference=None):
    """Positive authority is technical evidence, never a quality threshold."""
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
    if adjudication is not None:
        return adjudicated_fault(item,source,adjudication,authorization_reference)
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
    if (reservations[0]['slot']!=item['attempt']['slot']
            or reservations[0].get('attempt')!=item['attempt']['attempt']):
        raise ValueError('Reservation slot/attempt mismatch')
    return item


def preparation_execution_seen(source,manifest=None):
    """One predicate for refusing an unsent claim and retaining that refusal."""
    from outer.harness import util,live_usage
    if manifest is None:
        path=source/'manifest.json'
        try:manifest=util.read_json(path) if path.exists() else {}
        except (OSError,ValueError):return True
    if not isinstance(manifest,dict):return True
    staged,errors=live_usage.journal(source/'_controller/staged-input/events.jsonl')
    journal,_=live_usage.journal(source.parent/'_control/pair-journal.jsonl')
    return bool(any(manifest.get(k) is not None for k in ('started_at','ended_at','duration_seconds','native_session_id'))
        or manifest.get('model_called') not in (None,False)
        or util.tree_hashes(source/'usage/raw')
        or any((source/name).exists() for name in EXECUTION_FILES)
        or any((source/'snapshots').glob('*')) or errors
        or any(e.get('kind')!='planned' for e in staged)
        or any(e.get('kind')=='dispatch' for e in journal))


def preparation_state(item,source):
    """Negative evidence only; an absent Run UUID is never supplied to a manifest."""
    from research import pair_execution,live_pilot as live
    from outer.harness import util
    case=item['cases'][0];batch=source.parent;path=source/'manifest.json'
    campaign_root=Path(item['epoch']['campaign_config']['path']).parent
    for hold_path in (campaign_root/'_exclusion-holds'/case['run_instance_id']).glob('*.json'):
        hold=util.read_json(hold_path)
        if (hold.get('attempt')!=live.reference(item['path'])
                or hold.get('execution_evidence_seen') is True):
            raise ValueError('Previously observed execution evidence prevents an unsent exclusion')
    manifest=util.read_json(path) if path.exists() else {}
    if any(k in manifest and manifest[k]!=case[k] for k in ('run_id','run_instance_id')):
        raise ValueError('Preparation manifest identity mismatch')
    journal=batch/'_control/pair-journal.jsonl';events=pair_execution.events(journal)
    current=pair_execution.state(journal)
    if (current['dispatch'] or len(events)>1 or any(e['kind']!='pair_reserved' for e in events)
            or any(any(b.get(k)!=v for k,v in case.items()) for b in current['reserved'].values())
            or set(current['reserved']) not in (set(),{case['run_id']})):
        error=ValueError('Dispatch or ambiguous preparation journal; not proven unsent')
        error.execution_evidence_seen=bool(current['dispatch'])
        raise error
    if preparation_execution_seen(source,manifest):
        error=ValueError('Runtime/request or ambiguous execution evidence; not proven unsent')
        # Preserve the observed decision even if files disappear before the
        # exception handler writes the hold receipt after releasing its locks.
        error.execution_evidence_seen=True
        raise error
    invocation=batch/'pair-invocation.json'
    if invocation.exists():
        value=util.read_json(invocation)
        if (value.get('owner_plan')!=item['attempt']['epoch']
                or value.get('observer_phase')!=item['attempt']['phase']):
            raise ValueError('Foreign preparation invocation')
    elif source.exists() or events:
        raise ValueError('Materialized preparation lacks its owner invocation')
    refs={str(p):live.reference(p) for p in (journal,invocation) if p.exists()}
    return dict(manifest_present=path.exists(),manifest_uuid_present='run_instance_id' in manifest,
        measured_duration_seconds=manifest.get('duration_seconds'),references=refs,
        invocation_present=invocation.exists())


def observe_preparation(config,item,source):
    """Called under all owner locks. Reuse the existing observer close checker."""
    from research import campaign_launcher,live_pilot as live
    from outer.harness import util,runtime,run
    state=preparation_state(item,source);stop=Path(config['root'])/'STOP'
    if not stop.is_file():raise ValueError('Retained campaign STOP required for preparation exclusion')
    observers=campaign_launcher.close_observers(dict(epoch=item['epoch'],epoch_plan=item['attempt']['epoch'],
        pairs=[item['attempt']['slot']]))
    rows=observers.get('observers',[])
    if any(r.get('shutdown',{}).get('dispatched') for r in rows):
        error=ValueError('Observer retains dispatched execution; not proven unsent')
        error.execution_evidence_seen=True
        raise error
    if (observers.get('confirmed') is not True or (state['invocation_present'] and len(rows)!=1)
            or any(r.get('confirmed') is not True or r.get('shutdown_ack_confirmed') is not True
                or r.get('process_exit_verified') is not True
                or r.get('shutdown',{}).get('phase_sha256')!=item['attempt']['phase']['sha256']
                or r.get('shutdown',{}).get('dispatched')!=[] for r in rows)):
        raise ValueError('Undispatched observer ACK/exit unconfirmed')
    references=dict(state['references'])
    for row in rows:
        directory=live.safe_path(row['directory'])
        if directory.parent!=Path(item['epoch']['batch'])/'_observers':
            raise ValueError('Foreign preparation observer directory')
        for pattern in ('config.json','startup-ready.json','shutdown.json','shutdown-ack.json',
                        'scope-*.json'):
            paths=list(directory.glob(pattern))
            if not paths:raise ValueError('Observer closure evidence disappeared')
            for path in paths:references[str(path)]=live.reference(path)
    resources=[]
    for kind,args in (('containers',('ps','-a')),('networks',('network','ls'))):
        result=runtime.docker(*args,'--filter','label=sample2.instance='+item['cases'][0]['run_instance_id'],
            '--format','{{.ID}}',check=False,timeout=20)
        if result.returncode or result.stdout.strip():
            raise ValueError('Owned resource absence unconfirmed: '+kind)
        resources.append(dict(kind=kind,observed_ids=[],returncode=0))
    if preparation_state(item,source)!=state:raise ValueError('Preparation changed during closure observation')
    return dict(kind=PREPARATION,cause='unknown',interruption_stage='pre_dispatch_preparation',state=state,
        references=references,observer_closure=observers,owned_resources=resources,
        stop_sha256=util.sha256_file(stop),observed_at=run.now(),model_dispatch_proven_absent=True)


def preparation_usage(item,source,evidence):
    """Revalidate the sealed negative evidence without replaying physical probes."""
    from research import live_pilot as live,acquisition_pipeline as pipeline
    if (evidence.get('kind')!=PREPARATION or evidence.get('model_dispatch_proven_absent') is not True
            or evidence.get('state')!=preparation_state(item,source)
            or evidence.get('observer_closure',{}).get('confirmed') is not True
            or evidence.get('owned_resources')!=[dict(kind=k,observed_ids=[],returncode=0)
                for k in ('containers','networks')] or not evidence.get('stop_sha256')):
        raise ValueError('Changed or unconfirmed preparation exclusion')
    for ref in evidence['references'].values():live.checked(ref)
    # Zero counts are justified by the bound no-dispatch/closure proof. The
    # original absent/null measured duration is retained separately in state.
    observed=pipeline.usage_for_manifests([])
    return observed,json.loads(json.dumps(observed)),None


def preparation_exclusion(config,item):
    """Small read-only guard path for a sealed, originally incomplete Run."""
    from research import live_pilot as live,pair_execution
    from outer.harness import util
    if len(item['cases'])!=1:return None
    journal=pair_execution.events(Path(config['root'])/'events.jsonl')
    events=[e for e in journal
        if e['kind']=='attempt_excluded' and e.get('record')==str(item['path'])]
    if not events:return None
    if len(events)!=1:raise ValueError('Duplicate exclusion event')
    path=live.checked(events[0]['exclusion']);value=util.read_json(path)
    if value.get('evidence',{}).get('kind')!=PREPARATION:return None
    case=item['cases'][0]
    reservations=[e for e in journal if e['kind']=='reserved' and e.get('record')==str(item['path'])]
    if (path!=Path(config['root'])/'_exclusions'/(case['run_instance_id']+'.json')
            or len(reservations)!=1 or reservations[0].get('slot')!=item['attempt']['slot']
            or reservations[0].get('attempt')!=item['attempt']['attempt']
            or value.get('kind')!=KIND or value.get('config')!=live.reference(Path(config['root'])/'config.json')
            or value.get('attempt')!=live.reference(item['path'])
            or any(value.get(k)!=case[k] for k in ('run_id','run_instance_id'))
            or value.get('slot')!=item['attempt']['slot'] or events[0]['slot']!=value['slot']
            or value.get('reason')!='controller_interrupted' or value.get('adopted') is not False
            or value.get('retry_requires_new_uuid') is not True or not value.get('authorization_reference')
            or value.get('model_calls')!=0 or value.get('acquisition_count_increment')!=0):
        raise ValueError('Unbound preparation exclusion')
    observed,charged,charge=preparation_usage(item,item['path'].parent/case['run_id'],value['evidence'])
    if (value.get('observed_usage')!=observed or value.get('budget_usage')!=charged
            or value.get('conservative_duration_charge')!=charge):raise ValueError('Excluded preparation usage changed')
    return value


def record_preparation_hold(config,item,source,error,authorization_reference):
    """A refusal retains known raw components and unknowns; it authorizes nothing."""
    from research import live_pilot as live
    from outer.harness import live_usage,util,run
    starts,start_errors=live_usage.journal(source/'usage/raw/started.jsonl')
    rows,row_errors=live_usage.journal(source/'usage/raw/events.jsonl')
    components=[v for row in rows for k in ('input_tokens','output_tokens')
        if isinstance(row.get('usage'),dict) and type(v:=row['usage'].get(k)) is int and v>=0]
    ids=[row.get('request_id') for row in rows]
    distinct=all(isinstance(i,str) and i for i in ids) and len(set(ids))==len(ids)
    raw=util.tree_hashes(source/'usage/raw')
    path=Path(config['root'])/'_exclusion-holds'/item['cases'][0]['run_instance_id']/(uuid.uuid4().hex+'.json')
    util.write_new_json(path,dict(kind='pipeline_preparation_exclusion_held_v1',attempt=live.reference(item['path']),
        reason=type(error).__name__+': '+str(error),authorization_reference=authorization_reference,
        known_usage=dict(request_start_records=len(starts) if starts else None,
            observed_tokens_lower_bound=sum(components) if components and distinct and not row_errors else None,
            observed_token_components=components,
            totals_unknown=True,journal_errors=start_errors+row_errors),
        raw_usage=raw,execution_evidence_seen=bool(getattr(error,'execution_evidence_seen',False)
            or preparation_execution_seen(source)),
        adopted=False,retry_authorized=False,at=run.now()))


def exclude(repo,config_path,record,reason,authorization_reference,evaluation_adjudication=None,*,adjudication=None):
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
    if adjudication is not None:
        if evaluation_adjudication is not None:raise ValueError('Supply one adjudication reference')
        evaluation_adjudication=adjudication
    adjudication=live.reference(evaluation_adjudication) if evaluation_adjudication is not None else None
    if adjudication is not None and reason!='technical_failure':
        raise ValueError('Infrastructure adjudication requires technical_failure')
    manifest_path=source/'manifest.json'
    preparation=reason=='controller_interrupted'
    try:
        if preparation and manifest_path.exists():preparation=not util.read_json(manifest_path).get('started_at')
        with ExitStack() as stack:
            for control in (root/'_pipeline-supervisor',root/'_owner'):
                if (control/'dispatch.lock').exists():
                    stack.enter_context(_existing_lock(control/'dispatch.lock'))
                elif preparation:stack.enter_context(pair_execution.exclusive(control))
            if preparation or adjudication is not None:stack.enter_context(pair_execution.exclusive(source.parent/'_control'))
            if (source/'controller.lock').exists():stack.enter_context(_existing_lock(source/'controller.lock'))
            elif not preparation:raise ValueError('Original Run ownership lock missing')
            if not preparation:
                profiles.validate_run(source)
                manifest=util.read_json(manifest_path)
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
                        or receipt['reason']!=reason or receipt['authorization_reference']!=authorization_reference
                        or receipt['evidence'].get('adjudication')!=adjudication):
                    raise ValueError('Existing exclusion changed; reconcile without dispatch')
                if preparation:preparation_usage(item,source,receipt['evidence'])
                elif adjudication is not None and technical_reason(item,source,reason,adjudication,authorization_reference)!=receipt['evidence']:
                    raise ValueError('Infrastructure adjudication evidence changed')
            else:
                evidence=observe_preparation(config,item,source) if preparation else technical_reason(
                    item,source,reason,adjudication,authorization_reference)
                observed,charged,charge=(preparation_usage(item,source,evidence) if preparation else budget_usage(source))
                acquisition=source.parent/'pipeline-acquisition.json'
                if preparation and acquisition.exists():
                    saved=util.read_json(acquisition)
                    if saved.get('acquired') is not False or saved.get('usage')!=observed:
                        raise ValueError('Existing acquisition is not an unsent preparation')
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
    except Exception as exc:
        if preparation:record_preparation_hold(config,item,source,exc,authorization_reference)
        raise


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
            preparation=value.get('evidence',{}).get('kind')==PREPARATION
            if preparation:
                if preparation_exclusion(config,item)!=value:raise ValueError('Preparation exclusion differs')
                observed,charged,charge=preparation_usage(item,source,value['evidence'])
            else:observed,charged,charge=budget_usage(source)
            if (value['observed_usage']!=observed or value['budget_usage']!=charged
                    or value['conservative_duration_charge']!=charge):raise ValueError('Excluded usage changed')
            if not preparation and technical_reason(item,source,value['reason'],
                    value['evidence'].get('adjudication'),value['authorization_reference'])!=value['evidence']:
                raise ValueError('Technical evidence changed')
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
    parser.add_argument('--adjudication','--evaluation-adjudication',dest='adjudication',type=Path,
        help='Existing private, evidence-bound ruling for an acquisition, persistence or evaluation infrastructure fault')
    args=parser.parse_args();guard().dependencies(args.repo)
    print(json.dumps(exclude(args.repo,args.config,args.record,args.reason,args.authorization_reference,adjudication=args.adjudication)))


if __name__=='__main__':main()
