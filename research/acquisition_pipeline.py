"""Approved acquisition lanes and one independent evaluation/archive lane.

The inherited ledger is snapshotted once. Current inputs, owned resources,
usage, evaluation and archives remain checked; publication/history traversal
are not acquisition admission conditions. Failed attempts remain immutable.
"""
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from pathlib import Path
import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback
import uuid

from outer.harness import evaluate, live_usage, preserve, run, runtime, util
from research import acquisition_readiness as readiness, live_pilot, pair_execution
from research import repaired_campaign as campaign, next_phase_execution


def dispatch_pipeline(items, acquire, evaluate, acquired, evaluated, *, capacity=2, backlog=4):
    """Generation slots reopen before previous evaluations finish; bounded queue."""
    pending = iter(items)
    generation, scoring = {}, {}
    exhausted = False
    with ThreadPoolExecutor(capacity) as generators, ThreadPoolExecutor(1) as evaluator:
        while generation or scoring or not exhausted:
            while not exhausted and len(generation) < capacity and len(scoring) < backlog:
                try: item = next(pending)
                except StopIteration: exhausted = True; break
                generation[generators.submit(acquire, item)] = item
            done, _ = wait([*generation, *scoring], return_when=FIRST_COMPLETED)
            for future in done:
                if future in generation:
                    item = generation.pop(future)
                    result = future.result()
                    acquired(item, result)
                    if result['acquired']:
                        scoring[evaluator.submit(evaluate, item, result)] = item
                else:
                    item = scoring.pop(future)
                    evaluated(item, future.result())


def usage_for(batch):
    return usage_for_manifests(Path(batch).glob('*/manifest.json'))


def usage_for_manifests(paths):
    """Count every known token component once; incomplete request totals stay unknown."""
    result = dict(requests=0, observed_tokens=0, accumulated_run_seconds=0.,
                  dispatched_runs=0, unknown_usage_requests=[], missing_duration_runs=[])
    identities = set()
    for path in paths:
        manifest = util.read_json(path)
        identity = manifest['run_instance_id']
        if identity in identities: raise ValueError('Duplicate Run UUID in usage inventory')
        identities.add(identity)
        if manifest.get('started_at'): result['dispatched_runs'] += 1
        duration = manifest.get('duration_seconds')
        if type(duration) in (int, float) and duration >= 0:
            result['accumulated_run_seconds'] += duration
        else:
            if manifest.get('started_at') and not manifest.get('ended_at'):
                result['accumulated_run_seconds'] += max(0, (datetime.now(timezone.utc)-
                    datetime.fromisoformat(manifest['started_at'])).total_seconds())
            elif manifest.get('started_at'):
                result['missing_duration_runs'].append(identity)
        starts, errors = live_usage.journal(path.parent/'usage/raw/started.jsonl')
        rows, more = live_usage.journal(path.parent/'usage/raw/events.jsonl')
        if errors or more: raise ValueError('Malformed usage journal; retain and reconcile')
        started_ids = [r['request_id'] for r in starts]
        ended_ids = [r['request_id'] for r in rows]
        if len(set(started_ids)) != len(started_ids) or len(set(ended_ids)) != len(ended_ids):
            raise ValueError('Duplicate usage request; reconcile without double counting')
        result['requests'] += len(starts)
        for request_id in set(started_ids) - set(ended_ids):
            result['unknown_usage_requests'].append(dict(run_instance_id=identity,
                request_id=request_id, status='missing_terminal_event', observed_tokens=None))
        for row in rows:
            usage = row.get('usage')
            components = {key: usage[key] if isinstance(usage, dict)
                and type(usage.get(key)) is int and usage[key] >= 0 else None
                for key in ('input_tokens','output_tokens')}
            known = sum(value for value in components.values() if value is not None)
            result['observed_tokens'] += known
            missing = [key for key,value in components.items() if value is None]
            if missing:
                result['unknown_usage_requests'].append(dict(run_instance_id=manifest['run_instance_id'],
                    request_id=row.get('request_id'), status=row.get('status'), observed_tokens=None,
                    **components, observed_tokens_lower_bound=known, missing_usage_fields=missing))
    return result


class Budget:
    def __init__(self, config):
        self.config = config
        self.lock = threading.RLock()
        self.active = {}
        self.finished = {}
        self.total = copy.deepcopy(config['usage'])
        if self.total.get('unknown_usage_requests'):
            self.total['observed_tokens_are_lower_bound'] = True

    def check(self, identity, current):
        with self.lock:
            if identity is not None:self.active[identity] = current
            for field, bound in (('requests','request_count'), ('observed_tokens','observed_tokens'),
                                 ('accumulated_run_seconds','accumulated_run_seconds')):
                value = self.total[field] + sum(x.get(field, 0) for x in self.active.values())
                if value >= self.config['bounds'][bound]: raise RuntimeError('Cumulative budget: '+field)
            origin = datetime.fromisoformat(self.config['started_at'])
            if (datetime.now(timezone.utc)-origin).total_seconds() >= self.config['bounds']['wall_seconds']:
                raise RuntimeError('Cumulative wall budget')
            if Path(self.config['root'], 'STOP').exists(): raise RuntimeError('Explicit operator STOP')

    def finish(self, identity, usage):
        with self.lock:
            if identity in self.finished:
                if self.finished[identity] != usage:
                    raise ValueError("Completed attempt usage changed; reconcile retained evidence")
                return
            self.finished[identity] = copy.deepcopy(usage)
            self.active.pop(identity, None)
            for key in ('requests','observed_tokens','accumulated_run_seconds','dispatched_runs'):
                self.total[key] += usage[key]
            self.total.setdefault('unknown_usage_requests', []).extend(usage['unknown_usage_requests'])
            self.total['observed_tokens_are_lower_bound'] = bool(self.total['unknown_usage_requests'])
            self.total.setdefault('missing_duration_runs', []).extend(usage.get('missing_duration_runs', []))


class PairWatch(live_pilot.PilotWatch):
    """Each fault stops its owned assignment; shared budget remains cumulative."""
    def __init__(self, plan, digest, budget, identity):
        super().__init__(plan, digest)
        self.budget, self.identity = budget, identity

    def stop_receipt_path(self):
        # Separate acquisition owners share the epoch, not an exclusive receipt.
        import hashlib
        owner = hashlib.sha256(self.identity.encode('utf-8')).hexdigest()
        return self.base/'_control'/'acquisitions'/owner/'stop-receipt.json'

    def _check_locked(self):
        if self.fault: raise RuntimeError('Pair acquisition fault: '+self.fault['reason'])
        if self.source_repo: live_pilot.verify_pins(self.source_repo, self.plan['source_pins'])
        if self.plan_reference: live_pilot.checked(self.plan_reference)
        counters = self.counters()
        self.budget.check(self.identity, counters)
        if counters['longest_active_run_seconds'] >= self.plan['bounds']['run_seconds']:
            raise RuntimeError('Run wall ceiling')
        if counters['disk_free_bytes'] < self.budget.config['bounds']['disk_free_min_bytes']:
            raise RuntimeError('Disk floor')
        if counters['http_inflight'] > 2: raise RuntimeError('Pair HTTP ceiling')
        for monitor in list(self.monitors):
            sample = campaign._observer_snapshot(monitor)
            if sample.get('host_healthy') is not True or sample.get('resource_healthy') is not True:
                raise RuntimeError('Independent observer unhealthy')
            for key, limit in self.plan['thresholds'].items():
                if key == 'http_inflight_max': continue
                value = sample.get(key)
                if not isinstance(value, (int,float)) or (value < limit if key.endswith('_min_bytes') else value > limit):
                    raise RuntimeError('Resource threshold: '+key)
        return counters


def create_epoch(repo, config, number):
    root = Path(config['root'])/'epochs'/f'{number:03d}'
    root.mkdir(parents=True, exist_ok=False)
    plan = copy.deepcopy(config['template'])
    plan.update(phase_id=uuid.uuid4().hex, source_commit=config['source_commit'],
        source_pins=config['source_pins'], batch=str(root/'data'),
        cohort='pipeline-'+config['pipeline_id']+'/epoch-'+str(number), operational_pipeline=True)
    plan.pop('campaign_authority', None)
    plan.pop('reservation_only_until_wave_intent', None)
    plan['launch_supervisor'] = live_pilot.reference(repo/'research/live_pilot_launcher.py')
    if 'campaign_plan' in config:
        plan['campaign_config'] = live_pilot.reference(Path(config['root'])/'config.json')
    for rid, reference in plan['runtime_locks'].items():
        plan['runtime_locks'][rid] = live_pilot.reference(repo/'artifacts/runtime'/Path(reference['path']).parent.name/'lock.json')
    for pair in plan['assignments']:
        for case in pair['cases']: case['run_instance_id'] = uuid.uuid4().hex
    target = root/'plan.json'
    util.write_new_json(target, plan)
    readiness.create(repo, target)
    return target


def initialize(repo, summary_path, root):
    """One local, inspectable handoff; no recursive predecessor revalidation."""
    summary = util.read_json(summary_path)
    old_path = live_pilot.checked(summary['plan'])
    old = util.read_json(old_path); old_root = Path(old['batch'])
    if root.exists(): raise ValueError('Pipeline root already exists; resume it')
    if subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip():
        raise ValueError('Commit executing controller first')
    running = runtime.docker('ps','--format','{{.Names}}',timeout=20).stdout.splitlines()
    if any(n.startswith('s2-') for n in running): raise RuntimeError('Owned resources still active')
    from contextlib import ExitStack
    with ExitStack() as stack:
        for name in ('_launcher-owner','_control','_allocation'):
            stack.enter_context(pair_execution.exclusive(old_root/name))
        events = campaign.ledger(old)
        usage = campaign.observed_usage(old)
        if usage['usage_journal_errors'] or usage['missing_duration_runs']:
            raise ValueError('Resolve actual usage corruption or unstopped Runs first')
        template = util.read_json(live_pilot.checked(summary['epoch']))
        accepted = sorted({e['slot'] for e in events if e['kind']=='pair_accepted'})
        reserved = [e for e in events if e['kind']=='pair_attempt_reserved']
        sent = set()
        for row in reserved:
            epoch = util.read_json(live_pilot.checked(row['epoch_plan']))
            batch = Path(epoch['batch'])/f'pair-{row["slot"]}'
            if any(f.stat().st_size for f in batch.glob('*/usage/raw/started.jsonl')):
                sent.add(row['slot'])
        root.mkdir(parents=True)
        if os.name=='nt': subprocess.run(['compact','/C',str(root)],check=True,capture_output=True)
        util.write_new_json(root/'inherited-ledger.json',dict(events=events,source=summary['plan'],usage=usage))
        names = set(old['source_pins']) | {'research/acquisition_pipeline.py'}
        pins = {n:util.sha256_file(repo/n) for n in sorted(names)}
        template['protected_roots'] = list(dict.fromkeys(template['protected_roots']+[str(old_root),summary['repo']]))
        config = dict(kind='acquisition_evaluation_pipeline_v1',pipeline_id=uuid.uuid4().hex,
            root=str(root),repo=str(repo),source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
            source_pins=pins,template=template,inherited=live_pilot.reference(root/'inherited-ledger.json'),
            previous_summary=live_pilot.reference(summary_path),accepted_slots=accepted,
            attempted_slots=sorted(sent),reserved_pair_attempts=len(reserved),usage=usage,bounds=old['bounds'],
            started_at=util.read_json(old_root/'execution-start.json')['started_at'],
            policy=dict(active_model_pairs=2,active_model_runs=4,evaluation_workers=1,
                overlap_evaluation=True,history_validation='once_at_handoff',
                technical_retry='fresh_UUIDs_all_attempts_and_missing_usage_retained',
                quality_selection=False,intermediate_publication=False),
            authorization='User 2026-10-08: remove non-data-quality gates, retry failed acquisition, overlap model and evaluation; retain accurate scoring and all attempts.',
            at=run.now())
        util.write_new_json(root/'config.json',config)
        util.write_new_json(old_root/'_control/stop-events'/('pipeline-handoff-'+config['pipeline_id']+'.json'),
            dict(reason='explicit_stop',permanent=True,successor=live_pilot.reference(root/'config.json'),at=run.now()))
    return config


def evaluate_pair(repo, attempt):
    phase = util.read_json(live_pilot.checked(attempt['phase']))
    batch = Path(phase['batch'])
    plan = util.read_json(live_pilot.checked(attempt['epoch']))
    browser = util.read_json(live_pilot.checked(plan['browser_pin']))
    process = next_phase_execution.browser_postprocess({'browser': {'record': browser}})
    journal = batch/'_control/pair-journal.jsonl'
    execution = dict(plan_sha256=attempt['epoch']['sha256'], cohort=phase['cohort'], runtime=phase['runtime'])
    if plan.get('cases_per_assignment') == 1:
        execution['cases_per_assignment'] = 1
    with pair_execution.exclusive(batch/'_control'):
        current = pair_execution.state(journal)
        assignments = list(current['reserved'].values())
        pair_execution._verify_plan(execution, current)
        rows = {}
        observations = {}
        from research import campaign_reassessment, catalog_environment
        for binding in assignments:
            source = batch/binding['run_id']
            live_pilot.validate_owned_terminal(source, binding)
            condition = util.read_json(source/'condition.json')
            observation = campaign_reassessment._observation(source, condition, binding['run_instance_id'])
            old_archive = (source/'archive-reference.json').exists()
            used = evaluate.used_sequences(source)
            if observation.get('recoverable') and used:
                util.write_new_json(source/('evaluation-recovery-'+uuid.uuid4().hex+'.json'),
                    dict(reason=observation, sequence=max(used)+1, model_calls=0, at=run.now()))
                with catalog_environment.activated(browser, repo):
                    evaluate.score_run(repo, batch, binding['run_id'], sequence=max(used)+1)
            row = process(repo, batch, binding['run_id'], batch/'_archive')
            observation = campaign_reassessment._observation(source, condition, binding['run_instance_id'])
            if (observation.get('recoverable') or row.get('operation_status') == 'cleanup_failed'
                    or row['usage'].get('usage_complete') is not True):
                raise RuntimeError('Evaluator/cleanup/usage incomplete: '+str(observation))
            if old_archive:
                reference = preserve.pack_run(batch/'_archive', source, include=['evidence','workspace'])
                util.write_new_json(source/('reassessment-archive-'+uuid.uuid4().hex+'.json'),reference)
                row['archive'] = reference
            else: reference = util.read_json(source/'archive-reference.json')
            preserve.verify(batch/'_archive', reference['package_id'], reference['sha256'])
            rows[binding['condition']] = row
            observations[binding['condition']] = observation
        # Scored low quality and valid product/build failures are retained equally.
        outcome = dict(accepted=True, slot=attempt['slot'], attempt=attempt['attempt'],
            quality={k:r['quality'] for k,r in observations.items()}, rows=rows,
            observations=observations, at=run.now(), publication=False)
        util.write_new_json(batch/'pipeline-evaluation.json', outcome)
        return outcome


def attempt_runs(batch, attempt):
    """Small additive inventory in the existing acquisition receipt, including unsent slots.

    Evaluation indices remain separate: reassessment never creates another Run
    or another resource charge. Hash references describe this saved checkpoint.
    """
    phase = util.read_json(live_pilot.checked(attempt['phase']))
    state = pair_execution.state(Path(batch)/'_control/pair-journal.jsonl')
    rows = []
    for pair in phase['assignments']:
        for case in pair['cases']:
            root = Path(batch)/case['run_id']
            present = (root/'manifest.json').exists()
            dispatch = state['dispatch'].get(case['run_id'])
            rows.append(dict(run_instance_id=case['run_instance_id'], run_id=case['run_id'],
                slot=attempt['slot'], task=case['task'], condition=case['condition'],
                attempt=attempt['attempt'], materialized=present, dispatch=dispatch,
                manifest=dict(path=str(root/'manifest.json'),
                    sha256_at_acquisition=util.sha256_file(root/'manifest.json')) if present else None,
                missing_reason=None if present else 'not_materialized',
                usage=usage_for_run(root) if present else None))
    return rows


def usage_for_run(root):
    # Preserve per-Run resource and original/reassessment lookup locations;
    # raw usage remains in its existing hash-bound archive, never inline here.
    manifest = util.read_json(Path(root)/'manifest.json')
    return dict(duration_seconds=manifest.get('duration_seconds'),
                raw=util.tree_hashes(Path(root)/'usage/raw'),
                normalized=live_pilot.reference(Path(root)/'usage/normalized.json')
                    if (Path(root)/'usage/normalized.json').exists() else None,
                evaluations_index=str(Path(root)/'evaluations/index.jsonl'))


def pending_acquisition_slots(config, attempts, accepted):
    """Keep saved successful acquisitions out of model retries unless explicitly excluded."""
    held = set()
    for attempt in attempts:
        if attempt['slot'] in accepted: continue
        batch = Path(attempt['record']).parent
        saved = util.read_json(batch/'pipeline-acquisition.json')
        if not saved['acquired']:
            retry = config.get('template',{}).get('staged_retry')
            if retry is not None:
                # A gateway's absence alone does not prove no dispatch. The
                # owner writes dispatch before entering runtime.start, so only
                # a complete reserved journal without that event permits the
                # separately approved preparation retry. Unknown means hold.
                try:
                    current = pair_execution.state(batch/'_control/pair-journal.jsonl')
                    preparation = util.read_json(batch/'staged-preparation-failure.json')
                    attempt_record = util.read_json(attempt['record'])
                    phase = util.read_json(live_pilot.checked(attempt_record['phase']))
                    cases = [c for p in phase['assignments'] for c in p['cases']]
                    binding = preparation['binding']
                    safe = (preparation['kind']=='known_pre_dispatch_preparation_failure'
                        and len(cases)==1 and all(binding.get(k)==v for k,v in cases[0].items())
                        and not current['dispatch'])
                    safe = safe and saved['usage']['dispatched_runs'] == 0 and saved['usage']['requests'] == 0
                    safe = safe and not saved['usage']['unknown_usage_requests']
                    safe = safe and all(not util.read_json(p).get('started_at') for p in batch.glob('*/manifest.json'))
                except (OSError,ValueError,KeyError,TypeError): safe = False
                count = sum(a['slot'] == attempt['slot'] for a in attempts)
                shared = retry.get('retry_budget') == 'shared_reserved_attempts'
                if safe:
                    if shared and preparation.get('technical_retry_classification') != 'preparation_io_failure':
                        held.add(attempt['slot'])
                    if not shared and count > retry['known_pre_dispatch_retries']: held.add(attempt['slot'])
                else:
                    from research import technical_retry
                    eligible = (retry.get('technical_failure_policy') == technical_retry.POLICY
                        and shared
                        and technical_retry.eligible(batch, saved))
                    if eligible: confirm_owned_stopped(batch)
                    else: held.add(attempt['slot'])
            continue
        output = batch/'pipeline-evaluation.json'
        decision = util.read_json(output) if output.exists() else {}
        excluded = ('campaign_plan' not in config and decision.get('accepted') is False
            and decision.get('classification') == 'excluded_incomplete_evaluation_user_authorized_reacquisition'
            and decision.get('retry_requires_new_uuids') is True
            and decision.get('slot') == attempt['slot']
            and decision.get('attempt') == saved['attempt'])
        if not excluded: held.add(attempt['slot'])
    historical = set(config['attempted_slots']) | {a['slot'] for a in attempts}
    pending = [n for n in range(1, config.get('pair_count', 100)+1) if n not in accepted and n not in held]
    pending.sort(key=lambda n:(n in historical,n))
    return pending, held


def run_pipeline(repo, config_path, *, attempt_policy=None):
    config = util.read_json(config_path)
    if 'campaign_plan' in config:
        from research.campaign_initialization import verify_config
        verify_config(config, repo)
        if Path(config_path).resolve() != Path(config['root'])/'config.json':
            raise ValueError('Foreign campaign config path')
    root = Path(config['root']); events_path = root/'events.jsonl'
    def event(kind, **fields):
        with event_lock:
            pair_execution.append(events_path, dict(kind=kind, **fields))
    event_lock = threading.RLock()
    with pair_execution.exclusive(root/'_owner'):
        live_pilot.verify_pins(repo, config['source_pins'])
        existing = pair_execution.events(events_path)
        accepted = set(config['accepted_slots']) | {e['slot'] for e in existing if e['kind']=='accepted'}
        attempts = [e for e in existing if e['kind']=='reserved']
        if len({a['record'] for a in attempts}) != len(attempts):
            raise ValueError('Duplicate attempt reservation; reconcile without dispatch')
        selected = {}
        for e in existing:
            if e['kind'] == 'accepted':
                if e['slot'] in selected:
                    raise ValueError('Duplicate selected slot; reconcile without dispatch')
                selected[e['slot']] = e
        for a in attempts:
            record = live_pilot.safe_path(a['record'])
            if not record.is_relative_to(root/'epochs'):
                raise ValueError('Foreign attempt output')
        if attempts:
            # Resume saved completed acquisitions before considering fresh UUIDs.
            for attempt in attempts:
                if attempt['slot'] in accepted: continue
                if attempt_policy is not None and attempt_policy.excluded(attempt): continue
                path = Path(attempt['record'])
                if (path.parent/'pipeline-evaluation.json').exists():
                    result = util.read_json(path.parent/'pipeline-evaluation.json')
                    if result['accepted']:
                        event('accepted', slot=attempt['slot'], record=live_pilot.reference(path.parent/'pipeline-evaluation.json'))
                        accepted.add(attempt['slot'])
            if any(not (Path(a['record']).parent/'pipeline-acquisition.json').exists() for a in attempts):
                raise RuntimeError('Interrupted owned acquisition: recover owned resources before restart')
        budget = Budget(config)
        for a in attempts:
            saved = util.read_json(Path(a['record']).parent/'pipeline-acquisition.json')
            budget.finish(a['record'], attempt_policy.usage(a,saved) if attempt_policy is not None else saved['usage'])
        reserved_count = config['reserved_pair_attempts'] + len(attempts)
        event('started', pid=os.getpid(), accepted=sorted(accepted))
        complete = threading.Event()
        def heartbeat():
            while not complete.is_set():
                with budget.lock:
                    value = dict(at=run.now(), pid=os.getpid(), accepted_slots=sorted(accepted),
                        active_acquisitions=len(budget.active), usage=copy.deepcopy(budget.total),
                        disk_free_bytes=shutil.disk_usage(root).free)
                try: util.write_json_atomic(root/'heartbeat.json', value)
                except PermissionError:
                    event('heartbeat_write_retry', reason='Windows sharing violation')
                complete.wait(20)
        beat = threading.Thread(target=heartbeat, daemon=True); beat.start()
        try:
            # Saved postprocessing is retried without model calls; intents/errors are retained.
            for a in attempts:
                batch = Path(a['record']).parent
                if a['slot'] in accepted: continue
                if attempt_policy is not None and attempt_policy.excluded(a): continue
                saved = util.read_json(batch/'pipeline-acquisition.json')
                if saved['acquired']:
                    result = evaluation_process(repo, a['record'])
                    if result['accepted']:
                        accepted.add(a['slot']); event('accepted', slot=a['slot'], record=live_pilot.reference(batch/'pipeline-evaluation.json'))
            while len(accepted) < config.get('pair_count', 100):
                budget.check(None,{})
                number = 1 + len(list((root/'epochs').glob('*/plan.json')))
                epoch = create_epoch(repo, config, number); plan = util.read_json(epoch)
                eligible_history=[a for a in attempts if attempt_policy is None or not attempt_policy.excluded(a)]
                pending, held = pending_acquisition_slots(config, eligible_history, accepted)
                historical_slots=set(config['attempted_slots'])|{a['slot'] for a in attempts}
                pending.sort(key=lambda n:(n in historical_slots,n))
                if held: event('saved_evaluation_held', slots=sorted(held), model_resend=False)
                if not pending: raise RuntimeError('Saved evaluation recovery required; no model resend')
                historical = set(config['attempted_slots']) | {a['slot'] for a in attempts}
                remaining = config['bounds']['max_pair_attempts'] - reserved_count
                if remaining <= 0: raise RuntimeError('Attempt budget exhausted')
                pending = pending[:remaining]
                consecutive_faults = 0
                def acquire(slot):
                    nonlocal reserved_count
                    budget.check(None,{})
                    phase_path = Path(plan['batch'])/f'pair-{slot}'/'phase.json'
                    batch = phase_path.parent
                    attempt = dict(slot=slot, attempt=uuid.uuid4().hex, epoch=live_pilot.reference(epoch),
                        phase=live_pilot.reference(phase_path), at=run.now(),
                        reason='unattempted_slot' if slot not in historical else 'incomplete_measurement_retry_new_uuids')
                    record = batch/'pipeline-attempt.json'; util.write_new_json(record, attempt)
                    event('reserved', slot=slot, attempt=attempt['attempt'], record=str(record))
                    with event_lock:
                        reserved_count += 1
                        attempts.append(dict(slot=slot, record=str(record)))
                    watch = PairWatch(plan, util.sha256_file(epoch), budget, str(record))
                    error = None; acquired = False; reason = 'acquisition_interrupted'
                    try:
                        watch.check(); watch.start()
                        result = live_pilot.execute_owned_pair_scope(repo, epoch, phase_path,
                            budget_watch=watch, defer_postprocess=True)
                        reason = result.get('reason') or 'acquisition_incomplete_unspecified'
                        acquired = reason == 'evaluation_pending' and not watch.fault
                    except Exception as exc:
                        error = dict(type=type(exc).__name__, message=str(exc), traceback=traceback.format_exc())
                    finally:
                        joined = watch.finish()
                    if not joined: raise RuntimeError('Owned acquisition watcher did not stop')
                    confirm_owned_stopped(batch)
                    usage = usage_for(batch)
                    budget.finish(str(record), usage)
                    value = dict(acquired=acquired, slot=slot, attempt=attempt['attempt'], record=str(record),
                        usage=usage, reason=reason, runs=attempt_runs(batch, attempt),
                        fault=watch.fault, error=error, at=run.now())
                    retry = config.get('template',{}).get('staged_retry',{})
                    if not acquired and retry.get('technical_failure_policy') == 'sealed_infrastructure_failure_v1':
                        from research import technical_retry
                        value['technical_retry_evidence'] = technical_retry.seal(batch, usage)
                    util.write_new_json(batch/'pipeline-acquisition.json', value)
                    return value
                def acquired(slot, result):
                    nonlocal consecutive_faults
                    event('acquired' if result['acquired'] else 'technical_failure', slot=slot,
                        record=result['record'], fault=result['fault'], error=result['error'])
                    consecutive_faults = 0 if result['acquired'] else consecutive_faults+1
                    if (consecutive_faults >= 3 and config.get('template',{}).get('staged_retry',{}).get(
                            'retry_budget') != 'shared_reserved_attempts'):
                        raise RuntimeError('Repeated acquisition faults need repair')
                def evaluated(slot, result):
                    if not result['accepted']:
                        event('evaluation_fault', slot=slot, error=result.get('error'))
                        return
                    accepted.add(slot)
                    event('accepted', slot=slot, quality=result['quality'],
                        record=live_pilot.reference(Path(result['batch'])/'pipeline-evaluation.json'))
                def score(slot, result):
                    event('evaluation_started', slot=slot, record=result['record'])
                    return evaluation_process(repo,result['record'])
                dispatch_pipeline(pending, acquire, score, acquired, evaluated,
                    capacity=config['policy']['active_model_pairs'])
                # Persistent evaluation faults require saved-result repair, not a new model attempt.
                if any(a['slot'] not in accepted and (attempt_policy is None or not attempt_policy.excluded(a))
                       and util.read_json(Path(a['record']).parent/'pipeline-acquisition.json')['acquired']
                       for a in attempts):
                    raise RuntimeError('Saved evaluation recovery required; no model resend')
            event('complete', accepted=sorted(accepted), usage=budget.total)
        except Exception as exc:
            event('stopped', error=type(exc).__name__+': '+str(exc), traceback=traceback.format_exc())
            raise
        finally:
            complete.set(); beat.join(3)


def evaluation_process(repo, record):
    batch = Path(record).parent
    output = batch/'pipeline-evaluation.json'
    if output.exists(): return dict(util.read_json(output), batch=str(batch))
    # Re-evaluate an old epoch with its own frozen controller and browser pins.
    saved_attempt = util.read_json(record)
    saved_plan = util.read_json(live_pilot.checked(saved_attempt['epoch']))
    repo = live_pilot.checked(saved_plan['launch_supervisor']).parent.parent
    live_pilot.verify_pins(repo, saved_plan['source_pins'])
    for attempt in range(2):
        with (batch/'pipeline-evaluation.log').open('ab') as log:
            result = subprocess.run([sys.executable,'-B','-X','utf8','-m','research.acquisition_pipeline',
                'evaluate',str(record),'--repo',str(repo)],cwd=repo,stdout=log,stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        if result.returncode == 0 and output.exists(): break
        pair_execution.append(batch/'evaluation-recovery-events.jsonl',
            dict(kind='saved_artifact_retry', attempt=attempt+1, exit_code=result.returncode, model_calls=0))
    else: return dict(accepted=False, error='evaluation process failed; see '+str(batch/'pipeline-evaluation.log'))
    return dict(util.read_json(output),batch=str(batch))


def confirm_owned_stopped(batch):
    """Only exact resources owned by this attempt may be stopped or inspected."""
    for manifest_path in Path(batch).glob('*/manifest.json'):
        root = manifest_path.parent
        if not (root/'runtime.json').exists(): continue
        state = util.read_json(root/'runtime.json')
        manifest = util.read_json(manifest_path)
        if any(state.get(k) != manifest.get(k) for k in ('run_id','run_instance_id')):
            raise RuntimeError('Owned cleanup identity mismatch')
        for role in ('worker','gateway'):
            container = runtime._owned_container(state,role)
            if container and container['State']['Running']:
                runtime.request_stop(root)
                container = runtime._owned_container(util.read_json(root/'runtime.json'),role)
                if container and container['State']['Running']:
                    raise RuntimeError('Owned resource still active')
        manifest = util.read_json(manifest_path)
        if manifest.get('stop_confirmed') is not True:
            raise RuntimeError('Owned stop unconfirmed')
        if (manifest.get('network_cleanup') or {}).get('confirmed') is not True:
            raise RuntimeError('Owned network cleanup unconfirmed')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['init','run','evaluate']); parser.add_argument('path',type=Path)
    parser.add_argument('--approval', type=Path)
    parser.add_argument('--repo',type=Path,required=True); args=parser.parse_args()
    if args.action=='init':
        if args.approval is None: parser.error('init requires --approval bound to the exact plan hash')
        from research.campaign_initialization import initialize as initialize_campaign
        initialize_campaign(args.repo, args.path, args.approval)
    elif args.action=='evaluate': evaluate_pair(args.repo,util.read_json(args.path))
    else: run_pipeline(args.repo,args.path)


if __name__=='__main__': main()
