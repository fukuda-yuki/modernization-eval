"""Explicit four-Run Go readiness pilot; import/create never dispatch a model.

This is a new acquisition with repaired evaluators, not a v5 comparison. Two
independent pair journals retain their publication gates as pending. Observed
request/token/wall thresholds cause a stop after observation, not hard quotas.
There is deliberately no automatic retry, resume, replacement, or main start.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time

from outer.harness import live_usage, profiles, run, runtime, util
from research import pair_execution, next_phase_execution

KIND = 'live_go_readiness_pilot_v1'
MAIN_KIND = 'source_info_repaired_v6_main'
OBSERVER_KIND = 'live_go_readiness_pilot_observer_v1'
RECOVERY_KIND = 'repaired_pilot_readiness_recovery_v1'
RECOVERY_OBSERVER_KIND = 'repaired_pilot_readiness_recovery_observer_v1'
TRIAL_KIND = 'education_go_readiness_trial_v1'
TRIAL_OBSERVER_KIND = 'education_go_readiness_trial_observer_v1'
BOUNDS = dict(max_pairs=2, max_runs=4, run_seconds=1800, provider_seconds=600,
    accumulated_run_seconds=7200, request_count=600, observed_tokens=30000000,
    wall_seconds=9000, disk_free_min_bytes=8 * 1024**3)
DEFAULT_THRESHOLDS = dict(host_memory_available_min_bytes=1536*1024**2,
    docker_memory_available_min_bytes=1536*1024**2, disk_free_min_bytes=8*1024**3,
    cpu_percent_max=95, http_inflight_max=2)
PIN_FILES = ('research/live_pilot.py', 'research/resource_supervisor.py',
    'research/live_pilot_launcher.py',
    'research/repaired_runtime.py', 'research/acquisition_readiness.py',
    'research/next_phase.py','research/next_phase_design.py',
    'research/protocols/source-information-two-families-20261003-v5-100p2.json',
    'research/pair_execution.py', 'research/next_phase_execution.py',
    'research/catalog_environment.py', 'research/wave_campaign.py',
    'outer/harness/machine.py', 'outer/harness/profiles.py', 'outer/harness/run.py',
    'outer/harness/runtime.py', 'outer/harness/gateway.py', 'outer/harness/util.py',
    'outer/harness/ownership.py', 'outer/harness/security.py', 'outer/harness/live_usage.py',
    'outer/harness/evaluate.py', 'outer/harness/preserve.py', 'outer/harness/aggregate.py',
    'outer/profiles/runtimes/deepseek-music-repaired-v1.json',
    'outer/profiles/runtimes/deepseek-education-repaired-v1.json',
    'outer/profiles/task-revisions/evaluators-20261006/MS1-CONT-A.json',
    'outer/profiles/task-revisions/evaluators-20261006/MS1-CONT-B.json',
    'outer/profiles/task-revisions/evaluators-20261006/CU1-ENR-C.json',
    'outer/profiles/task-revisions/evaluators-20261006/CU1-ENR-D.json',
    'inner/spec/requirements-cont-A-1.6.0.json','inner/spec/requirements-cont-B-1.6.0.json',
    'inner/spec/requirements-cu-C-education-1.1.0.json','inner/spec/requirements-cu-D-education-1.1.0.json')


def reference(path):
    path = safe_path(path)
    return {'path':str(path), 'sha256':util.sha256_file(path)}


def safe_path(value):
    text=str(value)
    if text.startswith('\\\\?\\UNC\\'): text='\\\\'+text[8:]
    elif text.startswith('\\\\?\\'): text=text[4:]
    path = Path(text)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Absolute traversal-free pilot path required')
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError('Pilot path contains a link/junction')
    return path.resolve()


def within(path, root):
    path, root = str(path).casefold(), str(root).casefold().rstrip('/\\')
    return path == root or path.startswith(root + ('\\' if '\\' in root else '/'))


def checked(ref):
    path = safe_path(ref['path'])
    if not re.fullmatch('[a-f0-9]{64}', ref['sha256']) or util.sha256_file(path) != ref['sha256']:
        raise ValueError('Pinned pilot reference changed')
    return path


def verify_pins(repo, pins):
    repo = safe_path(repo)
    if not isinstance(pins, dict) or not set(PIN_FILES) <= set(pins):
        raise ValueError('All live controller source pins required')
    for name, digest in pins.items():
        if (not isinstance(name, str) or Path(name).is_absolute() or '..' in Path(name).parts
                or ':' in name or '\\' in name):
            raise ValueError('Unsafe source pin path')
        path = safe_path(repo / name)
        if not within(path, repo) or not re.fullmatch('[a-f0-9]{64}', digest) or util.sha256_file(path) != digest:
            raise ValueError('Live executing source changed: ' + name)


def validate_plan(plan, repo):
    """Pure validation, before mkdir, credential lookup, or observer start."""
    if (plan.get('kind') != KIND or type(plan.get('schema_version')) is not int
            or plan['schema_version'] != 1 or plan.get('require_fixed_instances') is not True
            or type(plan.get('pair_concurrency')) is not int or plan['pair_concurrency'] != 2):
        raise ValueError('Explicit fixed two-pair live pilot required')
    if not re.fullmatch('[a-f0-9]{32}', plan.get('phase_id','')):
        raise ValueError('Fresh pilot phase UUID required')
    if not re.fullmatch('[a-f0-9]{40}', plan.get('source_commit','')):
        raise ValueError('Committed source identity required')
    if plan.get('task_revision') != 'evaluators-20261006':
        raise ValueError('Explicit repaired evaluator task revision required')
    settings = plan.get('settings', {})
    if (settings.get('model_id') != 'deepseek-v4.1-flash' or settings.get('provider') != 'opencode-go'
            or settings.get('use_balance') is not False or settings.get('paid_fallback') is not False):
        raise ValueError('Fixed Go model, balance OFF, no paid fallback required')
    bounds = plan.get('bounds', {})
    if set(bounds) != set(BOUNDS) or any(type(bounds[k]) is not int or bounds[k] != v for k,v in BOUNDS.items()):
        raise ValueError('Exact bounded four-Run pilot required')
    cohort = plan.get('cohort')
    if (not isinstance(cohort,str) or not re.fullmatch(r'[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*',cohort)):
        raise ValueError('Safe relative cohort required')
    base, repo = safe_path(plan['batch']), safe_path(repo)
    protected = plan.get('protected_roots')
    if not isinstance(protected,list) or not protected:
        raise ValueError('Original protected roots must be explicit')
    protected = [safe_path(p) for p in protected] + [repo]
    if any(within(base,p) or within(p,base) for p in protected):
        raise ValueError('Pilot root overlaps a source/protected tree')
    if base.exists():
        for path in base.rglob('*'): safe_path(path)
    # Validate all planned writers before resolving existing path components.
    for relative in ('allocation.json','execution-intent.json','result.json','_control',
                     '_control/dispatch-stop.json','_control/dispatch.lock','_observers'):
        safe_path(base / relative)
    runtimes = {'MS1-CONT-A':'deepseek-music-repaired-v1',
                'CU1-ENR-C':'deepseek-education-repaired-v1'}
    if plan.get('runtime_by_task') != runtimes: raise ValueError('Exact repaired runtime map required')
    assignments = plan.get('assignments')
    if not isinstance(assignments,list) or len(assignments)!=2: raise ValueError('Exactly two pilot pairs required')
    names, instances = set(), set()
    for number, (pair,task) in enumerate(zip(assignments,runtimes),1):
        if type(pair.get('pair')) is not int or pair['pair'] != number:
            raise ValueError('Fixed pilot order required')
        cases=pair.get('cases')
        if not isinstance(cases,list) or len(cases)!=2: raise ValueError('Exactly two arms required')
        batch=safe_path(base / ('pair-'+str(number)))
        for relative in ('phase.json','_control','_control/pair-journal.jsonl','_archive'):
            safe_path(batch/relative)
        for slot,(case,arm) in enumerate(zip(cases,('explore','preload')),1):
            if (case.get('task')!=task or case.get('condition')!=arm
                    or any(type(case.get(k)) is not int or case[k]!=v for k,v in
                           dict(pair=number,slot=slot,attempt=1).items())
                    or case.get('run_id')!=run.run_id_for(task,arm,1)):
                raise ValueError('Fixed task/arm/Run identity mismatch')
            instance=case.get('run_instance_id','')
            if not re.fullmatch('[a-f0-9]{32}',instance) or instance in instances or case['run_id'].casefold() in names:
                raise ValueError('Duplicate/foreign pilot Run identity')
            names.add(case['run_id'].casefold()); instances.add(instance)
            safe_path(run.run_dir_for(batch,case['run_id']))
    thresholds=plan.get('thresholds',{})
    if thresholds!=DEFAULT_THRESHOLDS or any(type(v) is not int for v in thresholds.values()):
        raise ValueError('Exact reviewed operational resource thresholds required')
    verify_pins(repo,plan['source_pins'])
    from research.acquisition_readiness import protected_instance_ids, BASE_PROTOCOL, BASE_PROTOCOL_SHA256
    if plan['source_pins'].get(BASE_PROTOCOL)!=BASE_PROTOCOL_SHA256 or instances & protected_instance_ids():
        raise ValueError('Pilot identities collide with protected original200 or its protocol changed')
    monitor=checked(plan['resource_monitor']); probe=checked(plan['resource_probe'])
    if probe != monitor.with_name('wave_resource_probe.py'): raise ValueError('Wrong monitor sibling probe')
    checked(plan['browser_pin'])
    validate_runtime_locks(plan,repo)
    return plan


def validate_runtime_locks(plan,repo):
    """Both exact repaired namespaces and full accepted-build applicability."""
    from research.repaired_runtime import validate_repaired_runtime_binding
    refs=plan.get('runtime_locks',{})
    runtimes={'MS1-CONT-A':'deepseek-music-repaired-v1','CU1-ENR-C':'deepseek-education-repaired-v1'}
    if set(refs)!=set(runtimes.values()): raise ValueError('Both frozen repaired runtime locks required')
    for task,rid in runtimes.items():
        profile=profiles.read(repo,'runtimes',rid); lockpath=checked(refs[rid])
        if lockpath!=safe_path(profiles.runtime_root(repo,task,profile)/'lock.json'):
            raise ValueError('Foreign repaired runtime namespace')
        lock=util.read_json(lockpath); ledger=profiles.task_profile(repo,task,plan['task_revision'])
        if (lock.get('task_profile_revision')!=plan['task_revision'] or lock.get('runtime_profile_id')!=rid
                or lock.get('evaluator_version')!=ledger['evaluation']['evaluation_version']
                or lock.get('evaluator_project')!=ledger['evaluation']['project']):
            raise ValueError('Repaired runtime/ledger mismatch')
        validate_repaired_runtime_binding(repo,lock)


def owner_plan(path, repo):
    path=safe_path(path); plan=util.read_json(path)
    kind=plan.get('kind')
    if kind==KIND: return validate_plan(plan,repo)
    if kind==MAIN_KIND:
        from research import acquisition_readiness
        if plan.get('operational_pipeline') is True:
            return acquisition_readiness.verify_pipeline_phase(path, repo)
        return acquisition_readiness.verify_main_phase(path,repo)
    if kind==RECOVERY_KIND:
        from research import readiness_recovery
        return readiness_recovery.validate_plan(plan,repo)
    if kind==TRIAL_KIND:
        from research import education_readiness_trial
        return education_readiness_trial.validate_plan(plan,repo)
    raise ValueError('Unsupported live owner kind')


def observer_phase(plan, plan_reference, pair_number, *, batch=None, cohort=None):
    if plan.get('kind') not in (KIND,MAIN_KIND,RECOVERY_KIND,TRIAL_KIND): raise ValueError('Unsupported live owner kind')
    selected=next(p for p in plan['assignments'] if p['pair']==pair_number)
    task=selected['cases'][0]['task']
    observer_kind={KIND:OBSERVER_KIND,MAIN_KIND:MAIN_KIND,RECOVERY_KIND:RECOVERY_OBSERVER_KIND,TRIAL_KIND:TRIAL_OBSERVER_KIND}[plan['kind']]
    result = dict(schema_version=1,kind=observer_kind,owner_kind=plan['kind'],
        original_bundle=plan_reference,phase_id=plan['phase_id']+'-p'+str(pair_number),
        batch=str(batch or Path(plan['batch'])/('pair-'+str(pair_number))),
        cohort=cohort or plan['cohort']+'/pair-'+str(pair_number),
        runtime=plan['runtime_by_task'][task],assignments=[selected],two_pair_blocks=[[pair_number]],
        source_pins=plan['source_pins'],resource_monitor=plan['resource_monitor'],
        resource_probe=plan['resource_probe'],thresholds=plan['thresholds'])
    if plan.get('cases_per_assignment') == 1:
        result['cases_per_assignment'] = 1
    return result


def validate_observer_phase(phase,repo):
    kinds={KIND:OBSERVER_KIND,MAIN_KIND:MAIN_KIND,RECOVERY_KIND:RECOVERY_OBSERVER_KIND,TRIAL_KIND:TRIAL_OBSERVER_KIND}
    if phase.get('owner_kind') not in kinds or phase.get('kind')!=kinds[phase['owner_kind']]:
        raise ValueError('Explicit new live observer required')
    path=checked(phase['original_bundle']); plan=owner_plan(path,repo)
    if plan['kind']!=phase['owner_kind']: raise ValueError('Foreign live owner')
    if len(phase['assignments'])!=1: raise ValueError('One exact pair scope required')
    number=phase['assignments'][0]['pair']
    expected=observer_phase(plan,phase['original_bundle'],number)
    if {k:v for k,v in phase.items() if k!='_digest'}!=expected:
        raise ValueError('Derived live observer phase changed')
    safe_path(phase['batch'])
    return plan


def create(repo, frozen_plan_path):
    """Allocate exclusively; no model or observer process is started."""
    path=safe_path(frozen_plan_path); plan=validate_plan(util.read_json(path),repo)
    base=safe_path(plan['batch']); base.mkdir(parents=False,exist_ok=False)
    ref=reference(path)
    util.write_new_json(base/'allocation.json',dict(kind=KIND,plan=ref,phase_id=plan['phase_id']))
    for pair in plan['assignments']:
        phase=observer_phase(plan,ref,pair['pair']); batch=Path(phase['batch']); batch.mkdir()
        util.write_new_json(batch/'phase.json',phase)
    return {'allocated':True,'plan':ref,'batch':str(base),'model_dispatched':False}


def validate_owned_terminal(root,binding,*,observe_resources=True):
    """Pure bound saved evidence + optional read-only Docker observation.

    Ordinary runtime retains stopped containers. Accept only exact owned stopped
    or observably absent containers, an absent network, and a saved cleanup
    receipt matching the manifest. This never deletes or repairs a resource.
    """
    root=safe_path(root)
    for item in root.rglob('*'): safe_path(item)
    profiles.validate_run(root)
    manifest=util.read_json(root/'manifest.json'); state=util.read_json(root/'runtime.json')
    if (manifest.get('schema_version')!=2 or manifest.get('stop_confirmed') is not True
            or state.get('stop_confirmed') is not True
            or any(v.get(k)!=binding[k] for v in (manifest,state) for k in ('run_id','run_instance_id'))
            or manifest.get('prompt_sha256')!=binding['input_sha256']
            or manifest.get('condition_sha256')!=binding['condition_sha256']):
        raise ValueError('Owned terminal identity/stop/input evidence mismatch')
    cleanup=manifest.get('network_cleanup') or {}
    if (cleanup.get('confirmed') is not True or cleanup.get('status') not in ('removed','absent')
            or any(cleanup.get(k)!=binding[k] for k in ('run_id','run_instance_id'))
            or cleanup.get('network')!=state.get('network')
            or cleanup.get('network_id')!=state.get('network_id')):
        raise ValueError('Owned network cleanup receipt mismatch')
    receipts=[p for p in (root/'evidence/network-cleanup').glob('*-result.json')
              if util.read_json(p)==cleanup]
    if len(receipts)!=1: raise ValueError('Unique original cleanup receipt required')
    normalized=util.read_json(root/'usage/normalized.json'); provenance=util.read_json(root/'usage/provenance.json')
    from research.wave_campaign import journal_health
    condition=util.read_json(root/'condition.json')
    health=journal_health(root.parent,[binding],condition['runtime']['model_id'])
    starts,errors=live_usage.journal(root/'usage/raw/started.jsonl')
    if not starts or errors or not all(health[k] is True for k in ('journal_healthy','http_healthy','provider_healthy')):
        raise ValueError('Complete actual gateway originals required')
    implementation=util.read_json(root/'implementation-receipt.json')
    if (any(implementation.get(k)!=binding[k] for k in ('run_id','run_instance_id'))
            or implementation.get('stop_confirmed') is not True
            or implementation.get('raw')!=util.tree_hashes(root/'usage/raw')
            or implementation.get('snapshot_sha256')!=util.sha256_file(root/'snapshot.json')):
        raise ValueError('Implementation originals/snapshot identity changed')
    native,native_errors=live_usage.journal(root/'evidence/agent.jsonl')
    native_sessions=sorted({e['sessionID'] for e in native if isinstance(e.get('sessionID'),str)})
    native_steps=sum(e.get('type')=='step_finish' for e in native)
    sessions=provenance.get('native_agent_session_ids',[])
    reached = normalized.get('input_reached') is True
    if manifest.get('staged_input'):
        stage = normalized.get('staged_input') or {}
        context = util.read_json(root/'usage/context-evidence.json')
        reached = (context.get('first_request_prompt_reached') is True and stage.get('issues') == []
            and (stage.get('terminal') or {}).get('stop_confirmed') is True)
    if (normalized.get('usage_complete') is not True
            or not reached
            or normalized.get('run_instance_id')!=binding['run_instance_id']
            or provenance.get('run_id')!=binding['run_id'] or provenance.get('run_instance_id')!=binding['run_instance_id']
            or provenance.get('expected_sessions')!=[binding['run_instance_id']]
            or provenance.get('inventory_complete') is not True
            or len(sessions)!=1 or not isinstance(sessions[0],str) or not sessions[0]
            or native_errors or native_sessions!=sessions or native_steps!=provenance.get('native_step_count')
            or type(provenance.get('native_step_count')) is not int or provenance['native_step_count']<1):
        raise ValueError('Native/gateway session and complete usage binding required')
    observations={}
    if observe_resources:
        for role in ('worker','gateway'):
            name=state[role]
            if not re.fullmatch('s2-'+role+'-[a-f0-9]{16}',name): raise ValueError('Unexpected owned container name')
            listing=runtime.docker('ps','-a','--filter','name=^/'+name+'$','--format','{{.Names}}',timeout=10,check=False)
            if listing.returncode: raise RuntimeError('Owned container absence unobservable')
            names=listing.stdout.splitlines()
            if any(n!=name for n in names): raise ValueError('Foreign container listing')
            if name in names:
                item=runtime._owned_container(state,role)
                if item is None or any(item['State'].get(k) for k in ('Running','Paused','Restarting')):
                    raise ValueError('Owned container unresolved')
                observations[role]='owned_stopped_retained'
            else: observations[role]='observed_absent'
        listing=runtime.docker('network','ls','--no-trunc','--format','{{json .}}',timeout=10,check=False)
        if listing.returncode: raise RuntimeError('Owned network absence unobservable')
        import json
        networks=[json.loads(line) for line in listing.stdout.splitlines() if line.strip()]
        if any(n['Name']==state['network'] or n['ID']==state['network_id'] for n in networks):
            raise ValueError('Owned network still present')
        observations['network']='observed_absent'
    return dict(run_id=binding['run_id'],run_instance_id=binding['run_instance_id'],
        native_session_id=sessions[0],source_evidence={str(p.relative_to(root)):util.sha256_file(p) for p in
            [root/'manifest.json',root/'runtime.json',root/'condition.json',root/'usage/normalized.json',
             root/'usage/provenance.json',root/'evidence/agent.jsonl',root/'implementation-receipt.json',
             root/'snapshot.json',receipts[0]]},resource_observations=observations,
        resource_observation_performed=observe_resources)


def observer_terminal_receipt(monitor,plan_path,phase_path,ack):
    """Freeze actual process exit and observer original evidence after stop."""
    process=monitor.process
    receipt=dict(plan=reference(plan_path),phase=reference(phase_path),session=monitor.session,
        directory=str(monitor.directory),generation=monitor.generation,
        observer_pid=process.pid if process is not None else None,
        observer_exit_code=process.poll() if process is not None else None,
        observer_ack_verified=False,evidence_files={},evidence_error=None)
    try:
        directory=safe_path(monitor.directory)
        required=[directory/name for name in ('config.json','shutdown.json','shutdown-ack.json','status.json')]
        from research.resource_supervisor import STARTUP_KINDS,validate_startup_ready
        phase_data=util.read_json(phase_path)
        if phase_data.get('kind') in STARTUP_KINDS: required.append(directory/'startup-ready.json')
        paths=set(required)|set(directory.glob('scope-*.json'))
        for path in paths:
            receipt['evidence_files'][safe_path(path).relative_to(directory).as_posix()]=util.sha256_file(path)
        last=checked(util.read_json(directory/'status.json'))
        receipt['evidence_files'][last.relative_to(directory).as_posix()]=util.sha256_file(last)
        envelope=util.read_json(last)
        for path,digest in envelope['snapshot']['evidence_files'].items():
            ref={'path':path,'sha256':digest}; checked(ref)
            receipt['evidence_files'][safe_path(path).relative_to(directory).as_posix()]=digest
        config=util.read_json(directory/'config.json'); shutdown=util.read_json(directory/'shutdown.json')
        if phase_data.get('kind') in STARTUP_KINDS:
            validate_startup_ready(config,phase_data,receipt['observer_pid'],util.read_json(directory/'startup-ready.json'))
        confirmation=util.read_json(directory/'shutdown-ack.json')
        expected=dict(session=monitor.session,phase_sha256=util.sha256_file(phase_path),generation=monitor.generation)
        scope=monitor.last_scope or monitor.requested_scope
        if (config.get('session')!=monitor.session or safe_path(config['directory'])!=directory
                or config.get('phase')!=reference(phase_path) or scope is None
                or any(row.get(k)!=v for row in (shutdown,confirmation) for k,v in expected.items())
                or shutdown.get('dispatched')!=sorted(set(monitor.scope_provider()['dispatched']) &
                    {b['run_id'] for b in scope['bindings']})
                or confirmation.get('owned_resources_resolved') is not True
                or confirmation.get('monitor_stop_confirmed') is not True):
            raise ValueError('Actual observer shutdown/config/scope binding mismatch')
        receipt['observer_ack_verified']=ack is True and receipt['observer_exit_code']==0
    except Exception as exc:
        receipt['evidence_error']=type(exc).__name__
    return receipt


class PilotWatch:
    """Shared observed ceilings and fail-closed latch for the fixed owner.

    Marker publication precedes stop waits. Stops are attempted concurrently on
    exact known UUIDs only; unknown cleanup cannot become a successful receipt.
    """
    def __init__(self,plan,digest,*,wall_started_at=None):
        self.plan,self.digest=plan,digest
        self.base=Path(plan['batch']); self.lock=threading.RLock()
        self.bindings=[]; self.monitors=[]; self.started=time.monotonic()
        if wall_started_at is not None:
            started=datetime.fromisoformat(wall_started_at)
            if started.tzinfo is None: raise ValueError('UTC-aware durable wall origin required')
            elapsed=(datetime.now(timezone.utc)-started).total_seconds()
            if elapsed<0: raise ValueError('Future wall origin is invalid')
            self.started-=elapsed
        self.run_clocks={}; self.run_durations={}; self.fault=None
        self.terminal_history=set()
        self.native_sessions=set()
        self.done=threading.Event(); self.thread=None
        self.source_repo=None; self.plan_reference=None

    def register(self,batch,binding):
        with self.lock:
            expected=next((c for p in self.plan['assignments'] for c in p['cases']
                           if c['run_id']==binding['run_id']),None)
            if expected is None or any(binding.get(k)!=v for k,v in expected.items()):
                raise ValueError('Foreign watcher assignment')
            if safe_path(batch)!=safe_path(self.base/('pair-'+str(binding['pair']))):
                raise ValueError('Foreign watcher batch')
            key=(str(batch),binding['run_id'])
            if any((str(b),v['run_id'])==key for b,v in self.bindings): raise ValueError('Duplicate owned binding')
            if len(self.bindings)>=self.plan['bounds']['max_runs']: raise ValueError('Fixed owner Run cap')
            self.bindings.append((Path(batch),dict(binding)))

    def restore_completed(self,batch,bindings):
        """Future main caller must enroll all prior terminal dispatches, once.

        This never starts/repeats/re-scores them. The current pair's preparation
        uses register; only prior confirmed stops use this history route.
        """
        for binding in bindings:
            root=safe_path(Path(batch)/binding['run_id'])
            manifest=util.read_json(safe_path(root/'manifest.json'))
            proof=validate_owned_terminal(root,binding)
            if proof['native_session_id'] in self.native_sessions: raise ValueError('Repeated historical native session')
            if (any(manifest.get(k)!=binding[k] for k in ('run_id','run_instance_id'))
                    or manifest.get('stop_confirmed') is not True
                    or (manifest.get('network_cleanup') or {}).get('confirmed') is not True
                    or type(manifest.get('duration_seconds')) not in (int,float)
                    or manifest['duration_seconds']<0):
                raise ValueError('Historical pilot/main stop not confirmed')
            self.register(batch,binding)
            self.run_durations[binding['run_id']]=manifest['duration_seconds']
            self.terminal_history.add(binding['run_id'])
            self.native_sessions.add(proof['native_session_id'])

    def start(self):
        if self.thread is not None: raise ValueError('Watcher starts once')
        self.thread=threading.Thread(target=self._loop,name='live-pilot-observed-stop',daemon=True)
        self.thread.start()

    def counters(self):
        from research.wave_campaign import journal_health
        with self.lock: bindings=list(self.bindings); clocks=dict(self.run_clocks); durations=dict(self.run_durations)
        requests=tokens=0; inflight=0
        for batch,b in bindings:
            root=safe_path(batch/b['run_id'])
            for name in ('manifest.json','usage/raw/started.jsonl','usage/raw/events.jsonl',
                         'usage/raw/failure.jsonl','usage/raw/transmission.jsonl'):
                safe_path(root/name)
            health=journal_health(batch,[b],self.plan['settings']['model_id'])
            if not all(health[k] is True for k in ('journal_healthy','provider_healthy','http_healthy')):
                raise ValueError('Provider/HTTP/identity evidence fault')
            inflight+=health['http_inflight_max']
            starts,errors=live_usage.journal(root/'usage/raw/started.jsonl')
            events,more=live_usage.journal(root/'usage/raw/events.jsonl')
            if errors or more: raise ValueError('Damaged gateway evidence')
            requests+=len(starts)
            tokens+=sum(e['usage']['input_tokens']+e['usage']['output_tokens'] for e in events)
        tick=time.monotonic()
        elapsed={rid:tick-start for rid,start in clocks.items() if rid not in durations}
        return dict(requests=requests,observed_tokens=tokens,http_inflight=inflight,
            accumulated_run_seconds=sum(durations.values())+sum(elapsed.values()),
            longest_active_run_seconds=max(elapsed.values(),default=0),wall_seconds=tick-self.started,
            disk_free_bytes=shutil.disk_usage(self.base).free)

    def check(self):
        # Enrollment changes generation under this same admission lock. Never
        # mistake an in-progress legitimate generation handoff for stale health.
        with self.lock: return self._check_locked()

    def _check_locked(self):
        if self.fault is not None: raise RuntimeError('Live pilot fault latched')
        if self.source_repo is not None: verify_pins(self.source_repo,self.plan['source_pins'])
        if self.plan_reference is not None: checked(self.plan_reference)
        if (self.base/'_control/dispatch-stop.json').exists(): raise RuntimeError('Explicit pilot STOP')
        counters=self.counters(); bounds=self.plan['bounds']
        for key,limit in [('requests','request_count'),('observed_tokens','observed_tokens'),
                          ('accumulated_run_seconds','accumulated_run_seconds'),
                          ('longest_active_run_seconds','run_seconds'),('wall_seconds','wall_seconds')]:
            if counters[key]>=bounds[limit]: raise RuntimeError('Observed threshold: '+key)
        if counters['disk_free_bytes']<bounds['disk_free_min_bytes']: raise RuntimeError('Disk floor')
        for monitor in list(self.monitors):
            sample=monitor.snapshot()
            if sample.get('host_healthy') is not True or sample.get('resource_healthy') is not True:
                raise RuntimeError('Independent observer unhealthy')
            for key,limit in self.plan['thresholds'].items():
                value=counters['http_inflight'] if key=='http_inflight_max' else sample.get(key)
                if type(value) not in (int,float) or (value<limit if key.endswith('_min_bytes') else value>limit):
                    raise RuntimeError('Operational threshold: '+key)
        return counters

    def latch(self,reason):
        with self.lock:
            if self.fault is not None: return
            self.fault={'reason':reason,'plan_sha256':self.digest,'phase_id':self.plan['phase_id'],'at':run.now()}
            bindings=[item for item in self.bindings if item[1]['run_id'] not in self.terminal_history]
            monitors=list(self.monitors)
            paths=[self.base/'_control/dispatch-stop.json']+[
                Path(m.phase_path).parent/'_control'/util.read_json(m.phase_path)['phase_id']/'dispatch-stop.json' for m in monitors]
            for path in paths:
                try:
                    path=safe_path(path)
                    if not path.exists(): util.write_new_json(path,self.fault)
                except (OSError,ValueError): pass
            for batch,b in bindings:
                try:
                    root=safe_path(batch/b['run_id'])
                    safe_path(root/'stop-request.json')
                    if not (root/'stop-request.json').exists():
                        util.write_new_json(root/'stop-request.json',dict(run_id=b['run_id'],
                            run_instance_id=b['run_instance_id'],reason=reason,requested_at=run.now()))
                except (OSError,ValueError): pass
        def stop(item):
            batch,b=item
            try: root=safe_path(batch/b['run_id']); state=safe_path(root/'runtime.json')
            except ValueError: return {'run_id':b['run_id'],'stop_confirmed':False,'error_type':'unsafe_owned_path'}
            if not state.exists(): return {'run_id':b['run_id'],'stop_confirmed':None,'runtime_not_allocated':True}
            identity=util.read_json(state)
            if any(identity.get(k)!=b[k] for k in ('run_id','run_instance_id')):
                return {'run_id':b['run_id'],'stop_confirmed':False,'error_type':'foreign_runtime'}
            try: return runtime.request_stop(root)
            except Exception as exc: return {'run_id':b['run_id'],'stop_confirmed':False,'error_type':type(exc).__name__}
        with ThreadPoolExecutor(max_workers=4) as pool: receipts=list(pool.map(stop,bindings))
        target=safe_path(self.stop_receipt_path())
        util.write_new_json(target,dict(**self.fault,owned_stops=receipts))

    def stop_receipt_path(self):
        return self.base/'_control/stop-receipt.json'

    def _loop(self):
        while not self.done.wait(1):
            try: self.check()
            except Exception as exc:
                self.latch(type(exc).__name__); return

    def finish(self):
        self.done.set()
        if self.thread: self.thread.join(timeout=180)
        return self.thread is None or not self.thread.is_alive()


def serialized_postprocess(process,lock=None):
    def wrapped(*args,**kwargs):
        if lock is None: return process(*args,**kwargs)
        with lock: return process(*args,**kwargs)
    return wrapped


def retire_observer(monitor,budget_watch,*,owned_terminal_confirmed):
    """Atomically leave active health checks, then verify bounded shutdown.

    A normal pair may retire only after both owned terminal proofs. Failure
    paths must already have latched the owner. The shared lock orders removal
    against check()'s copied monitor list; shutdown waits never hold that lock.
    """
    try:
        with budget_watch.lock:
            if budget_watch.fault is None:
                if not owned_terminal_confirmed or monitor not in budget_watch.monitors:
                    raise RuntimeError('Observer retirement lacks owned terminal confirmation')
                # A pre-existing observer fault/exit is not authorized shutdown.
                budget_watch.check()
            if monitor in budget_watch.monitors: budget_watch.monitors.remove(monitor)
    except Exception as exc:
        budget_watch.latch(type(exc).__name__)
        with budget_watch.lock:
            if monitor in budget_watch.monitors: budget_watch.monitors.remove(monitor)
    try:
        ack=monitor.stop()
        confirmation=util.read_json(Path(monitor.directory)/'shutdown-ack.json') if ack else {}
        # The process can latch between the last active check and its exit.
        # Stop's bound ACK/exit plus these final bytes close that race without
        # weakening checks for another pair while this observer shuts down.
        faulted=((Path(monitor.directory)/'fault-latch.json').exists()
                 or confirmation.get('fault_latched') is True)
        if faulted:
            budget_watch.latch('observer_fault_during_shutdown')
        ack=ack and confirmation.get('fault_latched') is False and not faulted
    except Exception:
        ack=False
    if not ack: budget_watch.latch('observer_shutdown_unconfirmed')
    return ack


def execute_owned_pair_scope(repo,owner_plan_path,phase_path,*,budget_watch,postprocess_lock=None,
                             defer_postprocess=False):
    """One real production pair and observer session; no publication/resend."""
    from research.resource_supervisor import ProcessMonitor
    plan=owner_plan(owner_plan_path,repo); phase=util.read_json(checked(reference(phase_path)))
    validate_observer_phase(phase,repo)
    digest=util.sha256_file(owner_plan_path)
    if budget_watch.plan!=plan or budget_watch.digest!=digest: raise ValueError('Foreign owner watcher')
    budget_watch.source_repo=Path(repo); budget_watch.plan_reference=reference(owner_plan_path)
    batch=Path(phase['batch']); bindings=[]; ready=False
    util.write_new_json(safe_path(batch/'pair-invocation.json'),dict(owner_plan=reference(owner_plan_path),
        observer_phase=reference(phase_path),started_at=run.now()))
    scope=lambda: {'pairs':[phase['assignments'][0]['pair']] if bindings else [],
        'assignments':list(bindings),'dispatched':list(admitted)}
    monitor=ProcessMonitor(phase_path,Path(plan['batch'])/'_observers',scope)
    admitted=[]
    def prepare(binding):
        verify_pins(repo,plan['source_pins'])
        binding={**binding,'phase_sha256':util.sha256_file(phase_path)}
        model_options = ({'approved_model_id': plan['settings']['model_id']}
                         if 'campaign_config' in plan else {})
        if 'staged_inputs' in plan:
            model_options['staged_plan'] = util.read_json(checked(plan['staged_inputs'][binding['task']]))
        try:
            manifest=profiles.create(repo,batch,binding['task'],binding['condition'],binding['attempt'],
                phase['runtime'],run_instance_id=binding['run_instance_id'],assignment=binding,
                task_revision=plan['task_revision'], **model_options)
        except Exception as exc:
            if 'staged_inputs' in plan:
                util.write_new_json(batch/'staged-preparation-failure.json',
                    dict(binding=binding,kind='known_pre_dispatch_preparation_failure',at=run.now(),
                         technical_retry_classification='preparation_io_failure' if isinstance(exc,
                             (OSError,subprocess.TimeoutExpired,runtime.DockerCommandError)) else 'unclassified_preparation_failure'))
            raise
        attempt_path = batch/'pipeline-attempt.json'
        if attempt_path.is_file():
            attempt = util.read_json(attempt_path)
            manifest['acquisition'] = dict(slot=attempt['slot'], attempt=attempt['attempt'],
                attempt_record=reference(attempt_path), epoch=reference(owner_plan_path),
                campaign_config=plan.get('campaign_config'))
            run.save_manifest(batch, binding['run_id'], manifest)
        full={**binding,'input_sha256':manifest['prompt_sha256'],'condition_sha256':manifest['condition_sha256']}
        bindings.append(full); budget_watch.register(batch,full)
        return manifest
    @contextmanager
    def admit(binding):
        nonlocal ready
        with budget_watch.lock:
            owner_plan(owner_plan_path,repo); budget_watch.check()
            expected=next(b for b in bindings if b['run_id']==binding['run_id'])
            if any(expected.get(k)!=v for k,v in binding.items()): raise ValueError('Foreign admission binding')
            root=batch/binding['run_id']; condition=profiles.validate_run(root)
            manifest=util.read_json(root/'manifest.json')
            if plan['kind']==TRIAL_KIND:
                from research.education_readiness_trial import validate_input
                validate_input(plan,binding,manifest)
            if (manifest.get('run_instance_id')!=binding['run_instance_id']
                    or manifest['prompt_sha256']!=binding['input_sha256']
                    or manifest['condition_sha256']!=binding['condition_sha256']
                    or condition.get('task_profile_revision')!=plan['task_revision']
                    or condition['runtime']['id']!=phase['runtime']
                    or condition['runtime']['model_id']!=plan['settings']['model_id']
                    or condition['runtime']['timeout_seconds']!=plan['bounds']['run_seconds']
                    or condition['runtime']['provider_timeout_seconds']!=plan['bounds']['provider_seconds']):
                raise ValueError('Prepared pilot condition identity changed')
            if not ready: monitor.enroll_ready(scope()); ready=True
            monitor.admit(expected); budget_watch.check()
            if len(admitted)>=plan.get('cases_per_assignment', 2): raise ValueError('Assignment dispatch cap reached')
            admitted.append(binding['run_id']); budget_watch.run_clocks[binding['run_id']]=time.monotonic()
            yield
    def implement(repo,batch,rid):
        try: return next_phase_execution.guarded_implementation(repo,batch,rid)
        finally:
            with budget_watch.lock:
                budget_watch.run_durations[rid]=time.monotonic()-budget_watch.run_clocks[rid]
    browser=util.read_json(checked(plan['browser_pin']))
    browser_process=next_phase_execution.browser_postprocess({'browser':{'record':browser}})
    # Browser activation changes process environment. A parallel campaign
    # shares this lock across pairs; historical one-pair owners need none.
    postprocess=serialized_postprocess(browser_process,postprocess_lock)
    # Initial healthy samples precede registering this monitor with the watcher.
    ack=False; owned_terminal_confirmed=False
    try:
        monitor.start()
        monitor.enroll_ready(scope())
        with budget_watch.lock: budget_watch.monitors.append(monitor)
        width=plan.get('cases_per_assignment', 2)
        execution=dict(plan_sha256=digest,cohort=phase['cohort'],
            runtime=phase['runtime'],pair_concurrency=width,require_fixed_instances=True)
        if width == 1: execution['cases_per_assignment'] = 1
        result=pair_execution.execute_pair(execution,
            phase['assignments'][0]['cases'],batch,repo=repo,concurrency=width,
            prepare=prepare,implement=implement,postprocess=postprocess,admit=admit,
            defer_postprocess=defer_postprocess)
        budget_watch.check()
        if result.get('reason') not in ('pair_publication_restore_cleanup_required', 'evaluation_pending'):
            budget_watch.latch('pair_execution_held')
        else:
            proofs=[validate_owned_terminal(batch/b['run_id'],b) for b in bindings]
            sessions={p['native_session_id'] for p in proofs}
            if len(sessions)!=width or sessions & budget_watch.native_sessions:
                raise ValueError('Native session reused across Run instances')
            budget_watch.native_sessions.update(sessions)
            util.write_new_json(batch/'owned-terminal-proof.json',dict(plan_sha256=digest,
                phase_sha256=util.sha256_file(phase_path),runs=proofs))
            owned_terminal_confirmed=True
            # Already stopped Runs must never receive a later peer's stop marker.
            with budget_watch.lock:
                budget_watch.terminal_history.update(b['run_id'] for b in bindings)
        return result
    except BaseException as exc:
        budget_watch.latch(type(exc).__name__); raise
    finally:
        ack=retire_observer(monitor,budget_watch,owned_terminal_confirmed=owned_terminal_confirmed)
        terminal=observer_terminal_receipt(monitor,owner_plan_path,phase_path,ack)
        if terminal['observer_ack_verified'] is not True: budget_watch.latch('observer_shutdown_unconfirmed')
        util.write_new_json(batch/'observer-terminal.json',dict(plan_sha256=digest,
            phase_sha256=util.sha256_file(phase_path),**terminal))


def execute(repo,plan_path,approval_path):
    path=safe_path(plan_path); plan=validate_plan(util.read_json(path),repo); digest=util.sha256_file(path)
    approval=util.read_json(checked(reference(approval_path)))
    if (approval.get('plan_sha256')!=digest or approval.get('authorized') is not True
            or approval.get('approved_by')!='user' or not approval.get('authorization_reference')):
        raise ValueError('Exact pilot user authorization required')
    from research import next_phase, catalog_environment
    if (next_phase.git(repo,'rev-parse','HEAD')!=plan['source_commit']
            or next_phase.git(repo,'status','--porcelain')):
        raise ValueError('Clean committed pilot source required')
    catalog_environment.validate(util.read_json(checked(plan['browser_pin'])),repo)
    base=Path(plan['batch'])
    if util.read_json(base/'allocation.json')!=dict(kind=KIND,plan=reference(path),phase_id=plan['phase_id']):
        raise ValueError('Foreign pilot allocation')
    with pair_execution.exclusive(base/'_control'):
        # Exclusive immutable one-shot intent: uncertain interruption is held.
        util.write_new_json(base/'execution-intent.json',dict(plan_sha256=digest,
            approval=reference(approval_path),phase_id=plan['phase_id'],started_at=run.now()))
        watch=PilotWatch(plan,digest); watch.start(); results=[]
        try:
            for pair in plan['assignments']:
                watch.check()
                result=execute_owned_pair_scope(repo,path,base/('pair-'+str(pair['pair']))/'phase.json',budget_watch=watch)
                results.append(result)
                current=pair_execution.state(base/('pair-'+str(pair['pair']))/'_control/pair-journal.jsonl')
                if (watch.fault is not None or len(current['dispatch'])!=2 or len(current['results'])!=2
                        or any(pair_execution._postprocess_fault(r['row']) for r in current['results'].values())
                        or result.get('reason')!='pair_publication_restore_cleanup_required'):
                    watch.latch('pair_not_operationally_complete'); break
        except BaseException as exc:
            watch.latch(type(exc).__name__)
            raise
        finally:
            joined=watch.finish()
            if not joined and watch.fault is None: watch.fault={'reason':'watcher_shutdown_unconfirmed'}
            util.write_new_json(base/'result.json',dict(kind=KIND,plan_sha256=digest,
                pair_results=results,fault=watch.fault,watcher_shutdown_verified=joined,
                publication_gates_complete=False,automatic_main_start=False,
                pilot_operational_complete=len(results)==2 and watch.fault is None and joined))
    return util.read_json(base/'result.json')


def main():
    parser=argparse.ArgumentParser(description=__doc__); sub=parser.add_subparsers(dest='action',required=True)
    for action in ('create','execute'):
        command=sub.add_parser(action); command.add_argument('plan',type=Path); command.add_argument('--repo',type=Path,required=True)
        if action=='execute': command.add_argument('--approval',type=Path,required=True)
    args=parser.parse_args()
    return create(args.repo,args.plan) if args.action=='create' else execute(args.repo,args.plan,args.approval)


if __name__=='__main__': main()
