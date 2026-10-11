"""Bounded outer owner using the original pinned measurement checkout.

Invoke this file by absolute path with --repo pointing at the frozen checkout.
The adapter adds supervision, not another acquisition engine or epoch format.
It never changes config/genesis, model inputs, or existing saved evaluations.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

POLL = .2
SAMPLE_STALE = 30
STOP_SECONDS = 120


def identities():
    path = Path(__file__).with_name('process_identity.py')
    spec = importlib.util.spec_from_file_location('_pipeline_process_identity', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


def dependencies(repo):
    repo = Path(repo).resolve()
    sys.path.insert(0, str(repo))
    from research import live_pilot, pair_execution, acquisition_pipeline, campaign_launcher
    from outer.harness import runtime, util
    if Path(acquisition_pipeline.__file__).resolve() != repo/'research/acquisition_pipeline.py':
        raise ValueError('Use the guard file entrypoint; frozen controller import differs')
    return live_pilot, pair_execution, acquisition_pipeline, campaign_launcher, runtime, util


def context(repo, config_path):
    live, pairs, pipeline, launcher, runtime, util = dependencies(repo)
    from research.campaign_initialization import verify_config
    config_path = live.safe_path(config_path); config = util.read_json(config_path)
    if config_path != Path(config['root'])/'config.json' or 'campaign_plan' not in config:
        raise ValueError('Exact approved pipeline config required')
    verify_config(config, Path(repo))
    return config


def attempts(config):
    """Exact reserved scope, plus durable attempts not yet appended to the journal.

    A partial journal tail is not permission to overlook a late allocation.
    Epoch/phase hashes and original fixed UUIDs still bind every discovered row.
    """
    from research import live_pilot as live, pair_execution
    from outer.harness import util
    root = live.safe_path(config['root'])
    paths = set((root/'epochs').glob('*/data/pair-*/pipeline-attempt.json'))
    events = pair_execution.events(root/'events.jsonl')
    paths.update(Path(e['record']) for e in events if e['kind'] == 'reserved')
    found, seen = [], set()
    for path in sorted(paths):
        path = live.safe_path(path)
        if not path.is_relative_to(root/'epochs'): raise ValueError('Foreign attempt path')
        attempt = util.read_json(path)
        epoch_path = live.checked(attempt['epoch']); epoch = util.read_json(epoch_path)
        phase_path = live.checked(attempt['phase']); phase = util.read_json(phase_path)
        if (epoch.get('campaign_config') != live.reference(root/'config.json')
                or epoch.get('source_pins') != config['source_pins']
                or Path(phase['batch']) != path.parent or phase_path != path.parent/'phase.json'
                or path.parent != Path(epoch['batch'])/('pair-'+str(attempt['slot']))
                or phase.get('assignments') != [p for p in epoch['assignments'] if p['pair'] == attempt['slot']]):
            raise ValueError('Unbound pipeline attempt')
        cases = [c for p in phase['assignments'] for c in p['cases']]
        for case in cases:
            if case['run_instance_id'] in seen: raise ValueError('Duplicate owned Run UUID')
            seen.add(case['run_instance_id'])
        found.append(dict(path=path, attempt=attempt, epoch=epoch, phase=phase, cases=cases))
    return found


def owned_rows(config):
    from outer.harness import util
    rows = []
    for item in attempts(config):
        # The ordinary pipeline writes this receipt only after physical closure.
        # Historical sealed attempts are not the current controller's stop scope.
        if (item['path'].parent/'pipeline-acquisition.json').exists(): continue
        for case in item['cases']:
            root = item['path'].parent/case['run_id']
            state = util.read_json(root/'runtime.json') if (root/'runtime.json').exists() else None
            if state and any(state.get(k) != case[k] for k in ('run_id', 'run_instance_id')):
                raise ValueError('Foreign runtime identity')
            rows.append(dict(root=str(root), **{k: case[k] for k in ('run_id', 'run_instance_id')},
                allocated=state is not None, phase=item['phase']))
    return rows


def budget_snapshot(config):
    """Independent raw usage accounting, including conservative missing duration.

    The original null duration is never filled. A stopped Run's start-to-confirmed
    cleanup interval is a separate conservative budget charge, not a measurement.
    """
    from research import acquisition_pipeline as pipeline
    from outer.harness import util
    budget = pipeline.Budget(config); charges = []
    for item in attempts(config):
        paths = [item['path'].parent/c['run_id']/'manifest.json' for c in item['cases']]
        present = [p for p in paths if p.exists()]
        usage = pipeline.usage_for_manifests(present)
        for path in present:
            manifest = util.read_json(path)
            if manifest['run_instance_id'] not in usage['missing_duration_runs']: continue
            end = (manifest.get('network_cleanup') or {}).get('ended_at') or manifest.get('ended_at')
            if not end or not manifest.get('started_at'): raise ValueError('Missing bounded recovery duration')
            start, end = (datetime.fromisoformat(v) for v in (manifest['started_at'], end))
            if start.tzinfo is None or end.tzinfo is None or end < start:
                raise ValueError('Unbound recovery timestamps')
            seconds = (end-start).total_seconds()
            usage['accumulated_run_seconds'] += seconds
            charges.append(dict(run_instance_id=manifest['run_instance_id'], measured_duration_seconds=None,
                budget_only_seconds=seconds, basis='start_to_confirmed_cleanup_upper_bound'))
        budget.finish(str(item['path']), usage)
        for path in present:
            manifest = util.read_json(path)
            if manifest.get('started_at') and not manifest.get('ended_at'):
                elapsed = (datetime.now(timezone.utc)-datetime.fromisoformat(manifest['started_at'])).total_seconds()
                if elapsed >= config['bounds']['run_seconds']: raise RuntimeError('Run wall ceiling')
    budget.check(None, {})
    return dict(usage=budget.total, conservative_duration_charges=charges)


def latch(config, directory, reason, rows):
    from outer.harness import util, run
    value = dict(kind='pipeline_owner_stop_v1', reason=reason, at=run.now(),
                 pipeline_id=config['pipeline_id'], model_resend=False)
    paths = {Path(config['root'])/'STOP', Path(directory)/'stop.json'}
    errors = []
    for row in rows:
        phase = row['phase']; root = Path(row['root'])
        # A completed original must not acquire a later peer's stop marker.
        try: manifest = util.read_json(root/'manifest.json') if (root/'manifest.json').exists() else {}
        except (OSError,ValueError) as exc:
            manifest={};errors.append(dict(path=str(root/'manifest.json'),error_type=type(exc).__name__))
        if manifest.get('stop_confirmed') is True and (manifest.get('network_cleanup') or {}).get('confirmed') is True:
            continue
        paths.add(Path(phase['batch'])/'_control'/phase['phase_id']/'dispatch-stop.json')
        if root.exists(): paths.add(root/'stop-request.json')
    for path in sorted(paths):
        try:
            if not path.exists(): util.write_new_json(path, value)
        except OSError as exc: errors.append(dict(path=str(path), error_type=type(exc).__name__))
    return errors


def stopped(row):
    """Read-only confirmation; a manifest alone is not physical closure."""
    from outer.harness import runtime, util
    root = Path(row['root'])
    if not (root/'runtime.json').exists(): return not row['allocated']
    state = util.read_json(root/'runtime.json'); manifest = util.read_json(root/'manifest.json')
    if any(value.get(k) != row[k] for value in (state, manifest) for k in ('run_id', 'run_instance_id')):
        raise ValueError('Closure identity mismatch')
    if (manifest.get('stop_confirmed') is not True or state.get('stop_confirmed') is not True
            or (manifest.get('network_cleanup') or {}).get('confirmed') is not True): return False
    for role in ('worker', 'gateway'):
        name=state[role]
        listing=runtime.docker('ps','-a','--filter','name=^/'+name+'$',
                               '--format','{{.Names}}',timeout=10,check=False)
        if listing.returncode: return False
        names=listing.stdout.splitlines()
        if any(n != name for n in names): raise ValueError('Foreign container listing')
        if names:
            item = runtime._owned_container(state, role)
            if item is None or any(item['State'].get(k) for k in ('Running','Paused','Restarting')): return False
    network = runtime.docker('network', 'ls', '--no-trunc', '--format', '{{json .}}', timeout=10, check=False)
    if network.returncode: return False
    return not any(n['ID']==state['network_id'] or n['Name']==state['network']
                   for n in (json.loads(line) for line in network.stdout.splitlines() if line.strip()))


def lease_held(path):
    """OS lock state, not the age of a heartbeat. Never creates/truncates a lock."""
    import errno
    # Open errors are unknown, not proof that a legitimate owner holds a lock.
    with Path(path).open('rb') as stream:
        # Windows mandatory byte locks also deny reads through another handle.
        # Metadata and the lock operation establish state without reading bytes.
        if os.fstat(stream.fileno()).st_size < 1:raise ValueError('Uninitialized ownership lock')
        stream.seek(0)
        if os.name=='nt':
            import msvcrt
            try:msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            except OSError as exc:
                if exc.errno in (errno.EACCES,errno.EAGAIN):return True
                raise
            msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
        else:
            import fcntl
            try:fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return True
            fcntl.flock(stream.fileno(),fcntl.LOCK_UN)
        return False


def close_observers(config):
    from research import campaign_launcher
    groups={}
    for item in attempts(config):
        if (item['path'].parent/'pipeline-acquisition.json').exists(): continue
        key=item['attempt']['epoch']['sha256']
        group=groups.setdefault(key,dict(epoch=item['epoch'],epoch_plan=item['attempt']['epoch'],pairs=[]))
        group['pairs'].append(item['attempt']['slot'])
    results=[]
    for group in groups.values():
        results.append(campaign_launcher.close_observers(group))
    return results


def close_owned(config, repo, directory, reason, child=None, cached=()):
    """Reuse launcher child-handle termination and the ordinary owned stop API.

    Stop helpers are bounded processes; no arbitrary PID is signalled. Re-scan
    after controller exit to account for allocation racing the first STOP.
    """
    from research import campaign_launcher as launcher
    from outer.harness import util
    from outer.harness.security import child_environment
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    rows = {r['run_instance_id']: r for r in cached}; helpers = {}; logs = []
    # STOP must precede discovery, Docker calls, or any possibly blocking work.
    errors = latch(config,directory,reason,[])
    deadline = time.monotonic()+STOP_SECONDS
    def scan():
        try:
            rows.update({r['run_instance_id']: r for r in owned_rows(config)})
        except Exception as exc: errors.append(dict(stage='scope', error_type=type(exc).__name__))
        errors.extend(latch(config, directory, reason, rows.values()))
        for key, row in rows.items():
            if key in helpers: continue
            try:
                root=Path(row['root'])
                if not (root/'runtime.json').exists(): continue
                manifest=util.read_json(root/'manifest.json')
                if (manifest.get('stop_confirmed') is True
                        and (manifest.get('network_cleanup') or {}).get('confirmed') is True): continue
                # Start all allocated stop helpers before any blocking Docker
                # inspection. Each helper fences admission before grace waits.
                target = directory/'stops'/key; target.mkdir(parents=True, exist_ok=False)
                spec = target/'spec.json'; util.write_new_json(spec, row)
                log = (target/'process.log').open('xb'); logs.append(log)
                process = subprocess.Popen([sys.executable, '-B', '-X', 'utf8', str(Path(__file__).resolve()),
                    '_stop', str(spec), '--repo', str(repo)], cwd=repo, env=child_environment(),
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                helpers[key] = process
            except Exception as exc: errors.append(dict(stage='stop_start', run_instance_id=key, error_type=type(exc).__name__))
    try:
        scan()
        grace = min(deadline, time.monotonic()+60)
        while child is not None and child.poll() is None and time.monotonic() < grace: time.sleep(POLL)
        if child is not None: launcher._terminate(child)
        scan()
        while any(p.poll() is None for p in helpers.values()) and time.monotonic() < deadline: time.sleep(POLL)
    finally:
        for process in helpers.values(): launcher._terminate(process)
        for log in logs: log.close()
    closure = []
    for row in rows.values():
        try: confirmed = stopped(row)
        except Exception as exc:
            confirmed = False; errors.append(dict(stage='verify', error_type=type(exc).__name__))
        closure.append(dict(run_id=row['run_id'], run_instance_id=row['run_instance_id'], confirmed=confirmed))
    # The historical observer closer can wait on Docker/ACK. Bound it just as
    # the old launcher bounds its helper, never on the independent safety timer.
    observers=[];observer_process=None
    try:
        target=directory/'observer-close';target.mkdir()
        util.write_new_json(target/'config.json',config)
        with (target/'process.log').open('xb') as log:
            observer_process=subprocess.Popen([sys.executable,'-B','-X','utf8',str(Path(__file__).resolve()),
                '_observers',str(target/'config.json'),'--repo',str(repo)],cwd=repo,env=child_environment(),
                stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            try:observer_process.wait(timeout=30)
            except subprocess.TimeoutExpired:pass
            result_path=target/'result.json'
            observers=util.read_json(result_path) if result_path.exists() else [dict(confirmed=False,reason='observer_closure_unconfirmed')]
    except Exception as exc:observers=[dict(confirmed=False,error_type=type(exc).__name__)]
    finally:
        if observer_process is not None:launcher._terminate(observer_process)
    result = dict(reason=reason, at=datetime.now(timezone.utc).isoformat(),
                  owned_runs=closure, errors=errors, observer_closure=observers,
                  owned_closure_confirmed=not errors and all(r['confirmed'] for r in closure))
    util.write_new_json(directory/'result.json', result)
    return result


def code_binding():
    import hashlib
    return {str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (Path(__file__), Path(__file__).with_name('process_identity.py'),
                      Path(__file__).with_name('pipeline_retry.py'),Path(__file__).with_name('acquisition_pipeline.py'))}


def read_invocation(path):
    value = json.loads(Path(path).read_text(encoding='utf-8'))
    if value['guard_sources'] != code_binding(): raise ValueError('Guard source changed')
    live, _, _, _, _, util = dependencies(value['repo'])
    config = context(value['repo'], live.checked(value['config']))
    return value, config


def watch_owner(owner, lease, done, stop):
    """No heartbeat timeout: a retained process identity and an OS lease."""
    while not done.wait(POLL):
        try: healthy=owner.alive() and lease_held(lease)
        except (OSError,ValueError): healthy=False
        if not healthy:
            stop(); return


def acquired_ownership(directory, value, child_record):
    """A live PID or a busy lock alone never proves this invocation owns it."""
    from research import live_pilot
    from outer.harness import util
    directory=Path(directory);path=directory/'controller-owned.json'
    if not path.exists():return False
    proof=util.read_json(path)
    expected=dict(kind='pipeline_controller_ownership_v1',controller=child_record,
        supervisor=value['supervisor'],config=value['config'],
        invocation=live_pilot.reference(directory/'bound-invocation.json'))
    if proof!=expected or util.read_json(directory/'controller.json')!=child_record:
        raise ValueError('Controller ownership receipt differs from this child/invocation')
    return True


def close_registered(config,value,directory,reason,child,child_record,cached=()):
    """Stop only after bound acquisition; pre-owner failure terminates our child.

    After child exit, reacquire the campaign lock before touching its resources.
    A different legitimate owner that won that race must remain untouched.
    """
    from research import pair_execution,campaign_launcher
    try:owned=acquired_ownership(directory,value,child_record)
    except (OSError,ValueError):owned=False
    if not owned:
        campaign_launcher._terminate(child)
        # Acquisition/receipt publication can race the local termination above.
        try:owned=acquired_ownership(directory,value,child_record)
        except (OSError,ValueError):owned=False
    if not owned:
        return dict(ownership_acquired=False,campaign_stop_requested=False,
                    owned_closure_confirmed=False,reason='controller_never_acquired_or_unproven')
    def stop():
        return dict(close_owned(config,value['repo'],Path(directory)/'supervisor-stop',reason,child,cached),
                    ownership_acquired=True,campaign_stop_requested=True)
    # End only our registered child before taking over the campaign lease. A
    # proof of past ownership must not authorize stopping a newer direct owner
    # that acquired the lease as this controller was unwinding.
    if child.poll() is None:campaign_launcher._terminate(child)
    # Separate lock acquisition failures from errors inside owned cleanup.
    ownership=pair_execution.exclusive(Path(config['root'])/'_owner')
    try:ownership.__enter__()
    except OSError:
        return dict(ownership_acquired=True,campaign_stop_requested=False,
                    owned_closure_confirmed=False,reason='campaign_has_another_owner')
    try:return stop()
    finally:ownership.__exit__(None,None,None)


def controller(invocation):
    value, config = read_invocation(invocation)
    from outer.harness import util
    # The explicit outer control adapter is hash-bound separately. All original
    # runtime/evaluator/helper imports and --repo remain the frozen checkout.
    def load(name):
        spec=importlib.util.spec_from_file_location('_guard_'+name,Path(__file__).with_name(name+'.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
    pipeline=load('acquisition_pipeline');policy=load('pipeline_retry').Exclusions(value['config']['path'])
    identity = identities(); directory = Path(invocation).parent
    with identity.ProcessIdentity(value['supervisor']['pid'], value['supervisor']) as owner:
        if not owner.alive(): raise RuntimeError('Supervisor lost before controller start')
        record=identity.current_identity()
        util.write_new_json(directory/'controller.json',record)
        @contextmanager
        def owned_scope():
            from research import live_pilot
            # Entered strictly inside the pipeline's successfully acquired lock.
            if not owner.alive():raise RuntimeError('Supervisor lost before ownership publication')
            util.write_new_json(directory/'controller-owned.json',dict(kind='pipeline_controller_ownership_v1',
                controller=record,supervisor=value['supervisor'],config=value['config'],
                invocation=live_pilot.reference(invocation)))
            done=threading.Event()
            thread=threading.Thread(target=watch_owner,args=(owner,
                Path(config['root'])/'_pipeline-supervisor/dispatch.lock',done,
                lambda:close_owned(config,value['repo'],directory/'controller-stop','supervisor_lost')),daemon=True)
            thread.start()
            try:yield
            finally:
                done.set();thread.join(STOP_SECONDS+20)
        try:
            pipeline.run_pipeline(Path(value['repo']),Path(value['config']['path']),
                                  attempt_policy=policy,owner_scope=owned_scope)
        finally:
            util.write_new_json(directory/'controller-finished.json', identity.current_identity())


def supervise(invocation):
    value, config = read_invocation(invocation)
    from research import pair_execution, campaign_launcher as launcher
    from outer.harness import util
    from outer.harness.security import child_environment
    identity = identities(); directory = Path(invocation).parent
    separation = identity.assert_durable_host()
    with pair_execution.exclusive(Path(config['root'])/'_pipeline-supervisor'):
        # Bind actual guard identity before spawning any controller.
        value['supervisor'] = identity.current_identity()
        util.write_new_json(directory/'bound-invocation.json', value)
        command = [sys.executable, '-B', '-X', 'utf8', str(Path(__file__).resolve()),
                   '_controller', str(directory/'bound-invocation.json'), '--repo', value['repo']]
        with (directory/'controller.log').open('xb') as log:
            child = subprocess.Popen(command, cwd=value['repo'], env=child_environment(),
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            # Capture creation identity from the actual Popen child before poll
            # can reap it; a later PID-only registration is not ownership proof.
            try:process=identity.ProcessIdentity(child.pid)
            except BaseException:
                launcher._terminate(child);raise
            child_record=process.record
            reason = 'controller_nonzero'; cache = []; sample = {'tick': time.monotonic(), 'error': None}
            done = threading.Event()
            def sample_budget():
                transient_since=None
                while not done.is_set():
                    try:
                        rows = owned_rows(config); result = budget_snapshot(config)
                        sample.update(rows=rows, result=result, tick=time.monotonic())
                        transient_since=None
                    except Exception as exc:
                        transient=(isinstance(exc,(json.JSONDecodeError,PermissionError))
                            or isinstance(exc,ValueError) and ('Incomplete journal tail' in str(exc)
                                or 'Malformed usage journal' in str(exc)))
                        if transient:
                            if transient_since is None: transient_since=time.monotonic()
                            if time.monotonic()-transient_since < 2:
                                done.wait(.05); continue
                        sample.update(error=type(exc).__name__+': '+str(exc)); return
                    done.wait(1)
            thread = threading.Thread(target=sample_budget, daemon=True); thread.start()
            startup_deadline = time.monotonic()+30
            registered=False;owned=False;lease_seen=False;lease_lost=None;ready_written=False
            try:
                while child.poll() is None:
                    if (directory/'controller.json').exists() and not registered:
                        record = util.read_json(directory/'controller.json')
                        if record!=child_record: raise ValueError('Foreign controller registration')
                        registered=True
                    if registered and not owned:owned=acquired_ownership(directory,value,child_record)
                    if not ready_written and time.monotonic() >= startup_deadline:
                        reason='controller_registration_timeout'; break
                    if not process.alive(): reason='controller_lost'; break
                    lock=Path(config['root'])/'_owner/dispatch.lock'
                    if owned and lock.exists():
                        if lease_held(lock): lease_seen=True; lease_lost=None
                        elif lease_seen and not (directory/'controller-finished.json').exists():
                            if lease_lost is None: lease_lost=time.monotonic()
                            if time.monotonic()-lease_lost >= 1: reason='controller_lease_lost'; break
                    if owned and lease_seen and 'result' in sample and not ready_written:
                        util.write_new_json(directory/'ready.json', dict(supervisor=value['supervisor'],
                            controller=record, separation=separation, config=value['config'], guard_sources=value['guard_sources']))
                        ready_written=True
                    cache = sample.get('rows', cache)
                    if (Path(config['root'])/'STOP').exists(): reason='operator_stop'; break
                    age=(datetime.now(timezone.utc)-datetime.fromisoformat(config['started_at'])).total_seconds()
                    if age >= config['bounds']['wall_seconds']: reason='cumulative_wall_budget'; break
                    if sample['error']: reason='independent_budget_or_scope_fault'; break
                    if time.monotonic()-sample['tick'] > SAMPLE_STALE: reason='independent_budget_stale'; break
                    time.sleep(POLL)
                else:
                    reason = 'completed' if child.returncode == 0 else 'controller_nonzero'
                owned=acquired_ownership(directory,value,child_record)
                if reason=='completed' and not owned:reason='controller_never_acquired'
                if reason == 'completed':
                    rows = owned_rows(config)
                    if not all(stopped(row) for row in rows): reason='terminal_closure_unconfirmed'
                if reason != 'completed':
                    closure = close_registered(config,value,directory,reason,child,child_record,cache)
                else: closure = dict(owned_closure_confirmed=True)
                util.write_new_json(directory/'result.json', dict(reason=reason, child_exit_code=child.poll(),
                    at=datetime.now(timezone.utc).isoformat(),closure=closure,
                    last_budget=sample.get('result'), budget_error=sample.get('error')))
                return 0 if reason == 'completed' else 1
            except BaseException:
                close_registered(config,value,directory,'supervisor_exception',child,child_record,cache)
                raise
            finally:
                done.set(); thread.join(1)
                if process is not None: process.close()


def launch(repo, config_path):
    config = context(repo, config_path)
    from research import live_pilot as live, pair_execution
    from outer.harness import util
    from outer.harness.security import child_environment
    if os.name == 'nt': identities().assert_durable_host()
    if (Path(config['root'])/'STOP').exists(): raise ValueError('Pipeline STOP remains latched')
    # A competing launch never emits STOP or touches the legitimate owner.
    with pair_execution.exclusive(Path(config['root'])/'_pipeline-supervisor'):
        with pair_execution.exclusive(Path(config['root'])/'_owner'): pass
    directory = Path(config['root'])/'_supervision'/uuid.uuid4().hex
    directory.mkdir(parents=True)
    value = dict(kind='pipeline_owner_invocation_v1', repo=str(Path(repo).resolve()),
                 config=live.reference(config_path), guard_sources=code_binding(),
                 starter=identities().current_identity(),
                 started_at=datetime.now(timezone.utc).isoformat(),
                 launch_method='windows_terminal_no_job' if os.name=='nt' else 'posix_new_session')
    invocation = directory/'invocation.json'; util.write_new_json(invocation, value)
    with (directory/'supervisor.log').open('xb') as log:
        process = identities().persistent_popen([sys.executable, '-B', '-X', 'utf8', str(Path(__file__).resolve()),
            '_supervise', str(invocation), '--repo', str(repo)], cwd=repo, env=child_environment(),
            stdout=log, stderr=subprocess.STDOUT)
    deadline=time.monotonic()+35
    while time.monotonic() < deadline:
        ready = directory/'ready.json'
        if ready.exists(): return live.reference(ready)
        if process.poll() is not None: raise RuntimeError('Persistent launch failed; inspect '+str(directory))
        time.sleep(POLL)
    # Do not kill a potentially active supervisor merely because registration
    # observation timed out. Its bound startup deadline/closure remains active.
    raise RuntimeError('Launch acknowledgement unavailable; reconcile '+str(directory))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['launch', '_supervise', '_controller', '_stop', '_observers'])
    parser.add_argument('path', type=Path); parser.add_argument('--repo', type=Path, required=True)
    args=parser.parse_args(); dependencies(args.repo)
    if args.action == 'launch': print(json.dumps(launch(args.repo, args.path))); return 0
    if args.action == '_supervise': return supervise(args.path)
    if args.action == '_controller': controller(args.path); return 0
    from outer.harness import runtime, util
    if args.action == '_observers':
        config=util.read_json(args.path)
        result=close_observers(config);util.write_new_json(args.path.parent/'result.json',result)
        return 0 if all(r['confirmed'] for r in result) else 1
    row=util.read_json(args.path); root=Path(row['root'])
    state=util.read_json(root/'runtime.json')
    if any(state.get(k) != row[k] for k in ('run_id','run_instance_id')): raise ValueError('Foreign stop identity')
    # Physical closure cannot depend on a cooperative controller releasing its
    # lease. stop_owned fences first and only touches label-bound containers;
    # request_stop then owns lease-aware terminal evidence/network cleanup.
    physical=runtime.stop_owned(root)
    util.write_new_json(args.path.parent/'physical-stop.json',dict(run_id=row['run_id'],
        run_instance_id=row['run_instance_id'],stop_confirmed=physical))
    result=runtime.request_stop(root)
    util.write_new_json(args.path.parent/'result.json', dict(**result, run_instance_id=row['run_instance_id']))
    return 0 if result['stop_confirmed'] and result.get('network_cleanup',{}).get('confirmed') else 1


if __name__ == '__main__': raise SystemExit(main())
