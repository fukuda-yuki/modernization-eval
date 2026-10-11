"""Prospective independent resource observer and gateway-only fence responder.

No credential access, model dispatch, cohort journal, manifest/runtime writes,
worker stops, collection, scoring or publication. Controller retains those jobs.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

from outer.harness import runtime, util
from outer.harness.preserve import native_path

KIND = 'source_info_v5_central_fixed_wave_phase_v8'
V9_KIND = 'source_info_v5_central_fixed_wave_phase_v9'
V10_KIND = 'source_info_v5_central_fixed_wave_phase_v10'
FIXTURE_KIND = 'nonmodel_resource_supervisor_fixture_v1'
LIVE_PILOT_KIND = 'live_go_readiness_pilot_observer_v1'
REPAIRED_MAIN_KIND = 'source_info_repaired_v6_main'
RECOVERY_KIND = 'repaired_pilot_readiness_recovery_observer_v1'
TRIAL_KIND = 'education_go_readiness_trial_observer_v1'
CADENCE_SECONDS, STALE_SECONDS = 10, 30
STARTUP_SECONDS = 60
STARTUP_KINDS = (REPAIRED_MAIN_KIND, RECOVERY_KIND, TRIAL_KIND)


def now():
    return datetime.now(timezone.utc).isoformat()


def bounded_probe(probe, owned_roots):
    """Retry a collector timeout once inside the existing ten-second deadline.

    A fresh successful observation is required; never reuse stale counters or
    retry ownership/identity/resource faults. Keep the failed observation in the
    saved sample so transport missingness remains visible.
    """
    tick = time.monotonic()
    first = probe(owned_roots)
    if first.get('ok') is not False or first.get('error') not in ('TimeoutExpired', 'probe_timeout'):
        return first
    remaining = 10 - (time.monotonic() - tick)
    if remaining <= 0:
        return first
    result = dict(probe(owned_roots, timeout_seconds=remaining))
    elapsed = time.monotonic() - tick
    if result.get('ok') is True and elapsed > 10:
        result = {'schema': result.get('schema'), 'ok': False,
                  'error': 'probe_deadline_exceeded', 'late_collector_result': result}
    result.update(retry_evidence=[first], probe_elapsed_seconds=elapsed,
                  probe_started_at=first.get('probe_started_at'), timeout_seconds=10)
    return result


def checked(reference):
    path = Path(reference['path']).resolve()
    if util.sha256_file(path) != reference['sha256']:
        raise ValueError('Supervisor dependency changed')
    return path


def load_monitor(phase):
    path = checked(phase['resource_monitor'])
    if checked(phase['resource_probe']) != path.with_name('wave_resource_probe.py'):
        raise ValueError('Incorrect collector dependency')
    spec = importlib.util.spec_from_file_location('_independent_resource_monitor', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if (module.CADENCE_SECONDS, module.STALE_SECONDS) != (10, 30):
        raise ValueError('Existing cadence/deadline required')
    if (phase.get('cases_per_assignment') == 1
            and 1 not in getattr(module.ResourceMonitor, 'SUPPORTED_RUN_COUNTS', ())):
        raise ValueError('Pinned collector must explicitly support a one-Run scope')
    return module.ResourceMonitor


def validate_scope(phase, request):
    if request['phase_sha256'] != phase['_digest']:
        raise ValueError('Incorrect phase generation')
    cases = {c['run_id']: c for p in phase['assignments'] for c in p['cases']}
    bindings = request['bindings']
    allowed = (0, 1) if phase.get('cases_per_assignment') == 1 else (0, 2, 4)
    if len(bindings) not in allowed or len({b['run_id'] for b in bindings}) != len(bindings):
        raise ValueError('Invalid fixed scope')
    if bindings and sorted({b['pair'] for b in bindings}) != request['pairs']:
        raise ValueError('Wave differs from enrolled bindings')
    if bindings:
        if request['pairs'] not in phase['two_pair_blocks']:
            raise ValueError('Exact predefined two-pair block required')
        wanted = [c for p in phase['assignments'] if p['pair'] in request['pairs'] for c in p['cases']]
        if [b['run_id'] for b in bindings] != [c['run_id'] for c in wanted]:
            raise ValueError('Complete assigned block required')
    for binding in bindings:
        case = cases.get(binding['run_id'])
        if case is None or any(binding.get(k) != v for k, v in case.items()):
            raise ValueError('Unassigned Run identity')
        if (binding['phase_sha256'] != phase['_digest']
                or binding['plan_sha256'] != phase['original_bundle']['sha256']
                or binding['cohort'] != phase['cohort'] or binding['runtime'] != phase['runtime']):
            raise ValueError('Incorrect scoped plan/runtime')
        root = (Path(phase['batch']) / binding['run_id']).resolve()
        if not root.is_relative_to(Path(phase['batch']).resolve()):
            raise ValueError('Foreign root')
        manifest = util.read_json(root / 'manifest.json')
        assigned = manifest.get('assignment', manifest)
        if (any(assigned.get(k) != v for k, v in case.items())
                or manifest['run_id'] != binding['run_id']
                or manifest['run_instance_id'] != binding['run_instance_id']
                or manifest['prompt_sha256'] != binding['input_sha256']
                or manifest['condition_sha256'] != binding['condition_sha256']):
            raise ValueError('Prepared input binding changed')
    return [dict(b) for b in bindings]


def roots(phase, bindings):
    result = []
    for binding in bindings:
        root = Path(phase['batch']) / binding['run_id']
        if not (root / 'runtime.json').exists():
            continue
        state = util.read_json(root / 'runtime.json')
        manifest = util.read_json(root / 'manifest.json')
        if any(state.get(k) != binding[k] or manifest.get(k) != binding[k]
               for k in ('run_id', 'run_instance_id')):
            raise ValueError('Foreign runtime identity')
        if state.get('worker_started_at') or manifest.get('stop_confirmed'):
            result.append(root)
    return result


def safe_environment():
    """OS/Docker lookup only; never inherit provider credentials."""
    names = ('PATH', 'SystemRoot', 'WINDIR', 'COMSPEC', 'TEMP', 'TMP',
             'USERPROFILE', 'HOMEDRIVE', 'HOMEPATH', 'PATHEXT', 'LOCALAPPDATA')
    return {name: os.environ[name] for name in names if name in os.environ}


def validate_startup_ready(config,phase,pid,receipt,*,current_source=True):
    """A fresh startup receipt is process/config/source-bound, not health.

    Live startup and closure also require the executing supervisor source;
    saved historical receipts are verified under their own phase pins."""
    directory=native_path(config['directory'])
    if (type(receipt.get('schema_version')) is not int or receipt['schema_version']!=1
            or receipt.get('kind')!='resource_observer_startup_ready_v1' or receipt.get('validated') is not True
            or type(pid) is not int or pid<=0 or type(receipt.get('observer_pid')) is not int
            or receipt['observer_pid']!=pid
            or any(receipt.get(k)!=config[k] for k in ('session','phase','directory'))
            or receipt.get('config_sha256')!=util.sha256_file(directory/'config.json')
            or receipt.get('source_sha256')!=phase.get('source_pins',{}).get('research/resource_supervisor.py')
            or current_source and receipt.get('source_sha256')!=util.sha256_file(__file__)
            or util.sha256_file(checked(config['phase']))!=config['phase']['sha256']):
        raise ValueError('Foreign or unverified observer startup receipt')
    return True


# Controller readers (ProcessMonitor._status) open status.json with Python's
# open(), i.e. without FILE_SHARE_DELETE; while one holds it, the Windows
# replace below fails with a sharing violation. Retry only that, boundedly
# (about 1.9 s total); a persistent denial still raises and latches STOP.
POINTER_REPLACE_RETRIES = 20


def publish_pointer(path, value):
    path = Path(path)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with temp.open('w', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    for attempt in range(POINTER_REPLACE_RETRIES):
        try:
            temp.replace(path)
            return
        except PermissionError:
            if os.name != 'nt' or attempt == POINTER_REPLACE_RETRIES - 1:
                raise
            time.sleep(0.01 * (attempt + 1))


class ProcessMonitor:
    """Controller proxy; status is evidence, never an old-session health cache."""
    def __init__(self, phase_path, directory, scope_provider):
        self.phase_path = Path(phase_path).resolve()
        self.digest = util.sha256_file(self.phase_path)
        self.directory = native_path(Path(directory).resolve() / uuid.uuid4().hex)
        self.directory.mkdir(parents=True)
        self.scope_provider = scope_provider
        self.session = self.directory.name
        self.generation = 0
        self.acknowledged_generation = 0
        self.last_scope = None
        self.requested_scope = None
        self.process = None

    def start(self):
        if self.process is not None:
            raise RuntimeError('Supervisor starts once')
        startup_required=util.read_json(self.phase_path).get('kind') in STARTUP_KINDS
        config = self.directory / 'config.json'
        util.write_new_json(config, {'phase': {'path': str(self.phase_path), 'sha256': self.digest},
            'session': self.session, 'directory': str(self.directory)})
        self.log = (self.directory / 'process.log').open('xb')
        self.process = subprocess.Popen([sys.executable, '-B', '-X', 'utf8', '-m',
            'research.resource_supervisor', '--config', str(config)], env=safe_environment(),
            stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            cwd=Path(__file__).resolve().parents[1])
        try:
            phase=util.read_json(self.phase_path)
            if phase.get('kind') in STARTUP_KINDS:
                self.wait_startup(util.read_json(config),phase)
            self.enroll(self.scope_provider())
        except BaseException:
            if not startup_required: raise  # Historical caller recovery remains unchanged.
                # Nothing was enrolled or admitted. Bound only this Popen
                # handle; do not discover/kill PIDs or touch worker resources.
            try:
                if self.process.poll() is None: self.process.terminate()
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait(timeout=5)
            finally:
                self.log.close()
            raise
        return self

    def wait_startup(self,config,phase):
        deadline=time.monotonic()+STARTUP_SECONDS
        receipt=self.directory/'startup-ready.json'
        while time.monotonic()<deadline:
            if self.process.poll() is not None: raise RuntimeError('Observer exited before startup ready')
            if receipt.exists():
                try: ready=util.read_json(receipt)
                except json.JSONDecodeError: pass  # Writer has not completed its exclusive JSON yet.
                else:
                    validate_startup_ready(config,phase,self.process.pid,ready)
                    return
            time.sleep(.05)
        raise RuntimeError('Observer validation startup deadline exceeded')

    def enroll(self, scope):
        value = {'pairs': scope['pairs'], 'bindings': scope['assignments']}
        if value == self.last_scope:
            return
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError('Independent observer unavailable')
        self.generation += 1
        request = {'phase_sha256': self.digest, 'session': self.session,
                   'generation': self.generation, **value}
        self.requested_scope = value
        util.write_new_json(self.directory / f'scope-{self.generation:06d}.json', request)
        ack = self.directory / f'scope-ack-{self.generation:06d}.json'
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if ack.exists():
                value_ack = util.read_json(ack)
                if (value_ack.get('scope_sha256') != util.sha256_file(
                        self.directory / f'scope-{self.generation:06d}.json')
                        or value_ack.get('session') != self.session
                        or value_ack.get('accepted') is not True):
                    raise ValueError('Invalid observer enrollment ACK')
                self.last_scope = value
                self.acknowledged_generation = self.generation
                return
            if self.process.poll() is not None:
                break
            time.sleep(.05)
        raise RuntimeError('Observer enrollment deadline exceeded')

    def set_wave(self, wave_id, expected_runs):
        self.enroll(self.scope_provider())

    def enroll_ready(self, scope):
        self.enroll(scope)
        deadline, consecutive, previous = time.monotonic() + STALE_SECONDS, 0, None
        while time.monotonic() < deadline:
            try:
                value = self._status()['snapshot']
                healthy = value['resource_healthy'] is True
                stamp = value['sampled_at']
            except (OSError, ValueError, KeyError, RuntimeError):
                healthy, stamp = False, None
            if not healthy: consecutive = 0
            elif stamp != previous: consecutive += 1
            previous = stamp
            if consecutive >= 2:
                return
            time.sleep(.1)
        raise RuntimeError('Two distinct fresh current-scope samples required')

    def _status(self):
        pointer = util.read_json(self.directory / 'status.json')
        path = checked(pointer)
        value = util.read_json(path)
        if (value.get('session') != self.session or value.get('phase_sha256') != self.digest
                or value.get('generation') != self.generation
                or value.get('sampled_scope_generation') != self.generation
                or self.process.poll() is not None
                or time.monotonic() - value['published_tick'] > STALE_SECONDS):
            raise RuntimeError('Independent observer status stale or unbound')
        value['_reference'] = pointer
        return value

    def snapshot(self):
        value = self._status()
        sample = dict(value['snapshot'])
        if (self.directory / 'fault-latch.json').exists():
            sample['host_healthy'] = sample['resource_healthy'] = False
        reference = value['_reference']
        sample['evidence_files'] = {**sample['evidence_files'], reference['path']: reference['sha256']}
        return sample

    def diagnostics(self):
        return self._status()['diagnostics']

    def admit(self, binding=None):
        """Every dispatch boundary uses current scope and independent latch."""
        self.enroll(self.scope_provider())
        value = self._status()
        if (self.directory / 'fault-latch.json').exists():
            raise RuntimeError('Independent observer latched a stop')
        if binding is not None and binding not in self.last_scope['bindings']:
            raise ValueError('Run was not independently enrolled')
        if value['snapshot']['resource_healthy'] is not True:
            raise RuntimeError('Independent resource health unavailable')

    def stop(self):
        if self.process is None:
            return False
        scope = self.last_scope
        if scope is None:
            self._terminate_unstarted_observer()
            return False
        generation = self.acknowledged_generation
        request = self.directory / 'shutdown.json'
        if not request.exists():
            util.write_new_json(request, {'session': self.session,
                'phase_sha256': self.digest, 'generation': generation,
                'dispatched': sorted(set(self.scope_provider()['dispatched']) &
                    {b['run_id'] for b in scope['bindings']})})
        deadline = time.monotonic() + 15
        while self.process.poll() is None and time.monotonic() < deadline:
            time.sleep(.05)
        if self.process.poll() is None:
            self._terminate_unstarted_observer()
            return False
        self.log.close()
        ack = self.directory / 'shutdown-ack.json'
        if self.process.poll() != 0 or not ack.exists():
            return False
        value = util.read_json(ack)
        return bool(value.get('owned_resources_resolved') is True
            and value.get('monitor_stop_confirmed') is True and value.get('session') == self.session
            and value.get('phase_sha256') == self.digest and value.get('generation') == generation)

    def _terminate_unstarted_observer(self):
        """Reap only this Popen handle, and only with no possibly dispatched Run.

        A rejected/pending enrollment must not leak its observer. Active or
        ambiguous ownership retains the independent fence; no ACK is invented.
        """
        scope = self.scope_provider()
        if scope['dispatched']:
            return False
        phase = util.read_json(self.phase_path)
        bindings = list(scope['assignments'])
        bindings += (self.last_scope or {}).get('bindings', [])
        bindings += (self.requested_scope or {}).get('bindings', [])
        for binding in bindings:
            root = (Path(phase['batch']) / binding['run_id']).resolve()
            if not root.is_relative_to(Path(phase['batch']).resolve()):
                return False
            if (root / 'runtime.json').exists():
                return False
            manifest = util.read_json(root / 'manifest.json')
            if (manifest.get('started_at') or any(manifest.get(k) != binding[k]
                    for k in ('run_id', 'run_instance_id'))):
                return False
        try:
            if self.process.poll() is None:
                self.process.terminate()
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        finally:
            self.log.close()
        util.write_new_json(self.directory / 'forced-process-stop.json', {
            'session': self.session, 'phase_sha256': self.digest,
            'requested_generation': self.generation,
            'acknowledged_generation': self.acknowledged_generation,
            'observer_pid': self.process.pid, 'observer_exit_code': self.process.returncode,
            'no_dispatch_verified': True, 'normal_shutdown_ack': False, 'at': now()})
        return True


class Supervisor:
    """Independent deadline responder; never waits on monitor's sample lock."""
    def __init__(self, config):
        self.directory = native_path(config['directory'])
        self.session = config['session']
        self.phase_path = checked(config['phase'])
        self.phase = util.read_json(self.phase_path)
        self.phase['_digest'] = config['phase']['sha256']
        if self.phase['kind'] not in (KIND, V9_KIND, V10_KIND, FIXTURE_KIND, LIVE_PILOT_KIND, REPAIRED_MAIN_KIND, RECOVERY_KIND, TRIAL_KIND):
            raise ValueError('Independent observer requires its explicit frozen phase')
        source_root = Path(__file__).resolve().parents[1]
        if self.phase['kind'] in (LIVE_PILOT_KIND, REPAIRED_MAIN_KIND, RECOVERY_KIND, TRIAL_KIND):
            from research.live_pilot import validate_observer_phase
            validate_observer_phase(self.phase, source_root)
        # A separately frozen nonmodel plan may opt into the same independent
        # executing-source guard. Historical phase contracts below are intact.
        if self.phase['kind'] == FIXTURE_KIND and 'source_pins' in self.phase:
            from research.campaign_operational import verify_pins
            verify_pins(source_root, self.phase['source_pins'])
        if self.phase['kind'] in (KIND, V9_KIND, V10_KIND):
            for name in ('research/resource_supervisor.py', 'outer/harness/runtime.py',
                         'outer/harness/util.py', 'outer/harness/run.py', 'outer/harness/ownership.py'):
                if self.phase['source_pins'].get(name) != util.sha256_file(source_root / name):
                    raise ValueError('Independent observer executing source changed: ' + name)
            if Path(self.phase['batch']).resolve() != (source_root / self.phase['cohort']).resolve():
                raise ValueError('Incorrect independently observed canonical cohort')
        self.phase_control = Path(self.phase['batch']) / '_control' / self.phase['phase_id']
        self.bindings, self.generation = [], 0
        self.scope = {'bindings': [], 'generation': 0}
        self.started_tick = time.monotonic()
        Monitor = load_monitor(self.phase)
        probe = Monitor.__init__.__kwdefaults__['probe_fn']
        def scoped_probe(_):
            scope = self.scope
            sample = bounded_probe(probe, roots(self.phase, scope['bindings']))
            sample['observer_scope_generation'] = scope['generation']
            return sample
        self.monitor = Monitor(self.directory / 'samples', lambda: [], probe_fn=scoped_probe)
        self.futures, self.confirmed = {}, {}
        self.next_fence = {}
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.fault = None
        self.latest = None
        self.published_token = None
        self.status_done = threading.Event()
        self.shutdown_done = threading.Event()
        self.status_sequence = 0
        if self.phase['kind'] in STARTUP_KINDS:
            ready=dict(schema_version=1,kind='resource_observer_startup_ready_v1',validated=True,
                session=self.session,phase=config['phase'],directory=config['directory'],
                config_sha256=util.sha256_file(self.directory/'config.json'),observer_pid=os.getpid(),
                source_sha256=util.sha256_file(__file__),ready_at=now())
            validate_startup_ready(config,self.phase,os.getpid(),ready)
            util.write_new_json(self.directory/'startup-ready.json',ready)

    def status_publisher(self):
        while not self.status_done.is_set():
            sample = self.monitor.snapshot()
            diagnostics = self.monitor.diagnostics()
            references = sample['evidence_files']
            if references:
                record = util.read_json(next(iter(references)))
                generation = record['sample'].get('observer_scope_generation')
            else:
                generation = None
            if generation != self.generation:
                sample['host_healthy'] = sample['resource_healthy'] = False
            self.latest = (generation, sample, diagnostics)
            self.status_done.wait(.1)

    def publish(self):
        latest, scope = self.latest, self.scope
        if latest is None:
            return
        generation, sample, diagnostics = latest
        current_generation = scope['generation']
        token = (current_generation, generation, sample['sampled_at'], self.fault is not None)
        if token == self.published_token:
            return
        snapshot = dict(sample)
        if generation != current_generation or self.fault is not None:
            snapshot['host_healthy'] = snapshot['resource_healthy'] = False
        self.status_sequence += 1
        path = self.directory / 'statuses' / f'{self.status_sequence:08d}.json'
        util.write_new_json(path, {
            'session': self.session, 'phase_sha256': self.phase['_digest'],
            'generation': current_generation, 'sampled_scope_generation': generation, 'published_at': now(),
            'published_tick': time.monotonic(), 'snapshot': snapshot,
            'diagnostics': diagnostics})
        publish_pointer(self.directory / 'status.json',
            {'path': str(path), 'sha256': util.sha256_file(path)})
        self.published_token = token

    def enroll(self):
        path = self.directory / f'scope-{self.generation + 1:06d}.json'
        if not path.exists():
            return
        request = util.read_json(path)
        if (request['session'] != self.session or request['generation'] != self.generation + 1):
            raise ValueError('Foreign generation')
        if self.fault is not None and self.generation != 0:
            raise ValueError('Latched observer cannot accept a new scope')
        bindings = validate_scope(self.phase, request)
        if self.bindings and not self.owned_resolved([b['run_id'] for b in self.bindings]):
            raise ValueError('Previous scope ownership not resolved')
        self.monitor.set_wave('wave-' + '-'.join(map(str, request['pairs']))
            if request['pairs'] else 'initial-admission', len(bindings))
        self.bindings, self.generation = bindings, request['generation']
        self.scope = {'bindings': bindings, 'generation': self.generation}
        util.write_new_json(self.directory / f'scope-ack-{self.generation:06d}.json', {
            'accepted': True, 'session': self.session, 'generation': self.generation,
            'scope_sha256': util.sha256_file(path), 'accepted_at': now()})

    def latch(self, reason):
        if self.fault is not None:
            return
        self.fault = {'reason': reason, 'session': self.session,
            'phase_sha256': self.phase['_digest'], 'generation': self.generation,
            'decided_at': now(), 'decided_tick': time.monotonic(),
            'bindings': self.bindings, 'HTTP_fence_confirmed': False}
        # Physical fence proceeds even when either evidence write fails.
        for path in (self.directory / 'fault-latch.json',
                     self.phase_control / 'dispatch-stop.json'):
            try:
                if not path.exists(): util.write_new_json(path, self.fault)
            except OSError:
                pass

    def fence(self):
        for binding in self.bindings:
            rid = binding['run_id']
            root = Path(self.phase['batch']) / rid
            try:
                request = root / 'stop-request.json'
                if not request.exists():
                    try:
                        util.write_new_json(request, {'requested_at': now(), 'reason': self.fault['reason'],
                            'run_id': rid, 'run_instance_id': binding['run_instance_id']})
                    except OSError:
                        pass  # Marker loss must not skip the actual gateway fence.
                if rid in self.futures or not (root / 'runtime.json').exists():
                    continue
                if time.monotonic() < self.next_fence.get(rid, 0):
                    continue
                # Runtime identity checked before runtime.fence_owned checks Docker ownership.
                state = util.read_json(root / 'runtime.json')
                if any(state.get(k) != binding[k] for k in ('run_id', 'run_instance_id')):
                    raise ValueError('Foreign late runtime')
                self.futures[rid] = self.pool.submit(runtime.fence_owned, root)
                self.next_fence[rid] = time.monotonic() + 1
            except Exception as exc:
                try:
                    util.write_new_json(self.directory / ('fence-error-' + uuid.uuid4().hex + '.json'),
                        {'run_id': rid, 'error_type': type(exc).__name__, 'at': now()})
                except OSError:
                    pass
        for rid, future in list(self.futures.items()):
            if not future.done(): continue
            try: receipt = future.result()
            except Exception as exc: receipt = {'confirmed': False, 'error_type': type(exc).__name__}
            try:
                util.write_new_json(self.directory / ('fence-' + uuid.uuid4().hex + '.json'),
                    {'session': self.session, 'generation': self.generation, 'run_id': rid,
                     'receipt': receipt, 'observed_at': now()})
            except OSError:
                pass
            if receipt.get('confirmed') is True:
                self.confirmed[rid] = receipt
            del self.futures[rid]

    def owned_resolved(self, dispatched):
        if not set(dispatched) <= {b['run_id'] for b in self.bindings}:
            raise ValueError('Foreign shutdown Run')
        for binding in self.bindings:
            root = Path(self.phase['batch']) / binding['run_id']
            if not (root / 'runtime.json').exists():
                if binding['run_id'] in dispatched:
                    return False
                manifest = util.read_json(root / 'manifest.json')
                if manifest.get('started_at'):
                    return False
                continue
            state = util.read_json(root / 'runtime.json')
            if any(state.get(k) != binding[k] for k in ('run_id', 'run_instance_id')):
                raise ValueError('Foreign shutdown identity')
            if util.read_json(root / 'manifest.json').get('stop_confirmed') is not True:
                return False
            for role in ('worker', 'gateway'):
                name = state[role]
                listed = runtime.docker('ps', '-a', '--filter', 'name=^/' + name + '$',
                    '--format', '{{.Names}}', timeout=10, check=False)
                if listed.returncode:
                    raise RuntimeError('Owned resource absence unobservable')
                if name not in listed.stdout.splitlines():
                    continue
                item = runtime._owned_container(state, role)
                if item is None:
                    raise RuntimeError('Owned resource inspect unobservable')
                if item['State']['Running']:
                    return False
        return True

    def command_worker(self):
        """Potentially blocking validation/monitor lock/Docker checks stay off timer."""
        while not self.status_done.is_set():
            try:
                shutdown = self.directory / 'shutdown.json'
                if shutdown.exists():
                    request = util.read_json(shutdown)
                    if (request['session'] != self.session or request['phase_sha256'] != self.phase['_digest']
                            or request['generation'] != self.generation):
                        raise ValueError('Foreign shutdown request')
                    if self.owned_resolved(request['dispatched']):
                        self.shutdown_receipt = {
                            'session': self.session, 'generation': self.generation,
                            'phase_sha256': self.phase['_digest'],
                            'owned_resources_resolved': True, 'at': now(),
                            'fault_latched': self.fault is not None}
                        self.shutdown_done.set()
                        return
                    self.latch('shutdown_owned_resources_unresolved')
                else:
                    self.enroll()
            except Exception as exc:
                self.latch('scope_or_shutdown_' + type(exc).__name__)
            self.status_done.wait(.1)

    def run(self):
        self.monitor.start()
        publisher = threading.Thread(target=self.status_publisher, daemon=True)
        publisher.start()
        threading.Thread(target=self.command_worker, daemon=True).start()
        try:
            while True:
                self.safety_tick()
                try:
                    self.publish()
                except Exception:
                    self.latch('supervisor_status_evidence_fault')
                    self.fence()
                if self.shutdown_done.is_set():
                    self.status_done.set()
                    if self.monitor.stop() is False:
                        self.latch('monitor_stop_unconfirmed')
                        self.fence()
                        return 2
                    self.shutdown_receipt['monitor_stop_confirmed'] = True
                    util.write_new_json(self.directory / 'shutdown-ack.json', self.shutdown_receipt)
                    return 0
                time.sleep(.1)
        finally:
            self.status_done.set()
            if self.monitor.stop() is False:
                self.latch('monitor_stop_unconfirmed')
            self.pool.shutdown(wait=True)

    def safety_tick(self):
        """Never waits on sampling/enrollment/shutdown validation locks."""
        last_tick = self.monitor.last_tick
        if time.monotonic() - (last_tick if last_tick is not None else self.started_tick) > STALE_SECONDS:
            self.latch('resource_sample_stale')
        faults = tuple(self.monitor.faults)
        if faults: self.latch('resource_fault:' + ','.join(sorted(faults)))
        if (self.phase_control / 'dispatch-stop.json').exists():
            self.latch('controller_durable_dispatch_stop')
        if self.fault is not None: self.fence()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    return Supervisor(util.read_json(args.config)).run()


if __name__ == '__main__':
    raise SystemExit(main())
