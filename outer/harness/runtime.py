"""Docker preparation and Run ownership. No host agent or host submission execution."""
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time
import uuid

from . import profiles, run, util, ownership, catalog_input, staged_input, staged_runtime
from .security import child_environment


def command(args, *, cwd=None, timeout=120, check=True):
    result = subprocess.run(args, cwd=cwd, env=child_environment(), capture_output=True,
                            text=True, encoding='utf-8', errors='replace', timeout=timeout)
    if check and result.returncode:
        raise RuntimeError('Command failed: ' + ' '.join(args[:3]) + '\n' + result.stderr[-3000:])
    return result


class DockerCommandError(RuntimeError):
    """Typed infrastructure evidence; never classify arbitrary error prose."""
    def __init__(self, operation, error):
        super().__init__(str(error))
        self.operation = operation
        self.error_type = type(error).__name__


def docker(*args, **kwargs):
    try: return command(['docker', *map(str, args)], **kwargs)
    except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
        raise DockerCommandError(str(args[0]),error) from error


def image_id(name):
    return docker('image', 'inspect', name, '--format', '{{.Id}}', timeout=30).stdout.strip()


def controller_files(repo):
    return {p.name:util.sha256_file(p) for p in sorted((Path(repo)/'outer/harness').glob('*.py'))}


def source(repo, task):
    start = task['start_state']
    revision = start['source_commit']
    root = Path(repo) / 'artifacts' / 'sources'
    dest = root / revision
    if dest.exists():
        saved = util.read_json(root / (revision + '.json'))
        if util.tree_hashes(dest) != saved['files']:
            raise ValueError('Prepared source changed')
        return dest
    cache = root / ('git-' + uuid.uuid4().hex)
    cache.mkdir(parents=True)
    command(['git', 'init', '-q', str(cache)])
    url = 'https://github.com/' + start['source_repository'] + '.git'
    command(['git', '-C', str(cache), 'fetch', '-q', '--depth=1', url, revision], timeout=300)
    actual = command(['git', '-C', str(cache), 'rev-parse', 'FETCH_HEAD']).stdout.strip()
    if actual != revision:
        raise ValueError('Source commit mismatch')
    data = subprocess.run(['git', '-c', 'core.autocrlf=false', '-c', 'core.eol=lf',
                           '-C', str(cache), 'archive', '--format=tar', actual],
                          env=child_environment(), capture_output=True, check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        if any(m.issym() or m.islnk() or m.isdev() for m in archive.getmembers()):
            raise ValueError('Source contains links or special files')
        dest.mkdir()
        archive.extractall(dest, filter='data')
    util.write_new_json(root / (revision + '.json'), {'source': url, 'commit': actual,
                                                    'byte_basis': 'git_archive_without_autocrlf',
                                                    'archive_configuration': {'core.autocrlf': False, 'core.eol': 'lf'},
                                                    'files': util.tree_hashes(dest)})
    return dest


def prepare(repo, *, task_id='MS1-001', rebuild=False, runtime_id='deepseek', task_revision=None):
    repo = Path(repo).resolve()
    profiles.identifier(task_id)
    preset = profiles.read(repo, 'runtimes', runtime_id)
    attempt = profiles.runtime_root(repo, task_id, preset) / 'preparation' / uuid.uuid4().hex
    attempt.mkdir(parents=True)
    request = {'task':task_id, 'rebuild':rebuild, 'started_at':run.now()}
    if task_revision is not None:
        request['task_profile_revision'] = task_revision
    util.write_new_json(attempt / 'request.json', request)
    try:
        options = {'task_id': task_id, 'rebuild': rebuild, 'runtime_id': runtime_id}
        if task_revision is not None:
            options['task_revision'] = task_revision
        result = _prepare(repo, **options)
        util.write_new_json(attempt / 'result.json', result)
        return result
    except Exception as exc:
        util.write_new_json(attempt / 'failure.json', {'ended_at':run.now(), 'type':type(exc).__name__,
                                                     'message':str(exc)})
        raise RuntimeError('Preparation failed; retained record: ' + str(attempt / 'failure.json')) from exc


def _prepare(repo, *, task_id='MS1-001', rebuild=False, runtime_id='deepseek', task_revision=None):
    repo = Path(repo).resolve()
    docker('info', '--format', '{{.OSType}}', timeout=30)
    task = profiles.task_profile(repo, task_id, task_revision)
    source(repo, task)
    profile = profiles.read(repo, 'runtimes', runtime_id)
    root = profiles.runtime_root(repo, task_id, profile)
    root.mkdir(parents=True, exist_ok=True)
    if (root / 'lock.json').exists() and not rebuild:
        lock = util.read_json(root / 'lock.json')
        if task_revision is not None or 'task_profile_revision' in lock:
            if (lock.get('task_profile_revision') != task_revision
                    or lock.get('evaluator_version') != task['evaluation']['evaluation_version']
                    or lock.get('evaluator_project') != task['evaluation']['project']
                    or lock.get('controller_files') != controller_files(repo)):
                raise ValueError('Retained runtime revision/controller differs; prepare a new binding')
        for digest in lock['images'].values():
            image_id(digest)
        return lock
    build_id = uuid.uuid4().hex
    build_root = root / 'builds' / build_id
    build_root.mkdir(parents=True)
    images = {}
    for target in ('worker', 'evaluator', 'gateway'):
        tag = 'sample2-' + target + ':' + build_id
        args = ['docker', 'build', '--progress=plain', '-f', 'outer/runtime/Dockerfile',
                '--target', target, '--build-arg', 'OPENCODE_VERSION=' + profile['opencode_version'],
                '--build-arg', 'EVALUATOR_PROJECT=' + task['evaluation']['project'],
                '-t', tag, '.']
        print('Preparing ' + target, flush=True)
        with (build_root / (target + '.log')).open('xb') as log:
            result = subprocess.run(args, cwd=repo, env=child_environment(),
                                    stdout=log, stderr=subprocess.STDOUT, timeout=1800)
        if result.returncode:
            raise RuntimeError('Image build failed; inspect ' + str(build_root / (target + '.log')))
        images[target] = image_id(tag)
    container = 's2-extract-' + build_id
    docker('create', '--name', container, images['evaluator'])
    bundle = build_root / 'evaluator'
    try:
        docker('cp', container + ':/evaluator', str(bundle))
    finally:
        docker('rm', '-f', container, check=False)
    if (root / 'evaluator').exists():
        (root / 'evaluator').rename(root / ('evaluator-retained-' + uuid.uuid4().hex))
    shutil.copytree(bundle, root / 'evaluator')
    sdk = docker('run', '--rm', '--network', 'none', images['evaluator'], 'dotnet', '--version').stdout.strip()
    if sdk != task['environment']['sdk']:
        raise ValueError('Prepared .NET SDK version mismatch')
    opencode = docker('run', '--rm', '--network', 'none', images['worker'], 'opencode', '--version').stdout.strip()
    if opencode != profile['opencode_version']:
        raise ValueError('Prepared OpenCode version mismatch')
    lock = {'schema_version': 1, 'build_id': build_id, 'images': images,
            'controller_files': controller_files(repo),
            'opencode_version': opencode, 'versions': {'dotnet': sdk, 'opencode': opencode},
            'evaluator_files': util.tree_hashes(bundle),
            'evaluator_sha256': util.sha256_file(bundle / task['evaluation']['assembly']),
            'evaluator_build': {'source_path': 'inner/evaluator/' + task['evaluation']['project'],
                                'command': 'docker build --target evaluator', 'sdk_version': sdk,
                                'sha256_origin': build_root.relative_to(repo).as_posix(),
                                'clean_worktree': not bool(command(['git', 'status', '--porcelain'], cwd=repo).stdout),
                                'source_commit': command(['git', 'rev-parse', 'HEAD'], cwd=repo).stdout.strip()},
            'created_at': run.now()}
    if task_revision is not None:
        lock.update(task_profile_revision=task_revision,
                    evaluator_version=task['evaluation']['evaluation_version'],
                    evaluator_project=task['evaluation']['project'])
    util.write_new_json(build_root / 'lock.json', lock)
    util.write_json_atomic(root / 'lock.json', lock)
    return lock


def sandbox_args():
    return ['--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
            '--pids-limit=512', '--memory=6g', '--cpus=4', '--tmpfs', '/tmp:rw,exec,size=2g']


def mount(path, dest, readonly=False):
    return ['--mount', 'type=bind,source=' + str(Path(path).resolve()) + ',target=' + dest + (',readonly' if readonly else '')]


def opencode_config(condition):
    model = condition['runtime']['model_id']
    selected = 'sample2/' + model
    request_timeout = condition['runtime'].get('provider_timeout_seconds', 120) * 1000
    return {'$schema': 'https://opencode.ai/config.json', 'model': selected, 'small_model': selected,
            'enabled_providers': ['sample2'], 'autoupdate': False, 'share': 'disabled',
            'plugin': [], 'mcp': {}, 'lsp': False, 'compaction': condition['runtime']['compaction'],
            'permission': {'*': 'allow', 'task': 'deny', 'question': 'deny',
                           'webfetch': 'deny', 'websearch': 'deny'},
            'provider': {'sample2': {'npm': '@ai-sdk/openai-compatible', 'name': 'Sample2 gateway',
                'options': {'baseURL': 'http://gateway:8080/v1', 'apiKey': 'local-no-credential',
                            'timeout': request_timeout, 'headerTimeout': request_timeout,
                            'chunkTimeout': request_timeout},
                'models': {model: {'name': 'DeepSeek V4.1 Flash',
                    'limit': {'context': condition['runtime']['model_context_tokens'],
                              'output': condition['runtime']['model_output_tokens']}}}}}}


def isolation_probe(root, state, condition):
    """Exercise the same network/mount/user boundaries before any model call."""
    code = """import json, os, socket, urllib.request
from pathlib import Path
def reachable(host, port):
    try:
        with socket.create_connection((host, port), timeout=2): return True
    except OSError: return False
result = {
 'gateway_reachable': urllib.request.urlopen('http://gateway:8080/health', timeout=5).status == 200,
 'internet_blocked': not reachable('1.1.1.1',443),
 'provider_blocked': not reachable('opencode.ai',443),
 'credential_absent': 'OPENCODE_GO_API_KEY' not in os.environ,
 'research_absent': not any(Path(p).exists() for p in ['/assets','/evaluator','/records','/var/run/docker.sock']),
 'source_present': Path('/input/legacy-source').is_dir(),
 'non_root': os.getuid() != 0}
try:
 Path('/input/probe-write').write_text('probe')
 result['input_readonly'] = False
except OSError: result['input_readonly'] = True
print(json.dumps(result))
"""
    input_mount = condition['runtime'].get('input_mount', '/input')
    code = code.replace('/input/', input_mount + '/')
    output = docker('run', '--rm', '--network', state['network'], *sandbox_args(),
                    *mount(root / 'inputs', input_mount, True),
                    condition['runtime_lock']['images']['worker'], 'python3', '-c', code, timeout=45)
    evidence = json.loads(output.stdout)
    evidence['verified'] = all(evidence.values())
    util.write_new_json(root / 'evidence/isolation.json', evidence)
    if not evidence['verified']:
        raise RuntimeError('Worker isolation probe failed')
    return evidence


def _gateway_credential():
    # This bootstrap never places the value in environment/arguments/files.
    if os.name != 'nt':
        raise RuntimeError('Live gateway bootstrap requires Windows user environment')
    import winreg
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, 'Environment') as key:
        value, _ = winreg.QueryValueEx(key, 'OPENCODE_GO_API_KEY')
    if not value or '\n' in value or '\r' in value:
        raise RuntimeError('Gateway credential missing or invalid')
    return value


def inspect_container(name):
    result = docker('inspect', name, timeout=30, check=False)
    if result.returncode:
        return None
    return json.loads(result.stdout)[0]


class ContainerOwnershipError(RuntimeError):
    pass


def _owned_container(state, role):
    item = inspect_container(state[role])
    if item:
        labels = item['Config'].get('Labels') or {}
        if (labels.get('sample2.run') != state['run_id'] or
                state.get('run_instance_id') and labels.get('sample2.instance') != state['run_instance_id']):
            raise ContainerOwnershipError('Container ownership mismatch')
    return item


def _stop_owned_role(state, role):
    name = state[role]
    item = _owned_container(state, role)
    if item:
        if item['State'].get('Paused'):
            docker('unpause',name,timeout=10,check=False)
        docker('stop', '--time', '10', name, timeout=40, check=False)
        item = _owned_container(state, role)
        return bool(item and not item['State']['Running'])
    listed = docker('ps', '-a', '--format', '{{.Names}}', timeout=30, check=False)
    return listed.returncode == 0 and name not in listed.stdout.splitlines()


def fence_owned(root):
    """Close one owned gateway's dispatch boundary before worker grace waits.

    ACK loss/timeouts fall back to stopping this gateway only. They never prove
    an ACK, absent usage, or provider completion. No credentials enter control.
    """
    root = Path(root)
    state = util.read_json(root / 'runtime.json')
    item = _owned_container(state, 'gateway')
    receipt = {'run_id': state['run_id'], 'run_instance_id': state.get('run_instance_id'),
               'requested_at': run.now(), 'confirmed': False, 'method': 'unconfirmed'}
    if item and item['State']['Running']:
        script = """import json,sys,urllib.request
body=json.dumps({'run_id':sys.argv[1],'session_id':sys.argv[2]}).encode()
req=urllib.request.Request('http://127.0.0.1:8080/control/stop-admission',data=body,
    headers={'Content-Type':'application/json'})
with urllib.request.urlopen(req,timeout=5) as response:
 print(response.read(4096).decode())
"""
        try:
            result = docker('exec', state['gateway'], 'python3', '-c', script,
                           state['run_id'], state.get('run_instance_id', state['run_id']),
                           timeout=10, check=False)
            ack = json.loads(result.stdout) if result.returncode == 0 else None
            if (not isinstance(ack, dict) or ack.get('run_id') != state['run_id']
                    or ack.get('session_id') != state.get('run_instance_id', state['run_id'])
                    or ack.get('admission_closed') is not True or not ack.get('closed_at')):
                raise ValueError('Invalid admission acknowledgement')
            receipt.update(confirmed=True, method='gateway_ack', acknowledgement=ack)
        except (ValueError, OSError, subprocess.TimeoutExpired, RuntimeError) as exc:
            receipt['control_error_type'] = type(exc).__name__
    if not receipt['confirmed']:
        receipt.update(confirmed=_stop_owned_role(state, 'gateway'), method='gateway_stop')
    receipt['observed_at'] = run.now()
    try:
        util.write_new_json(root / 'evidence/admission-stop' / (uuid.uuid4().hex + '.json'), receipt)
        receipt['evidence_persisted'] = True
    except OSError:
        # Evidence loss is explicit; it cannot prevent physical owned shutdown.
        receipt['evidence_persisted'] = False
        receipt['evidence_error_type'] = 'OSError'
    return receipt


def stop_owned(root):
    root = Path(root)
    state = util.read_json(root / 'runtime.json')
    ownership_errors = []
    try:
        fence = fence_owned(root)
        confirmations = [fence['confirmed']]
    except ContainerOwnershipError as exc:
        ownership_errors.append(exc)
        confirmations = [False]
    except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError):
        confirmations = [False]
    # Normally admission closes without cancelling in-flight streams. The
    # worker can finish its grace period; final gateway shutdown retains partial
    # stream evidence. Control failure stops gateway before any worker wait.
    roles = ('worker', 'gateway') if confirmations[0] else ('gateway', 'worker')
    for role in roles:
        try:
            confirmations.append(_stop_owned_role(state, role))
        except ContainerOwnershipError as exc:
            ownership_errors.append(exc)
            confirmations.append(False)
        except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError):
            confirmations.append(False)
    if ownership_errors:
        # Foreign roles are untouched; independently owned peers were attempted.
        raise ownership_errors[0]
    return all(confirmations)


def cleanup_network(root, *, evidence_dir=None):
    """Remove one stopped Run's private network, retaining containers and originals.

    An external evidence directory supports maintenance of archived Runs without
    rewriting their manifests. Failure is a receipt, never proof of absence.
    """
    root = Path(root)
    state = util.read_json(root / 'runtime.json')
    manifest = util.read_json(root / 'manifest.json')
    started = time.monotonic()
    receipt = {'run_id': manifest['run_id'], 'run_instance_id': manifest.get('run_instance_id'),
               'network': state.get('network'), 'network_id': state.get('network_id'),
               'started_at': run.now(), 'confirmed': False, 'status': 'failed'}
    target = Path(evidence_dir) if evidence_dir else root / 'evidence/network-cleanup'
    target.mkdir(parents=True, exist_ok=True)
    attempt = uuid.uuid4().hex
    try:
        if (state.get('run_id') != manifest['run_id'] or not state.get('stop_confirmed')
                or not manifest.get('stop_confirmed') or
                state.get('run_instance_id') and state['run_instance_id'] != manifest.get('run_instance_id')):
            raise RuntimeError('Run identity or stop confirmation missing')
        name = state['network']
        import re
        if not re.fullmatch(r's2-net-[0-9a-f]{16}', name):
            raise RuntimeError('Unexpected private network name')
        # A successful listing is necessary even when inspect reports an error.
        networks = docker('network', 'ls', '--format', '{{json .}}', timeout=30)
        present = [json.loads(line) for line in networks.stdout.splitlines() if line.strip()]
        found = any(n['Name'] == name for n in present)
        containers = []
        for observation in range(3):
            ids = docker('ps', '-aq', timeout=30).stdout.split()
            if not ids:break
            fmt = '{"id":{{json .Id}},"name":{{json .Name}},"state":{{json .State}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}}}'
            try:
                containers = [json.loads(line) for line in docker('inspect', *ids, '--format', fmt).stdout.splitlines() if line.strip()]
                break
            except DockerCommandError:
                # A concurrent owned Run may remove its stopped containers
                # between ps and inspect. Re-list; never infer absent ownership
                # from failed inspection or use its partial stdout.
                receipt.setdefault('container_observation_retries',[]).append(dict(at=run.now(),attempt=observation+1))
                if observation==2:raise
        expected = {state['worker'], state['gateway']}
        for item in containers:
            owned = item['name'].lstrip('/') in expected
            if name in (item.get('networks') or {}) and not owned:
                raise RuntimeError('Foreign container refers to network')
            if owned:
                labels = item.get('labels') or {}
                if (labels.get('sample2.run') != manifest['run_id'] or
                        state.get('run_instance_id') and labels.get('sample2.instance') != state['run_instance_id']):
                    raise RuntimeError('Container ownership mismatch')
                if any(item['state'].get(k) for k in ('Running', 'Restarting', 'Paused')):
                    raise RuntimeError('Container still active')
        if found:
            network = json.loads(docker('network', 'inspect', name, timeout=30).stdout)[0]
            labels = network.get('Labels') or {}
            if (network['Name'] != name or labels.get('sample2.run') != manifest['run_id'] or
                    state.get('run_instance_id') and labels.get('sample2.instance') != state['run_instance_id'] or
                    state.get('network_id') and network['Id'] != state['network_id']):
                raise RuntimeError('Network ownership mismatch')
            if not network.get('Internal') or network.get('Driver') != 'bridge' or network.get('Containers'):
                raise RuntimeError('Network is not an unused internal bridge')
            receipt['network_id'] = network['Id']
            util.write_new_json(target / (attempt + '-intent.json'), {**receipt, 'network_snapshot': network,
                'containers': [c for c in containers if c['name'].lstrip('/') in expected]})
            docker('network', 'rm', network['Id'], timeout=30)
        after = docker('network', 'ls', '--no-trunc', '--format', '{{json .}}', timeout=30)
        if any(n['Name'] == name or receipt.get('network_id') == n['ID']
               for n in (json.loads(line) for line in after.stdout.splitlines() if line.strip())):
            raise RuntimeError('Network removal not confirmed')
        receipt.update(confirmed=True, status='removed' if found else 'absent', containers_retained=True)
    except Exception as exc:
        receipt['error'] = {'type': type(exc).__name__, 'message': str(exc)}
    receipt.update(ended_at=run.now(), duration_seconds=round(time.monotonic() - started, 3))
    util.write_new_json(target / (attempt + '-result.json'), receipt)
    return receipt


def record_cleanup(root):
    receipt = cleanup_network(root)
    manifest = util.read_json(Path(root) / 'manifest.json')
    manifest['network_cleanup'] = receipt
    run.save_manifest(Path(root).parent, Path(root).name, manifest)
    return receipt


def start(repo, runs_dir, run_id):
    root = run.run_dir_for(runs_dir, run_id)
    with ownership.lease(root):
        return _start(repo, runs_dir, run_id)


def _start(repo, runs_dir, run_id):
    root = run.run_dir_for(runs_dir, run_id)
    condition = profiles.validate_run(root)
    if controller_files(repo) != condition['runtime_lock']['controller_files']:
        raise ValueError('Controller code changed since preparation; rebuild and create a new Run')
    manifest = run.load_manifest(runs_dir, run_id)
    if manifest['started_at']:
        raise ValueError('Run already started')
    stage = None
    if manifest.get('staged_input'):
        ref = manifest['staged_input']
        path = root/'_controller/staged-input/contract.json'
        if (Path(ref['contract_path']).resolve() != path.resolve()
                or util.sha256_file(path) != ref['contract_sha256']
                or condition['intervention']['method'] not in ('staged-explore', 'explore-observed')):
            raise ValueError('Staged contract/condition mismatch')
        stage = staged_input.Controller(path.parent)
        if (stage.contract['run_instance_id'] != manifest['run_instance_id']
                or stage.contract['budget_seconds'] != condition['budget']['value']
                or stage.contract['boundary_contract'].get('transport') != 'opencode-server-response-barrier-v1'
                or condition['runtime']['opencode_version'] != '1.17.11'):
            raise ValueError('Unsupported staged transport or Run budget binding')
        if stage.contract['artifact_collection_policy'] != util.resolve_collection_policy(condition.get('collection_policy')):
            raise ValueError('Staged artifact collection policy differs from fixed condition')
        stage.audit_mounts()
        staged_runtime.validate_authorization(repo,root,manifest,stage,condition)
    elif condition['intervention']['method'] in ('staged-explore', 'explore-observed'):
        raise ValueError('Staged condition requires its frozen controller contract')
    lock = condition['runtime_lock']
    for digest in lock['images'].values():
        image_id(digest)
    unique = uuid.uuid4().hex[:16]
    private = 's2-net-' + unique
    state = {'run_id': run_id, 'run_instance_id': manifest['run_instance_id'], 'worker': 's2-worker-' + unique,
             'gateway': 's2-gateway-' + unique, 'network': private, 'started_at': run.now()}
    util.write_new_json(root / 'runtime.json', state)
    for name in ('workspace', 'state', 'usage/raw', 'evidence'):
        (root / name).mkdir(parents=True, exist_ok=True)
    config = root / 'state' / 'opencode.json'
    util.write_new_json(config, opencode_config(condition))
    catalog = condition['intervention']['method'] == catalog_input.METHOD
    stdin_prompt = condition['runtime'].get('prompt_transport') == 'stdin'
    gateway_args = ['create', '-i', '--name', state['gateway'], '--label', 'sample2.run=' + run_id,
                    '--label', 'sample2.instance=' + manifest['run_instance_id'],
                    '--network', private, '--network-alias', 'gateway', *sandbox_args(),
                    *mount(root / 'usage/raw', '/records'),
                    *mount(root / 'inputs', '/contract', True), lock['images']['gateway'],
                    '--run-id', run_id, '--model', condition['runtime']['model_id'],
                    '--session-id', manifest.get('run_instance_id', run_id),
                    '--upstream-timeout', str(condition['runtime'].get('provider_timeout_seconds', 120))]
    if catalog or stdin_prompt:
        gateway_args += ['--expected-prompt', '/contract/prompt.txt']
    if stage:
        at = gateway_args.index(lock['images']['gateway'])
        gateway_args[at:at] = mount(stage.root, '/stage', True)
        gateway_args += ['--staged-input', '/stage']
    manifest.update(started_at=run.now(), runner={'id': 'opencode', 'version': lock['opencode_version']},
                    synthetic=False, model_called=False)
    run.save_manifest(runs_dir, run_id, manifest)
    started = time.monotonic()
    reason = 'environment_failure'
    exit_code = None
    gateway_process = None
    worker_process = None
    infrastructure_phase = 'allocation'
    try:
        if stage: stage.start()
        state['network_id'] = docker('network', 'create', '--internal', '--label', 'sample2.run=' + run_id,
            '--label', 'sample2.instance=' + manifest['run_instance_id'], private).stdout.strip()
        util.write_json_atomic(root / 'runtime.json', state)
        docker(*gateway_args)
        # Only gateway has an outbound network; it does not proxy arbitrary URLs.
        docker('network', 'connect', 'bridge', state['gateway'])
        with (root / 'evidence/gateway.log').open('xb') as gateway_log:
            gateway_process = subprocess.Popen(['docker', 'start', '-ai', state['gateway']],
                env=child_environment(), stdin=subprocess.PIPE, stdout=gateway_log, stderr=subprocess.STDOUT)
            gateway_process.stdin.write((_gateway_credential() + '\n').encode())
            gateway_process.stdin.flush()
            gateway_process.stdin.close()
            ready = False
            for _ in range(30):
                if time.monotonic() - started >= min(60, condition['budget']['value']):
                    break
                probe = docker('exec', state['gateway'], 'python3', '-c',
                    "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health').read()", check=False, timeout=5)
                if probe.returncode == 0:
                    ready = True
                    break
                time.sleep(1)
            if not ready:
                raise RuntimeError('Gateway failed to start')
            isolation_probe(root, state, condition)
            if (root / 'stop-request.json').exists():
                reason = 'operator_stop'
                raise RuntimeError('Stopped before model dispatch')
            args = ['create', '--name', state['worker'], '--label', 'sample2.run=' + run_id,
                    '--label', 'sample2.instance=' + manifest['run_instance_id'],
                    '--network', private, *sandbox_args(),
                    *mount(root / 'inputs', condition['runtime'].get('input_mount', '/input'), True),
                    *mount(root / 'workspace', '/workspace'), *mount(root / 'state', '/state'),
                    '-e', 'OPENCODE_CONFIG=/state/opencode.json', lock['images']['worker'],
                    'opencode', 'run', 'Carry out the attached modernization task.',
                    '--pure', '--format', 'json', '--title', run_id,
                    '--model', 'sample2/' + condition['runtime']['model_id'],
                    '--file', '/input/prompt.txt']
            if stage:
                at = args.index(lock['images']['worker'])
                args = args[:at+1] + ['opencode','serve','--pure','--hostname','127.0.0.1',
                                     '--port',str(staged_runtime.PORT)]
            elif catalog:
                at = args.index(lock['images']['worker'])
                args = args[:at+1] + ['python3', '-c', catalog_input.WORKER_PREFLIGHT,
                                     'sample2/' + condition['runtime']['model_id']]
            elif stdin_prompt:
                at = args.index(lock['images']['worker'])
                args = args[:at+1] + ['python3', '-c',
                    "import os,sys; p=open(sys.argv[1],'rb'); os.dup2(p.fileno(),0); "
                    "os.execvp('opencode',['opencode','run','--pure','--format','json','--title',sys.argv[2],'--model',sys.argv[3]])",
                    condition['runtime'].get('input_mount', '/input') + '/prompt.txt', run_id,
                    'sample2/' + condition['runtime']['model_id']]
            docker(*args)
            state['worker_started_at'] = run.now()
            util.write_json_atomic(root / 'runtime.json', state)
            with (root / ('evidence/native-server.log' if stage else 'evidence/agent.jsonl')).open('xb') as log:
                worker_process = subprocess.Popen(['docker', 'start', '-a', state['worker']],
                    env=child_environment(), stdout=log, stderr=subprocess.STDOUT)
                if stage:
                    infrastructure_phase = 'native_execution'
                    exit_code, reason = staged_runtime.execute(root,state,stage,condition,sys.modules[__name__])
                else:
                    while worker_process.poll() is None:
                        if (root / 'usage/raw/failure.jsonl').exists():
                            reason = 'provider_failure'
                            break
                        if (root / 'stop-request.json').exists():
                            reason = 'operator_stop'
                            break
                        if time.monotonic() - started >= condition['budget']['value']:
                            reason = 'timeout'
                            break
                        time.sleep(1)
                    else:
                        exit_code = worker_process.returncode
                        reason = 'completed' if exit_code == 0 else 'agent_error'
            # Drain already dispatched auxiliary requests before stopping the gateway.
            drain_deadline = min(time.monotonic() + 120, started + condition['budget']['value'])
            from .live_usage import journal
            while reason == 'completed' and time.monotonic() < drain_deadline:
                beginnings, begin_errors = journal(root / 'usage/raw/started.jsonl')
                terminals, end_errors = journal(root / 'usage/raw/events.jsonl')
                if not (begin_errors or end_errors) and len(beginnings) == len(terminals):
                    break
                time.sleep(1)
            if (root / 'usage/raw/failure.jsonl').exists():
                reason = 'provider_failure'
    except Exception as exc:
        if (root/'stop-request.json').exists():reason='operator_stop'
        if stage and isinstance(exc,DockerCommandError) and infrastructure_phase == 'allocation':
            util.write_new_json(root/'evidence/infrastructure-failure.json',dict(
                kind='owned_docker_allocation_failure_v1',run_id=run_id,run_instance_id=manifest['run_instance_id'],
                phase=infrastructure_phase,operation=exc.operation,error_type=exc.error_type,at=run.now()))
        if stage:
            try:
                if any(e['kind']=='boundary_unknown' for e in stage.events()): reason='boundary_unknown'
            except (OSError,ValueError): pass
        util.write_new_json(root / 'evidence/runtime-error.json', {'type': type(exc).__name__, 'message': str(exc)})
    finally:
        if stage and reason=='agent_error':
            try:
                item=_owned_container(state,'worker')
                if item and item['State'].get('OOMKilled') is True:
                    util.write_new_json(root/'evidence/infrastructure-failure.json',dict(
                        kind='owned_worker_oom_v1',run_id=run_id,run_instance_id=manifest['run_instance_id'],
                        worker=state['worker'],exit_code=item['State'].get('ExitCode'),oom_killed=True,at=run.now()))
            except (OSError,RuntimeError,ValueError):pass  # No proof means no automatic retry.
        try:
            stopped = stop_owned(root)
        except Exception as exc:
            stopped = False
            util.write_new_json(root / 'evidence/stop-error.json', {'type': type(exc).__name__})
        for process in (worker_process, gateway_process):
            if process:
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        state['stopped_at'] = run.now()
        state['stop_confirmed'] = stopped
        util.write_json_atomic(root / 'runtime.json', state)
        if stage:
            stage_errors=[]
            try:
                if stopped:stage.checkpoint('final',{'reason':reason,'stop_confirmed':True})
                util.write_new_json(stage.root/'final-snapshot.json',staged_input.snapshot(
                    stage.contract['workspace'],stage.contract['boundary_contract']['snapshot_policy']))
            except (OSError,ValueError,KeyError) as exc:
                stage_errors.append({'operation':'final_checkpoint','type':type(exc).__name__})
            try:stage.finish(reason,stop_confirmed=stopped)
            except (OSError,ValueError,KeyError) as exc:
                stage_errors.append({'operation':'terminal_ledger','type':type(exc).__name__})
            if stage_errors:util.write_new_json(root/'evidence/staged-terminal-error.json',{'errors':stage_errors})
        manifest.update(ended_at=run.now(), duration_seconds=round(time.monotonic() - started, 3),
                        end_reason=reason if stopped else 'stop_unconfirmed', exit_code=exit_code,
                        stop_confirmed=stopped, stop_method='container_exit', stop_evidence=state)
        run.save_manifest(runs_dir, run_id, manifest)
        from .live_usage import update_manifest_evidence
        update_manifest_evidence(root)
        # The measured interval ends above. Cleanup cannot change its outcome,
        # duration or usage, and runs even when subsequent normalization fails.
        manifest['network_cleanup'] = record_cleanup(root)
    from . import live_usage
    live_usage.collect(root)
    return run.load_manifest(runs_dir, run_id)


def request_stop(root):
    root = Path(root)
    manifest = util.read_json(root / 'manifest.json')
    if manifest.get('schema_version') != 2 and manifest.get('ended_at') and manifest.get('stop_confirmed') and not (root / 'runtime.json').exists():
        return {'run_id': root.name, 'stop_confirmed': True, 'already_stopped': True}
    if not (root / 'runtime.json').exists():
        raise ValueError('Run has no allocated runtime to stop')
    if manifest.get('ended_at') and manifest.get('stop_confirmed'):
        try:
            with ownership.lease(root):
                receipt = record_cleanup(root)
        except BlockingIOError:
            deadline = time.monotonic() + 55
            while time.monotonic() < deadline:
                manifest = util.read_json(root / 'manifest.json')
                if manifest.get('network_cleanup'):
                    break
                time.sleep(1)
            receipt = manifest.get('network_cleanup', {'confirmed': False, 'status': 'pending'})
        return {'run_id': root.name, 'stop_confirmed': True, 'already_stopped': True,
                'network_cleanup': receipt}
    try:
        util.write_json_atomic(root / 'stop-request.json', {'requested_at': run.now()})
        if manifest.get('staged_input'):
            staged_input.Controller(Path(manifest['staged_input']['contract_path']).parent).stop('operator_stop')
    except (OSError,ValueError,KeyError):
        # The controller cannot observe a missing marker. Physical shutdown is
        # still necessary, even while it owns its lease; never call this saved.
        stop_owned(root)
        raise RuntimeError('Stop-request evidence unavailable; owned shutdown attempted')
    # The running controller may hold its lease throughout worker grace. Close
    # provider admission now, before waiting for that lease/controller exit.
    try:
        fence_owned(root)
    except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError):
        # Early control failure cannot skip the independently owned peer. Keep
        # the original fault, including ownership mismatch, after best effort.
        try:
            stop_owned(root)
        except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError):
            pass
        raise
    try:
        with ownership.lease(root):
            # No controller is alive. Reclaim only containers bound to this Run.
            stopped = stop_owned(root)
            state = util.read_json(root / 'runtime.json')
            state.update(stopped_at=run.now(), stop_confirmed=stopped, recovered=True)
            util.write_json_atomic(root / 'runtime.json', state)
            manifest = util.read_json(root / 'manifest.json')
            manifest.update(ended_at=run.now(), stop_confirmed=stopped, stop_evidence=state,
                            stop_method='container_exit',
                            end_reason='operator_stop' if stopped else 'stop_unconfirmed')
            run.save_manifest(root.parent, root.name, manifest)
            if manifest.get('staged_input'):
                controller = staged_input.Controller(Path(manifest['staged_input']['contract_path']).parent)
                if not controller.status()['terminal']:
                    if stopped and not (controller.root/'checkpoints/final/receipt.json').exists():
                        try:controller.checkpoint('final',{'reason':'operator_stop_recovered','stop_confirmed':True})
                        except (OSError,ValueError):pass
                    controller.finish('operator_stop_recovered',stop_confirmed=stopped)
            from . import live_usage
            live_usage.update_manifest_evidence(root)
            record_cleanup(root)
            live_usage.collect(root)
    except BlockingIOError:
        deadline = time.monotonic() + 55
        while time.monotonic() < deadline:
            manifest = util.read_json(root / 'manifest.json')
            if manifest.get('ended_at') and manifest.get('network_cleanup'):
                break
            time.sleep(1)
        stopped = manifest.get('stop_confirmed', False)
    manifest = util.read_json(root / 'manifest.json')
    result = {'run_id': root.name, 'stop_requested': True, 'stop_confirmed': stopped,
              'network_cleanup': manifest.get('network_cleanup', {'confirmed': False, 'status': 'pending'})}
    util.write_json_atomic(root / 'stop-result.json', result)
    return result


def prepare_native_container(command_args, destination, receipt_path):
    """Initialize only this container's empty scratch volume; evaluator remains non-root."""
    if command_args[:2] != ['docker', 'run'] or destination not in ('/work', '/data'):
        raise ValueError('Unsupported native scratch command')
    args = list(command_args[2:])
    if '-d' in args: args.remove('-d')
    name = args[args.index('--name') + 1]
    helper = name + '-volume-init'
    command(['docker', 'create', *args])
    detail = json.loads(docker('inspect', name).stdout)[0]
    mounts = [m for m in detail['Mounts'] if m['Destination'] == destination]
    if len(mounts) != 1 or mounts[0]['Type'] != 'volume':
        raise ValueError('Native scratch volume missing or ambiguous')
    volume = mounts[0]['Name']
    receipt = dict(container=name, volume=volume, destination=destination,
                   evaluator_user=detail['Config']['User'], helper=helper)
    try:
        result = docker('run', '--name', helper, '--network', 'none', '--user', '0:0',
                        '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                        '--mount', 'type=volume,source='+volume+',target=/owned',
                        detail['Image'], 'chmod', '0777', '/owned', check=False, timeout=30)
        receipt['exit_code'] = result.returncode
        if result.returncode: raise RuntimeError('Native scratch initialization failed')
    finally:
        removed = docker('rm', '-f', helper, check=False)
        receipt['helper_removed'] = removed.returncode == 0
        util.write_new_json(receipt_path, receipt)
        if removed.returncode: raise RuntimeError('Native scratch helper cleanup failed')
    return ['docker', 'start', '-a', name]


def scoring_command(condition, frozen, out, work, assets, version, sequence, *, native_work=False):
    name = 's2-score-' + uuid.uuid4().hex
    business = []
    migration = condition['evaluation'].get('migration_contract')
    if migration:
        import_input = migration.get('import_input', 'initial-store.sqlite')
        if import_input not in ('initial-store.sqlite', 'legacy-school.sqlite'):
            raise ValueError('Migration evaluation import input is not an allowed public database')
        initial = Path(assets) / import_input
        if not initial.is_file():
            raise ValueError('Migration evaluation requires its frozen initial business database')
        business = mount(initial, '/inputs/existing-business/' + import_input, True)
    cmd = ['docker', 'run', '--name', name, '--network', 'none', *sandbox_args(),
           *mount(frozen, '/artifact', True), *mount(assets, '/assets', True),
           *business, *mount(out, '/result'),
           *(['--mount', 'type=volume,destination=/work'] if native_work else mount(work, '/work')),
           condition['runtime_lock']['images']['evaluator'],
           'dotnet', '/assets/evaluator/' + condition['evaluation']['assembly'], '--artifact', '/artifact',
           '--out', '/result', '--work', '/work', '--spec', '/assets/requirements.json',
           '--catalog', '/assets/catalog.json', '--evaluation-version', version, '--sequence', str(sequence)]
    return name, cmd
