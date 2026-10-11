"""Run lifecycle: identify -> fix inputs -> run -> confirm stop -> freeze.

The order matters. An artifact is never frozen before the stop is confirmed,
because an artifact copied from a still-running process is not a submission.
"""
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import util
from .security import child_environment

END_REASONS = ('completed', 'agent_error', 'timeout', 'stop_unconfirmed', 'environment_failure')
EXECUTION_STATES = ('not_started', 'running') + END_REASONS
STOP_METHODS = ('process_exit', 'operator_process_check', 'declaration_only')

FORBIDDEN_INPUT_ROOTS = ('inner', 'docs')


def now():
    return datetime.now(timezone.utc).isoformat()


def condition_file(repo, task_id):
    return Path(repo) / 'outer' / 'conditions' / task_id / 'condition.json'


def load_condition(repo, task_id):
    path = condition_file(repo, task_id)
    if not path.is_file():
        raise FileNotFoundError('条件ファイルがありません: ' + str(path))
    return util.read_json(path)


def run_id_for(task_id, condition_id, attempt):
    return '{}-{}-{:03d}'.format(task_id, condition_id, attempt)


def run_dir_for(runs_dir, run_id):
    import re
    if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,195}', run_id):
        raise ValueError('Invalid Run identifier')
    return Path(runs_dir) / run_id


def load_manifest(runs_dir, run_id):
    path = run_dir_for(runs_dir, run_id) / 'manifest.json'
    if not path.is_file():
        raise FileNotFoundError('Run がありません: ' + run_id)
    return util.read_json(path)


def save_manifest(runs_dir, run_id, manifest):
    util.write_json_atomic(run_dir_for(runs_dir, run_id) / 'manifest.json', manifest)


def execution_state(manifest):
    if manifest.get('started_at') is None:
        return 'not_started'
    return manifest.get('end_reason') or 'running'


def _reject_links(root):
    root = Path(root)
    for path in root.rglob('*'):
        if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
            raise ValueError('入力にリンクが含まれています: ' + str(path))


def _resolve_input(name, source, destination, repo):
    """Validate one input and return its resolved path. Copies nothing."""
    source = Path(source)
    if source.is_symlink() or (hasattr(source, 'is_junction') and source.is_junction()):
        raise ValueError('入力がリンクです: ' + str(source))
    resolved = source.resolve(strict=True)
    repo = Path(repo).resolve()
    for forbidden in FORBIDDEN_INPUT_ROOTS:
        boundary = (repo / forbidden).resolve()
        if resolved == boundary or boundary in resolved.parents:
            raise ValueError('非公開領域を入力にはできません: ' + str(resolved))
    run_root = Path(destination).resolve().parents[1]
    if resolved == run_root or run_root in resolved.parents or resolved in run_root.parents:
        raise ValueError('Run ディレクトリと入力は独立でなければなりません')
    _reject_links(resolved)
    if not (resolved.is_dir() or resolved.is_file()):
        raise ValueError('通常のファイルまたはディレクトリが必要です: ' + str(resolved))
    return resolved


def _copy_input(resolved, destination):
    if resolved.is_dir():
        shutil.copytree(resolved, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolved, destination)


def create_run(repo, runs_dir, task_id, condition_id, attempt, inputs, *, resolved_condition=None):
    """Create a run directory exclusively. Never overwrite an existing run.

    Every input is validated before the run directory exists, so a refused
    input leaves no half-made run behind.
    """
    condition = resolved_condition or load_condition(repo, task_id)
    collection_policy = util.resolve_collection_policy(condition.get('collection_policy'))
    import re
    if not all(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', x or '')
               for x in (task_id, condition_id)) or type(attempt) is not int or attempt < 1:
        raise ValueError('Invalid task, condition or attempt')
    if condition.get('condition_id') != condition_id:
        raise ValueError('条件の condition_id が一致しません: ' + str(condition.get('condition_id')))
    if condition.get('task_id') != task_id:
        raise ValueError('条件の task_id が一致しません: ' + str(condition.get('task_id')))
    allowlist = list((condition.get('input_policy') or {}).get('allowlist') or [])
    run_id = run_id_for(task_id, condition_id, attempt)
    run_dir = run_dir_for(runs_dir, run_id)
    resolved_inputs = {}
    for name in sorted(inputs):
        if name not in allowlist:
            raise ValueError('許可リストに無い入力です: ' + name)
        resolved_inputs[name] = _resolve_input(name, inputs[name], run_dir / 'inputs' / name, repo)
    run_dir.mkdir(parents=True, exist_ok=False)
    util.write_new_json(run_dir / 'condition.json', condition)
    inputs_dir = run_dir / 'inputs'
    inputs_dir.mkdir()
    recorded, total_bytes, file_count = {}, 0, 0
    for name, resolved in resolved_inputs.items():
        _copy_input(resolved, inputs_dir / name)
        files = util.tree_hashes(inputs_dir / name)
        size = sum(entry['bytes'] for entry in files.values())
        recorded[name] = {'source': str(resolved), 'file_count': len(files),
                          'total_bytes': size, 'files': files}
        total_bytes += size
        file_count += len(files)
    manifest = {
        'schema_version': condition.get('schema_version', 1),
        'run_id': run_id,
        'task_id': task_id,
        'task_title': condition.get('task_title'),
        'condition_id': condition_id,
        'attempt': attempt,
        'condition_path': 'runs/{}/condition.json'.format(run_id),
        'condition_sha256': util.sha256_file(run_dir / 'condition.json'),
        'collection_policy': collection_policy,
        'created_at': now(),
        'inputs_manifest_sha256': None,
        'inputs_total_bytes': total_bytes,
        'inputs_file_count': file_count,
        'started_at': None,
        'ended_at': None,
        'duration_seconds': None,
        'runner': None,
        'agent': condition.get('agent'),
        'exit_code': None,
        'end_reason': None,
        'stop_confirmed': False,
        'stop_method': None,
        'stop_evidence': None,
        'submission_fixed': False,
        'model_called': False,
        'synthetic': False,
    }
    util.write_new_json(run_dir / 'inputs-manifest.json', {
        'schema_version': 1,
        'run_id': run_id,
        'allowlist': allowlist,
        'denied': (condition.get('input_policy') or {}).get('denied'),
        'inputs': recorded,
        'total_bytes': total_bytes,
        'file_count': file_count,
        'note': 'リポジトリに存在する情報量は渡した入力の総バイト数である。モデルの実入力ではない。',
    })
    manifest['inputs_manifest_sha256'] = util.sha256_file(run_dir / 'inputs-manifest.json')
    save_manifest(runs_dir, run_id, manifest)
    return manifest


def _agent_is_filled(agent):
    agent = agent or {}
    return any(agent.get(key) is not None
               for key in ('agent_id', 'model_id', 'agent_version', 'tool_versions'))


def _budget_seconds(condition):
    budget = condition.get('budget') or {}
    if budget.get('kind') == 'wall_clock_seconds' and budget.get('scope') == 'run':
        value = budget.get('value')
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
    return None


def start_run(repo, runs_dir, run_id, runner, *, synthetic=False, scenario=None,
              seed=None, timeout=None, note=None):
    """Run the runner and confirm that its process ended."""
    from . import runner as runner_mod
    repo = Path(repo).resolve()
    run_dir = run_dir_for(runs_dir, run_id)
    manifest = load_manifest(runs_dir, run_id)
    if manifest.get('started_at') is not None:
        raise RuntimeError('既に開始した Run です: ' + run_id)
    if runner not in runner_mod.RUNNERS:
        raise ValueError('未実装の実行器です: ' + str(runner))
    condition = util.read_json(run_dir / 'condition.json')
    if runner == 'dummy' and not synthetic:
        raise ValueError('ダミー実行器は --synthetic が必要です。'
                         'これは実モデル実行の記録には使えません')
    if not synthetic and not _agent_is_filled(condition.get('agent')):
        raise ValueError('条件の agent が埋まっていません。start を拒否します')
    if runner == 'dummy' and scenario not in runner_mod.DUMMY_SCENARIOS:
        raise ValueError('未知のシナリオです: ' + str(scenario))

    evidence = run_dir / 'evidence'
    evidence.mkdir(exist_ok=True)
    manifest['runner'] = {'id': runner, 'version': runner_mod.__name__,
                          'scenario': scenario if runner == 'dummy' else None}
    manifest['synthetic'] = bool(synthetic)
    manifest['model_called'] = False
    manifest['started_at'] = now()
    save_manifest(runs_dir, run_id, manifest)

    if runner == 'manual':
        manifest['ended_at'] = None
        manifest['exit_code'] = None
        manifest['end_reason'] = None
        manifest['stop_confirmed'] = False
        manifest['stop_method'] = None
        manifest['note'] = note or '人の実行を待つ。停止の確認までは採点しない。'
        save_manifest(runs_dir, run_id, manifest)
        return manifest

    command = [sys.executable, '-m', 'harness.runner',
               '--scenario', scenario,
               '--workspace', str(run_dir / 'workspace'),
               '--usage', str(run_dir / 'usage'),
               '--run-id', run_id]
    if seed is not None:
        command += ['--seed', str(seed)]
    environment = child_environment()
    # The harness package is imported from wherever this file lives, not from the
    # run's repository, so a test repository does not need to contain outer/.
    environment['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])
    environment['PYTHONIOENCODING'] = 'utf-8'
    if timeout is None:
        timeout = _budget_seconds(condition)

    timed_out = False
    started = datetime.now(timezone.utc)
    with (evidence / 'runner-stdout.log').open('wb') as out, \
            (evidence / 'runner-stderr.log').open('wb') as err:
        process = subprocess.Popen(command, cwd=str(repo), env=environment,
                                   stdout=out, stderr=err)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            process.wait()
    ended = datetime.now(timezone.utc)
    exit_code = process.returncode
    stop_confirmed = process.poll() is not None
    if not stop_confirmed:
        end_reason = 'stop_unconfirmed'
    elif timed_out:
        end_reason = 'timeout'
    elif exit_code == 0:
        end_reason = 'completed'
    else:
        end_reason = 'agent_error'
    manifest['ended_at'] = ended.isoformat()
    manifest['duration_seconds'] = round((ended - started).total_seconds(), 3)
    manifest['exit_code'] = exit_code
    manifest['end_reason'] = end_reason
    manifest['stop_confirmed'] = bool(stop_confirmed)
    manifest['stop_method'] = 'process_exit' if stop_confirmed else None
    manifest['stop_evidence'] = ('実行器のプロセスが終了し、終了コードを回収した'
                                 if stop_confirmed else 'プロセスが残っている')
    manifest['timeout_seconds'] = timeout
    save_manifest(runs_dir, run_id, manifest)
    return manifest


def stop_run(runs_dir, run_id, method, evidence_text=None, end_reason=None):
    """Confirm the stop of a manual run.

    A declaration alone is not a stop. `declaration_only` records
    stop_unconfirmed and the run is not scored.
    """
    if method not in STOP_METHODS:
        raise ValueError('未知の停止確認方法です: ' + str(method))
    manifest = load_manifest(runs_dir, run_id)
    if manifest.get('started_at') is None:
        raise RuntimeError('開始していない Run です: ' + run_id)
    manifest['ended_at'] = manifest.get('ended_at') or now()
    manifest['stop_method'] = method
    manifest['stop_evidence'] = evidence_text
    if method == 'declaration_only':
        manifest['stop_confirmed'] = False
        manifest['end_reason'] = 'stop_unconfirmed'
    else:
        manifest['stop_confirmed'] = True
        manifest['end_reason'] = end_reason or 'completed'
    save_manifest(runs_dir, run_id, manifest)
    return manifest


def collect_run(runs_dir, run_id, *, retry_deadline=None, retry_check=None):
    """Collect normally; only transient Windows JSON replacement is retried.

    Collection follows confirmed model stop and does not restart its budget.
    Optional monotonic deadline belongs to the caller's postprocessing budget.
    STOP still permits the first preservation write, but forbids waiting/retry.
    """
    root = run_dir_for(runs_dir, run_id)
    def check():
        if (any((parent/'STOP').exists() for parent in (root, *root.parents))
                or (root.parent/'_control/STOP').exists()):
            raise InterruptedError('Collection replacement retry stopped')
        if retry_deadline is not None and time.monotonic() >= retry_deadline:
            raise TimeoutError('Collection replacement retry deadline expired')
        if retry_check is not None:
            retry_check()
    with util.collection_atomic_retries(check):
        return _collect_run(runs_dir, run_id)


def _collect_run(runs_dir, run_id):
    """Freeze the submission and normalize usage. Requires a confirmed stop."""
    run_dir = run_dir_for(runs_dir, run_id)
    manifest = load_manifest(runs_dir, run_id)
    if not manifest.get('stop_confirmed'):
        raise RuntimeError('停止を確認していない Run は回収しません: ' + run_id)
    condition_path = run_dir / 'condition.json'
    if util.sha256_file(condition_path) != manifest['condition_sha256']:
        raise ValueError('Submission collection condition changed after assignment')
    condition = util.read_json(condition_path)
    policy = util.resolve_collection_policy(condition.get('collection_policy'))
    if util.resolve_collection_policy(manifest.get('collection_policy')) != policy:
        raise ValueError('Submission collection policy differs from assigned manifest')
    snapshot = {'schema_version': 1, 'run_id': run_id, 'collected_at': now(),
                'collection_policy': policy, 'collection_contract': util.collection_contract(policy)}
    workspace = run_dir / 'workspace'
    frozen = run_dir / 'frozen'
    if not workspace.is_dir():
        snapshot['artifact_state'] = 'missing_workspace'
        snapshot['artifact_sha256'] = None
        snapshot['artifact_sha256_collected'] = None
        snapshot['collected'] = {}
        snapshot['frozen'] = {}
        snapshot['normalized'] = []
        util.write_new_json(run_dir / 'snapshot.json', snapshot)
        manifest['submission_fixed'] = False
        save_manifest(runs_dir, run_id, manifest)
        return manifest, snapshot

    collected_before = util.collection_hash(workspace, policy=policy)
    result = util.collect(workspace, frozen, policy=policy)
    snapshot.update(result)
    snapshot['artifact_sha256_collected'] = collected_before
    snapshot['artifact_sha256'] = util.artifact_hash(frozen)
    snapshot['artifact_state'] = 'fixed'
    if (snapshot['artifact_sha256'] != collected_before
            and not snapshot['normalized']):
        raise RuntimeError('成果物ハッシュが変わったのに正規化の記録がありません')
    snapshot['artifact_hash_changed_by_collection'] = (
        snapshot['artifact_sha256'] != collected_before)
    snapshot['collection_note'] = (
        'artifact_sha256_collected hashes the workspace files selected by the assigned collection policy '
        'before text normalization; artifact_sha256 hashes the frozen evaluator identity. '
        'Any difference must be explained by recorded text normalization. Evaluation IDs use the latter.')
    util.write_new_json(run_dir / 'snapshot.json', snapshot)
    manifest['submission_fixed'] = True
    save_manifest(runs_dir, run_id, manifest)
    if manifest.get('schema_version') == 2:
        from .live_usage import collect as collect_usage
        collect_usage(run_dir)
    else:
        _normalize_usage(run_dir, run_id)
    return manifest, snapshot


def _normalize_usage(run_dir, run_id):
    from . import usage as usage_mod
    usage_dir = run_dir / 'usage'
    usage_dir.mkdir(exist_ok=True)
    provenance_path = usage_dir / 'provenance.json'
    events_path = usage_dir / 'events.jsonl'
    if not provenance_path.is_file():
        util.write_json_atomic(usage_dir / 'normalized.json', {
            'schema_version': 1, 'run_id': run_id, 'state': 'missing',
            'error': 'usage_provenance_missing',
            'total_tokens': None, 'observed_tokens': 0,
            'note': '原本の出所と網羅性の根拠が無いため、欠測として記録する。'})
        return
    provenance = util.read_json(provenance_path)
    expected = list(provenance.get('expected_sessions') or [])
    inventory_complete = bool(provenance.get('inventory_complete'))
    events = []
    if events_path.is_file():
        for line in events_path.read_text(encoding='utf-8').splitlines():
            if line.strip():
                events.append(json.loads(line))
    # 原本が別の Run のものなら、この Run の使用量として記録しない。
    declared = provenance.get('run_id')
    if declared is not None and declared != run_id:
        _reject_usage(usage_dir, run_id, expected, inventory_complete, provenance_path,
                      events_path,
                      'usage_provenance_run_mismatch: 出所の run_id {} がこの Run {} と一致しません'
                      .format(declared, run_id))
        return
    try:
        result = usage_mod.normalize(events, expected, inventory_complete, run_id=run_id)
    except ValueError as error:
        _reject_usage(usage_dir, run_id, expected, inventory_complete, provenance_path,
                      events_path, 'usage_events_rejected: ' + str(error))
        return
    result.update({'schema_version': 1, 'run_id': run_id,
                   'state': 'complete' if result['usage_complete'] else 'missing',
                   'error': None,
                   'expected_sessions': expected,
                   'inventory_complete': inventory_complete,
                   'provenance_sha256': util.sha256_file(provenance_path),
                   'events_sha256': util.sha256_file(events_path) if events_path.is_file() else None,
                   'raw_files': provenance.get('raw_files'),
                   'observable': provenance.get('observable'),
                   'not_observable': provenance.get('not_observable')})
    util.write_json_atomic(usage_dir / 'normalized.json', result)


def _reject_usage(usage_dir, run_id, expected, inventory_complete, provenance_path,
                  events_path, error):
    """Record a usage original that this Run must not be credited with.

    The events are kept on disk as evidence but are not turned into a number:
    a Run whose usage could not be attributed to it is a missing measurement,
    not a Run that used zero tokens.
    """
    util.write_json_atomic(usage_dir / 'normalized.json', {
        'schema_version': 1, 'run_id': run_id, 'state': 'missing', 'error': error,
        'total_tokens': None, 'observed_tokens': 0,
        'expected_sessions': expected, 'inventory_complete': inventory_complete,
        'provenance_sha256': util.sha256_file(provenance_path),
        'events_sha256': util.sha256_file(events_path) if events_path.is_file() else None,
        'note': 'この Run の使用量として採用できない原本のため、欠測として記録する。'
                '0 で置き換えない。'})


def read_usage(run_dir):
    path = Path(run_dir) / 'usage' / 'normalized.json'
    if not path.is_file():
        return {'state': 'missing', 'error': 'usage_not_collected',
                'total_tokens': None, 'observed_tokens': 0}
    return util.read_json(path)


def read_snapshot(run_dir):
    path = Path(run_dir) / 'snapshot.json'
    if not path.is_file():
        return {'artifact_state': 'not_fixed', 'artifact_sha256': None}
    return util.read_json(path)
