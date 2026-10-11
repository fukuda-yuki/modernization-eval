"""Bind accepted evaluator bytes to reused SDK images in a fresh runtime namespace.

No image build, source fetch, model client or credential access. Docker probes use
the ordinary harness's sanitized environment. Historical build facts are retained;
a clean current controller checkout is a separate, newly verified fact.
"""
import argparse
import copy
from pathlib import Path, PurePosixPath
import re
import shutil
import uuid

from outer.harness import profiles, run, runtime, util


FAMILIES = {
    'music': {'tasks': ('MS1-CONT-A', 'MS1-CONT-B'), 'version': '1.6.0',
              'runtime_id': 'deepseek-music-repaired-v1',
              'namespace': 'music-repaired-runtime-20261006',
              'assembly': 'MusicStore.Evaluator.dll', 'project': 'MusicStore.Evaluator',
              'assembly_sha256': 'eb62763802d6b11d0b5a5915f3454fb727775fbe76d6c3e24406082168938f0d'},
    'education': {'tasks': ('CU1-ENR-C', 'CU1-ENR-D'), 'version': 'education-1.1.0',
                  'runtime_id': 'deepseek-education-repaired-v1',
                  'namespace': 'education-repaired-runtime-20261006',
                  'assembly': 'Education.Evaluator.dll', 'project': 'Education.Evaluator',
                  'assembly_sha256': 'ff09d9f6e8446685fc9ce8217ea2fe2ce9d80f8f11c081a899dd3a494902bb12'},
}
FIXED_RUNTIME = {'model_id': 'deepseek-v4.1-flash', 'provider': 'opencode-go',
                 'endpoint': 'https://opencode.ai/zen/go/v1/chat/completions',
                 'opencode_version': '1.17.11', 'timeout_seconds': 1800,
                 'provider_timeout_seconds': 600, 'model_context_tokens': 1000000,
                 'model_output_tokens': 32768, 'concurrency': 1, 'subagents': False,
                 'input_mount': '/inputs', 'prompt_transport': 'stdin',
                 'compaction': {'auto': True, 'prune': False}}


def _runtime_family(repo, runtime_id, observation_revision=None):
    if observation_revision is None:
        return next((value for value in FAMILIES.values() if value['runtime_id'] == runtime_id), None)
    from research import observation_revision as revision
    names = [name for name in ('music', 'education') if runtime_id == 'deepseek-'+name+'-observation-v1']
    if len(names) != 1:
        raise ValueError('Observation revision requires its fresh runtime namespace')
    return revision.family_binding(repo, observation_revision, names[0])[0]


def _relative(value):
    path = PurePosixPath(value)
    if (not isinstance(value, str) or not value or '\\' in value or ':' in value
            or path.is_absolute() or '..' in path.parts or str(path) != value):
        raise ValueError('Unsafe receipt path')
    return path


def _git(repo, *args):
    return runtime.command(['git', '-c', 'safe.directory=' + repo.as_posix(), *args], cwd=repo).stdout.strip()


def _committed_files(repo, commit, names):
    """One read-only Git process, exact blob framing and unchanged SHA256 checks."""
    names = list(names)
    if not names:
        return
    for name in names:
        _relative(name)
        if '\n' in name or '\r' in name:
            raise ValueError('Git batch path contains a line break')
    if not isinstance(commit, str) or '\n' in commit or '\r' in commit:
        raise ValueError('Invalid Git revision')
    from outer.harness.security import child_environment
    import subprocess
    request = ''.join(commit + ':' + name + '\n' for name in names).encode()
    result = subprocess.run(['git', '-c', 'safe.directory=' + repo.as_posix(),
        'cat-file', '--batch'], input=request, cwd=repo,
        env=child_environment(), capture_output=True, check=True)
    data = result.stdout; offset = 0
    for name in names:
        boundary = data.find(b'\n', offset)
        if boundary < 0:
            raise ValueError('Truncated Git blob header')
        fields = data[offset:boundary].split()
        if len(fields) != 3 or fields[1] != b'blob' or not fields[2].isdigit():
            raise ValueError('Missing or non-blob committed source: ' + name)
        size = int(fields[2]); offset = boundary + 1
        blob = data[offset:offset + size]; offset += size
        if len(blob) != size or data[offset:offset + 1] != b'\n':
            raise ValueError('Truncated Git blob content')
        offset += 1
        if util.sha256_bytes(blob) != util.sha256_file(repo / name):
            raise ValueError('Current file differs from committed Git blob: ' + name)
    if offset != len(data):
        raise ValueError('Unexpected extra Git batch output')


def _receipt_binding(repo, bundle, receipt, family):
    if receipt.get('evaluation_version') != family['version'] or receipt.get('sdk') != '8.0.425':
        raise ValueError('Accepted receipt version/SDK mismatch')
    inventory = receipt.get('deployable_files', receipt.get('bundle_files'))
    if not isinstance(inventory, list) or not inventory:
        raise ValueError('Accepted receipt lacks bundle inventory')
    expected = {}
    for entry in inventory:
        name = str(_relative(entry['path']))
        if name in expected or name == 'build-receipt.json':
            raise ValueError('Duplicate or recursive accepted bundle entry')
        expected[name] = {'sha256': entry['sha256'], 'bytes': entry['bytes']}
    actual = util.tree_hashes(bundle)
    actual.pop('build-receipt.json', None)
    if actual != expected or actual.get(family['assembly'], {}).get('sha256') != family['assembly_sha256']:
        raise ValueError('Accepted bundle inventory/assembly mismatch')
    sources = receipt.get('source_files', receipt.get('sources'))
    if not isinstance(sources, list) or not sources:
        raise ValueError('Accepted receipt lacks source applicability evidence')
    source_hashes = {}
    for entry in sources:
        name = str(_relative(entry['path']))
        if name in source_hashes or util.sha256_file(repo / name) != entry['sha256']:
            raise ValueError('Current evaluator source differs from accepted source: ' + name)
        source_hashes[name] = entry['sha256']
    project_root = repo / 'inner/evaluator' / family['project']
    required = {p.relative_to(repo).as_posix() for p in project_root.rglob('*')
                if p.is_file() and p.suffix in ('.cs', '.csproj')
                and not any(part in ('bin', 'obj') for part in p.relative_to(project_root).parts)}
    if not required or not required <= source_hashes.keys():
        raise ValueError('Accepted receipt does not cover current evaluator project sources')
    if receipt.get('global_json_sha256'):
        source_hashes['global.json'] = receipt['global_json_sha256']
        if util.sha256_file(repo / 'global.json') != source_hashes['global.json']:
            raise ValueError('Accepted SDK source pin mismatch')
    return actual, source_hashes


def _input_binding(repo, task):
    commit = task['start_state']['source_commit']
    source = repo / 'artifacts/sources' / commit
    sidecar = source.parent / (commit + '.json')
    if not source.is_dir() or not util.tree_hashes(source) or util.tree_hashes(source) != util.read_json(sidecar)['files']:
        raise ValueError('Prepared upstream source differs from preserved inventory')
    catalog = repo / task['evaluation']['catalog_path']
    asset_root = catalog.parents[1]
    prepared = util.read_json(asset_root / 'preparation.json')
    files = {name: entry['sha256'] for name, entry in util.tree_hashes(asset_root).items()}
    files.pop('preparation.json', None)
    if (prepared.get('task_id') != task['task_id']
            or prepared.get('base_source_commit', prepared.get('source_commit')) != commit or prepared.get('files') != files
            or prepared.get('public_request_sha256') != util.sha256_bytes(task['migration_request'].encode('utf-8'))):
        raise ValueError('Prepared variant inputs differ from preserved inventory/request')
    spec = repo / task['evaluation']['spec_path']
    if util.sha256_file(spec) != task['evaluation']['spec_sha256']:
        raise ValueError('Revised ledger pin mismatch')
    return {'source_commit': commit, 'source_receipt_sha256': util.sha256_file(sidecar),
            'variant_receipt_sha256': util.sha256_file(asset_root / 'preparation.json'),
            'spec_sha256': util.sha256_file(spec)}


def _family_tasks(repo, family):
    tasks = {name: profiles.task_profile(repo, name, 'evaluators-20261006') for name in family['tasks']}
    for name, task in tasks.items():
        evaluation = task['evaluation']
        if (task.get('canonical_task_id') != name or evaluation['evaluation_version'] != family['version']
                or evaluation['assembly'] != family['assembly']
                or evaluation['project'] != family['project'] + '/' + family['project'] + '.csproj'
                or task['environment']['sdk'] != '8.0.425'):
            raise ValueError('Revised task/evaluator identity mismatch')
    return tasks


def validate_repaired_runtime_binding(repo, lock):
    """Read-only acceptance gate for profiles.create; raises on incomplete binding.

    Unrelated later commits are permitted. Every applicability byte must still
    equal both the preparation's committed Git blob and the current clean tree.
    """
    repo = Path(repo).resolve()
    binding = lock.get('repaired_runtime_binding', {})
    current = binding.get('current_applicability', {})
    if (binding.get('schema_version') != 1 or binding.get('kind') != 'accepted_bundle_reuse_v1'
            or current.get('clean_worktree') is not True
            or not re.fullmatch(r'[0-9a-f]{40}', current.get('source_commit', ''))
            or _git(repo, 'status', '--porcelain')):
        raise ValueError('Repaired runtime requires a clean current applicability binding')
    family = _runtime_family(repo, lock.get('runtime_profile_id'), binding.get('observation_revision'))
    if (family is None or lock.get('task_profile_revision') != 'evaluators-20261006'
            or lock.get('evaluator_version') != family['version']
            or lock.get('evaluator_project') != family['project'] + '/' + family['project'] + '.csproj'
            or lock.get('controller_source_commit') != current['source_commit']
            or lock.get('controller_clean_worktree') is not True
            or lock.get('controller_files') != runtime.controller_files(repo)):
        raise ValueError('Repaired controller/evaluator identity differs')
    source_files = current.get('files')
    if not isinstance(source_files, dict) or not source_files:
        raise ValueError('Repaired applicability inventory missing')
    for name, digest in source_files.items():
        _relative(name)
        if util.sha256_file(repo / name) != digest:
            raise ValueError('Repaired applicability file changed: ' + name)
    _committed_files(repo, current['source_commit'], source_files)
    profile_name = 'outer/profiles/runtimes/' + family['runtime_id'] + '.json'
    profile = util.read_json(repo / profile_name)
    if (profile.get('id') != family['runtime_id'] or profile.get('artifact_namespace') != family['namespace']
            or any(profile.get(key) != value for key, value in FIXED_RUNTIME.items())
            or lock.get('runtime_profile_sha256') != util.sha256_file(repo / profile_name)):
        raise ValueError('Repaired profile binding changed')
    tasks = _family_tasks(repo, family)
    task_pins = []
    for name, task in tasks.items():
        task_pins.extend(['outer/profiles/task-revisions/evaluators-20261006/' + name + '.json',
                          task['evaluation']['spec_path']])
    if (any(name not in source_files for name in task_pins)
            or lock.get('versions') != {'dotnet': '8.0.425', 'opencode': '1.17.11'}
            or lock.get('opencode_version') != '1.17.11'
            or lock.get('input_bindings') != {name: _input_binding(repo, task) for name, task in tasks.items()}):
        raise ValueError('Repaired family task/input/version binding changed')
    images = lock.get('images', {})
    reuse = lock.get('image_reuse', {})
    if (set(images) != {'worker', 'evaluator', 'gateway'}
            or any(not re.fullmatch(r'sha256:[0-9a-f]{64}', value) for value in images.values())
            or reuse.get('image_embedded_evaluator_used_for_scoring') is not False
            or reuse.get('reused_images') != images):
        raise ValueError('Repaired immutable image reuse binding differs')
    root = profiles.runtime_root(repo, family['tasks'][0], profile)
    bundle = root / 'evaluator'
    receipt_path = bundle / 'build-receipt.json'
    receipt_sha = util.sha256_file(receipt_path)
    if receipt_sha != binding.get('accepted_receipt_sha256'):
        raise ValueError('Prepared accepted receipt hash differs')
    receipt = util.read_json(receipt_path)
    inventory, accepted_sources = _receipt_binding(repo, bundle, receipt, family)
    if 'observation_revision' in binding:
        from research import observation_revision as revision
        _, entry, _, _ = revision.family_binding(repo, binding['observation_revision'],
                                                  revision.family_for_task(family['tasks'][0]))
        if (entry['images'] != images or entry['build_receipt']['sha256'] != receipt_sha
                or lock.get('evaluator_build', {}).get('observation_revision') != binding['observation_revision']
                or source_files.get('research/observation_revision.py') != util.sha256_file(repo/'research/observation_revision.py')):
            raise ValueError('Observation revision runtime/build binding differs')
    if (util.tree_hashes(bundle) != lock.get('evaluator_files')
            or lock.get('evaluator_sha256') != family['assembly_sha256']
            or any(source_files.get(name) != digest for name, digest in accepted_sources.items())
            or source_files.get(profile_name) != lock['runtime_profile_sha256']
            or any(source_files.get('outer/harness/' + name) != digest
                   for name, digest in lock['controller_files'].items())
            or source_files.get('research/repaired_runtime.py') != util.sha256_file(repo / 'research/repaired_runtime.py')):
        raise ValueError('Repaired bundle/applicability inventory differs')
    historical = lock.get('evaluator_build', {})
    if (historical.get('binding_kind') != 'accepted_bundle_reuse_v1'
            or historical.get('source_commit') != receipt.get('source_commit')
            or historical.get('clean_worktree') != receipt.get('source_worktree_clean_at_start')
            or historical.get('accepted_receipt_sha256') != receipt_sha
            or historical.get('command') != receipt.get('commands', [])
            or historical.get('sdk_version') != receipt.get('sdk')
            or historical.get('source_identity') != receipt.get('source_identity', 'accepted_receipt_commit_and_exact_inventory')
            or historical.get('source_files') != accepted_sources):
        raise ValueError('Historical accepted build facts differ from its receipt')
    return True


def prepare_repaired_runtime(repo, *, task_id, task_revision, runtime_id, prior_lock,
                             prior_lock_sha256, accepted_bundle, accepted_receipt,
                             accepted_receipt_sha256, controller_source_commit,
                             observation_revision=None):
    """Validate all bindings first, then exclusively create a new runtime root.

    The only containers run are network-none version probes. Old image-embedded
    evaluator assemblies are never scored; /assets/evaluator is the accepted copy.
    Repeated preparation fails instead of overwriting an existing namespace.
    """
    repo = Path(repo).resolve()
    if observation_revision is not None and not isinstance(observation_revision, dict):
        from research.live_pilot import reference
        observation_revision = reference(Path(observation_revision).resolve())
    family = _runtime_family(repo, runtime_id, observation_revision)
    if family is None or task_id not in family['tasks'] or task_revision != 'evaluators-20261006':
        raise ValueError('Unsupported repaired family/revision/runtime identity')
    if not re.fullmatch(r'[0-9a-f]{40}', controller_source_commit):
        raise ValueError('Full committed controller identity required')
    if (_git(repo, 'rev-parse', 'HEAD') != controller_source_commit
            or _git(repo, 'status', '--porcelain')):
        raise ValueError('Preparation requires the specified clean committed checkout')
    tasks = _family_tasks(repo, family)
    task = tasks[task_id]
    profile = profiles.read(repo, 'runtimes', runtime_id)
    if (profile.get('id') != runtime_id or profile.get('artifact_namespace') != family['namespace']
            or any(profile.get(key) != value for key, value in FIXED_RUNTIME.items())):
        raise ValueError('Repaired runtime profile differs from fixed execution contract')
    evaluation = task['evaluation']
    if (task.get('canonical_task_id') != task_id or evaluation['evaluation_version'] != family['version']
            or evaluation['assembly'] != family['assembly']
            or evaluation['project'] != family['project'] + '/' + family['project'] + '.csproj'
            or task['environment']['sdk'] != '8.0.425'):
        raise ValueError('Revised task/evaluator identity mismatch')
    root = profiles.runtime_root(repo, task_id, profile)
    if root.exists():
        raise FileExistsError('Repaired namespace already exists; retain it unchanged')
    prior_lock, accepted_bundle, accepted_receipt = map(lambda p: Path(p).resolve(),
                                                       (prior_lock, accepted_bundle, accepted_receipt))
    if root == prior_lock.parent or root.is_relative_to(accepted_bundle) or accepted_bundle.is_relative_to(root):
        raise ValueError('New namespace overlaps protected evidence')
    if (util.sha256_file(prior_lock) != prior_lock_sha256
            or util.sha256_file(accepted_receipt) != accepted_receipt_sha256):
        raise ValueError('Prior lock/accepted receipt pin mismatch')
    old, receipt = util.read_json(prior_lock), util.read_json(accepted_receipt)
    if observation_revision is not None:
        from research import observation_revision as revision
        _, entry, _, _ = revision.family_binding(repo, observation_revision, revision.family_for_task(task_id))
        if (Path(entry['bundle']).resolve() != accepted_bundle
                or Path(entry['build_receipt']['path']).resolve() != accepted_receipt
                or entry['build_receipt']['sha256'] != accepted_receipt_sha256
                or entry['images'] != old.get('images')):
            raise ValueError('Observation preparation differs from the explicit revision pins')
    if old.get('versions') != {'dotnet': '8.0.425', 'opencode': '1.17.11'}:
        raise ValueError('Prior runtime version receipt mismatch')
    if old.get('controller_files', {}).get('gateway.py') != util.sha256_file(repo / 'outer/harness/gateway.py'):
        raise ValueError('Reused gateway code differs from current gateway')
    bundle_files, source_hashes = _receipt_binding(repo, accepted_bundle, receipt, family)
    if (accepted_bundle / 'build-receipt.json').exists() and util.sha256_file(accepted_bundle / 'build-receipt.json') != accepted_receipt_sha256:
        raise ValueError('Bundle receipt differs from accepted external receipt')
    input_bindings = {name: _input_binding(repo, value) for name, value in tasks.items()}
    input_binding = input_bindings[task_id]
    controller = runtime.controller_files(repo)
    committed_names = [*source_hashes, *['outer/harness/' + name for name in controller],
                       'research/repaired_runtime.py', 'outer/profiles/runtimes/' + runtime_id + '.json']
    if observation_revision is not None:
        committed_names.append('research/observation_revision.py')
    for name, value in tasks.items():
        committed_names.extend(['outer/profiles/task-revisions/' + task_revision + '/' + name + '.json',
                                value['evaluation']['spec_path']])
    _committed_files(repo, controller_source_commit, committed_names)
    images = old.get('images', {})
    if set(images) != {'worker', 'evaluator', 'gateway'} or any(not re.fullmatch(r'sha256:[0-9a-f]{64}', value) for value in images.values()):
        raise ValueError('Prior image IDs must be immutable digests')
    for image in images.values():
        if runtime.image_id(image) != image:
            raise ValueError('Reused image identity mismatch')
    sdk = runtime.docker('run', '--rm', '--network', 'none', images['evaluator'], 'dotnet', '--version').stdout.strip()
    worker_sdk = runtime.docker('run', '--rm', '--network', 'none', images['worker'], 'dotnet', '--version').stdout.strip()
    opencode = runtime.docker('run', '--rm', '--network', 'none', images['worker'], 'opencode', '--version').stdout.strip()
    if sdk != '8.0.425' or worker_sdk != '8.0.425' or opencode != '1.17.11':
        raise ValueError('Actual reused image versions differ from pinned versions')
    # Close ordinary validation races before the first new evidence write.
    if (_git(repo, 'rev-parse', 'HEAD') != controller_source_commit or _git(repo, 'status', '--porcelain')
            or runtime.controller_files(repo) != controller
            or util.sha256_file(prior_lock) != prior_lock_sha256
            or util.sha256_file(accepted_receipt) != accepted_receipt_sha256
            or _receipt_binding(repo, accepted_bundle, receipt, family) != (bundle_files, source_hashes)
            or {name: _input_binding(repo, value) for name, value in tasks.items()} != input_bindings):
        raise ValueError('Preparation binding changed during validation')
    _committed_files(repo, controller_source_commit, committed_names)
    build_id = uuid.uuid4().hex
    build = {'binding_kind': 'accepted_bundle_reuse_v1',
             'source_path': 'inner/evaluator/' + evaluation['project'],
             'command': copy.deepcopy(receipt.get('commands', [])), 'sdk_version': sdk,
             'sha256_origin': str(accepted_receipt),
             'clean_worktree': receipt.get('source_worktree_clean_at_start'),
             'source_commit': receipt.get('source_commit'),
             'source_identity': receipt.get('source_identity', 'accepted_receipt_commit_and_exact_inventory'),
             'accepted_receipt_sha256': accepted_receipt_sha256, 'source_files': source_hashes,
             'current_source_matches_accepted': True}
    if observation_revision is not None:
        build['observation_revision'] = copy.deepcopy(observation_revision)
    lock = {'schema_version': 1, 'build_id': build_id, 'images': images,
            'controller_files': controller, 'controller_source_commit': controller_source_commit,
            'controller_clean_worktree': True, 'runtime_profile_id': runtime_id,
            'runtime_profile_sha256': util.sha256_file(repo / 'outer/profiles/runtimes' / (runtime_id + '.json')),
            'task_profile_revision': task_revision, 'evaluator_version': family['version'],
            'evaluator_project': evaluation['project'], 'versions': {'dotnet': sdk, 'opencode': opencode},
            'opencode_version': opencode, 'evaluator_sha256': family['assembly_sha256'],
            'evaluator_build': build, 'input_binding': input_binding, 'input_bindings': input_bindings,
            'repaired_runtime_binding': {'schema_version': 1, 'kind': 'accepted_bundle_reuse_v1',
                'accepted_receipt_sha256': accepted_receipt_sha256,
                'current_applicability': {'clean_worktree': True, 'source_commit': controller_source_commit,
                    'files': {name: util.sha256_file(repo / name) for name in committed_names}}},
            'image_reuse': {'prior_lock_path': str(prior_lock), 'prior_lock_sha256': prior_lock_sha256,
                            'prior_evaluator_build': copy.deepcopy(old.get('evaluator_build')),
                            'reused_images': copy.deepcopy(images), 'worker_sdk_version': worker_sdk,
                            'image_embedded_evaluator_used_for_scoring': False},
            'created_at': run.now()}
    if observation_revision is not None:
        lock['repaired_runtime_binding']['observation_revision'] = copy.deepcopy(observation_revision)
    root.mkdir(parents=True, exist_ok=False)
    # Copy receipt as immutable evidence, never alter the accepted source bundle.
    shutil.copytree(accepted_bundle, root / 'evaluator')
    if not (root / 'evaluator/build-receipt.json').exists():
        shutil.copy2(accepted_receipt, root / 'evaluator/build-receipt.json')
    lock['evaluator_files'] = util.tree_hashes(root / 'evaluator')
    copied = dict(lock['evaluator_files'])
    copied.pop('build-receipt.json', None)
    if copied != bundle_files:
        raise ValueError('Copied accepted bundle mismatch; retained incomplete new namespace')
    evidence_root = root / 'builds' / build_id
    evidence_root.mkdir(parents=True)
    with accepted_receipt.open('rb') as source, (evidence_root / 'accepted-build-receipt.json').open('xb') as dest:
        shutil.copyfileobj(source, dest)
    util.write_new_json(root / 'builds' / build_id / 'lock.json', lock)
    util.write_new_json(root / 'lock.json', lock)
    return lock


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('repo', 'task_id', 'task_revision', 'runtime_id', 'prior_lock', 'prior_lock_sha256',
                 'accepted_bundle', 'accepted_receipt', 'accepted_receipt_sha256', 'controller_source_commit'):
        parser.add_argument('--' + name.replace('_', '-'), required=True)
    parser.add_argument('--observation-revision', help='Explicit same-spec revision/build pin file for a fresh observation runtime')
    options = vars(parser.parse_args())
    repo = options.pop('repo')
    lock = prepare_repaired_runtime(repo, **options)
    print('Prepared ' + lock['runtime_profile_id'] + ' with accepted DLL ' + lock['evaluator_sha256'])


if __name__ == '__main__':
    main()
