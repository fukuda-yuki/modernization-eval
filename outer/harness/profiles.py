"""Task, intervention and runtime profiles; immutable, portable Run inputs."""
import copy
import fnmatch
import json
import re
import shutil
import uuid
from pathlib import Path

from . import run, util, catalog_input


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('Invalid profile identifier')
    return value


def read(repo, kind, name, *, task_revision=None):
    if task_revision is not None:
        if kind != 'tasks':
            raise ValueError('Revision lookup is only for task profiles')
        path = (Path(repo) / 'outer' / 'profiles' / 'task-revisions'
                / identifier(task_revision) / (identifier(name) + '.json'))
        value = util.read_json(path)
        if (value.get('revision_id') != task_revision or value.get('task_id') != name
                or value.get('canonical_task_id', name) != name):
            raise ValueError('Task revision identity mismatch')
        return value
    path = Path(repo) / 'outer' / 'profiles' / kind / (identifier(name) + '.json')
    return util.read_json(path)


def inventory(repo):
    return {kind: [util.read_json(p) for p in sorted(
        (Path(repo) / 'outer' / 'profiles' / kind).glob('*.json'))]
        for kind in ('tasks', 'interventions', 'runtimes')}


def task_profile(repo, name, task_revision=None):
    if task_revision is None:
        return read(repo, 'tasks', name)
    return read(repo, 'tasks', name, task_revision=task_revision)


def resolve(repo, task_id, intervention_id, runtime_id='deepseek', *, task_revision=None,
            approved_model_id=None):
    task = task_profile(repo, task_id, task_revision)
    intervention = read(repo, 'interventions', intervention_id)
    runtime = read(repo, 'runtimes', runtime_id)
    if task['task_id'] != task_id or intervention['id'] != intervention_id or runtime['id'] != runtime_id:
        raise ValueError('Profile identity mismatch')
    if intervention['method'] not in ('explore', 'preload', 'explained', 'staged-explore', 'explore-observed', catalog_input.METHOD):
        raise ValueError('Unsupported intervention method')
    if intervention['method'] in ('staged-explore', 'explore-observed') and (runtime.get('prompt_transport') != 'stdin' or runtime.get('opencode_version') != '1.17.11'):
        raise ValueError('Staged exploration requires the fixed stdin-capable OpenCode runtime')
    if intervention['method'] == catalog_input.METHOD:
        if (type(intervention.get('append_source_packet')) is not bool
                or runtime.get('input_mount') != '/inputs'
                or runtime.get('prompt_transport') != 'stdin'):
            raise ValueError('Catalog conditions require the versioned catalog runtime')
    # Historical callers retain their fixed model. A fresh campaign supplies
    # the model explicitly bound by its approved plan; no provider abstraction.
    expected_model = approved_model_id if approved_model_id is not None else 'deepseek-v4.1-flash'
    if not isinstance(expected_model, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}', expected_model):
        raise ValueError('Explicit supported model identifier required')
    if (runtime['model_id'] != expected_model
            or runtime['endpoint'] != 'https://opencode.ai/zen/go/v1/chat/completions'
            or runtime['concurrency'] != 1 or runtime['subagents'] is not False
            or type(runtime['timeout_seconds']) is not int or runtime['timeout_seconds'] <= 0):
        raise ValueError('Runtime violates the fixed provider, model or execution contract')
    if (type(runtime.get('provider_timeout_seconds', 120)) is not int
            or runtime.get('provider_timeout_seconds', 120) <= 0):
        raise ValueError('Provider timeout must be a positive number of seconds')
    condition = copy.deepcopy(task)
    condition.update(schema_version=2, condition_id=intervention_id,
                     intervention=intervention, runtime=runtime,
                     agent={'agent_id': 'opencode', 'model_id': runtime['model_id'],
                            'agent_version': runtime['opencode_version'],
                            'tool_versions': {}, 'subagent_policy': 'none'},
                     budget={'kind': 'wall_clock_seconds', 'scope': 'run',
                             'value': runtime['timeout_seconds']})
    if task_revision is not None:
        condition['task_profile_revision'] = task_revision
    return condition


def runtime_root(repo, task_id, runtime):
    return Path(repo) / 'artifacts/runtime' / identifier(runtime.get('artifact_namespace', task_id))


def prepare_prompt(condition, source, catalog=None):
    """Public contract is identical in all arms. No scorer/fixture is read here."""
    request = condition['migration_request']
    common = ('Implement the following modernization in /workspace. The original source is '
              'read-only in /input/legacy-source. Write the final web project under /workspace. '
              'Do not copy the original legacy project or build outputs into the submission. '
              'Inspect the existing code and test your implementation with dotnet. '
              'There is no interactive user during this run.\n\n'
              'Environment: .NET SDK 8; target net8.0. EF Core SQLite 8.0.31 and '
              'Microsoft.Data.Sqlite 8.0.31 are available offline. No Internet access. '
              'Use /tmp for scratch databases.\n\n' + request)
    if util.resolve_collection_policy(condition.get('collection_policy')) == util.STATIC_DB_COLLECTION_POLICY:
        common += ('\n\nSubmission collection contract (workspace-static-db-v2): '
            '/workspace contains the final project and its submitted static data assets. '
            'Regular .sqlite, .sqlite3 and .db files there are retained byte-for-byte with the project. '
            'Use /tmp for mutable application test databases and scratch databases; do not leave them '
            'in the submitted project. Close submitted static databases before finishing, and ensure '
            'their WAL, SHM and rollback-journal sidecars have been resolved by your application. '
            'Collection holds if a submitted database has a nonempty sidecar; the collector never '
            'checkpoints or changes a database. Generated bin, obj, .git, .vs, node_modules, testresults, '
            '.user files and database sidecars are excluded from the submission.')
    common = common.replace('/input/legacy-source',
                            condition['runtime'].get('input_mount', '/input') + '/legacy-source')
    if condition['intervention']['method'] == catalog_input.METHOD:
        common = common.replace('/input/legacy-source', '/inputs/legacy-source')
        derived = Path(catalog)
        confirmation = (derived / 'common.json').read_bytes().decode('utf-8')
        common += '\n\nCatalog preparation:\n' + confirmation
        blocks = [{'source': '/inputs/catalog-derived/common.json', 'text': confirmation}]
        prompt = common
        if condition['intervention']['append_source_packet']:
            raw = (derived / 'raw-first.txt').read_bytes().decode('utf-8')
            blocks.append({'source': '/inputs/catalog-derived/raw-first.txt', 'text': raw})
            prompt += raw
        return prompt, {'method': catalog_input.METHOD, 'common_sha256': util.sha256_bytes(common.encode()),
            'blocks': [{**b, 'sha256': util.sha256_bytes(b['text'].encode())} for b in blocks]}
    blocks = []
    method = condition['intervention']['method']
    if method == 'preload':
        patterns = condition['context_files']
        for p in sorted(Path(source).rglob('*')):
            name = p.relative_to(source).as_posix()
            if p.is_file() and any(fnmatch.fnmatch(name, pattern) for pattern in patterns):
                content = p.read_text(encoding='utf-8-sig')
                blocks.append({'source': name, 'text': content})
        if not blocks:
            raise ValueError('Preload matched no source files')
    elif method == 'explained':
        blocks.append({'source': 'public-explanation', 'text': condition['explanation']})
    prompt = common + ''.join('\n\n--- ' + b['source'] + ' ---\n' + b['text'] for b in blocks)
    return prompt, {'method': method, 'common_sha256': util.sha256_bytes(common.encode()),
                    'blocks': [{'source': b['source'], 'sha256': util.sha256_bytes(b['text'].encode()),
                                'text': b['text']} for b in blocks]}


def create(repo, runs_dir, task_id, intervention, attempt, runtime_id='deepseek', *,
           run_instance_id=None, assignment=None, task_revision=None, approved_model_id=None, staged_plan=None):
    repo = Path(repo).resolve()
    options = {}
    if task_revision is not None: options['task_revision'] = task_revision
    if approved_model_id is not None: options['approved_model_id'] = approved_model_id
    condition = resolve(repo, task_id, intervention, runtime_id, **options)
    prepared_root = runtime_root(repo, task_id, condition['runtime'])
    lock = util.read_json(prepared_root / 'lock.json')
    if 'repaired_runtime_binding' in lock:
        from research.repaired_runtime import validate_repaired_runtime_binding
        validate_repaired_runtime_binding(repo, lock)
    elif not lock.get('evaluator_build', {}).get('clean_worktree'):
        raise ValueError('Prepare an evaluator from a clean committed checkout before spending model usage')
    if lock['opencode_version'] != condition['runtime']['opencode_version']:
        raise ValueError('Prepared runtime does not match profile')
    if task_revision is not None or 'task_profile_revision' in lock:
        if (lock.get('task_profile_revision') != task_revision
                or lock.get('evaluator_version') != condition['evaluation']['evaluation_version']
                or lock.get('evaluator_project') != condition['evaluation']['project']):
            raise ValueError('Prepared evaluator revision does not match task profile')
    if condition.get('task_input_adapter') == 'migration-v1':
        from . import migration_input
        inputs = migration_input.prepare(repo, condition)
        source = inputs['legacy-source']
    else:
        source = repo / 'artifacts' / 'sources' / condition['start_state']['source_commit']
        source_lock = util.read_json(source.parent / (source.name + '.json'))
        if util.tree_hashes(source) != source_lock['files']:
            raise ValueError('Prepared source changed')
        inputs = {'legacy-source': source}
    if condition['intervention']['method'] == catalog_input.METHOD:
        inputs.update(catalog_input.prepare(repo, source))
        condition['input_policy']['allowlist'] += ['catalog-derived', 'catalog-tools']
    staged = condition['intervention']['method'] in ('staged-explore', 'explore-observed')
    if staged != (staged_plan is not None):
        raise ValueError('Staged condition and explicitly pinned request partition must agree')
    if staged:
        from . import staged_input
        prompt, context, additional, partition = staged_input.prepare_presentation(condition, staged_plan, source)
    else:
        prompt, context = prepare_prompt(condition, source, inputs.get('catalog-derived'))
    if 'catalog-derived' in inputs:
        receipt = inputs['catalog-derived'].parent / 'preparation.json'
        context['preparation'] = {'path': str(receipt), 'sha256': util.sha256_file(receipt),
                                  **util.read_json(receipt)}
    frozen_profiles = {'task': task_profile(repo, task_id, task_revision),
                       'intervention': read(repo, 'interventions', intervention),
                       'runtime': read(repo, 'runtimes', runtime_id)}
    condition['runtime_lock'] = lock
    condition['agent']['tool_versions'] = lock['versions']
    bundle = prepared_root / 'evaluator'
    if util.tree_hashes(bundle) != lock['evaluator_files']:
        raise ValueError('Prepared evaluator changed')
    condition['evaluation'].update(
        executor='docker', spec_path='evaluation-assets/requirements.json',
        catalog_path='evaluation-assets/catalog.json',
        evaluator_sha256=lock['evaluator_sha256'],
        evaluator_build=lock['evaluator_build'])
    manifest = run.create_run(repo, runs_dir, task_id, intervention, attempt,
                              inputs, resolved_condition=condition)
    root = Path(runs_dir) / manifest['run_id']
    for name, value in frozen_profiles.items():
        util.write_new_json(root / 'profiles' / (name + '.json'), value)
    assets = root / 'evaluation-assets'
    shutil.copytree(bundle, assets / 'evaluator')
    task = task_profile(repo, task_id, task_revision)
    for key, dest in [('spec_path', 'requirements.json'), ('catalog_path', 'catalog.json')]:
        shutil.copy2(repo / task['evaluation'][key], assets / dest)
    for source_path, dest in task['evaluation'].get('extra_assets', {}).items():
        if Path(dest).name != dest or ':' in dest or '\\' in dest:
            raise ValueError('Invalid extra evaluator asset name')
        source_path = (repo / source_path).resolve()
        if not source_path.is_relative_to(repo):
            raise ValueError('Extra evaluator asset escaped repository')
        shutil.copy2(source_path, assets / dest)
    if util.sha256_file(assets / 'requirements.json') != condition['evaluation']['spec_sha256']:
        raise ValueError('Task ledger hash mismatch; retain incomplete Run')
    (root / 'inputs' / 'prompt.txt').write_bytes(prompt.encode('utf-8'))
    util.write_new_json(root / 'context.json', context)
    manifest.update(schema_version=2, intervention_id=intervention,
                    run_instance_id=run_instance_id or uuid.uuid4().hex, evidence_version=3,
                    assignment=assignment,
                    profile_files=util.tree_hashes(root / 'profiles'),
                    context_sha256=util.sha256_file(root / 'context.json'),
                    prompt_sha256=util.sha256_file(root / 'inputs' / 'prompt.txt'),
                    assets_sha256=util.tree_hashes(assets),
                    input_files=util.tree_hashes(root / 'inputs'))
    run.save_manifest(runs_dir, manifest['run_id'], manifest)
    if staged:
        for name in ('workspace','state'): (root/name).mkdir(exist_ok=True)
        controller = staged_input.create(root/'_controller/staged-input', run_id=manifest['run_id'],
            run_instance_id=manifest['run_instance_id'], task=task_id, condition=intervention,
            workspace=root/'workspace', worker_roots=[root/'inputs',root/'state'],
            initial_prompt=prompt, additional_prompt=additional, budget_seconds=condition['budget']['value'],
            boundary_contract=staged_plan['boundary_contract'], partition=partition,
            artifact_collection_policy=condition.get('collection_policy'))
        controller.bind_run(root)
        manifest = run.load_manifest(runs_dir, manifest['run_id'])
    return manifest


def validate_run(root):
    root = Path(root)
    manifest = util.read_json(root / 'manifest.json')
    if util.sha256_file(root / 'condition.json') != manifest['condition_sha256']:
        raise ValueError('Frozen condition changed')
    if util.tree_hashes(root / 'inputs') != manifest['input_files']:
        raise ValueError('Frozen input changed')
    if util.tree_hashes(root / 'evaluation-assets') != manifest['assets_sha256']:
        raise ValueError('Frozen evaluation assets changed')
    if util.sha256_file(root / 'context.json') != manifest['context_sha256']:
        raise ValueError('Frozen context evidence changed')
    if util.tree_hashes(root / 'profiles') != manifest['profile_files']:
        raise ValueError('Frozen profiles changed')
    return util.read_json(root / 'condition.json')
