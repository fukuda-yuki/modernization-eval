"""Fresh approved campaigns for bounded one- or two-case assignments.

No scientific defaults, inherited authorization, acquisition or scoring at init.
The approval is a local user receipt, not a cryptographic identity assertion.
"""
import copy
import re
import subprocess
import uuid
from pathlib import Path

from outer.harness import profiles, run, util
from research import live_pilot

KIND = 'approved_acquisition_campaign_v1'
SINGLE_KIND = 'approved_single_condition_campaign_v1'
MIXED_KIND = 'approved_observed_presentation_campaign_v1'
FIELDS = {'kind', 'campaign_id', 'output_root', 'pair_count', 'pair_concurrency',
          'assignments', 'runtime_by_task', 'task_revision', 'bounds', 'settings',
          'protected_roots', 'resource_monitor', 'resource_probe', 'browser_pin',
          'runtime_locks', 'thresholds'}
BOUNDS = set(live_pilot.BOUNDS) | {'max_pair_attempts'}
STAGED_FIELDS = {'staged_inputs', 'staged_retry'}


def read_approved(plan_ref, approval_ref):
    plan = util.read_json(live_pilot.checked(plan_ref))
    approval = util.read_json(live_pilot.checked(approval_ref))
    if (approval.get('authorized') is not True or approval.get('approved_by') != 'user'
            or not approval.get('authorization_reference')
            or approval.get('plan_sha256') != plan_ref['sha256']
            or approval.get('campaign_id') != plan.get('campaign_id')):
        raise ValueError('Explicit user approval for this exact campaign plan required')
    return plan


def validate(plan, repo):
    mixed = plan.get('kind') == MIXED_KIND
    extra = set(plan)-FIELDS-{'resource_origin'}-({'presentation_allocation'} if mixed else set())
    if (not FIELDS <= set(plan) or extra not in (set(), STAGED_FIELDS)
            or plan.get('kind') not in (KIND, SINGLE_KIND, MIXED_KIND)):
        raise ValueError('Explicit fresh campaign fields required; no inherited template/history')
    width = 2 if plan['kind'] == KIND else 1
    if mixed:
        from research import presentation_allocation
        presentation_allocation.validate(plan.get('presentation_allocation',{}),plan['assignments'])
        if extra != STAGED_FIELDS or plan['pair_count']!=100:
            raise ValueError('Observed mixed cohort requires 100 frozen single-case assignments')
    if 'resource_origin' in plan:
        if width != 1:raise ValueError('Technical resource origin is limited to single-condition campaigns')
        from research import resource_origin
        resource_origin.read(plan['resource_origin'])
    if extra:
        retry = plan['staged_retry']
        legacy = (set(retry)=={'known_pre_dispatch_retries','after_dispatch_retries'}
            and type(retry['known_pre_dispatch_retries']) is int and retry['known_pre_dispatch_retries'] in (0,1)
            and type(retry['after_dispatch_retries']) is int and retry['after_dispatch_retries']==0)
        shared = retry == {'technical_failure_policy':'sealed_infrastructure_failure_v1',
                           'retry_budget':'shared_reserved_attempts'}
        if width != 1 or not (legacy or shared):
            raise ValueError('Explicit bounded staged retry policy required')
    profiles.identifier(plan['campaign_id'])
    root = live_pilot.safe_path(plan['output_root'])
    if not plan['protected_roots']:
        raise ValueError('Explicit protected roots required')
    for old in [repo, *plan['protected_roots']]:
        old = live_pilot.safe_path(old)
        if live_pilot.within(root, old) or live_pilot.within(old, root):
            raise ValueError('Campaign output overlaps source/protected root')
    n = plan['pair_count']
    if (type(n) is not int or n < 1 or type(plan['pair_concurrency']) is not int
            or plan['pair_concurrency'] not in range(1, 4//width+1)):
        raise ValueError('Positive explicit assignment count and at most four model Runs required')
    bounds = plan['bounds']
    if set(bounds) != BOUNDS or any(type(v) is not int or v <= 0 for v in bounds.values()):
        raise ValueError('Explicit positive finite budgets required')
    if bounds['max_pairs'] != n or bounds['max_runs'] != width*n or bounds['max_pair_attempts'] < n:
        raise ValueError('Count and attempt budget mismatch')
    settings = plan['settings']
    if (set(settings) != {'model_id', 'provider', 'use_balance', 'paid_fallback'}
            or settings['provider'] != 'opencode-go' or settings['use_balance'] is not False
            or settings['paid_fallback'] is not False or not settings['model_id']):
        raise ValueError('Explicit model and existing Go contract without paid fallback required')
    if plan['thresholds'] != live_pilot.DEFAULT_THRESHOLDS:
        raise ValueError('Existing resource safety thresholds required')
    if len(plan['assignments']) != n:
        raise ValueError('Explicit assignment count mismatch')
    if plan['kind'] == SINGLE_KIND and len({c['condition'] for p in plan['assignments'] for c in p['cases']}) != 1:
        raise ValueError('Single-condition campaign requires one condition across all assignments')
    tasks = set()
    for index, pair in enumerate(plan['assignments'], 1):
        if type(pair.get('pair')) is not int or pair['pair'] != index or len(pair.get('cases', [])) != width:
            raise ValueError('Contiguous assignments with the declared case count required')
        cases = pair['cases']
        if len({c['task'] for c in cases}) != 1 or len({c['condition'] for c in cases}) != width:
            raise ValueError('Each assignment needs one task and distinct conditions')
        for slot, case in enumerate(cases, 1):
            if set(case) != {'task', 'condition', 'pair', 'slot', 'attempt', 'run_id'}:
                raise ValueError('Plan assignments must not carry old Run UUIDs or state')
            if (type(case['pair']) is not int or type(case['slot']) is not int
                    or case['pair'] != index or case['slot'] != slot or type(case['attempt']) is not int
                    or case['attempt'] < 1 or case['run_id'] != run.run_id_for(case['task'], case['condition'], case['attempt'])):
                raise ValueError('Assignment identity mismatch')
            tasks.add(case['task'])
            condition = profiles.resolve(repo, case['task'], case['condition'], plan['runtime_by_task'][case['task']],
                                         task_revision=plan['task_revision'], approved_model_id=settings['model_id'])
            runtime = condition['runtime']
            if (condition['intervention']['method'] in ('staged-explore','explore-observed')) != bool(extra):
                raise ValueError('Staged condition requires explicit request partitions and retry policy')
            if extra:
                from outer.harness import staged_input
                partition=util.read_json(live_pilot.checked(plan['staged_inputs'][case['task']]))
                staged_input.request_partition(condition['migration_request'],partition)
                if (mixed or condition['intervention']['method']=='explore-observed') and not staged_input.observed(partition['boundary_contract']):
                    raise ValueError('Explicit shared resumption observation policy required')
            if (runtime['model_id'] != plan['settings']['model_id']
                    or runtime['timeout_seconds'] != bounds['run_seconds']
                    or runtime['provider_timeout_seconds'] != bounds['provider_seconds']):
                raise ValueError('Approved budget/model differs from pinned runtime profile')
    if extra and set(plan['staged_inputs']) != tasks:
        raise ValueError('Exact task/staged partition mapping required')
    if set(plan['runtime_by_task']) != tasks or set(plan['runtime_locks']) != set(plan['runtime_by_task'].values()):
        raise ValueError('Exact task/runtime lock mapping required')
    for task in tasks:
        rid = plan['runtime_by_task'][task]
        lockpath = live_pilot.checked(plan['runtime_locks'][rid])
        profile = profiles.read(repo, 'runtimes', rid)
        if lockpath != profiles.runtime_root(repo, task, profile)/'lock.json':
            raise ValueError('Foreign runtime lock')
        lock = util.read_json(lockpath)
        ledger = profiles.task_profile(repo, task, plan['task_revision'])
        if (lock.get('task_profile_revision') != plan['task_revision'] or lock.get('runtime_profile_id') != rid
                or lock.get('evaluator_version') != ledger['evaluation']['evaluation_version']
                or lock.get('evaluator_project') != ledger['evaluation']['project']):
            raise ValueError('Evaluator lock differs from approved profile')
        from research.repaired_runtime import validate_repaired_runtime_binding
        validate_repaired_runtime_binding(repo, lock)
    monitor = live_pilot.checked(plan['resource_monitor'])
    if live_pilot.checked(plan['resource_probe']) != monitor.with_name('wave_resource_probe.py'):
        raise ValueError('Foreign resource probe')
    from research.resource_supervisor import load_monitor
    load_monitor(dict(resource_monitor=plan['resource_monitor'], resource_probe=plan['resource_probe'],
                      cases_per_assignment=width))
    live_pilot.checked(plan['browser_pin'])
    return root


def template(plan):
    result = {k: copy.deepcopy(plan[k]) for k in (
        'assignments', 'runtime_by_task', 'task_revision', 'bounds', 'settings',
        'protected_roots', 'resource_monitor', 'resource_probe', 'browser_pin',
        'runtime_locks', 'thresholds', 'pair_concurrency')} | dict(
        kind=live_pilot.MAIN_KIND, schema_version=1, require_fixed_instances=True)
    # Preserve historical templates byte-for-byte; single-case mode is explicit.
    if plan['kind'] != KIND:
        result['cases_per_assignment'] = 1
    if plan['kind']==MIXED_KIND:result['presentation_allocation']=copy.deepcopy(plan['presentation_allocation'])
    for name in STAGED_FIELDS & set(plan): result[name] = copy.deepcopy(plan[name])
    return result


def initialize(repo, plan_path, approval_path):
    repo = live_pilot.safe_path(repo)
    plan_ref, approval_ref = live_pilot.reference(plan_path), live_pilot.reference(approval_path)
    plan = read_approved(plan_ref, approval_ref)
    root = validate(plan, repo)
    if root.exists():
        raise ValueError('Campaign root already exists; resume its config')
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo, text=True).strip():
        raise ValueError('Commit executing controller first')
    names = set(live_pilot.PIN_FILES) | {'research/acquisition_pipeline.py', 'research/campaign_initialization.py'}
    # Pin every tracked Python controller and profile used by the existing engine.
    names.update(subprocess.check_output(['git', 'ls-files', 'research/*.py', 'outer/harness/*.py', 'outer/profiles/*.json'], cwd=repo, text=True).splitlines())
    config = dict(kind='acquisition_evaluation_pipeline_v1', pipeline_id=uuid.uuid4().hex,
        campaign_id=plan['campaign_id'], campaign_plan=plan_ref, campaign_approval=approval_ref,
        root=str(root), repo=str(repo), pair_count=plan['pair_count'],
        source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
        source_pins={n: util.sha256_file(repo/n) for n in sorted(names)}, template=template(plan),
        accepted_slots=[], attempted_slots=[], reserved_pair_attempts=0,
        usage=dict(requests=0, observed_tokens=0, accumulated_run_seconds=0., dispatched_runs=0,
                   unknown_usage_requests=[]), bounds=copy.deepcopy(plan['bounds']), started_at=run.now(),
        policy=dict(active_model_pairs=plan['pair_concurrency'],
                    active_model_runs=(2 if plan['kind'] == KIND else 1)*plan['pair_concurrency'],
                    evaluation_workers=1, quality_selection=False, intermediate_publication=False), at=run.now())
    if 'resource_origin' in plan:
        from research import resource_origin
        config['resource_origin']=copy.deepcopy(plan['resource_origin'])
        config['started_at'],config['usage']=resource_origin.read(plan['resource_origin'])
    root.mkdir(parents=True, exist_ok=False)
    util.write_new_json(root/'config.json', config)
    util.write_new_json(root/'genesis.json', dict(config=live_pilot.reference(root/'config.json')))
    return config


def verify_config(config, repo):
    plan = read_approved(config['campaign_plan'], config['campaign_approval'])
    root = validate(plan, repo)
    genesis = util.read_json(root/'genesis.json')
    initial_usage=dict(requests=0, observed_tokens=0, accumulated_run_seconds=0.,dispatched_runs=0,unknown_usage_requests=[])
    if 'resource_origin' in plan:
        from research import resource_origin
        origin,initial_usage=resource_origin.read(plan['resource_origin'])
        if config.get('resource_origin')!=plan['resource_origin'] or config['started_at']!=origin:
            raise ValueError('Shared technical resource origin changed')
    if (live_pilot.checked(genesis['config']) != root/'config.json'
            or util.read_json(root/'config.json') != config):
        raise ValueError('Campaign genesis changed; resume must preserve time and state')
    if (str(root) != config['root'] or config['campaign_id'] != plan['campaign_id']
            or config['pair_count'] != plan['pair_count'] or config['template'] != template(plan)
            or config['bounds'] != plan['bounds']
            or config['policy']['active_model_pairs'] != plan['pair_concurrency']
            or config['policy']['active_model_runs'] != (2 if plan['kind'] == KIND else 1)*plan['pair_concurrency']
            or config['policy'].get('evaluation_workers') != 1
            or config['policy'].get('quality_selection') is not False
            or config['accepted_slots'] or config['attempted_slots'] or config['reserved_pair_attempts']
            or config['usage'] != initial_usage):
        raise ValueError('Fresh campaign config differs from approved plan/genesis')
    live_pilot.verify_pins(repo, config['source_pins'])
    return plan


def verify_epoch(path, repo):
    epoch = util.read_json(path)
    config = util.read_json(live_pilot.checked(epoch['campaign_config']))
    verify_config(config, repo)
    expected = template(read_approved(config['campaign_plan'], config['campaign_approval']))
    for key, value in expected.items():
        if key == 'assignments':
            actual = copy.deepcopy(epoch[key])
            ids = []
            for pair in actual:
                for case in pair['cases']:
                    ids.append(case.pop('run_instance_id', None))
            if actual != value or len(set(ids)) != len(ids) or any(not isinstance(i, str) or not re.fullmatch('[a-f0-9]{32}', i) for i in ids):
                raise ValueError('Epoch assignments differ or UUIDs repeat')
        elif epoch.get(key) != value:
            raise ValueError('Epoch differs from approved campaign: '+key)
    batch = live_pilot.safe_path(epoch['batch'])
    if (not batch.is_relative_to(Path(config['root'])/'epochs') or batch != Path(path).parent/'data'
            or epoch['source_pins'] != config['source_pins'] or epoch['source_commit'] != config['source_commit']
            or live_pilot.checked(epoch['launch_supervisor']) != Path(repo)/'research/live_pilot_launcher.py'):
        raise ValueError('Foreign campaign epoch/code/output')
    return epoch
