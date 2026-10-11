"""Run a repaired evaluator on a saved artifact in an independent assessment.

This is not acquisition: it never calls a provider and never writes a Run index.
New specifications and explicitly pinned same-spec observation revisions are
separate paths; old contracts and original Run indexes are never relabeled.
The existing no-network evaluator container and isolated browser path are reused.
"""
import argparse
import json
import re
from pathlib import Path
import shutil
import uuid

from outer.harness import browser_cleanup, browser_review, evaluate, profiles, runtime, util
from outer.harness.security import child_environment


def byte_inventory(root):
    """Include bin/obj, DB sidecars and previous output, not just artifact hash."""
    root = Path(root).resolve()
    result = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink() or path.is_junction():
            raise ValueError('Saved evidence must not contain links')
        if path.is_file():
            result[path.relative_to(root).as_posix()] = util.sha256_file(path)
    return result


def _inside(path, root):
    return path == root or path.is_relative_to(root)


def requirement_inventory(spec):
    """Descriptions may be clarified; identity, severity and checks may not drift."""
    return [(r['id'], r.get('severity'), r.get('weight'), [c['id'] for c in r['checks']])
            for r in spec['requirements']]


def controller_inventory(repo, *, observation=False):
    repo = Path(repo).resolve()
    paths = list((repo/'outer/harness').glob('*.py')) + list((repo/'inner/browser').glob('*.cjs'))
    paths += [repo/'research/saved_reassessment.py', repo/'research/repair_spec.py']
    if observation:
        paths += [repo/'research'/name for name in ('observation_revision.py', 'repaired_runtime.py',
                                                  'catalog_environment.py', 'live_pilot.py')]
    if not paths or any(not p.is_file() for p in paths):
        raise ValueError('Assessment requires its actual controller source tree')
    return {p.relative_to(repo).as_posix(): util.sha256_file(p) for p in sorted(paths)}


def prepare(*, source, destination, evaluator_bundle, assembly, spec,
            evaluation_version, repair_contract, observation_plan=None):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    bundle, spec = Path(evaluator_bundle).resolve(), Path(spec).resolve()
    if _inside(destination, source) or _inside(source, destination):
        raise ValueError('Assessment and original Run must have separate roots')
    if Path(assembly).name != assembly or not assembly.endswith('.dll'):
        raise ValueError('Expected one evaluator DLL filename')
    manifest = util.read_json(source/'manifest.json')
    condition = util.read_json(source/'condition.json')
    observation = None
    if observation_plan is not None:
        from research import observation_revision
        if not isinstance(observation_plan, dict):
            from research.live_pilot import reference
            observation_plan = reference(Path(observation_plan).resolve())
        observation = observation_revision.selected_source(Path(__file__).resolve().parents[1], observation_plan, source)
        plan, selected, family, entry = observation
        observation_revision.validate_destination(Path(__file__).resolve().parents[1], plan, destination,
                                                   plan_ref=observation_plan)
        if (bundle != Path(entry['bundle']).resolve() or assembly != family['assembly']
                or repair_contract != observation_revision.REVISION
                or evaluation_version != family['version']
                or util.sha256_file(spec) != condition['evaluation']['spec_sha256']):
            raise ValueError('Observation revision must use its exact accepted bundle and unchanged spec')
    if condition.get('schema_version') != 2:
        raise ValueError('Only frozen isolated schema-2 Runs are supported')
    profiles.validate_run(source)
    snapshot = util.read_json(source/'snapshot.json')
    original_spec = util.read_json(source/'evaluation-assets/requirements.json')
    new_spec = util.read_json(spec)
    if manifest.get('stop_confirmed') is not True or manifest.get('submission_fixed') is not True:
        raise ValueError('Source acquisition must be stopped and fixed')
    if not re.fullmatch(r'[0-9a-f]{32}', manifest.get('run_instance_id', '')) or not repair_contract:
        raise ValueError('Source UUID and explicit repair contract are required')
    if ((evaluation_version == condition['evaluation']['evaluation_version'] and observation is None)
            or new_spec.get('specVersion') != evaluation_version):
        raise ValueError('An explicit new matching evaluation/spec version is required')
    if (new_spec.get('taskId') != original_spec.get('taskId')
            or requirement_inventory(new_spec) != requirement_inventory(original_spec)
            or new_spec.get('migrationContract') != original_spec.get('migrationContract')):
        raise ValueError('This repair adapter does not change requirement/check inventories')
    artifact = util.artifact_hash(source/'frozen')
    if manifest.get('condition_sha256') != util.sha256_file(source/'condition.json'):
        raise ValueError('Source manifest does not bind its actual condition')
    if snapshot.get('run_id') != manifest['run_id']:
        raise ValueError('Snapshot belongs to another source Run')
    if artifact != snapshot.get('artifact_sha256'):
        raise ValueError('Original frozen artifact does not match its snapshot')
    if util.sha256_file(source/'evaluation-assets/requirements.json') != condition['evaluation']['spec_sha256']:
        raise ValueError('Original specification does not match its condition')
    before = byte_inventory(source)
    build_inventory = byte_inventory(bundle)
    if assembly not in build_inventory:
        raise ValueError('Repaired evaluator bundle is missing its DLL')
    assessment_id = uuid.uuid4().hex
    # One prepared observation per selected UUID in this output. An interrupted
    # or duplicate invocation cannot silently allocate a replacement assessment.
    stage = destination/(manifest['run_instance_id'] if observation else assessment_id)
    stage.mkdir(parents=True, exist_ok=False)
    # Source remains untouched. A fresh work DB is initialized from copied assets.
    shutil.copytree(source/'frozen', stage/'frozen')
    shutil.copytree(source/'evaluation-assets', stage/'evaluation-assets',
        ignore=lambda directory, names: ['evaluator']
        if Path(directory).resolve() == source/'evaluation-assets' else [])
    shutil.copytree(bundle, stage/'evaluation-assets/evaluator')
    if observation and 'build-receipt.json' not in build_inventory:
        from research.live_pilot import checked
        shutil.copyfile(checked(entry['build_receipt']), stage/'evaluation-assets/evaluator/build-receipt.json')
        build_inventory = byte_inventory(stage/'evaluation-assets/evaluator')
    shutil.copyfile(spec, stage/'evaluation-assets/requirements.json')
    copied_assets = byte_inventory(stage/'evaluation-assets')
    for name, digest in before.items():
        if name.startswith('evaluation-assets/'):
            relative = name[len('evaluation-assets/'):]
            if relative != 'requirements.json' and not relative.startswith('evaluator/'):
                if copied_assets.get(relative) != digest:
                    raise RuntimeError('Independent source asset copy mismatch')
    new_condition = json.loads(json.dumps(condition))
    new_condition['evaluation'].update(assembly=assembly, evaluation_version=evaluation_version,
        spec_sha256=util.sha256_file(spec), evaluator_sha256=build_inventory[assembly],
        evaluator_build={'kind': 'repaired_bundle', 'inventory': build_inventory,
            'build_receipt_sha256': build_inventory.get('build-receipt.json'),
            'source': 'assessment-bound repaired bundle; original build remains in source condition'})
    new_condition['runtime_lock'].update(evaluator_sha256=build_inventory[assembly],
        evaluator_build=new_condition['evaluation']['evaluator_build'],
        evaluator_files={name: {'sha256': sha, 'bytes': (stage/'evaluation-assets/evaluator'/name).stat().st_size}
                         for name, sha in build_inventory.items()})
    if observation:
        new_condition['evaluation']['evaluator_build']['observation_revision'] = plan['revision']
        new_condition['runtime_lock']['images'] = dict(entry['images'])
    controllers = controller_inventory(Path(__file__).resolve().parents[1], observation=observation is not None)
    new_condition['runtime_lock']['controller_files'] = controllers
    # This is assessment execution configuration, never a new acquisition condition.
    util.write_new_json(stage/'execution-config.json', new_condition)
    record = {'schema_version': 1, 'kind': 'saved_artifact_reassessment',
        'assessment_id': assessment_id, 'source_path_private': str(source),
        'source_run_id': manifest['run_id'], 'source_run_instance_id': manifest['run_instance_id'],
        'acquisition_count_increment': 0, 'source_artifact_sha256': artifact,
        'source_condition_sha256': util.sha256_file(source/'condition.json'),
        'source_spec_sha256': condition['evaluation']['spec_sha256'],
        'source_evaluation_version': condition['evaluation']['evaluation_version'],
        'evaluation_version': evaluation_version, 'repair_contract': repair_contract,
        'evaluator_sha256': build_inventory[assembly], 'build_inventory': build_inventory,
        'new_spec_sha256': util.sha256_file(spec), 'source_byte_inventory': before,
        'execution_config_sha256': util.sha256_file(stage/'execution-config.json'),
        'assessment_assets_inventory': byte_inventory(stage/'evaluation-assets'),
        'controller_inventory': controllers,
        'scope': 'fresh evaluation of saved artifact; not historical action reconstruction'}
    if observation:
        record.update(observation_revision=observation_revision.REVISION, observation_plan=observation_plan,
                      source_evaluation=selected['evaluation'], source_evaluation_id=selected['evaluation_id'])
    util.write_new_json(stage/'assessment.json', record)
    if byte_inventory(source) != before or util.artifact_hash(stage/'frozen') != artifact:
        raise RuntimeError('Source changed while assessment inputs were copied; do not execute')
    return stage


def execute(stage, *, repo, timeout=1800):
    if not 1 <= timeout <= 1800:
        raise ValueError('Assessment timeout must be finite and at most 1800 seconds')
    assessment = util.read_json(Path(stage)/'assessment.json')
    config = util.read_json(Path(stage)/'execution-config.json')
    if (config['evaluation'].get('evaluator_build', {}).get('observation_revision')
            and 'observation_plan' not in assessment):
        raise ValueError('Observation build requires its selected plan at execution')
    if 'observation_plan' in assessment:
        from research import observation_revision, catalog_environment, live_pilot
        plan = observation_revision.validate_assessment(repo, stage, assessment)
        if (Path(stage)/'output').exists() or (Path(stage)/'evaluator-runtime-probe.json').exists():
            raise FileExistsError('Retain existing assessment execution/probe; no implicit retry')
        image = config['runtime_lock']['images']['evaluator']
        if runtime.image_id(image) != image:
            raise ValueError('Observation evaluator image identity mismatch')
        sdk = runtime.docker('run', '--rm', '--network', 'none', image, 'dotnet', '--version').stdout.strip()
        if sdk != '8.0.425':
            raise ValueError('Observation evaluator requires the pinned SDK 8.0.425')
        util.write_new_json(Path(stage)/'evaluator-runtime-probe.json',
            {'image':image,'dotnet':sdk,'model_called':False})
        browser = util.read_json(live_pilot.checked(plan['browser_pin']))
        with catalog_environment.activated(browser, repo):
            return _execute(stage, repo=repo, timeout=timeout)
    return _execute(stage, repo=repo, timeout=timeout)


def _execute(stage, *, repo, timeout=1800):
    stage, repo = Path(stage).resolve(), Path(repo).resolve()
    if not 1 <= timeout <= 1800:
        raise ValueError('Assessment timeout must be finite and at most 1800 seconds')
    assessment = util.read_json(stage/'assessment.json')
    source = Path(assessment['source_path_private']).resolve()
    if _inside(stage, source) or _inside(source, stage):
        raise ValueError('Assessment execution must remain outside the original Run')
    if controller_inventory(repo, observation='observation_plan' in assessment) != assessment['controller_inventory']:
        raise ValueError('Assessment controller differs from its prepared source bytes')
    if byte_inventory(source) != assessment['source_byte_inventory']:
        raise ValueError('Original changed since preparation')
    if (util.artifact_hash(stage/'frozen') != assessment['source_artifact_sha256']
            or util.sha256_file(stage/'evaluation-assets/requirements.json') != assessment['new_spec_sha256']
            or byte_inventory(stage/'evaluation-assets/evaluator') != assessment['build_inventory']
            or byte_inventory(stage/'evaluation-assets') != assessment['assessment_assets_inventory']
            or util.sha256_file(stage/'execution-config.json') != assessment['execution_config_sha256']):
        raise ValueError('Assessment input/build changed since preparation')
    output = stage/'output'
    output.mkdir(exist_ok=False)  # Partial/interrupted attempts cannot be overwritten.
    record = {'schema_version': 1, 'assessment_id': assessment['assessment_id'],
              'operation_status': 'not_completed', 'adopted': False, 'quality': None,
              'source_unchanged': None, 'cleanup_confirmed': False, 'errors': []}
    if 'observation_plan' in assessment:
        record.update({key: assessment[key] for key in ('observation_revision', 'observation_plan',
            'source_run_id', 'source_run_instance_id', 'source_evaluation', 'source_evaluation_id')})
        record['evaluator_runtime_probe_sha256'] = util.sha256_file(stage/'evaluator-runtime-probe.json')
    if (stage/'STOP').exists():
        record.update(operation_status='stopped_before_execution', source_unchanged=True)
        util.write_new_json(stage/'result.json', record)
        return record
    condition = util.read_json(stage/'execution-config.json')
    http = output/'http-only'; http.mkdir()
    work = stage/'work'; work.mkdir()
    name = None
    try:
        name, command = runtime.scoring_command(condition, stage/'frozen', http, work,
            stage/'evaluation-assets', assessment['evaluation_version'], 1)
        if command[:2] != ['docker', 'run'] or not re.fullmatch(r's2-score-[0-9a-f]{32}', name):
            name = None
            raise ValueError('Expected the isolated, independently owned evaluator command')
        owner = browser_cleanup.register(http, assessment['assessment_id'], 'container', name)
        command[2:2] = ['--label', 'sample2.browser-review='+owner,
                        '--label', 'sample2.assessment='+assessment['assessment_id']]
        with (output/'stdout.log').open('xb') as stdout, (output/'stderr.log').open('xb') as stderr:
            code, timed_out = evaluate.run_evaluator(command, repo, child_environment(), stdout, stderr, timeout)
        record.update(evaluator_exit_code=code, timed_out=timed_out)
        # Cleanup precedes further product execution, as in the existing score route.
        record['cleanup_confirmed'] = browser_cleanup.cleanup(http, locked=True)['confirmed']
        name = None if record['cleanup_confirmed'] else name
        if not record['cleanup_confirmed']:
            raise RuntimeError('Evaluator container cleanup unconfirmed')
        if (stage/'STOP').exists():
            record['operation_status'] = 'stopped_after_http'
            code = 2
        if browser_review.http_phase_eligible(condition,http,exit_code=record['evaluator_exit_code'],
                timed_out=timed_out,cleanup_confirmed=record['cleanup_confirmed'],
                stopped=(stage/'STOP').exists(),frozen=stage/'frozen',
                artifact_hash=assessment['source_artifact_sha256'],
                spec=stage/'evaluation-assets/requirements.json',spec_hash=assessment['new_spec_sha256'],
                evaluator_hash=assessment['evaluator_sha256']):
            if browser_review.required(assessment['evaluation_version']):
                code = browser_review.complete_evaluation(repo, condition, stage/'frozen', http,
                    work/'publish', stage/'evaluation-assets', output, assessment['assessment_id'], 1)
                record['browser_exit_code'] = code
        if record['operation_status'] != 'stopped_after_http':
            record['operation_status'] = 'evaluation_finished' if code == 0 else 'evaluation_partial_or_fault'
    except (Exception, KeyboardInterrupt) as exc:
        record['errors'].append(type(exc).__name__ + ': ' + str(exc))
        record['operation_status'] = 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'evaluation_fault'
    finally:
        if name:
            try:
                record['cleanup_confirmed'] = browser_cleanup.cleanup(http, locked=True)['confirmed']
            except Exception as exc:
                record['errors'].append('cleanup: ' + type(exc).__name__)
            if not record['cleanup_confirmed']:
                record['operation_status'] = 'cleanup_unconfirmed'
        try:
            record['source_unchanged'] = byte_inventory(source) == assessment['source_byte_inventory']
            for candidate in (output/'evaluation.json', http/'evaluation.json'):
                if candidate.is_file():
                    record['raw_result'] = candidate.relative_to(stage).as_posix()
                    result = util.read_json(candidate)
                    record['raw_verdict'] = result.get('verdict')
                    mismatches = evaluate.check_mismatches(result, condition, assessment['evaluation_version'],
                        stage/'frozen', assessment['source_artifact_sha256'],
                        stage/'evaluation-assets/requirements.json', assessment['new_spec_sha256'])
                    record['mismatches'] = mismatches
                    reported_build = evaluate._reported_evaluator_sha256(candidate.parent)
                    record['evaluator_sha256_reported'] = reported_build
                    if reported_build != assessment['evaluator_sha256']:
                        mismatches.append({'check': 'actual_evaluator_build',
                            'expected': assessment['evaluator_sha256'], 'actual': reported_build})
                    cleanup_receipt = browser_cleanup.latest(output)
                    full_browser = (browser_review.coverage_complete(result)
                        and result.get('reviewRunInstanceId') == assessment['assessment_id']
                        and browser_review.stored_coverage_complete(output, assessment['assessment_id'],
                            assessment['source_artifact_sha256'], assessment['new_spec_sha256'])
                        and cleanup_receipt.get('confirmed') is True
                        and cleanup_receipt.get('run_instance_id') == assessment['assessment_id'])
                    record['full_bound_browser_observation'] = full_browser
                    record['adopted'] = (not mismatches and record['source_unchanged']
                        and record['cleanup_confirmed'] and record['operation_status'] == 'evaluation_finished'
                        and candidate == output/'evaluation.json' and result.get('researchStatus') == 'complete'
                        and full_browser and result.get('quality') is not None)
                    record['quality'] = result.get('quality') if record['adopted'] else None
                    break
        except Exception as exc:
            record.update(operation_status='postprocessing_fault', adopted=False, quality=None)
            record['errors'].append('postprocessing: ' + type(exc).__name__)
        if record['source_unchanged'] is False:
            record.update(operation_status='source_invariance_failed', adopted=False, quality=None)
        elif record['source_unchanged'] is None:
            record.update(operation_status='source_invariance_unverified', adopted=False, quality=None)
        util.write_new_json(stage/'result.json', record)
    return record


def _saved_cleanup_bound(directory, instance):
    """Read existing ownership/removal receipts; never inspect or remove Docker."""
    try:
        state = util.read_json(Path(directory)/'browser-resources.json')
        receipt = browser_cleanup.latest(directory)
        if (receipt.get('confirmed') is not True or receipt.get('status') != 'complete'
                or state.get('run_instance_id') != instance or receipt.get('run_instance_id') != instance
                or state.get('owner') != receipt.get('owner')
                or not re.fullmatch(r's2-browser-[0-9a-f]{32}', state.get('owner', ''))): return False
        resources, removed = state.get('resources'), receipt.get('resources')
        if not isinstance(resources, list) or not resources or not isinstance(removed, list): return False
        keys = lambda items: [(r['kind'], r['name']) for r in items]
        expected, actual = keys(resources), keys(removed)
        if len(expected) != len(set(expected)) or len(actual) != len(set(actual)) or set(expected) != set(actual): return False
        by_key = {(r['kind'], r['name']): r for r in removed}
        for resource in resources:
            item = by_key[(resource['kind'], resource['name'])]
            if (resource['kind'] not in ('container', 'network')
                    or not re.fullmatch(r's2-(?:browser|score)-[0-9a-f]{32}(?:-net)?', resource['name'])
                    or item.get('confirmed') is not True or item.get('status') not in ('removed', 'absent')
                    or resource.get('id') and resource['id'] != item.get('id')): return False
            if item['status'] == 'removed' and not re.fullmatch(r'[0-9a-f]{64}', item.get('id', '')): return False
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def revalidate(stage, destination, *, repo=None):
    """Validate saved execution with current readers, writing only a new receipt.

    This does not repeat scoring, product/browser actions, cleanup, or acquisition.
    Historical result and execution-controller bytes remain unchanged. A finite
    reported failure is separate from full observation and numeric quality.
    """
    import math
    stage, destination = Path(stage).resolve(), Path(destination).resolve()
    repo = Path(repo or Path(__file__).resolve().parents[1]).resolve()
    assessment = util.read_json(stage/'assessment.json')
    source = Path(assessment['source_path_private']).resolve()
    if (_inside(stage, source) or _inside(source, stage)
            or _inside(destination, source) or _inside(destination, stage)):
        raise ValueError('Validation receipt must remain outside source and assessment roots')
    # This is outside the receipt-producing try/finally: even invalid evidence
    # must never write a failure receipt into another selected Run or its inputs.
    if 'observation_plan' in assessment:
        from research import observation_revision, live_pilot
        plan = util.read_json(live_pilot.checked(assessment['observation_plan']))
        observation_revision.validate_destination(repo, plan, destination,
                                                   plan_ref=assessment['observation_plan'])
    elif assessment.get('observation_revision'):
        raise ValueError('Observation validation requires its selected plan before any receipt')
    if destination.exists(): raise FileExistsError(destination)
    receipt = {'schema_version':1, 'kind':'saved_assessment_read_only_validation',
        'validation_id':uuid.uuid4().hex, 'assessment_id':assessment['assessment_id'],
        'source_run_id':assessment['source_run_id'], 'source_run_instance_id':assessment['source_run_instance_id'],
        'model_calls':0, 'acquisition_count_increment':0, 'adopted':False, 'quality':None,
        'validation_status':'invalid_evidence', 'finite_failure':None,
        'observer_fault':None, 'observer_faults':None,
        'finite_failure_scope':'Bound reported requirement labels; not proof that every dependent operation was observed.',
        'evaluation_controller_inventory':assessment.get('controller_inventory'),
        'validation_controller_inventory':controller_inventory(repo, observation='observation_plan' in assessment), 'errors':[]}
    receipt['evaluation_controller_matches_current'] = (
        receipt['evaluation_controller_inventory'] == receipt['validation_controller_inventory'])
    receipt['validation_implementation_sha256'] = util.sha256_file(Path(__file__).resolve())
    source_before = stage_before = None
    def require(ok, name):
        if not ok: raise ValueError(name)
    def inventory_digest(value):
        return util.sha256_bytes(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())
    try:
        source_before, stage_before = byte_inventory(source), byte_inventory(stage)
        require(source_before == assessment['source_byte_inventory'], 'source_byte_inventory')
        profiles.validate_run(source)
        if 'observation_plan' in assessment:
            from research import observation_revision
            observation_revision.validate_assessment(repo, stage, assessment)
        source_manifest, snapshot = util.read_json(source/'manifest.json'), util.read_json(source/'snapshot.json')
        require(source_manifest.get('stop_confirmed') is True and source_manifest.get('submission_fixed') is True,
                'source_stop_and_fixed')
        require(source_manifest.get('run_id') == assessment['source_run_id']
                and source_manifest.get('run_instance_id') == assessment['source_run_instance_id']
                and snapshot.get('run_id') == assessment['source_run_id'], 'source_identity')
        require(util.sha256_file(source/'condition.json') == assessment['source_condition_sha256'], 'source_condition')
        source_condition = util.read_json(source/'condition.json')
        require(source_condition['evaluation']['evaluation_version'] == assessment['source_evaluation_version']
                and source_condition['evaluation']['spec_sha256'] == assessment['source_spec_sha256']
                == util.sha256_file(source/'evaluation-assets/requirements.json')
                and (assessment['evaluation_version'] != assessment['source_evaluation_version']
                     or 'observation_plan' in assessment), 'source_evaluation_contract')
        require(assessment.get('kind') == 'saved_artifact_reassessment'
                and assessment.get('acquisition_count_increment') == 0
                and re.fullmatch(r'[0-9a-f]{32}', assessment['assessment_id']) is not None, 'assessment_identity')
        require(util.sha256_file(stage/'execution-config.json') == assessment['execution_config_sha256'], 'execution_config')
        condition = util.read_json(stage/'execution-config.json'); config = condition['evaluation']
        require(condition.get('schema_version') == 2 and config['evaluation_version'] == assessment['evaluation_version'], 'execution_version')
        require(util.artifact_hash(stage/'frozen') == assessment['source_artifact_sha256']
                == snapshot['artifact_sha256'] == util.artifact_hash(source/'frozen'), 'artifact_identity')
        require(byte_inventory(stage/'evaluation-assets') == assessment['assessment_assets_inventory'], 'assessment_assets')
        bundle = stage/'evaluation-assets/evaluator'
        require(byte_inventory(bundle) == assessment['build_inventory'], 'actual_bundle')
        assembly = config['assembly']
        require(Path(assembly).name == assembly and assembly.endswith('.dll'), 'assembly_filename')
        require(util.sha256_file(bundle/assembly) == assessment['evaluator_sha256'] == config['evaluator_sha256'], 'actual_dll')
        require(condition['runtime_lock']['evaluator_sha256'] == assessment['evaluator_sha256']
                and condition['runtime_lock']['evaluator_files'] == util.tree_hashes(bundle)
                and condition['runtime_lock']['controller_files'] == assessment['controller_inventory'], 'historical_runtime_binding')
        spec_path = stage/'evaluation-assets/requirements.json'
        require(util.sha256_file(spec_path) == assessment['new_spec_sha256'] == config['spec_sha256'], 'spec_hash')
        spec = util.read_json(spec_path)
        require(spec['specVersion'] == assessment['evaluation_version'] and spec['taskId'] == condition['task_id'], 'spec_identity')
        original = util.read_json(stage/'result.json')
        if 'observation_plan' in assessment:
            probe = util.read_json(stage/'evaluator-runtime-probe.json')
            require(probe == {'image':condition['runtime_lock']['images']['evaluator'],
                              'dotnet':'8.0.425','model_called':False}
                    and original.get('evaluator_runtime_probe_sha256') == util.sha256_file(stage/'evaluator-runtime-probe.json')
                    and all(original.get(k) == assessment[k] for k in ('observation_revision','observation_plan',
                        'source_run_id','source_run_instance_id','source_evaluation','source_evaluation_id')),
                    'observation_revision_execution_binding')
            receipt.update({key: assessment[key] for key in ('observation_revision','observation_plan',
                                                            'source_evaluation','source_evaluation_id')})
        receipt['original_result_sha256'] = util.sha256_file(stage/'result.json')
        require(original.get('assessment_id') == assessment['assessment_id']
                and original.get('cleanup_confirmed') is True and original.get('source_unchanged') is True
                and type(original.get('evaluator_exit_code')) is int
                and original.get('evaluator_exit_code') in (0,2) and original.get('timed_out') is False
                and original.get('errors') == [] and original.get('operation_status') in
                    ('evaluation_finished', 'evaluation_partial_or_fault'), 'original_execution_confirmations')
        out, http = stage/'output', stage/'output/http-only'
        require(original.get('raw_result') == 'output/evaluation.json', 'original_output_path')
        output = util.read_json(out/'evaluation.json'); baseline = util.read_json(http/'evaluation.json')
        require((original['evaluator_exit_code']==0 and assessment['evaluation_version']!='1.6.0')
            or browser_review.http_phase_eligible(condition,http,
            exit_code=original['evaluator_exit_code'],timed_out=original['timed_out'],
            cleanup_confirmed=original['cleanup_confirmed'],stopped=(stage/'STOP').exists(),
            frozen=stage/'frozen',artifact_hash=assessment['source_artifact_sha256'],
            spec=spec_path,spec_hash=assessment['new_spec_sha256'],evaluator_hash=assessment['evaluator_sha256']),
            'http_browser_phase_eligibility')
        for directory, result in ((out, output), (http, baseline)):
            require(not evaluate.check_mismatches(result, condition, assessment['evaluation_version'],
                    stage/'frozen', assessment['source_artifact_sha256'], spec_path, assessment['new_spec_sha256']), 'output_identity')
            require(evaluate._reported_evaluator_sha256(directory) == assessment['evaluator_sha256'], 'reported_dll')
            require(_saved_cleanup_bound(directory, assessment['assessment_id']), 'owned_cleanup_receipts')
        require(output.get('reviewRunInstanceId') == assessment['assessment_id'], 'output_assessment_identity')
        require(output.get('baselineEvaluationSha256') == util.sha256_file(http/'evaluation.json')
                and output.get('baselineResultsSha256') == util.sha256_file(http/'results.jsonl'), 'baseline_evidence_links')
        require(original.get('raw_verdict') == output.get('verdict')
                and original.get('evaluator_sha256_reported') == assessment['evaluator_sha256'], 'original_report_binding')
        faults = output.get('evaluatorFaults')
        require(isinstance(faults, list), 'observer_fault_inventory')
        receipt['observer_faults'] = faults
        receipt['observer_fault'] = bool(faults) or (out/'browser-fault.json').is_file()
        receipt['browser_fault_receipt_sha256'] = (util.sha256_file(out/'browser-fault.json')
            if (out/'browser-fault.json').is_file() else None)
        evaluation_hash = util.sha256_file(out/'evaluation.json')
        receipt.update(output_sha256=evaluation_hash, evaluation_version=assessment['evaluation_version'],
                       raw_verdict=output.get('verdict'), raw_research_status=output.get('researchStatus'))
        receipt['finite_failure'] = browser_review.stored_failure(out, assessment['assessment_id'],
            assessment['source_artifact_sha256'], assessment['new_spec_sha256'], evaluation_hash, baseline_directory=http)
        receipt['dependent_check_cascade_warning'] = (assessment['evaluation_version'] == '1.4.0'
            and output.get('researchStatus') != 'complete'
            and bool({'R-014','R-015'} & set((receipt['finite_failure'] or {}).get('requirements', []))))
        if receipt['dependent_check_cascade_warning']:
            receipt['finite_failure_scope'] += (' C-015/C-016 labels may follow a failed AddToCart prerequisite; '
                'their reported failures do not establish actual removal failures. Raw labels are preserved.')
        complete = (browser_review.coverage_complete(output)
            and browser_review.stored_coverage_complete(out, assessment['assessment_id'],
                assessment['source_artifact_sha256'], assessment['new_spec_sha256'])
            and output.get('researchStatus') == 'complete' and receipt['observer_fault'] is False
            and original['operation_status'] == 'evaluation_finished' and original.get('browser_exit_code') == 0)
        quality = output.get('quality')
        finite_quality = type(quality) in (int, float) and math.isfinite(quality) and 0 <= quality <= 100
        receipt['adopted'] = complete and finite_quality
        receipt['quality'] = quality if receipt['adopted'] else None
        receipt['validation_status'] = ('complete_observation' if receipt['adopted'] else
            'observer_fault' if receipt['observer_fault'] else 'partial_observation')
    except (Exception, KeyboardInterrupt) as exc:
        receipt['errors'].append({'type':type(exc).__name__, 'check':str(exc)})
        receipt.update(validation_status='invalid_evidence', adopted=False, quality=None, finite_failure=None)
    finally:
        for name, root, before in (('source',source,source_before), ('assessment',stage,stage_before)):
            try:
                after = byte_inventory(root)
                unchanged = before is not None and before == after
                receipt[name+'_bytes_unchanged'] = unchanged
                receipt[name+'_inventory_sha256_before'] = inventory_digest(before) if before is not None else None
                receipt[name+'_inventory_sha256_after'] = inventory_digest(after)
                if not unchanged:
                    receipt.update(validation_status='input_changed_or_unverified', adopted=False, quality=None, finite_failure=None)
            except Exception as exc:
                receipt[name+'_bytes_unchanged'] = None
                receipt['errors'].append({'type':type(exc).__name__, 'check':name+'_final_inventory'})
                receipt.update(validation_status='input_changed_or_unverified', adopted=False, quality=None, finite_failure=None)
        util.write_new_json(destination, receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    prep = sub.add_parser('prepare')
    for name in ('source', 'destination', 'evaluator-bundle', 'assembly', 'spec', 'evaluation-version', 'repair-contract'):
        prep.add_argument('--'+name, required=True)
    prep.add_argument('--observation-plan', help='Hash-bound selected same-spec revision plan; never inferred from a version')
    run = sub.add_parser('execute'); run.add_argument('stage'); run.add_argument('--repo', required=True)
    run.add_argument('--timeout', type=int, default=1800)
    validation = sub.add_parser('revalidate', help='Read saved evidence; write a new separate validation receipt')
    validation.add_argument('stage'); validation.add_argument('destination'); validation.add_argument('--repo')
    args = vars(parser.parse_args()); action = args.pop('action')
    if action == 'prepare': print(prepare(**args))
    elif action == 'execute': print(json.dumps(execute(**args), ensure_ascii=False, indent=2))
    else: print(json.dumps(revalidate(**args), ensure_ascii=False, indent=2))


if __name__ == '__main__': main()
