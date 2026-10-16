"""Finite Fineract acceptance contract. Constructing a plan never calls a model."""
import hashlib
import json
import re

TASKS = ('FINERACT-DELINQUENCY-RESUME', 'FINERACT-SAVINGS-MONTH-END', 'FINERACT-CHARGE-TEMPLATE')
CORE = ('explore-original', 'medium-original', 'explore-indexed', 'medium-indexed')
EXTRA = ('selection-indexed', 'small-indexed', 'large-indexed')
LIMITS = {'normal_runs':15, 'technical_retries':2, 'max_runs':17, 'concurrency':4,
          'evaluators':1, 'run_seconds':1800, 'provider_seconds':600,
          'total_run_seconds':30600, 'wall_seconds':14400,
          'requests':1700, 'observed_input_output_tokens':90000000}
IDENTITY = ('campaign_id', 'assignment_id', 'task', 'condition', 'doc_condition',
            'repetition', 'run_uuid', 'attempt', 'session', 'wave')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(',', ':')).encode()).hexdigest()


def hash_value(value):
    return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None


def make_plan(manifest, *, campaign_id, seed, runtime_sha256, evaluator_sha256, selection_seal_sha256):
    if not re.fullmatch('[a-zA-Z0-9_-]{1,80}', campaign_id) or type(seed) is not int:
        raise ValueError('Explicit campaign identity and integer ordering seed required')
    if not all(hash_value(x) for x in (runtime_sha256,evaluator_sha256,selection_seal_sha256)):
        raise ValueError('Prepared runtime, actual evaluator and selection seal must be bound')
    rows = manifest['conditions']
    expected = {(t,c) for t in TASKS for c in CORE+EXTRA}
    if len(rows)!=21 or {(r['task'],r['condition']) for r in rows}!=expected:
        raise ValueError('Exactly the sealed three-task seven-condition corpus is required')
    for key in ('source_inventory_sha256','semantic_corpus_sha256'):
        if len({r[key] for r in rows})!=1:raise ValueError('All tasks must share the same '+key)
    assignments=[]
    for task_i, task in enumerate(TASKS):
        task_rows = [r for r in rows if r['task']==task]
        for name in ('request_sha256','source_inventory_sha256','semantic_corpus_sha256','evaluation_spec_sha256'):
            values={r[name] for r in task_rows}
            if len(values)!=1 or not all(hash_value(v) for v in values):
                raise ValueError('Task conditions must share valid '+name)
        for row in task_rows:
            if row.get('evaluation_spec_kind')!='execution-spec':
                raise ValueError('A public-contract snapshot is not an implemented evaluator')
            if not hash_value(row['initial_sha256']) or row['initial_bytes']<=0:
                raise ValueError('Exact nonempty initial message required')
            condition=row['condition']
            scheduled=condition in CORE or task_i==0
            assignment={'campaign_id':campaign_id,'assignment_id':f'{task}:{condition}',
                'task':task,'condition':condition,'doc_condition':row['dose'],
                'organization':row['organization'],'repetition':1,
                'wave':task_i+1 if condition in CORE else (4 if task_i==0 else None),
                'planned_status':'scheduled' if scheduled else 'not_run',
                'not_run_reason':None if scheduled else 'outside_finite_acceptance_allocation',
                'material_binding':{k:row[k] for k in ('request_sha256','source_inventory_sha256',
                    'semantic_corpus_sha256','evaluation_spec_sha256','initial_sha256','initial_bytes',
                    'navigation_sha256','body_sha256','body_bytes','selection_index_sha256')}}
            assignments.append(assignment)
    for wave in range(1,5):
        selected=sorted([a for a in assignments if a['wave']==wave],
            key=lambda a:digest([seed,a['assignment_id']]))
        for order, assignment in enumerate(selected):assignment['wave_order']=order
    return {'kind':'fineract-acceptance-v1','schema_version':1,'campaign_id':campaign_id,
            'seed':seed,'ordering':'ascending SHA256 of canonical JSON [seed, assignment_id]',
            'limits':dict(LIMITS),'runtime_sha256':runtime_sha256,
            'selection_seal_sha256':selection_seal_sha256,
            'evaluator_sha256':evaluator_sha256,'manifest_sha256':digest(manifest),
            'assignments':sorted(assignments,key=lambda a:(a['wave'] or 99,a.get('wave_order',0),a['assignment_id'])),
            'retry_policy':'sealed technical infrastructure evidence only; new UUID; prioritize untried',
            'quality_tolerance':0,'interpretation':'technical acceptance only; no effect or noninferiority claim'}


def validate_approval(plan, receipt):
    # This binds an existing instruction, not a fabricated signature or a new
    # permission inferred from an old campaign. Callers preserve its exact reference.
    if (receipt.get('plan_sha256')!=digest(plan)
            or receipt.get('scope')!='finite_existing_opencode_go_acceptance'
            or receipt.get('authority_source')!='parent_delegation'
            or not receipt.get('instruction_reference')
            or receipt.get('additional_purchase_allowed') is not False
            or receipt.get('main_merge_allowed') is not False):
        raise ValueError('Current instruction must explicitly bind this exact finite plan')


def validate_readiness(plan, approval, *, materials, runtime, calibration, billing, now):
    """Fail closed on a review hold, incomplete scoring or unknown paid fallback.

    Receipts come from the trusted controller and documented read-only checks.
    Merely generating a corpus, finding a key or calling a Go endpoint cannot
    satisfy these gates. This function never changes a setting or invokes a model.
    """
    validate_approval(plan,approval)
    if plan.get('limits')!=LIMITS:raise ValueError('Finite campaign bounds changed')
    semantic={r['material_binding']['semantic_corpus_sha256'] for r in plan['assignments']}
    if (materials.get('decision')!='approved' or not materials.get('review_reference')
            or materials.get('selection_seal_sha256')!=plan['selection_seal_sha256']
            or semantic!={materials.get('semantic_corpus_sha256')}):
        raise ValueError('Exact resealed materials still require review')
    if (runtime.get('runtime_sha256')!=plan['runtime_sha256'] or runtime.get('verified') is not True
            or runtime.get('isolated_workers')!=4 or runtime.get('offline_fresh_build') is not True):
        raise ValueError('Runtime must pass fresh build and four-worker isolation')
    required={'FINERACT-DELINQUENCY-RESUME':{'java','api','database','cob'},
              'FINERACT-SAVINGS-MONTH-END':{'java','api','database'},
              'FINERACT-CHARGE-TEMPLATE':{'java','api','database'}}
    if calibration.get('evaluator_sha256')!=plan['evaluator_sha256']:
        raise ValueError('Actual evaluator calibration binding missing')
    review=calibration.get('independent_review',{})
    if (review.get('decision')!='approved' or not review.get('review_reference')
            or review.get('evaluator_sha256')!=plan['evaluator_sha256']):
        raise ValueError('Exact calibrated evaluator still requires independent review')
    for task,layers in required.items():
        item=calibration.get('tasks',{}).get(task,{})
        mutants=item.get('mutants',[])
        if (item.get('baseline_new_behavior')!='failed' or item.get('valid_gold')!='passed'
                or not mutants or any(m.get('verdict')!='failed' for m in mutants)
                or not layers.issubset(set(item.get('verified_layers',[])))):
            raise ValueError('Real Java/API/DB calibration incomplete for '+task)
    observed=billing.get('observed_at_epoch')
    if (billing.get('provider')!='opencode-go' or billing.get('use_balance') is not False
            or not hash_value(billing.get('workspace_binding_sha256'))
            or not billing.get('read_only_evidence_reference')
            or type(observed) not in (int,float) or not 0<=now-observed<=3600):
        raise ValueError('Recent read-only workspace proof of paid fallback OFF required')
    return {'ready':True,'plan_sha256':digest(plan),'review_reference':materials['review_reference']}
