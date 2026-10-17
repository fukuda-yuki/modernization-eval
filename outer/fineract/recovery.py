"""Bounded recovery of explicitly owned resources; never evaluates or resends."""
from pathlib import Path
import uuid
from outer.harness import runtime,util,run


def evaluation_resources(root,settings,ledger):
    found=[];errors=[]
    base=Path(settings.get('preparation',root)).resolve().parent.parent/'fineract-evaluations'
    for attempt in ledger['attempts']:
        for request in (root/'runs'/attempt['run_uuid']/'evaluations').glob('*/request.json'):
            try:
                data=util.read_json(request)
                owner={'campaign_uuid':ledger['id'],'run_uuid':attempt['run_uuid'],'evaluation_id':request.parent.name}
                if data.get('owner')!=owner:raise ValueError('Legacy or foreign evaluation ownership requires explicit review')
                binding={**owner,'request_sha256':util.sha256_file(request)}
                ref=request.parent/'build-intent.json'
                if not ref.exists():continue  # No Docker creation precedes the stage reference.
                link=util.read_json(ref);build=Path(link['record']).resolve()
                if build.parent!=base or build.name!=link['id']:raise ValueError('Build outside owned stage directory')
                stage=util.read_json(build/'request.json')
                if stage.get('owner')!=binding or util.sha256_file(build/'request.json')!=link['request_sha256']:
                    raise ValueError('Build ownership binding changed')
                if stage.get('submission_sha256')!=data.get('submission_sha256') or data.get('submission_sha256')!=attempt.get('submission_sha256'):
                    raise ValueError('Owned submission binding changed')
                if stage['id']!=build.name:raise ValueError('Stage ID mismatch')
                found.append((build,stage['container'],'modernization.evaluation',stage['id']))
                for intent in build.glob('services/*/intent.json'):
                    server=util.read_json(intent)
                    if server['id']!=intent.parent.name or server['preparation_id']!=stage['id'] or server['build_basis']!='canonical_evaluation':
                        raise ValueError('Service parent mismatch')
                    for role in ('app','database'):
                        found.append((intent.parent,server['names'][role],'modernization.service',server['id']))
                    for pattern,label in (('java-api-checks/*/request.json','modernization.java_api'),('api-checks/*/request.json','modernization.api_check')):
                        for check in intent.parent.glob(pattern):
                            item=util.read_json(check)
                            if item['id']!=check.parent.name or item['service_id']!=server['id'] or Path(item['service_record']).resolve()!=intent.parent:
                                raise ValueError('Check parent mismatch')
                            found.append((check.parent,item['container'],label,item['id']))
            except Exception as error:
                errors.append({'evaluation_id':request.parent.name,'error_type':type(error).__name__,'message':str(error) if isinstance(error,ValueError) else None})
    return found,errors


def stop_resource(record,name,label,ident):
    evidence={'record':str(record),'container':name,'label':label,'id':ident,'stop_confirmed':False}
    try:
        item=runtime.inspect_container(name)
        if item is None:
            listed=runtime.docker('ps','-a','--format','{{.Names}}',timeout=30)
            if name in listed.stdout.splitlines():raise ValueError('Container inspection unconfirmed')
            evidence.update(stop_confirmed=True,absent=True);return evidence
        if (item['Config'].get('Labels') or {}).get(label)!=ident:raise ValueError('Container ownership mismatch')
        evidence['was_running']=item['State']['Running']
        if item['State']['Running']:runtime.docker('stop','--time','10',name,timeout=40)
        after=runtime.inspect_container(name)
        if after and (after['Config'].get('Labels') or {}).get(label)!=ident:raise ValueError('Container ownership changed')
        if after is None:raise ValueError('Stopped container inspection unconfirmed')
        evidence.update(stop_confirmed=not after['State']['Running'],state=after['State'])
    except Exception as error:evidence['error_type']=type(error).__name__
    return evidence


def recover_owned(root,settings,plan,ledger):
    from . import box,controller
    folder=root/'recoveries'/uuid.uuid4().hex;folder.mkdir(parents=True)
    (folder/'ledger.before.json').write_bytes((root/'ledger.json').read_bytes())
    receipt={'at':run.now(),'admission_fenced':False,'model_resources_stopped':False,
             'evaluation_resources_stopped':False,'recovery_complete':False,'resources':[],
             'errors':[],'collections':[],'incomplete_evaluations':[],'model_calls':0,'scorer_calls':0}
    def persist():util.write_json_atomic(folder/'progress.json',receipt)
    try:
        before=box.budget_action(settings,'snapshot');util.write_new_json(folder/'budget.before.json',before)
    except Exception as error:receipt['errors'].append({'operation':'budget_snapshot_before','type':type(error).__name__})
    try:
        box.budget_action(settings,'close',{'reason':'recovery'})
        after=box.budget_action(settings,'snapshot');util.write_new_json(folder/'budget.after.json',after)
        receipt['admission_fenced']=bool(after.get('closed_reason'));ledger['budget']=after
    except Exception as error:receipt['errors'].append({'operation':'budget_close','type':type(error).__name__})
    persist()
    models=[];model_errors=[]
    for attempt in ledger['attempts']:
        record=root/'runs'/attempt['run_uuid']
        if not (record/'box.json').exists():continue
        try:
            data=util.read_json(record/'box.json')
            if data['id']!=attempt['run_uuid']:raise ValueError('Run ownership mismatch')
            for role in ('gateway','worker','database'):models.append((record,data['names'][role],'modernization.run',data['id'],role))
        except Exception as error:model_errors.append({'run_uuid':attempt['run_uuid'],'error_type':type(error).__name__})
    # Fence every owned gateway before touching workers or evaluation resources.
    models.sort(key=lambda r:r[4]!='gateway')
    for record,name,label,ident,role in models:
        receipt['resources'].append(dict(stop_resource(record,name,label,ident),scope='model',role=role));persist()
    stages,errors=evaluation_resources(root,settings,ledger)
    receipt['errors']+=model_errors+errors
    # Checks precede their services. Every failed stop leaves other resources eligible.
    stages.sort(key=lambda r:r[2]=='modernization.service')
    for record,name,label,ident in stages:
        receipt['resources'].append(dict(stop_resource(record,name,label,ident),scope='evaluation'));persist()
    receipt['model_resources_stopped']=not model_errors and all(x['stop_confirmed'] for x in receipt['resources'] if x['scope']=='model')
    receipt['evaluation_resources_stopped']=not errors and all(x['stop_confirmed'] for x in receipt['resources'] if x['scope']=='evaluation')
    from . import evaluation,java_api,api_check
    collectors={'fineract-canonical-build-v1':evaluation.collect,
                'fineract-private-java-api-run-v1':java_api.collect,
                'fineract-private-api-db-v1':api_check.collect}
    seen=set()
    for item in receipt['resources']:
        record=Path(item['record'])
        if item['scope']!='evaluation' or not item['stop_confirmed'] or str(record) in seen:continue
        seen.add(str(record))
        if (record/'result.json').exists():continue  # Never rewrite an existing stage verdict.
        request=record/'request.json'
        if not request.exists():continue
        collector=collectors.get(util.read_json(request).get('kind'))
        if collector is None:continue  # Dummy fixtures have state evidence only.
        try:
            collector(record)
            receipt['collections'].append({'record':str(record),'collected':True})
        except Exception as error:
            receipt['errors'].append({'operation':'collect_evaluation','record':str(record),'type':type(error).__name__})
        persist()
    for attempt in ledger['attempts']:
        record=root/'runs'/attempt['run_uuid']
        states=[x for x in receipt['resources'] if x['scope']=='model' and x['id']==attempt['run_uuid']]
        stopped=not any(x['run_uuid']==attempt['run_uuid'] for x in model_errors) and all(x['stop_confirmed'] for x in states)
        if not attempt.get('stop_confirmed'):
            attempt.update(stop_confirmed=stopped,status='finished' if stopped else 'stop_unconfirmed',end_reason='recovered_interruption')
        if stopped and (record/'initialized.json').exists() and not (record/'artifact.json').exists():
            try:attempt['submission_sha256']=box.collect(record,settings)['sha256']
            except Exception as error:receipt['errors'].append({'operation':'collect','run_uuid':attempt['run_uuid'],'type':type(error).__name__})
        for request in (record/'evaluations').glob('*/request.json'):
            if not request.with_name('result.json').exists():receipt['incomplete_evaluations'].append({'run_uuid':attempt['run_uuid'],'evaluation_id':request.parent.name,'request_sha256':util.sha256_file(request),'quality':None})
    receipt['recovery_complete']=all(receipt[k] for k in ('admission_fenced','model_resources_stopped','evaluation_resources_stopped')) and not receipt['errors']
    ledger['halt_reason']='recovery';ledger['phase']='recovered' if receipt['recovery_complete'] else 'recovery_required'
    for assignment in ledger['assignments']:
        if not any(a['assignment_id']==assignment['assignment_id'] for a in ledger['attempts']):
            assignment.update(status='not_run',not_run_reason='outside_finite_acceptance_allocation' if assignment['planned_status']!='scheduled' else 'recovery')
    controller.save(root,ledger,'recovered_without_model_replay')
    receipt['ended_at']=run.now();util.write_new_json(folder/'result.json',receipt)
    return {'record':str(root),'phase':ledger['phase'],'stop_confirmed':receipt['recovery_complete'],'recovery_record':str(folder),**{k:receipt[k] for k in ('admission_fenced','model_resources_stopped','evaluation_resources_stopped','recovery_complete')}}
