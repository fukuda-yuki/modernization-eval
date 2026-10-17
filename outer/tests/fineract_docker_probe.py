"""Explicit offline Docker recovery probes. No provider, prompts, or new grading."""
import argparse
import json
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

from outer.fineract import controller,box,service,recovery,lifecycle
from outer.fineract.campaign import LIMITS,digest
from outer.harness import runtime,util
from outer.harness.security import child_environment
from outer.tests.test_fineract_recovery import campaign,owned_evaluation,stage_resources


def fixture_setup(root,original_build,image):
    settings,plan,ledger,_=campaign(root)
    settings.update(gateway_image=image,budget_volume='fe-operational-budget-'+uuid.uuid4().hex)
    util.write_json_atomic(root/'settings.json',settings);ledger['settings_sha256']=digest(settings)
    original=util.read_json(original_build/'request.json') if original_build else None
    outer,build=owned_evaluation(root,ledger,build_request=original)
    if original_build:
        shutil.copyfile(original_build/'result.json',build/'result.json')
        archive=original_build.parent.parent/'fineract-preparation/source.tar.gz'
        target=build.parent.parent/'fineract-preparation/source.tar.gz';target.parent.mkdir(parents=True)
        shutil.copyfile(archive,target)
        assert util.sha256_file(target)==original['archive_sha256']
    runtime.docker('volume','create','--label','modernization.campaign='+ledger['id'],settings['budget_volume'])
    runtime.docker('run','--rm','--network','none','--user','0:0','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--entrypoint','chmod',
        '--mount',f'type=volume,source={settings["budget_volume"]},target=/budget',image,'0777','/budget')
    box.budget_action(settings,'create',{'campaign_id':plan['campaign_id'],'plan_sha256':digest(plan),
        'max_requests':LIMITS['requests'],'max_tokens':LIMITS['observed_input_output_tokens'],'deadline':ledger['deadline']})
    util.write_new_json(root/'probe-binding.json',{'build':str(build),'outer':str(outer),'image':image,
        'mode':'service' if original_build else 'intents','original_build':str(original_build) if original_build else None})


def child(root):
    binding=util.read_json(root/'probe-binding.json');build=Path(binding['build'])
    with lifecycle.writer(root):
        if binding['mode']=='service':
            started=service.start_evaluation(build)
            observed=service.inspect(started['record'])
            # No readiness success required and no evaluator client is launched.
            util.write_new_json(root/'live-resources.json',{'service':started['record'],
                'states':observed['states'],'health_observed':True,'model_calls':0,'scorer_calls':0})
        else:
            stage_resources(build)
            _,settings,_,ledger=controller.load(root)
            resources,errors=recovery.evaluation_resources(root,settings,ledger);assert not errors
            for record,name,label,ident in resources:
                runtime.docker('run','-d','--network','none','--read-only','--name',name,'--label',label+'='+ident,
                    '--entrypoint','python3',binding['image'],'-c','import time; time.sleep(300)')
            assert not list(build.rglob('started.json'))
            util.write_new_json(root/'live-resources.json',{'resources':len(resources),'started_records':0,'model_calls':0,'scorer_calls':0})
        time.sleep(300)


def probe(root,original_build,image):
    fixture_setup(root,original_build,image)
    before=util.read_json(root/'ledger.json');binding=util.read_json(root/'probe-binding.json')
    outer=Path(binding['outer']);source=root/'runs'/before['attempts'][0]['run_uuid']/'submission.tar'
    preserved={str(p):util.sha256_file(p) for p in (source,outer/'request.json')}
    command=[sys.executable,'-u','-m','outer.tests.fineract_docker_probe','child','--output',str(root)]
    with (root/'child.stdout.log').open('x') as out,(root/'child.stderr.log').open('x') as err:
        process=subprocess.Popen(command,stdout=out,stderr=err,env=child_environment())
    identity=lifecycle.identity(process.pid)
    try:
        until=time.monotonic()+120
        while not (root/'live-resources.json').exists():
            if process.poll() is not None:raise RuntimeError('Fixture child failed; inspect private stderr')
            if time.monotonic()>until:raise TimeoutError('Fixture resource launch timeout')
            time.sleep(.2)
        process.terminate();process.wait(timeout=15)  # Retained child handle, never a numeric PID kill.
        util.write_new_json(root/'child-exit.json',{'identity':identity,'exit_code':process.returncode,
            'purpose':'injected exit during evaluation startup/wait','model_calls':0})
    finally:
        if process.poll() is None:process.terminate();process.wait(timeout=15)
        result=subprocess.run([sys.executable,'-m','outer.tests.fineract_docker_probe','recover','--output',str(root)],
            capture_output=True,text=True,env=child_environment(),timeout=240)
        (root/'recovery.stdout.log').write_text(result.stdout,encoding='utf-8')
        (root/'recovery.stderr.log').write_text(result.stderr,encoding='utf-8')
        result.check_returncode()
    recovered=util.read_json(root/'recovery-proof.json');assert recovered['recovery_complete']
    assert all(util.sha256_file(Path(p))==h for p,h in preserved.items())
    assert not (outer/'result.json').exists()
    after=util.read_json(root/'ledger.json');assert [a['run_uuid'] for a in before['attempts']]==[a['run_uuid'] for a in after['attempts']]
    snapshot=box.budget_action(controller.load(root)[1],'snapshot')
    assert snapshot['closed_reason']=='recovery' and snapshot['reserved_requests']==0
    util.write_new_json(root/'result.json',{'passed':True,'mode':binding['mode'],'recovery':recovered,
        'original_fixture_evidence_unchanged':True,'evaluation_quality':None,'new_evaluations':0,'model_calls':0,
        'scorer_calls':0,'budget':snapshot})
    return {'passed':True,'mode':binding['mode'],'model_calls':0,'scorer_calls':0}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['run','child','recover'])
    p.add_argument('--output',required=True,type=Path);p.add_argument('--saved-build',type=Path);p.add_argument('--image')
    a=p.parse_args();root=a.output.resolve()
    if a.action=='child':child(root)
    elif a.action=='recover':
        result=controller.recover(root);util.write_new_json(root/'recovery-proof.json',result);print(json.dumps(result))
    else:print(json.dumps(probe(root,a.saved_build.resolve() if a.saved_build else None,a.image)))
