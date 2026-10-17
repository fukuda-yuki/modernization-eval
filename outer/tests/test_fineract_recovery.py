"""Non-model failure injection; fake Docker never has a provider route."""
import copy
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from outer.fineract import controller,recovery,supervisor,lifecycle,evaluation
from outer.fineract.campaign import LIMITS,digest,make_plan
from outer.harness import util,runtime
from outer.harness.campaign_budget import Budget
from outer.tests.test_fineract_campaign import fixture


def campaign(root,phase='running'):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    plan=make_plan(fixture(),campaign_id='offline-fault-fixture',seed=1,runtime_sha256='a'*64,evaluator_sha256='b'*64,selection_seal_sha256='c'*64)
    settings={'synthetic':False,'preparation':str(root/'assets/fineract-preparation/fixture')}
    ledger={'id':uuid.uuid4().hex,'ownership_version':1,'phase':phase,'settings_sha256':digest(settings),'plan_sha256':digest(plan),
        'started_epoch':time.time(),'deadline':time.time()+600,'attempts':[],
        'assignments':copy.deepcopy(plan['assignments']),'halt_reason':None}
    for n,d in [('settings',settings),('plan',plan),('ledger',ledger)]:util.write_new_json(root/(n+'.json'),d)
    budget=Budget.create(root/'fixture-budget.sqlite',campaign_id=plan['campaign_id'],plan_sha256=digest(plan),
        max_requests=LIMITS['requests'],max_tokens=LIMITS['observed_input_output_tokens'],deadline=ledger['deadline'])
    return settings,plan,ledger,budget


def owned_evaluation(root,ledger,*,run_id=None,build_request=None):
    ident=run_id or uuid.uuid4().hex
    source_hash=(build_request or {}).get('submission_sha256','d'*64)
    if not any(a['run_uuid']==ident for a in ledger['attempts']):
        ledger['attempts'].append({'run_uuid':ident,'assignment_id':ledger['assignments'][0]['assignment_id'],
            'status':'finished','stop_confirmed':True,'submission_sha256':source_hash})
    run_root=root/'runs'/ident;run_root.mkdir(parents=True,exist_ok=True)
    if not (run_root/'submission.tar').exists():(run_root/'submission.tar').write_bytes(b'non-model frozen fixture')
    outer=run_root/'evaluations'/uuid.uuid4().hex;outer.mkdir(parents=True)
    owner={'campaign_uuid':ledger['id'],'run_uuid':ident,'evaluation_id':outer.name}
    util.write_new_json(outer/'request.json',{'owner':owner,'at':'fixture','submission_sha256':source_hash,'model_calls':0})
    build=root/'assets/fineract-evaluations'/uuid.uuid4().hex;build.mkdir(parents=True)
    data={**(build_request or {}),'id':build.name,'container':'fe-evaluate-'+build.name,'submission_sha256':source_hash,
        'owner':{**owner,'request_sha256':util.sha256_file(outer/'request.json')}}
    util.write_new_json(build/'request.json',data)
    util.write_new_json(outer/'build-intent.json',{'id':build.name,'record':str(build.resolve()),'request_sha256':util.sha256_file(build/'request.json')})
    util.write_json_atomic(root/'ledger.json',ledger)
    return outer,build


def stage_resources(build):
    server=build/'services'/uuid.uuid4().hex;server.mkdir(parents=True)
    names={role:'fe-'+role+'-'+server.name for role in ('app','database')}
    util.write_new_json(server/'intent.json',{'id':server.name,'preparation_id':build.name,'build_basis':'canonical_evaluation','names':names})
    api=server/'api-checks'/uuid.uuid4().hex;api.mkdir(parents=True)
    util.write_new_json(api/'request.json',{'id':api.name,'service_id':server.name,'service_record':str(server.resolve()),'container':'fe-api-check-'+api.name})
    return server,api


class RecoveryTests(unittest.TestCase):
    @unittest.skipUnless(os.name=='nt','Windows CIM launch failure boundary')
    def test_launch_rejection_has_no_interactive_fallback_or_second_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'probe'
            with patch.object(supervisor.subprocess,'run',side_effect=OSError('Injected CIM rejection')),patch.object(supervisor.subprocess,'Popen') as fallback:
                with self.assertRaises(OSError):supervisor.launch_probe(root)
                with self.assertRaises(FileExistsError):supervisor.launch_probe(root)
            fallback.assert_not_called()
            self.assertFalse(util.read_json(root/'supervisor/launch-error.json')['dispatch_permitted'])

    def test_owner_recovers_once_on_zero_nonterminal_or_exception_exit(self):
        import os
        for code,exception in ((0,False),(1,True)):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);folder=root/'supervisor';folder.mkdir()
                intent={'launch_id':'fixture','action':'run','binding':{'controller_code_sha256':'code'}}
                util.write_new_json(folder/'intent.json',intent);util.write_new_json(folder/'launch-ready.json',{'launch_id':'fixture'})
                util.write_new_json(root/'ledger.json',{'phase':'running'})
                if exception:util.write_new_json(folder/'controller-exception.json',{'error_type':'RuntimeError'})
                process=SimpleNamespace(pid=os.getpid(),poll=lambda:code,returncode=code,_handle=0)
                with patch.object(supervisor,'_in_job',return_value=False),patch.object(supervisor,'code_hash',return_value='code'),patch.object(supervisor.subprocess,'Popen',return_value=process),patch.object(lifecycle,'identity',return_value={'pid':os.getpid(),'created':'fixture'}),patch.object(lifecycle,'handle_times',return_value={'exited_at':'fixture-time'}),patch.object(controller,'recover',return_value={'recovery_complete':False}) as recover:
                    result=supervisor.owner(root)
                recover.assert_called_once_with(root)
                self.assertEqual(result['exit_code'],code);self.assertFalse(result['recovery']['recovery_complete'])
                self.assertEqual(result['classification'],'controller_exception' if exception else 'nonterminal_exit')

    def test_build_ownership_and_outer_reference_precede_first_docker_operation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);prep=root/'assets/fineract-preparation/prep';prep.mkdir(parents=True)
            source=prep.parent/'source.tar.gz';source.write_bytes(b'fixture archive')
            util.write_new_json(prep/'result.json',{'success':True,'id':'prep','source_commit':'source',
                'archive_sha256':util.sha256_file(source),'java_image_id':'fixture-image'})
            seed=root/'seed';util.write_new_json(seed/'result.json',{'preparation_id':'prep','inventory_sha256':'seed','volume':'seed'})
            private=root/'private';util.write_new_json(private/'spec.json',{'expected_suites':['fixture'],'groups':['fixture'],'tests':['fixture']})
            outer=root/'outer';owner={'campaign_uuid':'campaign','run_uuid':'run','evaluation_id':'outer'}
            util.write_new_json(outer/'request.json',{'owner':owner})
            binding={**owner,'request_sha256':util.sha256_file(outer/'request.json')}
            with patch.object(evaluation,'volume',side_effect=RuntimeError('Injected before Docker')),patch.object(runtime,'docker') as docker:
                with self.assertRaises(RuntimeError):evaluation.begin(prep,seed,private,owner=binding,outer_record=outer)
            docker.assert_not_called();link=util.read_json(outer/'build-intent.json')
            request=Path(link['record'])/'request.json'
            self.assertEqual(link['request_sha256'],util.sha256_file(request));self.assertEqual(binding,util.read_json(request)['owner'])

    def setup_case(self,root):
        settings,plan,ledger,budget=campaign(root)
        outer,build=owned_evaluation(root,ledger);server,api=stage_resources(build)
        resources,errors=recovery.evaluation_resources(root,settings,ledger);self.assertFalse(errors)
        state={name:{'Config':{'Labels':{label:ident}},'State':{'Running':True,'ExitCode':0}} for _,name,label,ident in resources}
        def docker(*args,**kwargs):
            if args[0]=='stop':state[args[-1]]['State']['Running']=False
            return SimpleNamespace(stdout='\n'.join(state),returncode=0)
        def action(settings,operation,arguments=None):
            if operation=='close':budget.close(arguments['reason'])
            return budget.snapshot()
        return settings,ledger,budget,outer,build,state,docker,action

    def test_intents_before_started_and_incomplete_evaluation_remains_null(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,ledger,budget,outer,build,state,docker,action=self.setup_case(root)
            source=root/'runs'/ledger['attempts'][0]['run_uuid']/'submission.tar';before=source.read_bytes()
            with patch.object(runtime,'inspect_container',side_effect=state.get),patch.object(runtime,'docker',side_effect=docker),patch.object(controller.box,'budget_action',side_effect=action):
                result=controller.recover(root)
            self.assertTrue(result['recovery_complete']);self.assertFalse(any(s['State']['Running'] for s in state.values()))
            self.assertEqual(before,source.read_bytes());self.assertFalse((outer/'result.json').exists())
            receipt=util.read_json(Path(result['recovery_record'])/'result.json')
            self.assertIsNone(receipt['incomplete_evaluations'][0]['quality']);self.assertEqual(budget.snapshot()['closed_reason'],'recovery')

    def test_started_missing_stage_is_collected_once_without_new_evaluation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,ledger,budget,outer,build,state,docker,action=self.setup_case(root)
            request=util.read_json(build/'request.json');request['kind']='fineract-canonical-build-v1'
            util.write_json_atomic(build/'request.json',request)
            link=util.read_json(outer/'build-intent.json');link['request_sha256']=util.sha256_file(build/'request.json');util.write_json_atomic(outer/'build-intent.json',link)
            def collect(record):util.write_new_json(record/'result.json',{'collected_existing_output':True})
            with patch.object(runtime,'inspect_container',side_effect=state.get),patch.object(runtime,'docker',side_effect=docker),patch.object(controller.box,'budget_action',side_effect=action),patch.object(evaluation,'collect',side_effect=collect) as collected:
                first=controller.recover(root);second=controller.recover(root)
            collected.assert_called_once_with(build)
            self.assertTrue(first['recovery_complete']);self.assertTrue(second['recovery_complete']);self.assertFalse((outer/'result.json').exists())

    def test_foreign_same_submission_and_label_mismatch_are_not_stopped(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,ledger,budget,outer,build,state,docker,action=self.setup_case(root)
            foreign=copy.deepcopy(ledger);foreign['id']='other-campaign'
            foreign_outer,foreign_build=owned_evaluation(root,foreign,run_id=ledger['attempts'][0]['run_uuid'])
            foreign_name=util.read_json(foreign_build/'request.json')['container']
            state[foreign_name]={'Config':{'Labels':{'modernization.evaluation':foreign_build.name}},'State':{'Running':True}}
            own_name=util.read_json(build/'request.json')['container'];state[own_name]['Config']['Labels']={'modernization.evaluation':'wrong'}
            util.write_json_atomic(root/'ledger.json',ledger)
            with patch.object(runtime,'inspect_container',side_effect=state.get),patch.object(runtime,'docker',side_effect=docker),patch.object(controller.box,'budget_action',side_effect=action):
                result=controller.recover(root)
            self.assertFalse(result['recovery_complete']);self.assertTrue(state[foreign_name]['State']['Running']);self.assertTrue(state[own_name]['State']['Running'])
            self.assertEqual(sum(s['State']['Running'] for s in state.values()),2)

    def test_stop_and_budget_failure_continue_other_resources_then_repeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,ledger,budget,outer,build,state,docker,action=self.setup_case(root)
            victim=next(iter(state));original=util.sha256_file(outer/'request.json')
            def fail_stop(*args,**kwargs):
                if args[0]=='stop' and args[-1]==victim:raise TimeoutError('Injected bounded stop failure')
                return docker(*args,**kwargs)
            with patch.object(runtime,'inspect_container',side_effect=state.get),patch.object(runtime,'docker',side_effect=fail_stop),patch.object(controller.box,'budget_action',side_effect=TimeoutError('Injected budget failure')):
                first=controller.recover(root)
            self.assertFalse(first['recovery_complete']);self.assertFalse(first['admission_fenced']);self.assertEqual(sum(s['State']['Running'] for s in state.values()),1)
            with patch.object(runtime,'inspect_container',side_effect=state.get),patch.object(runtime,'docker',side_effect=docker),patch.object(controller.box,'budget_action',side_effect=action):
                second=controller.recover(root);third=controller.recover(root)
            self.assertTrue(second['recovery_complete']);self.assertTrue(third['recovery_complete']);self.assertEqual(original,util.sha256_file(outer/'request.json'))

    def test_interrupted_recovery_keeps_snapshot_and_can_repeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,ledger,budget,outer,build,state,docker,action=self.setup_case(root)
            real=recovery.stop_resource;count=0
            def interrupt(*a):
                nonlocal count
                count+=1
                if count==2:raise KeyboardInterrupt('Injected owner loss')
                return real(*a)
            with patch.object(runtime,'inspect_container',side_effect=state.get),patch.object(runtime,'docker',side_effect=docker),patch.object(controller.box,'budget_action',side_effect=action):
                with patch.object(recovery,'stop_resource',side_effect=interrupt),self.assertRaises(KeyboardInterrupt):controller.recover(root)
                self.assertEqual(budget.snapshot()['closed_reason'],'recovery')
                result=controller.recover(root)
            self.assertTrue(result['recovery_complete']);self.assertEqual(len(list((root/'recoveries').glob('*/ledger.before.json'))),2)

    def test_writer_conflict_and_pid_identity_mismatch_fail_closed(self):
        import os
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);campaign(root)
            with lifecycle.writer(root),self.assertRaises(RuntimeError):controller.recover(root)
            ident=lifecycle.identity(os.getpid());ident['created']='wrong'
            with self.assertRaises(ValueError):lifecycle.alive(ident)
            with self.assertRaises(ValueError):lifecycle.terminate(ident)

    def test_closed_and_deadline_reject_before_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,plan,ledger,budget=campaign(root,phase='recovered');before=util.tree_hashes(root)
            for action in ('run','continue'):
                with self.assertRaises(ValueError):supervisor.launch(root,action)
            self.assertEqual(before,util.tree_hashes(root))
            ledger.update(phase='paused_for_confirmation_scope',deadline=time.time()-1);util.write_json_atomic(root/'ledger.json',ledger)
            with self.assertRaises(ValueError):supervisor.validate_start(root,'continue')
            ledger['deadline']=time.time()+60;util.write_json_atomic(root/'ledger.json',ledger);budget.close('recovery')
            with patch.object(controller.box,'budget_action',return_value=budget.snapshot()),self.assertRaises(ValueError):supervisor.validate_start(root,'continue')

    def test_legacy_recovery_refuses_without_touching_original_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);_,_,ledger,_=campaign(root);ledger.pop('ownership_version');util.write_json_atomic(root/'ledger.json',ledger)
            before=util.tree_hashes(root)
            with self.assertRaises(ValueError):controller.recover(root)
            self.assertEqual(before,util.tree_hashes(root))

    def test_all_fifteen_assignments_once_without_model_adapter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,plan,ledger,budget=campaign(root,phase='prepared');allocated=[]
            def allocate(record,settings,assignment,ident):
                record.mkdir(parents=True);allocated.append(assignment['assignment_id']);return {'id':ident}
            with patch.object(controller,'readiness'),patch.object(controller,'score_completed'),patch.object(controller.box,'allocate',side_effect=allocate),patch.object(controller.box,'start_gateway',return_value=None),patch.object(controller.box,'launch'),patch.object(controller.box,'poll',return_value={'finished':True,'state':{'ExitCode':0}}),patch.object(controller.box,'budget_action',return_value={'reserved_requests':0,'observed_tokens_lower_bound':0}):
                result=controller.execute(root)
            self.assertEqual(result['phase'],'finished');self.assertEqual(len(allocated),15);self.assertEqual(len(set(allocated)),15)
            for condition in ('small-indexed','selection-indexed','large-indexed'):self.assertTrue(any(a.endswith(':'+condition) for a in allocated))


if __name__=='__main__':unittest.main()
