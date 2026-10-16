import copy
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from outer.fineract import controller
from outer.fineract.campaign import LIMITS,digest,make_plan
from outer.fineract.execution import case_status,quality,completed_successfully
from outer.harness import runtime,util
from outer.tests.test_fineract_campaign import fixture


class FiniteControllerTests(unittest.TestCase):
    def test_runtime_binding_includes_trusted_build_and_api_images(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name,body in [('prep',{'source_commit':'source','archive_sha256':'a'*64,'java_image_id':'java-a'}),
                              ('seed',{'inventory_sha256':'b'*64}),
                              ('sdk',{'success':True,'private_inputs':False,'archive_sha256':'a'*64,
                                      'inventory_sha256':'c'*64,'build':{'jar_sha256':'d'*64}})]:
                (root/name).mkdir();util.write_new_json(root/name/'result.json',body)
            settings={'preparation':str(root/'prep'),'seed':str(root/'seed'),'public_sdk':str(root/'sdk'),
                      'worker_image':'worker','gateway_image':'gateway','api_image':'api-a','agent':{},
                      'worker_build_policy':{'kind':'prepared_public_sdk','decision_reference':'test'}}
            before=digest(controller.runtime_definition(settings))
            settings['api_image']='api-b'
            self.assertNotEqual(before,digest(controller.runtime_definition(settings)))
            settings['api_image']='api-a'
            prep=util.read_json(root/'prep/result.json');prep['java_image_id']='java-b'
            util.write_json_atomic(root/'prep/result.json',prep)
            self.assertNotEqual(before,digest(controller.runtime_definition(settings)))

    def test_complete_reports_do_not_override_failed_process_exit(self):
        good={'state':{'ExitCode':0,'Running':False,'OOMKilled':False}}
        self.assertTrue(completed_successfully(good))
        for key,value in [('ExitCode',1),('ExitCode',137),('Running',True),('OOMKilled',True)]:
            bad=copy.deepcopy(good);bad['state'][key]=value
            self.assertFalse(completed_successfully(bad))
        self.assertFalse(completed_successfully({}))
        self.assertFalse(completed_successfully({**good,'environment_failure':True}))

    def ledger(self):
        return {'attempts':[],'deadline':2000,'halt_reason':None}

    def assignment(self,i):
        return {'assignment_id':str(i),'task':'task','condition':'condition','wave':1}

    def test_active_four_and_reserved_seconds_survive_missing_completion(self):
        ledger=self.ledger()
        for i in range(4):controller.new_attempt(ledger,self.assignment(i),100)
        with self.assertRaises(ValueError):controller.new_attempt(ledger,self.assignment(4),101)
        for a in ledger['attempts']:a['status']='finished'
        ledger['attempts']*=4
        controller.new_attempt(ledger,self.assignment(20),102)
        self.assertFalse(controller.admitted(ledger,103))
        self.assertEqual(len(ledger['attempts'])*LIMITS['run_seconds'],30600)

    def test_deadline_and_closed_campaign_deny_before_launch(self):
        ledger=self.ledger()
        self.assertFalse(controller.admitted(ledger,2000))
        ledger['halt_reason']='operator_stop'
        self.assertFalse(controller.admitted(ledger,100))

    def test_business_failure_cannot_be_retried_or_normal_replayed(self):
        ledger=self.ledger();row=self.assignment(0)
        previous=controller.new_attempt(ledger,row,100)
        previous.update(status='finished',end_reason='agent_error',stop_confirmed=True)
        with self.assertRaises(ValueError):controller.new_attempt(ledger,row,101,retry=True)
        with self.assertRaises(ValueError):controller.new_attempt(ledger,row,101)
        previous['technical_failure']=True
        previous['stop_confirmed']=False
        with self.assertRaises(ValueError):controller.new_attempt(ledger,row,101,retry=True)

    def test_only_two_technical_retries_and_new_identifiers(self):
        ledger=self.ledger()
        for i in range(3):
            row=controller.new_attempt(ledger,self.assignment(i),100)
            row.update(status='finished',technical_failure=True,stop_confirmed=True)
        for i in range(2):
            row=controller.new_attempt(ledger,self.assignment(i),101,retry=True)
            row['status']='finished'
        with self.assertRaises(ValueError):controller.new_attempt(ledger,self.assignment(2),102,retry=True)
        self.assertEqual(len({a['run_uuid'] for a in ledger['attempts']}),5)

    def test_exact_behavior_inventory_and_unknown_layers_never_pass(self):
        self.assertEqual(case_status([{'id':'a','status':'passed'}],['a']),'passed')
        for cases in ([],[{'id':'b','status':'passed'}],
                      [{'id':'a','status':'passed'},{'id':'a','status':'passed'}],
                      [{'id':'a','status':'skipped'}]):
            self.assertEqual(case_status(cases,['a']),'unknown')
        self.assertEqual(quality('passed','passed','unknown',True),'unknown')
        self.assertEqual(quality('passed','failed','unknown',True),'failed')
        self.assertEqual(quality('passed','passed','passed',False),'unknown')
        self.assertEqual(quality('build_failed','passed','passed',True),'build_failed')

    def make_record(self,root):
        plan=make_plan(fixture(),campaign_id='unit-test-no-model',seed=1,runtime_sha256='a'*64,
                       evaluator_sha256='b'*64,selection_seal_sha256='c'*64)
        settings={'synthetic':False}
        ledger={'phase':'prepared','settings_sha256':digest(settings),'plan_sha256':digest(plan),
                'attempts':[],'assignments':copy.deepcopy(plan['assignments']),'halt_reason':None}
        for name,value in [('plan',plan),('settings',settings),('ledger',ledger)]:
            util.write_new_json(root/(name+'.json'),value)
        return ledger

    def test_real_supervisor_orders_untried_before_two_retries_and_never_replays(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.make_record(root);launched=[];allocations=[]
            def allocate(record,settings,assignment,ident):
                record.mkdir(parents=True)
                util.write_new_json(record/'box.json',{'id':ident})
                util.write_new_json(record/'initialized.json',{})
                allocations.append(assignment['assignment_id'])
                if len(allocations)<=3:
                    raise runtime.DockerCommandError('create',RuntimeError('synthetic infrastructure failure'))
                return {'id':ident}
            def launch(record,*args):launched.append(record.name)
            def budget(*args):return {'reserved_requests':0,'observed_tokens_lower_bound':0}
            with patch.object(controller,'readiness'),patch.object(controller,'score_completed'),\
                 patch.object(controller.box,'allocate',side_effect=allocate),\
                 patch.object(controller.box,'start_gateway',return_value=None),\
                 patch.object(controller.box,'launch',side_effect=launch),\
                 patch.object(controller.box,'budget_action',side_effect=budget),\
                 patch.object(controller.box,'poll',return_value={'finished':True,'state':{'ExitCode':0}}),\
                 patch.object(controller.box,'stop',return_value={'stop_confirmed':True}),\
                 patch.object(controller.box,'collect',return_value={'sha256':'d'*64}):
                result=controller.execute(root)
                with self.assertRaises(ValueError):controller.execute(root)
            saved=util.read_json(root/'ledger.json')
            self.assertEqual(result['attempts'],17)
            self.assertEqual(len(set(allocations[:15])),15)
            self.assertEqual(allocations[15:],allocations[:2])
            self.assertEqual(sum(a['retry'] for a in saved['attempts']),2)
            self.assertEqual(sum(a['status']=='not_run' for a in saved['assignments']),6)
            self.assertEqual(len(launched),14)

    def test_recovery_fences_before_stop_and_preserves_attempt_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);ledger=self.make_record(root)
            ledger.update(phase='recovery_required',deadline=time.time()+100)
            attempt=controller.new_attempt(ledger,ledger['assignments'][0],time.time())
            ident=attempt['run_uuid'];record=root/'runs'/ident;record.mkdir(parents=True)
            util.write_new_json(record/'box.json',{'id':ident});util.write_new_json(record/'initialized.json',{})
            util.write_json_atomic(root/'ledger.json',ledger);events=[]
            with patch.object(controller.box,'budget_action',side_effect=lambda *a:events.append(a[1]) or {}),\
                 patch.object(controller.box,'stop',side_effect=lambda *a:events.append('stop') or {'stop_confirmed':True}),\
                 patch.object(controller.box,'collect',return_value={'sha256':'d'*64}),\
                 patch.object(controller.box,'launch') as launch:
                result=controller.recover(root)
            self.assertEqual(events[:2],['close','stop']);launch.assert_not_called()
            self.assertTrue(result['stop_confirmed'])
            self.assertEqual(util.read_json(root/'ledger.json')['attempts'][0]['run_uuid'],ident)

    def test_missing_budget_cannot_prevent_owned_worker_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);ledger=self.make_record(root)
            ledger.update(phase='recovery_required',deadline=time.time()+100)
            attempt=controller.new_attempt(ledger,ledger['assignments'][0],time.time())
            record=root/'runs'/attempt['run_uuid'];record.mkdir(parents=True)
            util.write_new_json(record/'box.json',{'id':attempt['run_uuid']})
            util.write_json_atomic(root/'ledger.json',ledger)
            with patch.object(controller.box,'budget_action',side_effect=runtime.DockerCommandError('budget',RuntimeError('missing'))),\
                 patch.object(controller.box,'stop',return_value={'stop_confirmed':True}) as stop,\
                 patch.object(controller.box,'launch') as launch:
                result=controller.recover(root)
            stop.assert_called_once();launch.assert_not_called()
            self.assertTrue(result['stop_confirmed'])
            self.assertIsNone(util.read_json(root/'ledger.json')['budget'])


if __name__=='__main__':
    unittest.main()
