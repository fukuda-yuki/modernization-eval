import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from outer.fineract import campaign,controller,continuation
from outer.harness import util
from outer.harness.campaign_budget import Budget
from outer.tests.test_fineract_campaign import fixture

def readiness_fixture():
    plan=campaign.make_plan(fixture(),campaign_id='scope-test',seed=1,runtime_sha256='b'*64,
        evaluator_sha256='c'*64,selection_seal_sha256='d'*64)
    approval={'plan_sha256':campaign.digest(plan),'scope':'finite_existing_opencode_go_acceptance',
        'authority_source':'parent_delegation','instruction_reference':'test-only','additional_purchase_allowed':False,
        'main_merge_allowed':False,'user_confirmation_sha256':'f'*64}
    inputs={'materials':{'decision':'approved','review_reference':'test','selection_seal_sha256':'d'*64,'semantic_corpus_sha256':'a'*64},
        'runtime':{'runtime_sha256':'b'*64,'verified':True,'isolated_workers':4,'offline_fresh_build':True},
        'calibration':{'evaluator_sha256':'c'*64,'independent_review':{'decision':'approved','review_reference':'test','evaluator_sha256':'c'*64},
            'tasks':{t:{'baseline_new_behavior':'failed','valid_gold':'passed','mutants':[{'verdict':'failed'}],
                       'verified_layers':['java','api','database','cob']} for t in campaign.TASKS}},
        'billing':{'provider':'opencode-go','use_balance':False,'workspace_binding_sha256':'e'*64,
            'read_only_evidence_reference':'user-transcript:test','observed_at_epoch':100,
            'verification_method':'explicit_user_confirmation','confirmation_record_sha256':'f'*64,
            'screen_setting_observed':False,'actual_workspace_setting_independently_observed':None}}
    scope={'kind':'explicit-user-confirmation-campaign-scope-v1','campaign_instance':'instance',
        'plan_sha256':campaign.digest(plan),'workspace_binding_sha256':'e'*64,'confirmation_record_sha256':'f'*64,
        'confirmed_at_epoch':100,'started_epoch':200,'deadline':14600,'revoked':False,
        'contradictory_setting_evidence':False,'decision_reference':'test-only'}
    context={'campaign_instance':'instance','started_epoch':200,'deadline':14600,'scope':scope}
    return plan,approval,inputs,context

class ConfirmationScopeTests(unittest.TestCase):
    def test_same_confirmation_valid_through_original_campaign_only(self):
        plan,approval,inputs,context=readiness_fixture();before=copy.deepcopy(inputs)
        for now in (200,3700,3701,14599):
            self.assertTrue(campaign.validate_readiness(plan,approval,**inputs,now=now,campaign_context=context)['ready'])
        for now in (199,14600,14601):
            with self.assertRaises(ValueError):campaign.validate_readiness(plan,approval,**inputs,now=now,campaign_context=context)
        self.assertEqual(inputs,before)
        self.assertFalse(inputs['billing']['screen_setting_observed'])

    def test_scope_mismatches_and_revocation_rejected(self):
        plan,approval,inputs,context=readiness_fixture()
        for key,value in [('campaign_instance','other'),('plan_sha256','0'*64),('workspace_binding_sha256','0'*64),
            ('confirmation_record_sha256','0'*64),('confirmed_at_epoch',101),('started_epoch',201),
            ('deadline',14601),('revoked',True),('contradictory_setting_evidence',True)]:
            bad=copy.deepcopy(context);bad['scope'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):
                campaign.validate_readiness(plan,approval,**inputs,now=4000,campaign_context=bad)
        for key,value in [('observed_at_epoch',5000),('use_balance',None),('use_balance',True),
                          ('revoked',True),('contradictory_setting_evidence',True)]:
            bad=copy.deepcopy(inputs);bad['billing'][key]=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):
                campaign.validate_readiness(plan,approval,**bad,now=4000,campaign_context=context)

    def test_no_scope_does_not_relax_other_evidence_policy(self):
        plan,approval,inputs,context=readiness_fixture()
        with self.assertRaises(ValueError):campaign.validate_readiness(plan,approval,**inputs,now=4000)
        inputs['billing']['verification_method']='independent_settings_observation'
        with self.assertRaises(ValueError):campaign.validate_readiness(plan,approval,**inputs,now=4000,campaign_context=context)

    def test_administrative_delta_cannot_change_frozen_dependencies_or_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            base={'worker_image':'same-worker','gateway_image':'same-gateway','controller_files':{
                'campaign.py':'a'*64,'controller.py':'b'*64,'execution.py':'c'*64}}
            actual=copy.deepcopy(base);actual['controller_files'].update({'campaign.py':'d'*64,
                'controller.py':'e'*64,'continuation.py':'f'*64})
            plan={'runtime_sha256':campaign.digest(base)};settings={'budget_volume':'same-budget'}
            ledger={'id':'instance','started_epoch':200,'deadline':14600,'settings_sha256':campaign.digest(settings)}
            for name,value in [('ledger',ledger),('approval',{}),('readiness',{})]:util.write_new_json(root/(name+'.json'),value)
            scope={'kind':'explicit-user-confirmation-campaign-scope-v1','original_runtime_definition':base,
                'amended_runtime_sha256':campaign.digest(actual),'settings_sha256':ledger['settings_sha256'],
                'budget_volume':'same-budget','approval_file_sha256':util.sha256_file(root/'approval.json'),
                'readiness_file_sha256':util.sha256_file(root/'readiness.json')}
            util.write_new_json(root/'confirmation-scope.json',scope)
            continuation.scoped_context(root,settings,plan,actual)
            for what in ('image','grader','receipt','revocation'):
                changed=copy.deepcopy(actual);test_scope=copy.deepcopy(scope)
                if what=='image':changed['worker_image']='different'
                if what=='grader':changed['controller_files']['execution.py']='0'*64
                test_scope['amended_runtime_sha256']=campaign.digest(changed)
                util.write_json_atomic(root/'confirmation-scope.json',test_scope)
                if what=='receipt':util.write_json_atomic(root/'readiness.json',{'changed':True})
                if what=='revocation':util.write_new_json(root/'confirmation-revoked.json',{})
                with self.subTest(what=what),self.assertRaises(ValueError):continuation.scoped_context(root,settings,plan,changed)
                util.write_json_atomic(root/'readiness.json',{})

    def make_checkpoint(self,root):
        plan,_,_,_=readiness_fixture()
        settings={'synthetic':False,'budget_volume':'same-budget','preparation':str(root/'prep/result')}
        ledger={'id':'same-instance','phase':'running','started_epoch':200,'deadline':14600,
            'settings_sha256':campaign.digest(settings),'plan_sha256':campaign.digest(plan),
            'attempts':[],'assignments':copy.deepcopy(plan['assignments']),'halt_reason':None}
        budget=Budget.create(root/'budget.sqlite',campaign_id=plan['campaign_id'],plan_sha256=campaign.digest(plan),
            max_requests=campaign.LIMITS['requests'],max_tokens=campaign.LIMITS['observed_input_output_tokens'],deadline=14600)
        for assignment in ledger['assignments'][:4]:
            row=controller.new_attempt(ledger,assignment,200);row.update(status='finished',stop_confirmed=True,
                launch_epoch=200,ended_epoch=210,evaluation={'quality':'failed','record':'retained-original'})
            record=root/'runs'/row['run_uuid'];record.mkdir(parents=True)
            (record/'submission.tar').write_bytes(b'original-submission')
            row['submission_sha256']=util.sha256_file(record/'submission.tar')
            util.write_new_json(record/'box.json',{'id':row['run_uuid']})
            budget.reserve(row['run_uuid'],row['run_uuid'],now=200)
            budget.finish(row['run_uuid'],{'input_tokens':10,'output_tokens':1},'completed')
        for name,value in [('plan',plan),('settings',settings)]:util.write_new_json(root/(name+'.json'),value)
        folder=root/'handoff';folder.mkdir()
        util.write_new_json(folder/'ledger.original.json',ledger);util.write_new_json(folder/'budget.original.json',budget.snapshot())
        util.write_new_json(root/'confirmation-scope.json',{'test':'separate policy tests cover scope binding'})
        ledger['phase']='paused_for_confirmation_scope'
        ledger['administrative_handoff']={'record':str(folder),'original_ledger_sha256':util.sha256_file(folder/'ledger.original.json'),
            'scope_sha256':util.sha256_file(root/'confirmation-scope.json')}
        util.write_new_json(root/'ledger.json',ledger)
        return settings,plan,ledger,budget

    def test_continuation_preserves_real_sqlite_budget_uuid_and_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);settings,plan,original,budget=self.make_checkpoint(root);actions=[];launched=[]
            def budget_action(settings,action,args=None):
                actions.append(action)
                if action=='close':budget.close(args['reason'])
                elif action!='snapshot':raise AssertionError('Budget recreation/reopening forbidden')
                return budget.snapshot()
            def wave(root,settings,plan,ledger,assignments,**kwargs):
                for a in assignments:
                    launched.append(a['assignment_id']);row=controller.new_attempt(ledger,a,300,**kwargs)
                    row.update(status='finished',stop_confirmed=True)
                    budget.reserve(row['run_uuid'],row['run_uuid'],now=300)
                    budget.finish(row['run_uuid'],{'input_tokens':10,'output_tokens':1},'completed')
            with patch.object(continuation.time,'time',return_value=300),\
                 patch.object(controller,'readiness'),patch.object(controller,'score_completed'),\
                 patch.object(controller,'wave',side_effect=wave),\
                 patch.object(continuation.box,'owned',return_value={'State':{'Running':False}}),\
                 patch.object(continuation.box,'budget_action',side_effect=budget_action):
                result=continuation.continue_campaign(root)
                with self.assertRaises(ValueError):continuation.continue_campaign(root)
            saved=util.read_json(root/'ledger.json')
            self.assertEqual(saved['attempts'][:4],original['attempts'])
            self.assertEqual((saved['id'],saved['deadline'],saved['started_epoch']),('same-instance',14600,200))
            self.assertEqual(len(launched),11);self.assertEqual(len(set(launched)),11)
            self.assertFalse(set(launched)&{a['assignment_id'] for a in original['attempts']})
            self.assertEqual(result['budget']['reserved_requests'],15)
            self.assertEqual(result['budget']['observed_tokens_lower_bound'],165)
            self.assertEqual(result['attempts'],15);self.assertNotIn('create',actions)

    def test_closed_or_changed_budget_and_live_attempt_block_continuation(self):
        for bad in ('closed','changed','live','deadline'):
            with self.subTest(bad=bad),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);settings,plan,ledger,budget=self.make_checkpoint(root)
                if bad=='closed':budget.close('original-stop')
                if bad=='changed':budget.reserve('extra','run',now=300);budget.finish('extra',{'input_tokens':1,'output_tokens':1},'completed')
                if bad=='live':ledger['attempts'][0]['stop_confirmed']=False;util.write_json_atomic(root/'ledger.json',ledger)
                with patch.object(continuation.time,'time',return_value=14600 if bad=='deadline' else 300),\
                     patch.object(continuation.box,'owned',return_value={'State':{'Running':False}}),\
                     patch.object(continuation.box,'budget_action',return_value=budget.snapshot()),\
                     patch.object(controller,'wave') as wave:
                    with self.assertRaises(ValueError):continuation.continue_campaign(root)
                    wave.assert_not_called()

if __name__=='__main__':unittest.main()
