import copy
import unittest

from outer.fineract.campaign import CORE, EXTRA, TASKS, digest, make_plan, validate_approval, validate_readiness


def fixture():
    return {'conditions':[{'task':t,'condition':c,'dose':c.split('-')[0],
        'organization':c.split('-')[1], 'evaluation_spec_kind':'execution-spec',
        'initial_bytes':100,'body_bytes':40, **{k:'a'*64 for k in (
            'request_sha256','source_inventory_sha256','semantic_corpus_sha256',
            'evaluation_spec_sha256','initial_sha256','navigation_sha256','body_sha256',
            'selection_index_sha256')}} for t in TASKS for c in CORE+EXTRA]}


class FineractCampaignTests(unittest.TestCase):
    def plan(self,m=None):
        return make_plan(m or fixture(),campaign_id='synthetic',seed=123,
                         runtime_sha256='b'*64,evaluator_sha256='c'*64,selection_seal_sha256='d'*64)

    def test_exact_fifteen_allocation_and_six_explicit_unrun(self):
        plan=self.plan();rows=plan['assignments']
        self.assertEqual(len(rows),21)
        self.assertEqual(sum(r['planned_status']=='scheduled' for r in rows),15)
        self.assertEqual([sum(r['wave']==w for r in rows) for w in range(1,5)],[4,4,4,3])
        self.assertTrue(all(r['not_run_reason'] for r in rows if r['wave'] is None))
        self.assertEqual(plan,self.plan())

    def test_changed_semantics_or_missing_condition_denied(self):
        for change in ('missing','semantic','public-only'):
            m=fixture()
            if change=='missing':m['conditions'].pop()
            elif change=='semantic':m['conditions'][0]['request_sha256']='d'*64
            else:m['conditions'][0]['evaluation_spec_kind']='public-contract'
            with self.subTest(change=change),self.assertRaises(ValueError):self.plan(m)

    def test_approval_does_not_transfer_to_a_changed_plan(self):
        plan=self.plan()
        receipt={'plan_sha256':digest(plan),'scope':'finite_existing_opencode_go_acceptance',
            'authority_source':'parent_delegation','instruction_reference':'synthetic-test-only',
            'additional_purchase_allowed':False,'main_merge_allowed':False}
        validate_approval(plan,receipt)
        changed=copy.deepcopy(plan);changed['limits']['requests']+=1
        with self.assertRaises(ValueError):validate_approval(changed,receipt)
        with self.assertRaises(ValueError):validate_approval(plan,{})

    def test_review_calibration_and_unknown_billing_all_fail_closed(self):
        plan=self.plan()
        approval={'plan_sha256':digest(plan),'scope':'finite_existing_opencode_go_acceptance',
            'authority_source':'parent_delegation','instruction_reference':'synthetic-test-only',
            'additional_purchase_allowed':False,'main_merge_allowed':False}
        inputs={'materials':{'decision':'approved','review_reference':'synthetic',
            'selection_seal_sha256':'d'*64,'semantic_corpus_sha256':'a'*64},
            'runtime':{'runtime_sha256':'b'*64,'verified':True,'isolated_workers':4,'offline_fresh_build':True},
            'calibration':{'evaluator_sha256':'c'*64,'independent_review':{
                'decision':'approved','review_reference':'synthetic','evaluator_sha256':'c'*64},
                'tasks':{t:{'baseline_new_behavior':'failed',
                'valid_gold':'passed','mutants':[{'verdict':'failed'}],
                'verified_layers':['java','api','database','cob']} for t in TASKS}},
            'billing':{'provider':'opencode-go','use_balance':False,'workspace_binding_sha256':'e'*64,
                'read_only_evidence_reference':'synthetic','observed_at_epoch':100},'now':200}
        self.assertTrue(validate_readiness(plan,approval,**inputs)['ready'])
        for field,updates in [('materials',{'decision':'pending'}),('materials',{'selection_seal_sha256':'f'*64}),
                ('runtime',{'offline_fresh_build':False}),('calibration',{'tasks':{}}),
                ('calibration',{'independent_review':{}}),
                ('calibration',{'independent_review':{'decision':'approved','review_reference':'synthetic','evaluator_sha256':'f'*64}}),
                ('billing',{'use_balance':None}),('billing',{'use_balance':True}),
                ('billing',{'observed_at_epoch':-4000})]:
            changed=copy.deepcopy(inputs);changed[field].update(updates)
            with self.subTest(field=field,updates=updates),self.assertRaises(ValueError):
                validate_readiness(plan,approval,**changed)
