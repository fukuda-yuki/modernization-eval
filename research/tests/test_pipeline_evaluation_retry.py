"""Explicit infrastructure adjudication; only artificial, providerless originals."""
import copy
import unittest
from unittest.mock import patch

import test_pipeline_retry as retry_fixture
from outer.harness import evaluate,util
from research import acquisition_pipeline as pipeline,acquisition_readiness,campaign_initialization
from research import live_pilot as live,pair_execution,pipeline_guard as guard,pipeline_retry as retry

REPO=retry_fixture.REPO


class EvaluationInfrastructureRetryTests(unittest.TestCase):
    def setUp(self):
        retry_fixture.RetryTests.setUp(self)
        self.authorization='synthetic-existing-explicit-infrastructure-ruling'
        assets=self.source/'evaluation-assets'
        (assets/'requirements.json').write_text('{}')
        (assets/'evaluator').mkdir();(assets/'evaluator/toy.py').write_text('# synthetic, never executed')
        self.evaluation=dict(evaluation_version='synthetic-v1',spec_path='evaluation-assets/requirements.json',
            spec_sha256=util.sha256_file(assets/'requirements.json'),assembly='toy.py',
            evaluator_sha256=util.sha256_file(assets/'evaluator/toy.py'))
        condition=util.read_json(self.source/'condition.json');condition['evaluation']=self.evaluation
        util.write_json_atomic(self.source/'condition.json',condition)
        (self.source/'frozen').mkdir();(self.source/'frozen/toy.txt').write_text('synthetic saved artifact')
        self.artifact=util.artifact_hash(self.source/'frozen')
        self.manifest.update(end_reason='completed',submission_fixed=True,
            assets_sha256=util.tree_hashes(assets),condition_sha256=util.sha256_file(self.source/'condition.json'))
        util.write_json_atomic(self.source/'manifest.json',self.manifest)
        util.write_new_json(self.source/'snapshot.json',dict(run_id='R',artifact_sha256=self.artifact))
        util.append_line(self.source/'usage/raw/started.jsonl',dict(request_id='known'))
        util.append_line(self.source/'usage/raw/events.jsonl',dict(request_id='known',usage=dict(input_tokens=11,output_tokens=3)))
        util.append_line(self.source/'usage/raw/started.jsonl',dict(request_id='unknown'))
        self.acquisition=self.record.parent/'pipeline-acquisition.json'
        util.write_new_json(self.acquisition,dict(acquired=True,slot=1,attempt='attempt-a',reason='evaluation_pending',
            usage=pipeline.usage_for_manifests([self.source/'manifest.json'])))
        self.directory=self.source/'evaluations/evaluator_fault-001-synthetic'
        self.output=dict(taskId='toy',evaluationVersion='synthetic-v1',artifactPath='/artifact',
            artifactSha256=self.artifact,specSha256=self.evaluation['spec_sha256'],reviewRunInstanceId='1'*32,
            evaluationId='synthetic-evaluation',verdict='error',quality=None,researchStatus='incomplete',
            evaluatorFaults=['Synthetic evaluator infrastructure failure'],requirements=[dict(id='toy',judgement='error')])
        self.scoring=evaluate._base_record('R',1,'synthetic-v1',2,self.artifact,self.evaluation['spec_sha256'],
            'evaluator_fault',self.output,[],['synthetic-never-executed'],
            evaluator_sha256=self.evaluation['evaluator_sha256'],timed_out=False,artifact_sha256_frozen=self.artifact)
        self.scoring.update(directory='evaluations/'+self.directory.name,operation_status='evaluation_incomplete')
        self.write_scoring()
        util.write_new_json(self.directory/'evaluator-manifest.json',dict(evaluatorSha256=self.evaluation['evaluator_sha256']))
        self.closure=dict(confirmed=True,status='no_browser_resources_created',resources=[],run_instance_id='1'*32,
            artifact_sha256=self.artifact,spec_sha256=self.evaluation['spec_sha256'],evaluator_container_removed=True)
        util.write_new_json(self.directory/'browser-cleanup.json',self.closure)
        self.fault=self.source/'evidence/scoring-001-stderr.log'
        self.fault.parent.mkdir(exist_ok=True);self.fault.write_text('Synthetic database open error 14; cause unknown')
        self.ruling_path=self.root/'operator-rulings/synthetic.json'
        self.ruling=dict(kind=retry.EVALUATION_ADJUDICATION,run_id='R',run_instance_id='1'*32,
            authorization_reference=self.authorization,
            decision=dict(classification='evaluation_infrastructure_fault',low_level_cause='unknown',product_defect=False,
                quality_selection=False,ordinary_timeout=False,retry_requires_new_uuid=True,
                basis='Synthetic explicit ruling from fixed originals; no inference from the error number'))
        self.bind_ruling()

    def write_scoring(self):
        util.write_json_atomic(self.directory/'evaluation.json',self.output)
        self.scoring['evaluation_sha256']=util.sha256_file(self.directory/'evaluation.json')
        util.write_json_atomic(self.directory/'record.json',self.scoring)
        (self.source/'evaluations/index.jsonl').unlink(missing_ok=True)
        evaluate._append_index(self.source,self.scoring)

    def bind_ruling(self):
        for key,path in dict(config=self.config_path,attempt=self.record,acquisition=self.acquisition,
                manifest=self.source/'manifest.json',snapshot=self.source/'snapshot.json',
                evaluation_index=self.source/'evaluations/index.jsonl',evaluation_record=self.directory/'record.json').items():
            self.ruling[key]=live.reference(path)
        self.ruling['fault_evidence']=[live.reference(self.fault)]
        pipeline_evaluation=self.record.parent/'pipeline-evaluation.json'
        self.ruling['pipeline_evaluation']=live.reference(pipeline_evaluation) if pipeline_evaluation.exists() else None
        util.write_json_atomic(self.ruling_path,self.ruling)

    def exclude(self):
        return retry.exclude(REPO,self.config_path,self.record,'technical_failure',self.authorization,self.ruling_path)

    def assert_refused(self):
        originals=retry.source_inventory(self.source);acquisition=self.acquisition.read_bytes()
        events=(self.root/'events.jsonl').read_bytes()
        with self.assertRaises((ValueError,OSError,KeyError,TypeError)):self.exclude()
        self.assertEqual(retry.source_inventory(self.source),originals)
        self.assertEqual(self.acquisition.read_bytes(),acquisition)
        self.assertEqual((self.root/'events.jsonl').read_bytes(),events)
        self.assertFalse((self.root/'_exclusions').exists())

    def test_seals_originals_and_unknown_cause_idempotently_retaining_all_costs(self):
        original=retry.source_inventory(self.source);acquisition=self.acquisition.read_bytes()
        ref=self.exclude();receipt=util.read_json(ref['path'])
        self.assertEqual(self.exclude(),ref)
        self.assertEqual(receipt['evidence']['low_level_cause'],'unknown')
        self.assertFalse(receipt['adopted']);self.assertEqual(receipt['model_calls'],0)
        self.assertEqual(retry.source_inventory(self.source),original);self.assertEqual(self.acquisition.read_bytes(),acquisition)
        self.assertEqual(receipt['budget_usage']['observed_tokens'],14)
        self.assertEqual(receipt['budget_usage']['requests'],2)
        self.assertEqual(len(receipt['budget_usage']['unknown_usage_requests']),1)
        self.assertIsNone(util.read_json(self.source/'manifest.json')['duration_seconds'])
        self.assertEqual(receipt['budget_usage']['accumulated_run_seconds'],30)
        policy=retry.Exclusions(self.config_path);attempt=dict(slot=1,record=str(self.record))
        budget=pipeline.Budget(self.config)
        for _ in range(2):budget.finish(str(self.record),policy.usage(attempt,util.read_json(self.acquisition)))
        self.assertEqual(budget.total['requests'],2)
        self.assertEqual(sum(e['kind']=='reserved' for e in pair_execution.events(self.root/'events.jsonl')),1)
        self.assertEqual(pipeline.pending_acquisition_slots(self.config,[attempt],set()),([],{1}))
        self.assertEqual(pipeline.pending_acquisition_slots(self.config,[a for a in [attempt] if not policy.excluded(a)],set()),([1],set()))

    def test_regular_resume_skips_original_evaluation_and_keeps_assignment_with_fresh_uuid(self):
        self.exclude();policy=retry.Exclusions(self.config_path)
        with patch.object(campaign_initialization,'verify_config'),patch.object(live,'verify_pins'), \
                patch.object(pipeline.Budget,'check',side_effect=RuntimeError('fixture stop before dispatch')), \
                patch.object(pipeline,'evaluation_process',side_effect=AssertionError('No original rescoring')) as score:
            with self.assertRaisesRegex(RuntimeError,'fixture stop'):
                pipeline.run_pipeline(REPO,self.config_path,attempt_policy=policy)
        score.assert_not_called()
        old=guard.attempts(self.config)[0]
        self.config['template'].update(runtime_locks={},assignments=old['phase']['assignments'])
        with patch.object(acquisition_readiness,'create'):
            new=util.read_json(pipeline.create_epoch(REPO,self.config,2))['assignments'][0]['cases'][0]
        self.assertNotEqual(new['run_instance_id'],old['cases'][0]['run_instance_id'])
        for key in ('run_id','task','condition','slot'):self.assertEqual(new[key],old['cases'][0][key])

    def test_error_number_and_fault_record_without_adjudication_are_not_authority(self):
        with self.assertRaisesRegex(ValueError,'No supported technical failure'):
            retry.exclude(REPO,self.config_path,self.record,'technical_failure',self.authorization)
        self.assertFalse((self.root/'_exclusions').exists())

    def test_explicit_ruling_fields_and_authorization_are_required(self):
        baseline=copy.deepcopy(self.ruling)
        for key,value in [('classification','product_defect'),('low_level_cause','sqlite'),('product_defect',True),
                ('ordinary_timeout',True),('quality_selection',True),('retry_requires_new_uuid',False),('basis','')]:
            with self.subTest(key=key):
                self.ruling=copy.deepcopy(baseline);self.ruling['decision'][key]=value;self.bind_ruling();self.assert_refused()
        self.ruling=copy.deepcopy(baseline);self.ruling['authorization_reference']='different instruction'
        self.bind_ruling();self.assert_refused()

    def test_quality_failure_build_prerequisite_and_partial_product_failure_refuse(self):
        baseline=copy.deepcopy(self.output)
        for changes in (dict(verdict='fail',quality=0),dict(verdict='fail_critical'),
                dict(browserReviewCoverage='not_run_product_prerequisite'),
                dict(requirements=[dict(id='toy',judgement='fail')])):
            with self.subTest(changes=changes):
                self.output=dict(baseline,**changes);self.write_scoring();self.bind_ruling();self.assert_refused()

    def test_http_only_product_failure_with_latest_error_is_not_erased(self):
        util.append_line(self.directory/'http-only/results.jsonl',dict(checkId='synthetic',judgement='fail'))
        self.assert_refused()

    def test_earlier_product_failure_and_adopted_results_are_not_erased(self):
        index=self.source/'evaluations/index.jsonl';latest=index.read_bytes()
        for old in (dict(scoring_state='scored',adopted=True),dict(verdict='fail_critical')):
            with self.subTest(old=old):
                index.unlink();util.append_line(index,old)
                with index.open('ab') as stream:stream.write(latest)
                self.bind_ruling();self.assert_refused()

    def test_timeout_and_unknown_evaluator_exit_are_not_infrastructure_authority(self):
        baseline=copy.deepcopy(self.scoring)
        for changes in (dict(evaluator_timed_out=True),dict(evaluator_exit_code=-1),dict(evaluator_exit_code=124),
                dict(evaluator_exit_code=None),dict(scoring_state='evaluation_incomplete'),dict(mismatches=[dict(check='artifact')])):
            with self.subTest(changes=changes):
                self.scoring=dict(baseline,**changes);self.write_scoring();self.bind_ruling();self.assert_refused()

    def test_missing_or_foreign_fixed_evidence_refuses(self):
        baseline=copy.deepcopy(self.ruling)
        for key in ('config','attempt','manifest','snapshot','acquisition','evaluation_index','evaluation_record'):
            with self.subTest(key=key):
                self.ruling=copy.deepcopy(baseline);self.ruling[key]=live.reference(self.fault)
                util.write_json_atomic(self.ruling_path,self.ruling);self.assert_refused()
        self.ruling=copy.deepcopy(baseline);self.ruling['fault_evidence']=[]
        util.write_json_atomic(self.ruling_path,self.ruling);self.assert_refused()

    def test_changed_original_fault_is_not_accepted(self):
        self.fault.write_text('changed synthetic original');self.assert_refused()

    def test_foreign_artifact_evaluator_or_run_identity_refuses_even_with_rebound_ruling(self):
        baseline=copy.deepcopy(self.scoring)
        for changes in (dict(artifact_sha256_outer='0'*64),dict(evaluator_sha256='0'*64),dict(run_id='foreign')):
            with self.subTest(changes=changes):
                self.scoring=dict(baseline,**changes);self.write_scoring();self.bind_ruling();self.assert_refused()
        self.scoring=baseline;self.output['reviewRunInstanceId']='2'*32
        self.write_scoring();self.bind_ruling();self.assert_refused()

    def test_uncompleted_acquisition_and_missing_explicit_decision_refuse(self):
        acquisition=self.acquisition.read_bytes()
        saved=util.read_json(self.acquisition);saved['acquired']=False
        util.write_json_atomic(self.acquisition,saved);self.bind_ruling();self.assert_refused()
        self.acquisition.write_bytes(acquisition)
        self.ruling['decision'].pop('ordinary_timeout');self.bind_ruling();self.assert_refused()

    def test_changed_acquisition_or_ruling_after_sealing_blocks_resume(self):
        self.exclude();saved=self.acquisition.read_bytes()
        self.acquisition.write_text('{}')
        with self.assertRaises(ValueError):retry.Exclusions(self.config_path)
        self.acquisition.write_bytes(saved)
        self.ruling['decision']['basis']='changed after seal';util.write_json_atomic(self.ruling_path,self.ruling)
        with self.assertRaises(ValueError):retry.Exclusions(self.config_path)

    def test_pipeline_evaluation_receipt_presence_and_bytes_remain_bound(self):
        path=self.record.parent/'pipeline-evaluation.json'
        util.write_new_json(path,dict(accepted=False,error='synthetic evaluation failure'))
        self.assert_refused()  # It was absent when adjudicated.
        self.bind_ruling();original=path.read_bytes();self.exclude()
        self.assertEqual(path.read_bytes(),original)
        util.write_json_atomic(path,dict(accepted=False,error='changed synthetic failure'))
        with self.assertRaisesRegex(ValueError,'pipeline evaluation receipt changed'):
            retry.Exclusions(self.config_path)

    def test_unconfirmed_or_foreign_evaluator_closure_refuses(self):
        for changes in (dict(confirmed=False),dict(run_instance_id='2'*32),dict(evaluator_container_removed=False)):
            with self.subTest(changes=changes):
                util.write_json_atomic(self.directory/'browser-cleanup.json',dict(self.closure,**changes));self.assert_refused()

    def test_missing_output_with_fixed_missing_output_record_is_supported(self):
        (self.directory/'evaluation.json').unlink();self.scoring.pop('evaluation_sha256')
        self.scoring.update(verdict=None,evaluation_id=None)
        util.write_json_atomic(self.directory/'record.json',self.scoring)
        (self.source/'evaluations/index.jsonl').unlink();evaluate._append_index(self.source,self.scoring)
        self.bind_ruling();self.exclude()
        self.assertFalse((self.directory/'evaluation.json').exists())

    def test_deleted_previously_recorded_output_refuses(self):
        (self.directory/'evaluation.json').unlink();self.assert_refused()

    def test_adopted_slot_and_active_evaluation_owner_refuse(self):
        with pair_execution.exclusive(self.source.parent/'_control'):self.assert_refused()
        pair_execution.append(self.root/'events.jsonl',dict(kind='accepted',slot=1,record={'path':'synthetic','sha256':'synthetic'}))
        self.assert_refused()


if __name__=='__main__':unittest.main()
