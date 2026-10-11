"""Common adjudication of synthetic acquisition/persistence failures, no models."""
from concurrent.futures import Future
import copy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_pipeline_retry as fixture
import test_pipeline_evaluation_retry as evaluation_fixture
from outer.harness import util,runtime
from research import acquisition_pipeline as pipeline,acquisition_readiness
from research import live_pilot as live,pair_execution,pipeline_guard as guard,pipeline_retry as retry

REPO=fixture.REPO
REAL_STOPPED=guard.stopped


class InfrastructureRetryTests(unittest.TestCase):
    def setUp(self):
        fixture.RetryTests.setUp(self)
        self.authorization='synthetic-existing-infrastructure-adjudication'
        self.manifest.update(end_reason='environment_failure',duration_seconds=2.5)
        util.write_json_atomic(self.source/'manifest.json',self.manifest)
        self.runtime=dict(run_id='R',run_instance_id='1'*32,stop_confirmed=True,
            worker='synthetic-owned-worker',gateway='synthetic-owned-gateway',
            network='synthetic-owned-network',network_id='synthetic-network-id')
        util.write_new_json(self.source/'runtime.json',self.runtime)
        self.case=guard.attempts(self.config)[0]['cases'][0]
        self.journal=self.source.parent/'_control/pair-journal.jsonl'
        self.write_failure(PermissionError('Synthetic atomic persistence failure'))
        self.acquisition=self.source.parent/'pipeline-acquisition.json'
        self.saved=dict(acquired=False,slot=1,attempt='attempt-a',reason='implementation_fault',
            usage=pipeline.usage_for_manifests([self.source/'manifest.json']),error=None,fault=None)
        util.write_new_json(self.acquisition,self.saved)
        self.ruling_path=self.root/'operator-rulings/synthetic-infrastructure.json'
        self.ruling=dict(kind=retry.INFRASTRUCTURE_ADJUDICATION,run_id='R',run_instance_id='1'*32,
            authorization_reference=self.authorization,
            decision=dict(classification='infrastructure_fault',low_level_cause='unknown',product_defect=False,
                ordinary_timeout=False,quality_selection=False,retry_requires_new_uuid=True,
                basis='Explicit synthetic adjudication of fixed controller records; cause unknown'))
        self.bind_ruling()

    def write_failure(self,error):
        self.journal.unlink(missing_ok=True)
        pair_execution.append(self.journal,dict(kind='pair_reserved',assignments=[self.case]))
        pair_execution.append(self.journal,dict(kind='dispatch',**self.case))
        future=Future()
        if error is None:future.set_result(dict(run_id='R',run_instance_id='1'*32,stop_confirmed=True))
        else:future.set_exception(error)
        receipt,_=pair_execution._implementation_result(future,self.case,self.source.parent)
        pair_execution._record_implementation(self.journal,self.case,receipt,self.source.parent)
        if error is not None:pair_execution.append(self.journal,dict(kind='pause',pair=1,reason='implementation_fault'))

    def bind_ruling(self,fault=None):
        paths=dict(config=self.config_path,attempt=self.record,acquisition=self.acquisition,
            manifest=self.source/'manifest.json',runtime=self.source/'runtime.json',pair_journal=self.journal,
            snapshot=self.source/'snapshot.json',evaluation_index=self.source/'evaluations/index.jsonl',
            pipeline_evaluation=self.source.parent/'pipeline-evaluation.json')
        self.ruling.update({key:live.reference(path) if path.exists() else None for key,path in paths.items()})
        self.ruling['evaluation_record']=None
        self.ruling['fault_evidence']=[live.reference(fault or self.journal)]
        util.write_json_atomic(self.ruling_path,self.ruling)

    def exclude(self):
        return retry.exclude(REPO,self.config_path,self.record,'technical_failure',self.authorization,adjudication=self.ruling_path)

    def assert_refused(self):
        original=retry.source_inventory(self.source);acquisition=self.acquisition.read_bytes()
        events=(self.root/'events.jsonl').read_bytes()
        with self.assertRaises((ValueError,OSError,KeyError,TypeError)):self.exclude()
        self.assertEqual(original,retry.source_inventory(self.source))
        self.assertEqual(acquisition,self.acquisition.read_bytes())
        self.assertEqual(events,(self.root/'events.jsonl').read_bytes())
        self.assertFalse((self.root/'_exclusions').exists())

    def test_dispatched_zero_request_failure_retains_originals_cost_and_reservation(self):
        original=retry.source_inventory(self.source);acquisition=self.acquisition.read_bytes();journal=self.journal.read_bytes()
        ref=self.exclude();self.assertEqual(self.exclude(),ref)
        receipt=util.read_json(ref['path']);policy=retry.Exclusions(self.config_path)
        self.assertEqual(receipt['evidence']['kind'],'adjudicated_infrastructure_fault')
        self.assertEqual(receipt['evidence']['low_level_cause'],'unknown')
        self.assertFalse(receipt['adopted']);self.assertEqual(receipt['model_calls'],0)
        self.assertEqual(receipt['budget_usage']['dispatched_runs'],1)
        self.assertEqual(receipt['budget_usage']['requests'],0)
        self.assertEqual(receipt['budget_usage']['observed_tokens'],0)
        self.assertEqual(receipt['budget_usage']['accumulated_run_seconds'],2.5)
        self.assertEqual(original,retry.source_inventory(self.source))
        self.assertEqual(acquisition,self.acquisition.read_bytes());self.assertEqual(journal,self.journal.read_bytes())
        self.assertFalse((self.source/'snapshot.json').exists());self.assertFalse((self.source/'evaluations').exists())
        attempts=[dict(slot=1,record=str(self.record))]
        self.assertEqual(pipeline.pending_acquisition_slots(self.config,attempts,set()),([],{1}))
        self.assertEqual(pipeline.pending_acquisition_slots(self.config,[a for a in attempts if not policy.excluded(a)],set()),([1],set()))
        self.assertEqual(sum(e['kind']=='reserved' for e in pair_execution.events(self.root/'events.jsonl')),1)
        old=guard.attempts(self.config)[0]
        self.config['template'].update(runtime_locks={},assignments=old['phase']['assignments'])
        with patch.object(acquisition_readiness,'create'):
            new=util.read_json(pipeline.create_epoch(REPO,self.config,2))['assignments'][0]['cases'][0]
        self.assertNotEqual(new['run_instance_id'],self.case['run_instance_id'])
        for key in ('run_id','task','condition','slot'):self.assertEqual(new[key],self.case[key])

    def test_unlisted_exception_uses_same_proof_without_extending_an_allowlist(self):
        class UnlistedControllerStorageFault(Exception):pass
        self.write_failure(UnlistedControllerStorageFault('Synthetic storage failure'))
        self.bind_ruling();self.exclude()

    def test_acquisition_error_and_fault_receipts_use_the_same_adjudication(self):
        self.write_failure(None)
        self.saved.update(reason='acquisition_interrupted',error=dict(type='SyntheticIOFailure',message='synthetic'))
        util.write_json_atomic(self.acquisition,self.saved);self.bind_ruling(self.acquisition)
        self.exclude()

    def test_known_usage_components_and_null_measurements_are_never_refunded(self):
        self.manifest['duration_seconds']=None;util.write_json_atomic(self.source/'manifest.json',self.manifest)
        util.append_line(self.source/'usage/raw/started.jsonl',dict(request_id='partial'))
        util.append_line(self.source/'usage/raw/events.jsonl',dict(request_id='partial',usage=dict(input_tokens=7,output_tokens=None)))
        self.saved['usage']=pipeline.usage_for_manifests([self.source/'manifest.json'])
        util.write_json_atomic(self.acquisition,self.saved);self.bind_ruling()
        value=util.read_json(self.exclude()['path']);usage=value['budget_usage']
        self.assertEqual(usage['observed_tokens'],7);self.assertEqual(usage['requests'],1)
        self.assertIsNone(usage['unknown_usage_requests'][0]['observed_tokens'])
        self.assertIsNone(util.read_json(self.source/'manifest.json')['duration_seconds'])
        self.assertEqual(usage['accumulated_run_seconds'],30)

    def test_removed_raw_usage_cannot_override_original_acquisition_cost(self):
        self.saved['usage']['observed_tokens']=9
        util.write_json_atomic(self.acquisition,self.saved);self.bind_ruling();self.assert_refused()

    def test_journal_dispatch_cannot_be_discounted_by_missing_start_metadata(self):
        self.manifest['started_at']=None;util.write_json_atomic(self.source/'manifest.json',self.manifest)
        self.saved['usage']=pipeline.usage_for_manifests([self.source/'manifest.json'])
        util.write_json_atomic(self.acquisition,self.saved);self.bind_ruling();self.assert_refused()

    def test_explicit_ruling_and_fixed_failure_are_both_required(self):
        with self.assertRaisesRegex(ValueError,'No supported technical failure'):
            retry.exclude(REPO,self.config_path,self.record,'technical_failure',self.authorization)
        self.write_failure(None);self.bind_ruling();self.assert_refused()

    def test_error_prose_alone_and_foreign_evidence_are_not_authority(self):
        self.write_failure(None)
        log=self.source/'evidence/controller.log';log.parent.mkdir();log.write_text('Synthetic PermissionError, database error')
        self.bind_ruling(log);self.assert_refused()
        self.write_failure(PermissionError('synthetic'))
        self.ruling['fault_evidence']=[live.reference(self.config_path)]
        self.ruling['pair_journal']=live.reference(self.journal)
        util.write_json_atomic(self.ruling_path,self.ruling);self.assert_refused()

    def test_ruling_cannot_select_quality_product_failure_or_timeout(self):
        baseline=copy.deepcopy(self.ruling)
        for key in ('quality_selection','product_defect','ordinary_timeout'):
            self.ruling=copy.deepcopy(baseline);self.ruling['decision'][key]=True
            self.bind_ruling();self.assert_refused()
        self.ruling=baseline
        self.manifest['end_reason']='timeout';util.write_json_atomic(self.source/'manifest.json',self.manifest)
        self.bind_ruling();self.assert_refused()

    def test_typed_timeout_failure_is_not_overridden_by_an_infrastructure_ruling(self):
        self.write_failure(TimeoutError('Synthetic ordinary timeout'))
        self.bind_ruling();self.assert_refused()

    def test_retained_product_failure_blocks_the_acquisition_path_too(self):
        self.saved['verdict']='fail';util.write_json_atomic(self.acquisition,self.saved)
        self.bind_ruling();self.assert_refused()

    def test_unindexed_product_or_evaluation_evidence_cannot_be_discarded(self):
        util.write_new_json(self.source/'evaluations/unindexed/record.json',dict(scoring_state='scored',verdict='pass'))
        self.bind_ruling();self.assert_refused()

    def test_journal_preserves_a_scored_result_even_when_its_evaluation_index_is_missing(self):
        path=self.source.parent/'_control/synthetic-result.json'
        row=dict(scoring=dict(state='scored'),quality=1,verdict='pass')
        util.write_new_json(path,dict(row=row,**self.case))
        pair_execution.append(self.journal,dict(kind='result',receipt=str(path),receipt_sha256=util.sha256_file(path),
            row=row,**self.case))
        self.bind_ruling();self.assert_refused()

    def test_foreign_dispatch_or_runtime_identity_is_rejected(self):
        self.runtime['run_instance_id']='2'*32
        util.write_json_atomic(self.source/'runtime.json',self.runtime);self.bind_ruling();self.assert_refused()
        self.runtime['run_instance_id']='1'*32;util.write_json_atomic(self.source/'runtime.json',self.runtime)
        self.case['run_instance_id']='2'*32;self.write_failure(PermissionError('synthetic'))
        self.bind_ruling();self.assert_refused()

    def test_runtime_pair_receipt_and_optional_evidence_remain_fixed_after_sealing(self):
        self.exclude();path=self.source/'snapshot.json'
        util.write_new_json(path,dict(unexpected='later artifact'))
        with self.assertRaises(ValueError):retry.Exclusions(self.config_path)
        path.unlink()
        event=pair_execution.state(self.journal)['implementations']['R']
        Path(event['receipt_path']).write_text('{}')
        with self.assertRaises(ValueError):retry.Exclusions(self.config_path)

    def test_existing_physical_stop_checks_are_used_and_failure_is_held(self):
        with patch.object(guard,'stopped',side_effect=REAL_STOPPED), \
                patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=1,stdout='')):
            self.assert_refused()
        with patch.object(guard,'stopped',side_effect=REAL_STOPPED), \
                patch.object(runtime,'docker',return_value=SimpleNamespace(returncode=0,stdout='')) as docker:
            self.exclude()
        self.assertEqual(docker.call_count,3)

    def test_live_owner_and_adopted_attempt_cannot_be_excluded(self):
        with pair_execution.exclusive(self.root/'_owner'):self.assert_refused()
        pair_execution.append(self.root/'events.jsonl',dict(kind='accepted',slot=1,record={'path':'synthetic','sha256':'synthetic'}))
        self.assert_refused()

    def test_generic_format_uses_the_same_existing_evaluation_validator(self):
        case=evaluation_fixture.EvaluationInfrastructureRetryTests()
        case.setUp()
        try:
            binding=guard.attempts(case.config)[0]['cases'][0]
            journal=case.source.parent/'_control/pair-journal.jsonl'
            pair_execution.append(journal,dict(kind='pair_reserved',assignments=[binding]))
            pair_execution.append(journal,dict(kind='dispatch',**binding))
            util.write_new_json(case.source/'runtime.json',dict(run_id='R',run_instance_id='1'*32))
            case.ruling['kind']=retry.INFRASTRUCTURE_ADJUDICATION
            case.ruling['decision']['classification']='infrastructure_fault'
            case.ruling['runtime']=live.reference(case.source/'runtime.json')
            case.ruling['pair_journal']=live.reference(journal)
            util.write_json_atomic(case.ruling_path,case.ruling)
            ref=case.exclude()
            self.assertEqual(util.read_json(ref['path'])['evidence']['kind'],'adjudicated_infrastructure_fault')
            retry.Exclusions(case.config_path)
        finally:case.doCleanups()


if __name__=='__main__':unittest.main()
