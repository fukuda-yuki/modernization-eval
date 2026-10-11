"""Explicit technical exclusions retain originals/costs and use fresh UUIDs."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_pipeline_owner import fixture,REPO
from outer.harness import util
from research import acquisition_pipeline as pipeline,acquisition_readiness,campaign_initialization
from research import live_pilot,pair_execution,pipeline_guard as guard,pipeline_retry as retry


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name);self.config,self.record,self.source,self.manifest=fixture(self.base)
        self.root=Path(self.config['root']);self.config_path=self.root/'config.json'
        self.config.update(accepted_slots=[],reserved_pair_attempts=0,source_commit='a'*40)
        util.write_json_atomic(self.config_path,self.config)
        # Rebind the fixture's immutable epoch/attempt after its config changes.
        epoch_path=Path(util.read_json(self.record)['epoch']['path']);epoch=util.read_json(epoch_path)
        epoch['campaign_config']=live_pilot.reference(self.config_path);util.write_json_atomic(epoch_path,epoch)
        attempt=util.read_json(self.record);attempt['epoch']=live_pilot.reference(epoch_path);util.write_json_atomic(self.record,attempt)
        for name in ('inputs','profiles','evaluation-assets'):(self.source/name).mkdir()
        util.write_new_json(self.source/'condition.json',dict(schema_version=2,task_id='toy',runtime={'model_id':'fixture-model'}))
        util.write_new_json(self.source/'context.json',dict(method='explore',blocks=[]))
        self.manifest.update(end_reason='operator_stop',condition_sha256=util.sha256_file(self.source/'condition.json'),
            context_sha256=util.sha256_file(self.source/'context.json'),input_files={},assets_sha256={},profile_files={})
        util.write_json_atomic(self.source/'manifest.json',self.manifest)
        for patcher in (patch.object(retry,'guard',return_value=guard),patch.object(guard,'context',return_value=self.config),
                        patch.object(guard,'stopped',return_value=True)):
            patcher.start();self.addCleanup(patcher.stop)

    def exclude(self,reason='controller_interrupted'):
        return retry.exclude(REPO,self.config_path,self.record,reason,'explicit-user-retry-instruction-fixture')

    def test_interruption_is_excluded_not_adopted_or_relabelled_provider_failure(self):
        before=util.tree_hashes(self.source);config_before=self.config_path.read_bytes()
        ref=self.exclude();value=util.read_json(ref['path'])
        self.assertFalse(value['adopted']);self.assertEqual(value['evidence']['cause'],'unknown')
        self.assertFalse(value['evidence']['exit_code_inferred']);self.assertEqual(value['model_calls'],0)
        self.assertEqual(util.tree_hashes(self.source),before);self.assertEqual(self.config_path.read_bytes(),config_before)
        self.assertFalse(any(e['kind']=='accepted' for e in pair_execution.events(self.root/'events.jsonl')))
        self.assertFalse((self.source/'evaluations').exists())

    def test_null_duration_and_inflight_cost_survive_exclusion_and_resume(self):
        util.append_line(self.source/'usage/raw/started.jsonl',dict(request_id='inflight'))
        ref=self.exclude();value=util.read_json(ref['path']);policy=retry.Exclusions(self.config_path)
        self.assertEqual(value['observed_usage']['accumulated_run_seconds'],0)
        self.assertEqual(value['budget_usage']['accumulated_run_seconds'],30)
        self.assertIsNone(value['conservative_duration_charge']['measured_duration_seconds'])
        self.assertIsNone(util.read_json(self.source/'manifest.json')['duration_seconds'])
        attempt={'slot':1,'record':str(self.record)};saved=util.read_json(self.record.parent/'pipeline-acquisition.json')
        budget=pipeline.Budget(self.config);budget.finish(str(self.record),policy.usage(attempt,saved))
        budget.finish(str(self.record),policy.usage(attempt,saved))
        self.assertEqual(budget.total['requests'],1);self.assertEqual(budget.total['accumulated_run_seconds'],30)
        self.assertEqual(budget.total['unknown_usage_requests'][0]['request_id'],'inflight')

    def test_authorized_exclusion_removes_hold_but_not_original_reservation(self):
        self.exclude();attempts=[{'slot':1,'record':str(self.record)}]
        self.assertEqual(pipeline.pending_acquisition_slots(self.config,attempts,set()),([],{1}))
        policy=retry.Exclusions(self.config_path)
        eligible=[a for a in attempts if not policy.excluded(a)]
        self.assertEqual(pipeline.pending_acquisition_slots(self.config,eligible,set()),([1],set()))
        self.assertEqual(sum(e['kind']=='reserved' for e in pair_execution.events(self.root/'events.jsonl')),1)

    def test_ordinary_epoch_gives_replacement_new_uuid(self):
        self.exclude();old=guard.attempts(self.config)[0]
        self.config['template'].update(runtime_locks={},assignments=old['phase']['assignments'])
        with patch.object(acquisition_readiness,'create'):
            path=pipeline.create_epoch(REPO,self.config,2)
        new=util.read_json(path)['assignments'][0]['cases'][0]
        self.assertNotEqual(new['run_instance_id'],old['cases'][0]['run_instance_id'])
        for key in ('run_id','task','condition','slot'):self.assertEqual(new[key],old['cases'][0][key])

    def test_exclusion_is_idempotent_and_cannot_replace_changed_source(self):
        ref=self.exclude();before=util.tree_hashes(self.root)
        self.assertEqual(self.exclude(),ref);self.assertEqual(util.tree_hashes(self.root),before)
        (self.source/'extra-original.txt').write_text('changed')
        with self.assertRaises(ValueError):retry.Exclusions(self.config_path)
        with self.assertRaises(ValueError):self.exclude()

    def test_exclusion_never_hashes_held_controller_lock_but_keeps_nested_evidence(self):
        nested=self.source/'workspace/controller.lock';nested.parent.mkdir();nested.write_bytes(b'product evidence')
        actual=util.sha256_file
        def hash_file(path):
            if Path(path)==self.source/'controller.lock':raise PermissionError('Windows locked byte')
            return actual(path)
        with patch.object(util,'sha256_file',side_effect=hash_file):
            ref=self.exclude();policy=retry.Exclusions(self.config_path)
        receipt=util.read_json(ref['path'])
        self.assertEqual(receipt['inventory_excluded_paths'],['controller.lock'])
        self.assertNotIn('controller.lock',receipt['source_inventory'])
        self.assertIn('workspace/controller.lock',receipt['source_inventory'])
        self.assertTrue(policy.excluded({'record':str(self.record)}))

    def test_completed_quality_or_product_failure_is_not_technical_authority(self):
        self.manifest['end_reason']='completed';util.write_json_atomic(self.source/'manifest.json',self.manifest)
        util.write_new_json(self.record.parent/'pipeline-acquisition.json',dict(acquired=True,reason='evaluation_pending'))
        util.write_new_json(self.record.parent/'pipeline-evaluation.json',dict(accepted=False,quality=0,verdict='fail'))
        for reason in ('controller_interrupted','technical_failure'):
            with self.assertRaises(ValueError):self.exclude(reason)
        self.assertFalse((self.root/'_exclusions').exists())

    def collection_fault(self):
        self.manifest['end_reason']='completed';util.write_json_atomic(self.source/'manifest.json',self.manifest)
        util.write_new_json(self.source/'snapshot.json',{'fixture':'saved collection'})
        util.write_new_json(self.source/'implementation-receipt.json',dict(run_id='R',run_instance_id='1'*32,
            collection_status='collection_fault',collection_error_type='PermissionError',stop_confirmed=True,
            raw=util.tree_hashes(self.source/'usage/raw'),snapshot_sha256=util.sha256_file(self.source/'snapshot.json')))

    def test_collection_failure_can_be_excluded_without_projection_or_rescoring(self):
        self.collection_fault()
        original=dict(acquired=False,attempt='attempt-a',slot=1,usage=dict(self.config['usage']),reason='original_collection_error')
        path=self.record.parent/'pipeline-acquisition.json';util.write_new_json(path,original)
        before=util.tree_hashes(self.source);saved=path.read_bytes()
        ref=self.exclude('technical_failure')
        self.assertEqual(util.read_json(ref['path'])['evidence']['kind'],'recorded_collection_fault')
        self.assertEqual(path.read_bytes(),saved);self.assertEqual(util.tree_hashes(self.source),before)
        self.assertTrue(retry.Exclusions(self.config_path).excluded({'record':str(self.record)}))

    def test_adopted_slot_and_live_owner_cannot_be_excluded(self):
        with pair_execution.exclusive(self.root/'_owner'):
            with self.assertRaises(OSError):self.exclude()
        pair_execution.append(self.root/'events.jsonl',dict(kind='accepted',slot=1,record={'path':'prior','sha256':'prior'}))
        with self.assertRaisesRegex(ValueError,'adopted'):self.exclude()

    def test_adapter_skips_saved_evaluation_of_excluded_attempt(self):
        self.collection_fault()
        acquisition=self.record.parent/'pipeline-acquisition.json'
        util.write_new_json(acquisition,dict(acquired=True,slot=1,attempt='attempt-a',usage=dict(self.config['usage'])))
        self.exclude('technical_failure');policy=retry.Exclusions(self.config_path)
        with patch.object(campaign_initialization,'verify_config'),patch.object(live_pilot,'verify_pins'), \
             patch.object(pipeline.Budget,'check',side_effect=RuntimeError('fixture stop before any dispatch')), \
             patch.object(pipeline,'evaluation_process',side_effect=AssertionError('Excluded saved evaluation must not run')) as score:
            with self.assertRaisesRegex(RuntimeError,'fixture stop'):
                pipeline.run_pipeline(REPO,self.config_path,attempt_policy=policy)
        score.assert_not_called()
        self.assertFalse(any(e['kind']=='accepted' for e in pair_execution.events(self.root/'events.jsonl')))


if __name__=='__main__':unittest.main()
