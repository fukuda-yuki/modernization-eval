"""Synthetic campaign lifecycle; no model, private scorer, browser or Docker."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from outer.harness import profiles, run, util
from research import acquisition_pipeline as pipeline, campaign_initialization as fresh, live_pilot

REPO = Path(__file__).resolve().parents[2]


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        refs = {}
        for name in ('monitor.py', 'wave_resource_probe.py', 'browser.json', 'lock.json'):
            util.write_new_json(self.root/name, {})
            refs[name] = live_pilot.reference(self.root/name)
        (self.root/'monitor.py').write_text('CADENCE_SECONDS=10\nSTALE_SECONDS=30\n'
            'class ResourceMonitor:\n    SUPPORTED_RUN_COUNTS=(0,1,2,4,8)\n', encoding='utf-8')
        refs['monitor.py'] = live_pilot.reference(self.root/'monitor.py')
        self.plan = dict(kind=fresh.KIND, campaign_id='synthetic-a', output_root=str(self.root/'a'),
            pair_count=1, pair_concurrency=1, task_revision='evaluators-20261006',
            assignments=[dict(pair=1, cases=[dict(task='MS1-CONT-A', condition=arm, pair=1, slot=i,
                attempt=1, run_id=run.run_id_for('MS1-CONT-A', arm, 1)) for i, arm in enumerate(('explore', 'preload'), 1)])],
            runtime_by_task={'MS1-CONT-A': 'deepseek-music-repaired-v1'},
            bounds=dict(live_pilot.BOUNDS, max_pairs=1, max_runs=2, max_pair_attempts=3,
                        request_count=100, observed_tokens=100000, wall_seconds=9000),
            settings=dict(model_id='deepseek-v4.1-flash', provider='opencode-go', use_balance=False, paid_fallback=False),
            protected_roots=[str(self.root/'original')], resource_monitor=refs['monitor.py'],
            resource_probe=refs['wave_resource_probe.py'], browser_pin=refs['browser.json'],
            runtime_locks={'deepseek-music-repaired-v1': refs['lock.json']}, thresholds=live_pilot.DEFAULT_THRESHOLDS)
        runtime = profiles.read(REPO, 'runtimes', 'deepseek-music-repaired-v1')
        self.plan['bounds'].update(run_seconds=runtime['timeout_seconds'], provider_seconds=runtime['provider_timeout_seconds'])
        task = profiles.task_profile(REPO, 'MS1-CONT-A', 'evaluators-20261006')
        util.write_json_atomic(self.root/'lock.json', dict(task_profile_revision='evaluators-20261006',
            runtime_profile_id='deepseek-music-repaired-v1', evaluator_version=task['evaluation']['evaluation_version'],
            evaluator_project=task['evaluation']['project']))
        self.plan['runtime_locks']['deepseek-music-repaired-v1'] = live_pilot.reference(self.root/'lock.json')
        self.patch(fresh.profiles, 'runtime_root', return_value=self.root)
        from research import repaired_runtime
        self.patch(repaired_runtime, 'validate_repaired_runtime_binding')
        actual = fresh.subprocess.check_output
        self.patch(fresh.subprocess, 'check_output', side_effect=lambda args, **kw: '' if args == ['git', 'status', '--porcelain'] else actual(args, **kw))

    def patch(self, target, name, **kw):
        p = patch.object(target, name, **kw)
        result = p.start(); self.addCleanup(p.stop)
        return result

    def initialize(self, name='a'):
        plan = copy.deepcopy(self.plan)
        plan.update(campaign_id='synthetic-'+name, output_root=str(self.root/name))
        path = self.root/(name+'-plan.json'); util.write_new_json(path, plan)
        approval = self.root/(name+'-approval.json')
        util.write_new_json(approval, dict(authorized=True, approved_by='user', authorization_reference='synthetic-only',
            campaign_id=plan['campaign_id'], plan_sha256=util.sha256_file(path)))
        return fresh.initialize(REPO, path, approval)

    def test_two_geneses_do_not_inherit_state_and_duplicate_root_refused(self):
        a, b = self.initialize(), self.initialize('b')
        self.assertNotEqual(a['pipeline_id'], b['pipeline_id'])
        for config in (a, b):
            self.assertEqual(config['usage']['requests'], 0)
            self.assertEqual(config['accepted_slots'], [])
            self.assertNotIn('previous_summary', config)
            self.assertNotIn('authorization', config)
            fresh.verify_config(config, REPO)
        with self.assertRaisesRegex(ValueError, 'already exists'):
            fresh.initialize(REPO, self.root/'a-plan.json', self.root/'a-approval.json')

    def test_unapproved_or_changed_plan_and_inherited_fields_refused_before_output(self):
        path = self.root/'draft.json'; approval = self.root/'approval.json'
        util.write_new_json(path, self.plan); util.write_new_json(approval, dict(authorized=False))
        with self.assertRaises(ValueError): fresh.initialize(REPO, path, approval)
        self.assertFalse((self.root/'a').exists())
        old = dict(self.plan, authorization='old permission')
        with self.assertRaises(ValueError): fresh.validate(old, REPO)
        config = self.initialize()
        util.write_json_atomic(self.root/'a-plan.json', dict(self.plan, pair_count=100))
        with self.assertRaises(ValueError): fresh.verify_config(config, REPO)

    def test_budget_model_conditions_and_output_are_explicit(self):
        for key, value in [('pair_count', 100), ('pair_concurrency', 3), ('output_root', str(REPO)),
                           ('settings', dict(self.plan['settings'], model_id='unapproved'))]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                fresh.validate(dict(self.plan, **{key:value}), REPO)
        config = self.initialize()
        for key, value in [('pair_count', 100), ('accepted_slots', [1]), ('bounds', {}),
                           ('started_at', '2099-01-01T00:00:00+00:00')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                fresh.verify_config(dict(config, **{key:value}), REPO)

    def test_observation_runtime_uses_the_existing_campaign_and_epoch_path(self):
        runtime_id='deepseek-music-observation-v1'
        old=self.plan['runtime_by_task']['MS1-CONT-A']
        lock=util.read_json(self.root/'lock.json');lock['runtime_profile_id']=runtime_id
        util.write_json_atomic(self.root/'lock.json',lock)
        self.plan['runtime_by_task']['MS1-CONT-A']=runtime_id
        self.plan['runtime_locks']={runtime_id:live_pilot.reference(self.root/'lock.json')}
        config=self.initialize('observation')
        fresh.verify_config(config,REPO)
        self.assertEqual(config['template']['runtime_by_task'],{'MS1-CONT-A':runtime_id})
        self.assertNotIn(old,config['template']['runtime_locks'])

    def setup_engine(self):
        # The production epoch relocation uses the real prepared runtime path.
        self.patch(pipeline, 'PairWatch', side_effect=lambda *a: Mock(fault=None, finish=Mock(return_value=True)))
        # Fixture has no Docker resources; cleanup is tested by the owned-close suite.
        self.patch(pipeline, 'confirm_owned_stopped')
        self.uuids = []
        self.calls = 0
        def acquire(repo, epoch, phase_path, **kw):
            phase = util.read_json(phase_path)
            self.calls += 1
            for case in phase['assignments'][0]['cases']:
                self.uuids.append(case['run_instance_id'])
                root = Path(phase['batch'])/case['run_id']
                util.write_new_json(root/'manifest.json', dict(case, duration_seconds=2,
                    started_at='2026-10-10T00:00:00+00:00', ended_at='2026-10-10T00:00:02+00:00'))
                util.append_line(root/'usage/raw/started.jsonl', dict(request_id='r'))
                util.append_line(root/'usage/raw/events.jsonl', dict(request_id='r', usage=dict(input_tokens=4, output_tokens=1)))
            return dict(reason='technical_failure' if self.calls == 1 else 'evaluation_pending')
        self.patch(pipeline.live_pilot, 'execute_owned_pair_scope', side_effect=acquire)
        def score(repo, record):
            attempt = util.read_json(record)
            phase = util.read_json(live_pilot.checked(attempt['phase']))
            quality = {case['condition']: 0 for pair in phase['assignments'] for case in pair['cases']}
            output = dict(accepted=True, slot=attempt['slot'], attempt=attempt['attempt'], quality=quality)
            util.write_new_json(Path(record).parent/'pipeline-evaluation.json', output)
            return dict(output, batch=str(Path(record).parent))
        # Relocation remains real in production. Only fixture's locked path is redirected.
        original = pipeline.create_epoch
        def epoch(repo, config, number):
            with patch.object(live_pilot, 'reference', wraps=live_pilot.reference) as ref:
                actual = ref._mock_wraps
                ref.side_effect = lambda p: actual(self.root/'lock.json' if Path(p).as_posix().endswith('/runtime/'+self.root.name+'/lock.json') else p)
                return original(repo, config, number)
        self.patch(pipeline, 'create_epoch', side_effect=epoch)

        return self.patch(pipeline, 'evaluation_process', side_effect=score)

    def test_retry_fresh_uuid_all_resources_low_quality_resume_and_no_double_dispatch(self):
        config = self.initialize()
        self.setup_engine()
        path = Path(config['root'])/'config.json'
        pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)
        self.assertEqual(len(set(self.uuids)), 4)
        events = pipeline.pair_execution.events(Path(config['root'])/'events.jsonl')
        complete = [e for e in events if e['kind']=='complete'][-1]
        self.assertEqual(complete['usage']['requests'], 4)
        self.assertEqual(complete['usage']['observed_tokens'], 20)
        receipts = [util.read_json(Path(e['record']).parent/'pipeline-acquisition.json')
                    for e in events if e['kind']=='reserved']
        self.assertEqual([r['reason'] for r in receipts], ['technical_failure', 'evaluation_pending'])
        self.assertTrue(all(len(r['runs']) == 2 for r in receipts))

        self.assertEqual(len([e for e in events if e['kind']=='accepted']), 1)
        pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)
        final = pipeline.pair_execution.events(Path(config['root'])/'events.jsonl')[-1]
        self.assertEqual(final['usage'], complete['usage'])

    def test_saved_evaluation_recovered_without_model_resend(self):
        config = self.initialize(); scorer = self.setup_engine()
        normal = scorer.side_effect
        scorer.side_effect = RuntimeError('synthetic scoring outage')
        path = Path(config['root'])/'config.json'
        with self.assertRaisesRegex(RuntimeError, 'scoring outage'):
            pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)
        scorer.side_effect = normal
        pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)

    def test_partial_failed_attempt_exhausts_cap_before_fresh_uuid_retry_dispatch(self):
        self.plan['kind']=fresh.SINGLE_KIND
        self.plan['assignments'][0]['cases']=self.plan['assignments'][0]['cases'][:1]
        self.plan['bounds'].update(max_runs=1,observed_tokens=50)
        config=self.initialize();self.setup_engine()
        original=pipeline.live_pilot.execute_owned_pair_scope.side_effect
        def fail_with_partial_usage(repo,epoch,phase_path,**kwargs):
            result=original(repo,epoch,phase_path,**kwargs)
            case=util.read_json(phase_path)['assignments'][0]['cases'][0]
            path=Path(phase_path).parent/case['run_id']/'usage/raw/events.jsonl'
            path.unlink()
            util.append_line(path,dict(request_id='r',status='transport_error',
                usage=dict(input_tokens=100,output_tokens=None)))
            return result
        pipeline.live_pilot.execute_owned_pair_scope.side_effect=fail_with_partial_usage
        path=Path(config['root'])/'config.json'
        with self.assertRaisesRegex(RuntimeError,'observed_tokens'):pipeline.run_pipeline(REPO,path)
        self.assertEqual(self.calls,1)
        events=pipeline.pair_execution.events(Path(config['root'])/'events.jsonl')
        reservations=[event for event in events if event['kind']=='reserved']
        self.assertEqual(len(reservations),1)
        saved=util.read_json(Path(reservations[0]['record']).parent/'pipeline-acquisition.json')
        self.assertEqual(saved['usage']['observed_tokens'],100)
        self.assertIsNone(saved['usage']['unknown_usage_requests'][0]['output_tokens'])
        with self.assertRaisesRegex(RuntimeError,'observed_tokens'):pipeline.run_pipeline(REPO,path)
        self.assertEqual(self.calls,1)

    def test_save_before_accept_event_recovers_and_duplicate_reservation_stops(self):
        config = self.initialize(); scorer = self.setup_engine()
        normal = scorer.side_effect
        def save_then_interrupt(*args):
            normal(*args)
            raise RuntimeError('synthetic interruption after save')
        scorer.side_effect = save_then_interrupt
        path = Path(config['root'])/'config.json'
        with self.assertRaisesRegex(RuntimeError, 'after save'):
            pipeline.run_pipeline(REPO, path)
        scorer.side_effect = AssertionError('Saved evaluation must not run twice')
        pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)
        journal = Path(config['root'])/'events.jsonl'
        reserved = next(e for e in pipeline.pair_execution.events(journal) if e['kind']=='reserved')
        pipeline.pair_execution.append(journal, reserved)
        with self.assertRaisesRegex(ValueError, 'Duplicate attempt'):
            pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)

    def test_budget_finish_is_idempotent_but_changed_usage_requires_reconcile(self):
        config = self.initialize(); budget = pipeline.Budget(config)
        usage = dict(requests=2, observed_tokens=10, accumulated_run_seconds=4, dispatched_runs=2, unknown_usage_requests=[])
        budget.finish('attempt', usage); budget.finish('attempt', usage)
        self.assertEqual(budget.total['requests'], 2)
        with self.assertRaises(ValueError): budget.finish('attempt', dict(usage, requests=3))

    def test_single_condition_uses_one_run_per_slot_and_preserves_retry_resources(self):
        self.plan['kind'] = fresh.SINGLE_KIND
        self.plan['assignments'][0]['cases'] = self.plan['assignments'][0]['cases'][:1]
        self.plan['bounds']['max_runs'] = 1
        config = self.initialize()
        self.assertEqual(config['template']['cases_per_assignment'], 1)
        self.assertEqual(config['policy']['active_model_runs'], 1)
        fresh.verify_config(config, REPO)
        self.setup_engine()
        path = Path(config['root'])/'config.json'
        pipeline.run_pipeline(REPO, path)
        self.assertEqual(len(set(self.uuids)), 2)
        events = pipeline.pair_execution.events(Path(config['root'])/'events.jsonl')
        complete = [e for e in events if e['kind'] == 'complete'][-1]
        self.assertEqual(complete['usage']['requests'], 2)
        self.assertEqual(complete['usage']['observed_tokens'], 10)
        self.assertEqual(complete['usage']['accumulated_run_seconds'], 4)
        self.assertEqual([e['quality'] for e in events if e['kind'] == 'accepted'], [{'explore': 0}])
        for event in events:
            if event['kind'] == 'reserved':
                receipt = util.read_json(Path(event['record']).parent/'pipeline-acquisition.json')
                self.assertEqual(len(receipt['runs']), 1)
                self.assertEqual(receipt['runs'][0]['condition'], 'explore')
        pipeline.run_pipeline(REPO, path)
        self.assertEqual(self.calls, 2)

    def test_single_mode_requires_explicit_kind_width_and_matching_budget(self):
        self.plan['assignments'][0]['cases'] = self.plan['assignments'][0]['cases'][:1]
        with self.assertRaises(ValueError): fresh.validate(self.plan, REPO)
        self.plan['kind'] = fresh.SINGLE_KIND
        with self.assertRaises(ValueError): fresh.validate(self.plan, REPO)
        self.plan['bounds']['max_runs'] = 1
        fresh.validate(self.plan, REPO)
        self.plan['assignments'][0]['cases'] *= 2
        with self.assertRaises(ValueError): fresh.validate(self.plan, REPO)

    def test_single_mode_cannot_mix_condition_names_across_assignments(self):
        cases = self.plan['assignments'][0]['cases']
        self.plan.update(kind=fresh.SINGLE_KIND, pair_count=2,
            assignments=[dict(pair=i, cases=[dict(case, pair=i, slot=1)])
                         for i, case in enumerate(cases, 1)])
        self.plan['bounds'].update(max_pairs=2, max_runs=2)
        with self.assertRaisesRegex(ValueError, 'one condition'):
            fresh.validate(self.plan, REPO)

    def test_single_condition_four_runs_and_one_evaluator_explicit(self):
        self.plan['kind']=fresh.SINGLE_KIND
        self.plan['assignments'][0]['cases']=self.plan['assignments'][0]['cases'][:1]
        self.plan['bounds']['max_runs']=1;self.plan['pair_concurrency']=4
        config=self.initialize()
        self.assertEqual(config['policy']['active_model_runs'],4)
        self.assertEqual(config['policy']['evaluation_workers'],1)
        for lanes in (0,5,True):
            with self.assertRaises(ValueError):fresh.validate(dict(self.plan,pair_concurrency=lanes),REPO)

    def test_resource_origin_is_measured_and_genesis_bound_without_adopted_slots(self):
        self.plan['kind']=fresh.SINGLE_KIND
        self.plan['assignments'][0]['cases']=self.plan['assignments'][0]['cases'][:1]
        self.plan['bounds']['max_runs']=1
        path=self.root/'technical/manifest.json'
        util.write_new_json(path,dict(run_instance_id='technical',stop_confirmed=True,
            network_cleanup={'confirmed':True},started_at='2026-10-01T00:00:00+00:00',
            ended_at='2026-10-01T00:00:02+00:00',duration_seconds=2))
        util.append_line(path.parent/'usage/raw/started.jsonl',dict(request_id='unknown'))
        origin=self.root/'technical-origin.json'
        util.write_new_json(origin,dict(kind='technical_acceptance_resources_v1',
            started_at='2026-10-01T00:00:00+00:00',runs=[dict(manifest=live_pilot.reference(path),
                raw_usage=util.tree_hashes(path.parent/'usage/raw'))]))
        self.plan['resource_origin']=live_pilot.reference(origin)
        config=self.initialize();fresh.verify_config(config,REPO)
        self.assertEqual(config['usage']['requests'],1);self.assertEqual(config['accepted_slots'],[])
        self.assertEqual(config['reserved_pair_attempts'],0)
        self.assertEqual(config['started_at'],'2026-10-01T00:00:00+00:00')
        util.append_line(path.parent/'usage/raw/events.jsonl',dict(request_id='unknown',usage=None))
        with self.assertRaises(ValueError):fresh.verify_config(config,REPO)


    def test_staged_partition_requires_exact_request_and_explicit_zero_postdispatch_retry(self):
        from outer.harness import staged_input
        self.plan['kind']=fresh.SINGLE_KIND;self.plan['bounds']['max_runs']=1
        case=self.plan['assignments'][0]['cases'][0]
        case.update(condition='staged-explore',run_id=run.run_id_for(case['task'],'staged-explore',1))
        self.plan['assignments'][0]['cases']=[case]
        with self.assertRaisesRegex(ValueError,'explicit request'):fresh.validate(self.plan,REPO)
        request=profiles.task_profile(REPO,case['task'],self.plan['task_revision'])['migration_request']
        at=len(request)//2;policy={'suffixes':['.cs'],'excluded_directories':['obj','bin']}
        partition=dict(kind='staged_request_partition_v1',request_sha256=util.sha256_bytes(request.encode()),
            sections=[dict(id='a',text=request[:at]),dict(id='b',text=request[at:])],initial_ids=['a'],additional_ids=['b'],
            boundary_contract=dict(transport='opencode-server-response-barrier-v1',snapshot_policy=policy,
                                   snapshot_policy_sha256=staged_input.digest(policy)))
        path=self.root/'synthetic-partition.json';util.write_new_json(path,partition)
        self.plan.update(staged_inputs={case['task']:live_pilot.reference(path)},
            staged_retry=dict(known_pre_dispatch_retries=1,after_dispatch_retries=0))
        fresh.validate(self.plan,REPO)
        self.assertEqual(self.plan['staged_inputs'],fresh.template(self.plan)['staged_inputs'])
        for retry in (dict(known_pre_dispatch_retries=2,after_dispatch_retries=0),
                      dict(known_pre_dispatch_retries=1,after_dispatch_retries=1)):
            with self.assertRaises(ValueError):fresh.validate(dict(self.plan,staged_retry=retry),REPO)
        retry=dict(retry_budget='shared_reserved_attempts',technical_failure_policy='sealed_infrastructure_failure_v1')
        fresh.validate(dict(self.plan,staged_retry=retry),REPO)
        with self.assertRaises(ValueError):
            fresh.validate(dict(self.plan,staged_retry=dict(retry,technical_failure_policy='any_failure')),REPO)
        partition['sections'][0]['text']+='unapproved requirement'
        util.write_json_atomic(path,partition)
        with self.assertRaises(ValueError):fresh.validate(self.plan,REPO)
        self.plan['staged_inputs'][case['task']]=live_pilot.reference(path)
        with self.assertRaisesRegex(ValueError,'Frozen original request'):fresh.validate(self.plan,REPO)

    def test_staged_retry_only_one_proven_preparation_failure_no_dispatch_or_unknown(self):
        config=dict(template={'staged_retry':dict(known_pre_dispatch_retries=1,after_dispatch_retries=0)},
                    attempted_slots=[],pair_count=1)
        batch=self.root/'attempt';batch.mkdir()
        binding=dict(run_id='synthetic-run',run_instance_id='synthetic-instance',condition='staged-explore',task='toy')
        phase=batch/'phase.json';util.write_new_json(phase,dict(assignments=[dict(cases=[binding])]))
        record=batch/'pipeline-attempt.json';util.write_new_json(record,dict(phase=live_pilot.reference(phase)))
        saved=dict(acquired=False,usage=dict(dispatched_runs=0,requests=0,unknown_usage_requests=[]))
        util.write_new_json(batch/'pipeline-acquisition.json',saved)
        attempts=[dict(slot=1,record=str(record))]
        self.assertEqual(([],{1}),pipeline.pending_acquisition_slots(config,attempts,set()))

        util.write_new_json(batch/'staged-preparation-failure.json',dict(kind='known_pre_dispatch_preparation_failure',binding=binding))
        self.assertEqual(([1],set()),pipeline.pending_acquisition_slots(config,attempts,set()))
        self.assertEqual(([],{1}),pipeline.pending_acquisition_slots(config,attempts*2,set()))
        config['template']['staged_retry']['known_pre_dispatch_retries']=0
        self.assertEqual(([],{1}),pipeline.pending_acquisition_slots(config,attempts,set()))
        config['template']['staged_retry']['known_pre_dispatch_retries']=1
        for key,value in [('dispatched_runs',1),('requests',1),('unknown_usage_requests',[{'status':'unknown'}])]:
            changed=copy.deepcopy(saved);changed['usage'][key]=value
            util.write_json_atomic(batch/'pipeline-acquisition.json',changed)
            self.assertEqual(([],{1}),pipeline.pending_acquisition_slots(config,attempts,set()))
        util.write_json_atomic(batch/'pipeline-acquisition.json',saved)
        util.write_new_json(batch/'synthetic-run/manifest.json',dict(started_at='synthetic-start'))
        self.assertEqual(([],{1}),pipeline.pending_acquisition_slots(config,attempts,set()))

    def test_shared_retry_uses_fresh_uuids_past_three_failures_and_retains_all_usage(self):
        from outer.harness import staged_input
        self.plan.update(kind=fresh.SINGLE_KIND,pair_concurrency=4)
        self.plan['bounds'].update(max_runs=1,max_pair_attempts=5)
        case=self.plan['assignments'][0]['cases'][0]
        case.update(condition='staged-explore',run_id=run.run_id_for(case['task'],'staged-explore',1))
        self.plan['assignments'][0]['cases']=[case]
        request=profiles.task_profile(REPO,case['task'],self.plan['task_revision'])['migration_request']
        policy={'suffixes':['.cs'],'excluded_directories':['obj','bin']};at=len(request)//2
        partition=dict(kind='staged_request_partition_v1',request_sha256=util.sha256_bytes(request.encode()),
            sections=[dict(id='a',text=request[:at]),dict(id='b',text=request[at:])],initial_ids=['a'],additional_ids=['b'],
            boundary_contract=dict(transport='opencode-server-response-barrier-v1',boundary_policy=staged_input.GROUP_BOUNDARY,
                snapshot_policy=policy,snapshot_policy_sha256=staged_input.digest(policy)))
        path=self.root/'partition.json';util.write_new_json(path,partition)
        self.plan.update(staged_inputs={case['task']:live_pilot.reference(path)},
            staged_retry=dict(technical_failure_policy='sealed_infrastructure_failure_v1',retry_budget='shared_reserved_attempts'))
        config=self.initialize();self.setup_engine()
        normal=pipeline.live_pilot.execute_owned_pair_scope.side_effect
        def acquire(repo,epoch,phase_path,**kwargs):
            result=normal(repo,epoch,phase_path,**kwargs)
            if self.calls<=4:
                batch=Path(phase_path).parent;phase=util.read_json(phase_path);binding=phase['assignments'][0]['cases'][0]
                root=batch/binding['run_id'];manifest=util.read_json(root/'manifest.json')
                util.write_json_atomic(root/'manifest.json',dict(manifest,end_reason='provider_failure',
                    stop_confirmed=True,network_cleanup={'confirmed':True}))
                events=root/'usage/raw/events.jsonl';events.unlink()
                util.append_line(events,dict(run_id=binding['run_id'],session_id=binding['run_instance_id'],
                    request_id='r',status='provider_error',http_status=503,usage=None))
                util.append_line(root/'usage/raw/failure.jsonl',dict(request_id='r'))
                journal=batch/'_control/pair-journal.jsonl'
                pipeline.pair_execution.append(journal,dict(kind='pair_reserved',assignments=[binding]))
                pipeline.pair_execution.append(journal,dict(kind='dispatch',**binding))
                return dict(reason='technical_failure')
            return result
        pipeline.live_pilot.execute_owned_pair_scope.side_effect=acquire
        config_path=Path(config['root'])/'config.json';pipeline.run_pipeline(REPO,config_path)
        self.assertEqual(self.calls,5);self.assertEqual(len(set(self.uuids)),5)
        events=pipeline.pair_execution.events(Path(config['root'])/'events.jsonl')
        final=events[-1];self.assertEqual(final['kind'],'complete')
        self.assertEqual(final['usage']['requests'],5);self.assertEqual(final['usage']['observed_tokens'],5)
        self.assertEqual(len(final['usage']['unknown_usage_requests']),4)
        self.assertEqual(final['usage']['accumulated_run_seconds'],10)
        self.assertEqual([e['quality'] for e in events if e['kind']=='accepted'],[{'staged-explore':0}])
        pipeline.run_pipeline(REPO,config_path);self.assertEqual(self.calls,5)



if __name__ == '__main__': unittest.main()
