"""Boundary adapter fixtures: no Docker process, credentials or model calls."""
import copy
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import support
from harness import gateway, profiles, runtime, staged_input as staged, staged_runtime, util
from test_staged_input import StageFixture

REPO=Path(__file__).resolve().parents[2]

class BoundaryTests(StageFixture):
    def setup_boundary(self, ids=('call',), children=()):
        self.controller.contract['boundary_contract']['snapshot_policy']=self.policy
        self.controller.start();self.controller.bind_session('native')
        self.before=staged.snapshot(self.root/'workspace',self.policy)
        self.barrier={'barrier_id':'b','request_id':'request','request_sha256':'hash',
            'run_id':'R','run_instance_id':'instance','tool_call_ids':list(ids)}
        self.events=[{'type':'tool_use','sessionID':'native','part':{'sessionID':'native',
            'messageID':'message','callID':i,'state':{'status':'completed'}}} for i in ids]
        self.transport=Mock(children=Mock(return_value=list(children)),send=Mock(side_effect=self.ack))

    def observe(self):
        return staged_runtime.observe_boundary(self.controller,self.transport,self.barrier,self.events,self.before)

    def test_failed_tool_change_delivers_once_then_releases_with_native_ack(self):
        self.setup_boundary();self.events[0]['part']['state']['status']='error'
        (self.root/'workspace/Program.cs').write_text('synthetic change')
        after,delivered=self.observe()
        self.assertTrue(delivered);self.assertNotEqual(after,self.before)
        self.transport.send.assert_called_once()
        self.transport.release.assert_called_once_with('b','additional-native-message')
        with self.assertRaises(ValueError):self.observe()
        self.transport.send.assert_called_once()

    def test_readonly_tool_records_snapshot_and_releases_without_delivering(self):
        self.setup_boundary();after,delivered=self.observe()
        self.assertFalse(delivered);self.assertEqual(self.before,after)
        self.transport.send.assert_not_called();self.transport.release.assert_called_once_with('b')
        self.assertEqual('unchanged_boundary',self.controller.events()[-1]['kind'])

    def test_parallel_mutation_holds_first_boundary_without_later_substitution(self):
        self.setup_boundary(('a','b'));(self.root/'workspace/Program.cs').write_text('synthetic change')
        with self.assertRaisesRegex(ValueError,'ambiguous'):self.observe()
        self.transport.send.assert_not_called();self.transport.release.assert_not_called()
        self.assertEqual('boundary_unknown',self.controller.events()[-1]['kind'])
        self.assertIsNone(self.controller.status()['terminal'])
        self.controller.finish('boundary_unknown',stop_confirmed=True)

    def test_unfinished_tools_or_live_children_do_not_establish_quiescence(self):
        self.setup_boundary(('a','b'));self.events.pop()
        (self.root/'workspace/Program.cs').write_text('synthetic change')
        self.assertEqual((self.before,False),self.observe())
        self.transport.release.assert_not_called();self.transport.send.assert_not_called()
        self.events[0]['part']['callID']='call';self.barrier['tool_call_ids']=['call']
        self.transport.children.return_value=[123]
        with self.assertRaisesRegex(ValueError,'child lifetime'): self.observe()
        self.transport.send.assert_not_called()
        self.assertEqual('live_children_at_tool_terminal',self.controller.events()[-1]['reason'])

    def test_lost_ack_never_releases_model_loop(self):
        self.setup_boundary();(self.root/'workspace/Program.cs').write_text('synthetic change')
        self.transport.send.side_effect=TimeoutError()
        with self.assertRaises(RuntimeError):self.observe()
        self.transport.release.assert_not_called()
        self.assertEqual('unknown',self.controller.status()['delivery_state'])

    def test_foreign_session_never_dispatches(self):
        self.setup_boundary();self.events[0]['sessionID']='foreign'
        with self.assertRaises(ValueError):self.observe()
        self.transport.send.assert_not_called()

    def test_gateway_barrier_before_first_native_event_waits_without_dispatch(self):
        self.setup_boundary();self.events=[]
        self.assertEqual((self.before,False),self.observe())
        self.transport.send.assert_not_called();self.transport.release.assert_not_called()

    def test_docker_api_requires_owned_mount_and_sends_future_body_only_on_stdin(self):
        self.setup_boundary(); owner=Mock()
        owner._owned_container.return_value={'State':{'Running':True},
            'Mounts':[{'Destination':'/workspace','Source':str(self.root/'workspace')}]}
        state={'worker':'owned-worker','gateway':'owned-gateway','run_id':'R','run_instance_id':'instance'}
        transport=staged_runtime.DockerTransport(self.root,state,self.controller,owner,'toy-model')
        with patch.object(staged_runtime.subprocess,'run',return_value=Mock(returncode=0,stdout=b'{}')) as child:
            transport.api('worker','/session/native/message',{'noReply':True,'parts':[{'text':self.additional}]})
            args,kwargs=child.call_args
            self.assertNotIn(self.additional,str(args));self.assertNotIn(self.additional,str(kwargs['env']))
            self.assertEqual(self.additional,json.loads(kwargs['input'])['body']['parts'][0]['text'])
            self.assertLessEqual(kwargs['timeout'],10.5)
            owner._owned_container.return_value['Mounts'][0]['Source']=str(self.root/'foreign')
            with self.assertRaises(ValueError):transport.api('worker','/session')
            self.assertEqual(1,child.call_count)

class GroupBoundaryTests(StageFixture):
    setup_boundary=BoundaryTests.setup_boundary
    observe=BoundaryTests.observe
    def setup_group(self, ids=('a','b')):
        self.setup_boundary(ids,children=[123])
        self.controller.contract['boundary_contract'].update(boundary_policy=staged.GROUP_BOUNDARY,
            transport='opencode-server-response-barrier-v1')
        self.paused=False
        @contextmanager
        def freeze():
            self.paused=True
            try:yield {'paused':True,'run_instance_id':'instance','children_observed':[123],
                       'at_unix':self.controller.clock(),'paused_at_monotonic':self.controller.monotonic(),'pause_limit_seconds':5.}
            finally:self.paused=False
        self.transport.frozen=freeze
        def send(*args):
            self.assertFalse(self.paused,'Native API must resume before additional input')
            return self.ack(*args)
        self.transport.send.side_effect=send

    def test_parallel_error_and_live_children_record_group_and_continue_once(self):
        self.setup_group();self.events[1]['part']['state']['status']='error'
        (self.root/'workspace/Program.cs').write_text('synthetic group mutation')
        self.assertTrue(self.observe()[1]);self.transport.send.assert_called_once()
        boundary=next(e for e in self.controller.events() if e['kind']=='boundary_observed')
        self.assertEqual([t['tool_call_id'] for t in boundary['tools']],['a','b'])
        self.assertEqual(boundary['attribution'],'response_tool_group')
        self.assertNotIn('tool_call_id',boundary)
        self.assertEqual(boundary['fence']['worker_pause']['children_observed'],[123])
        with self.assertRaises(ValueError):self.observe()
        self.transport.send.assert_called_once()

    def test_missing_archive_is_observation_missingness_not_delivery_failure(self):
        self.setup_group();(self.root/'workspace/Program.cs').write_text('synthetic group mutation')
        self.controller.contract['artifact_collection_policy']=util.STATIC_DB_COLLECTION_POLICY
        (self.root/'workspace/example.sqlite').write_bytes(b'synthetic database')
        (self.root/'workspace/example.sqlite-wal').write_bytes(b'unsealed database sidecar')
        self.assertTrue(self.observe()[1]);self.transport.send.assert_called_once()
        receipt=util.read_json(self.controller.root/'checkpoints/before-additional/receipt.json')
        self.assertIsNone(receipt['archive']);self.assertTrue(receipt['reason'].startswith('checkpoint_failed:'))
        self.assertEqual((self.root/'workspace/example.sqlite-wal').read_bytes(),b'unsealed database sidecar')

    def test_stop_during_pause_cannot_send_after_resume(self):
        self.setup_group();(self.root/'workspace/Program.cs').write_text('synthetic group mutation')
        original=self.controller.checkpoint
        def stop(*args):
            result=original(*args);self.controller.stop('synthetic stop');return result
        with patch.object(self.controller,'checkpoint',side_effect=stop),self.assertRaisesRegex(ValueError,'Stopped'):
            self.observe()
        self.assertFalse(self.paused);self.transport.send.assert_not_called();self.transport.release.assert_not_called()

    def test_owned_pause_failure_after_command_still_unpauses(self):
        self.setup_group();owner=Mock();state={'worker':'owned','run_instance_id':'instance'}
        owner._owned_container.return_value={'State':{'Running':True,'Paused':True}}
        transport=staged_runtime.DockerTransport(self.root,state,self.controller,owner,'toy')
        transport.children=Mock(return_value=[123]);transport.owned=Mock()
        def docker(op,*args,**kwargs):
            if op=='pause':raise TimeoutError('synthetic command timeout after pause')
            owner._owned_container.return_value['State']['Paused']=False
        owner.docker.side_effect=docker
        with self.assertRaises(TimeoutError):
            with transport.frozen():self.fail('Unconfirmed pause must not observe')
        self.assertEqual([c.args[0] for c in owner.docker.call_args_list],['pause','unpause'])

    def test_pause_time_limit_releases_owned_worker_during_slow_copy(self):
        self.setup_group();owner=Mock();state={'worker':'owned','run_instance_id':'instance'}
        owner._owned_container.return_value={'State':{'Running':True,'Paused':False}}
        transport=staged_runtime.DockerTransport(self.root,state,self.controller,owner,'toy')
        transport.children=Mock(return_value=[]);transport.owned=Mock()
        import threading
        resumed=threading.Event()
        def docker(op,*args,**kwargs):
            owner._owned_container.return_value['State']['Paused']=op=='pause'
            if op=='unpause':resumed.set()
        owner.docker.side_effect=docker
        with patch.object(self.controller,'remaining',return_value=.02):
            with transport.frozen():
                self.assertTrue(resumed.wait(2))
        event=self.controller.events()[-1]
        self.assertEqual(event['kind'],'worker_resumed');self.assertEqual(event['reason'],'pause_limit')
        self.assertFalse(event['paused'])

    def test_tree_observation_failure_unpauses_without_sending_or_releasing_barrier(self):
        self.setup_group()
        with patch.object(staged,'snapshot',side_effect=OSError()),self.assertRaises(staged_runtime.ObservationPending):
            self.observe()
        self.assertFalse(self.paused);self.transport.send.assert_not_called();self.transport.release.assert_not_called()


class StreamFenceTests(unittest.TestCase):
    def test_every_chunk_split_preserves_bytes_and_withholds_terminal_marker(self):
        raw=b'data: {"choices":[]}\r\n\r\ndata: [DONE]\r\n\r\n'
        prefix=raw[:raw.index(b'data: [DONE]')]
        for at in range(len(raw)+1):
            fence=gateway.ResponseEndFence()
            visible=fence.feed(raw[:at])+fence.feed(raw[at:])+fence.feed(b'',final=True)
            self.assertTrue(fence.ended);self.assertEqual(prefix,visible);self.assertEqual(raw,visible+fence.tail)
        fence=gateway.ResponseEndFence()
        visible=b''.join(fence.feed(bytes([c])) for c in raw)+fence.feed(b'',final=True)
        self.assertEqual(prefix,visible);self.assertEqual(raw,visible+fence.tail)

class ProducerTests(unittest.TestCase):
    def test_create_keeps_exact_full_request_private_and_only_initial_prompt_in_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);repo=root/'repo'
            shutil.copytree(REPO/'outer/profiles',repo/'outer/profiles')
            shutil.copytree(REPO/'inner/spec',repo/'inner/spec')
            task=profiles.read(repo,'tasks','MS1-001');task['migration_request']='Synthetic part one.\nSynthetic part two.\n'
            util.write_json_atomic(repo/'outer/profiles/tasks/MS1-001.json',task)
            profile=profiles.read(repo,'runtimes','deepseek');profile['prompt_transport']='stdin'
            util.write_json_atomic(repo/'outer/profiles/runtimes/deepseek.json',profile)
            source=repo/'artifacts/sources'/task['start_state']['source_commit'];source.mkdir(parents=True)
            (source/'Original.cs').write_text('// original source');(source/'original.sqlite').write_bytes(b'original database')
            util.write_new_json(source.parent/(source.name+'.json'),{'files':util.tree_hashes(source)})
            prepared=repo/'artifacts/runtime/MS1-001';bundle=prepared/'evaluator';bundle.mkdir(parents=True)
            (bundle/'MusicStore.Evaluator.dll').write_bytes(b'nonexecutable synthetic fixture')
            util.write_new_json(prepared/'lock.json',{'opencode_version':'1.17.11','versions':{},'images':{},
                'controller_files':runtime.controller_files(REPO),'evaluator_files':util.tree_hashes(bundle),
                'evaluator_sha256':util.sha256_file(bundle/'MusicStore.Evaluator.dll'),'evaluator_build':{'clean_worktree':True}})
            policy={'suffixes':['.cs'],'excluded_directories':['obj']}
            partition={'kind':'staged_request_partition_v1','request_sha256':util.sha256_bytes(task['migration_request'].encode()),
                'sections':[{'id':'a','text':'Synthetic part one.\n'},{'id':'b','text':'Synthetic part two.\n'}],
                'initial_ids':['a'],'additional_ids':['b'],'boundary_contract':{
                    'transport':'opencode-server-response-barrier-v1','snapshot_policy':policy,'snapshot_policy_sha256':staged.digest(policy)}}
            with self.assertRaises(ValueError):profiles.create(repo,root/'runs','MS1-001','staged-explore',1)
            manifest=profiles.create(repo,root/'runs','MS1-001','staged-explore',1,staged_plan=partition)
            runroot=root/'runs'/manifest['run_id'];condition=profiles.validate_run(runroot)
            self.assertEqual(task['migration_request'],condition['migration_request'])
            controller=staged.Controller(Path(manifest['staged_input']['contract_path']).parent);controller.audit_mounts()
            prompt=(runroot/'inputs/prompt.txt').read_text()
            self.assertIn('Synthetic part one.',prompt);self.assertNotIn('Synthetic part two.',prompt)
            self.assertEqual([],util.read_json(runroot/'context.json')['blocks'])
            self.assertEqual(util.tree_hashes(source),util.tree_hashes(runroot/'inputs/legacy-source'))
            self.assertEqual('Synthetic part two.\n',(controller.root/'additional.txt').read_text())
            # Runtime owns the existing Run. Only the gateway sees /stage;
            # the native server is loopback-only with the original pinned model.
            process=Mock();process.poll.return_value=0;process.returncode=0
            fake_docker=Mock(return_value=Mock(stdout='network-id',returncode=0))
            with patch.object(runtime,'docker') as refused:
                with self.assertRaisesRegex(ValueError,'approved campaign'):
                    runtime.start(REPO,root/'runs',manifest['run_id'])
                refused.assert_not_called()
            def execute(*args):
                args[2].bind_session('native');return 0,'completed'
            with patch.object(runtime,'docker',fake_docker),patch.object(runtime,'image_id'),\
                    patch.object(runtime,'isolation_probe'),patch.object(runtime,'_gateway_credential',return_value='synthetic'),\
                    patch.object(runtime.subprocess,'Popen',return_value=process),patch.object(runtime,'stop_owned',return_value=True),\
                    patch.object(runtime,'record_cleanup',return_value={}),patch('harness.live_usage.update_manifest_evidence'),\
                    patch('harness.live_usage.collect'),patch.object(staged_runtime,'execute',side_effect=execute):
                # Empty image map is adequate for producer tests; add synthetic
                # names only for the mocked Docker command-construction fixture.
                condition['runtime_lock']['images']={'worker':'synthetic-worker','gateway':'synthetic-gateway'}
                with patch.object(profiles,'validate_run',return_value=condition),patch.object(staged_runtime,'validate_authorization'):
                    runtime.start(REPO,root/'runs',manifest['run_id'])
            calls=[list(c.args) for c in fake_docker.call_args_list if c.args[0]=='create']
            worker=next(c for c in calls if 'synthetic-worker' in c);proxy=next(c for c in calls if 'synthetic-gateway' in c)
            self.assertIn('serve',worker);self.assertIn('127.0.0.1',worker)
            self.assertFalse(any('target=/stage' in str(c) for c in worker))
            self.assertTrue(any('target=/stage' in str(c) for c in proxy));self.assertIn('--staged-input',proxy)
            self.assertIsNotNone(controller.status()['terminal'])
            self.assertEqual(1,len([e for e in controller.events() if e['kind']=='started']))

class GatewayBarrierTests(StageFixture):
    def setUp(self):
        super().setUp()
        self.proxy=gateway.Gateway(('127.0.0.1',0),self.root/'usage/raw','R','toy-model','synthetic',
            session_id='instance',expected_prompt=self.root/'inputs/prompt.txt',staged_input=self.controller.root)
        self.addCleanup(self.proxy.server_close)
        self.proxy.stage['boundary_contract']['transport']='opencode-server-response-barrier-v1'
        self.raw=b'data: {"choices":[{"delta":{"tool_calls":[{"id":"call"}]}}]}\n\ndata: [DONE]\n\n'
        self.event={'request_id':'request','request_sha256':'hash'}

    def hold(self):
        import threading,time
        self.faults=[]
        def target():
            try:self.proxy.hold_response_end(self.event,self.raw)
            except Exception as error:self.faults.append(error)
        thread=threading.Thread(target=target);thread.start()
        self.addCleanup(lambda:(self.proxy.stage_barrier_event.set(),thread.join(2)))
        for _ in range(100):
            barrier=self.proxy.inspect_stage_barrier()
            if barrier:return thread,barrier
            time.sleep(.01)
        self.fail('Fixture barrier failed to hold')

    def test_readonly_release_preserves_ids_and_duplicate_or_foreign_release_fails(self):
        thread,barrier=self.hold();self.assertTrue(thread.is_alive())
        with self.assertRaises(ValueError):self.proxy.release_stage_barrier('foreign')
        self.proxy.release_stage_barrier(barrier['barrier_id']);thread.join(2)
        self.assertFalse(thread.is_alive());self.assertEqual([],self.faults)
        with self.assertRaises(ValueError):self.proxy.release_stage_barrier(barrier['barrier_id'])
        rows=util.read_lines(self.root/'usage/raw/response-barriers.jsonl')
        self.assertEqual(['held','released'],[r['kind'] for r in rows])
        self.assertEqual(['call'],rows[0]['tool_call_ids'])

    def test_arm_requires_owned_live_barrier_and_ack_before_release(self):
        payload={k:self.proxy.stage[k] for k in ('run_id','run_instance_id','delivery_id','contract_sha256','additional_sha256')}
        payload.update(native_session_id='native',barrier_id='missing')
        with self.assertRaises(ValueError):self.proxy.arm_stage(payload)
        thread,barrier=self.hold();payload['barrier_id']=barrier['barrier_id']
        self.proxy.arm_stage(payload)
        with self.assertRaises(ValueError):self.proxy.release_stage_barrier(barrier['barrier_id'])
        self.proxy.release_stage_barrier(barrier['barrier_id'],'native-message');thread.join(2)
        self.assertEqual([],self.faults)

    def test_stop_wakes_boundary_waiter_and_disallows_native_release(self):
        thread,barrier=self.hold();self.proxy.close_admission();thread.join(2)
        self.assertFalse(thread.is_alive());self.assertIsInstance(self.faults[0],TimeoutError)
        with self.assertRaises(ValueError):self.proxy.release_stage_barrier(barrier['barrier_id'])
        self.assertEqual(['held'],[r['kind'] for r in util.read_lines(self.root/'usage/raw/response-barriers.jsonl')])

class CheckpointTests(StageFixture):
    def test_both_boundaries_restore_after_overwrite_and_deletion_and_final_is_distinct(self):
        from harness import preserve,staged_artifacts
        self.controller.start();self.controller.bind_session('native')
        self.controller.checkpoint('initial',{'native_session_id':'native'})
        workspace=self.root/'workspace'
        (workspace/'Program.cs').write_bytes(b'// first\r\n')
        (workspace/'deleted.txt').write_bytes(b'original bytes')
        before=self.controller.checkpoint('before-additional',{'request_id':'before-request','tool_call_id':'before-tool'})
        (workspace/'Program.cs').write_bytes(b'// after additional\r\n')
        (workspace/'deleted.txt').unlink();(workspace/'added.txt').write_bytes(b'new bytes')
        after=self.controller.checkpoint('after-additional',{'request_id':'after-request','tool_call_id':'after-tool'})
        (workspace/'Program.cs').unlink();(workspace/'added.txt').write_bytes(b'final overwrite')
        self.controller.checkpoint('final',{'reason':'completed'})
        self.controller.finish('completed',stop_confirmed=True)
        archived=self.controller.root/'checkpoint-archive'
        for label,receipt,expected in [('before',before,b'// first\r\n'),('after',after,b'// after additional\r\n')]:
            target=Path(self.tmp.name)/('restored-'+label)
            preserve.restore(archived,receipt['archive'],target)
            self.assertEqual(expected,(target/'workspace/Program.cs').read_bytes())
            if label=='before':self.assertEqual(b'original bytes',(target/'workspace/deleted.txt').read_bytes())
            else:
                self.assertFalse((target/'workspace/deleted.txt').exists())
                tree=util.read_json(target/'tree.json')
                self.assertEqual(['deleted.txt'],tree['difference']['removed'])
                self.assertEqual(['added.txt'],tree['difference']['added'])
                self.assertEqual(['Program.cs'],tree['difference']['changed'])
        summary=staged_artifacts.summary(self.controller,'completed')
        self.assertNotEqual(summary['after-additional']['archive'],summary['final']['archive'])
        self.assertEqual({'initial','before-additional','after-additional','final'},set(summary))

    def test_unreached_post_checkpoint_remains_null_even_when_final_has_changes(self):
        self.controller.start();self.controller.bind_session('native')
        (self.root/'workspace/Program.cs').write_text('// final only')
        self.controller.checkpoint('final',{'reason':'early_completion'})
        result=self.controller.finish('early_completion',stop_confirmed=True)['terminal']['checkpoints']
        self.assertIsNone(result['after-additional']['archive'])
        self.assertEqual('additional_input_not_acknowledged',result['after-additional']['reason'])
        self.assertIsNotNone(result['final']['archive'])
        self.assertEqual('boundary_never_reached',result['before-additional']['reason'])

    def test_unsealed_db_checkpoint_holds_without_mutating_or_hiding_database(self):
        self.controller.contract['artifact_collection_policy']=util.STATIC_DB_COLLECTION_POLICY
        self.controller.start();self.controller.bind_session('native')
        (self.root/'workspace/data.sqlite').write_bytes(b'synthetic database')
        (self.root/'workspace/data.sqlite-wal').write_bytes(b'nonempty unsealed sidecar')
        original=util.tree_hashes(self.root/'workspace')
        with self.assertRaisesRegex(ValueError,'checkpoint unavailable'):self.controller.checkpoint('before-additional',{})
        self.assertEqual(original,util.tree_hashes(self.root/'workspace'))
        result=self.controller.finish('checkpoint_failed',stop_confirmed=True)['terminal']['checkpoints']
        self.assertIsNone(result['before-additional']['archive'])
        self.assertEqual('checkpoint_failed:ValueError',result['before-additional']['reason'])

    def test_receipt_tamper_is_not_replaced_by_a_final_snapshot(self):
        from harness import staged_artifacts
        self.controller.start();self.controller.bind_session('native')
        self.controller.checkpoint('before-additional',{})
        receipt=self.controller.root/'checkpoints/before-additional/receipt.json'
        util.write_json_atomic(receipt,{**util.read_json(receipt),'bytes':999})
        with self.assertRaisesRegex(ValueError,'receipt changed'):staged_artifacts.summary(self.controller,'completed')

class ActivationTests(unittest.TestCase):
    def test_matching_approved_dispatch_and_partition_are_required_at_run_entry(self):
        from research import campaign_initialization,live_pilot,pair_execution
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary);root=base/'epoch/data/pair-1/R'
            for name in ('workspace','state','inputs'): (root/name).mkdir(parents=True)
            condition=profiles.resolve(REPO,'MS1-001','explore')
            condition['intervention']['method']='staged-explore';condition['migration_request']='Synthetic initial.\nSynthetic later.\n'
            policy={'suffixes':['.cs'],'excluded_directories':['obj']}
            plan=dict(kind='staged_request_partition_v1',request_sha256=util.sha256_bytes(condition['migration_request'].encode()),
                sections=[dict(id='a',text='Synthetic initial.\n'),dict(id='b',text='Synthetic later.\n')],
                initial_ids=['a'],additional_ids=['b'],boundary_contract=dict(transport='opencode-server-response-barrier-v1',
                    snapshot_policy=policy,snapshot_policy_sha256=staged.digest(policy)))
            initial,later,partition=staged.request_partition(condition['migration_request'],plan)
            prompt,_=profiles.prepare_prompt({**condition,'migration_request':initial},root/'inputs')
            partition.update(request_initial_sha256=partition['initial_sha256'],initial_sha256=util.sha256_bytes(prompt.encode()))
            controller=staged.create(root/'_controller/staged-input',run_id='R',run_instance_id='instance',task='MS1-001',
                condition='staged-explore',workspace=root/'workspace',worker_roots=[root/'inputs',root/'state'],
                initial_prompt=prompt,additional_prompt=later,budget_seconds=30,boundary_contract=plan['boundary_contract'],partition=partition)
            util.write_new_json(base/'partition.json',plan);util.write_new_json(base/'config.json',{'root':str(base/'campaign')})
            util.write_new_json(base/'epoch.json',{})
            assignment=dict(run_id='R',run_instance_id='instance',task='MS1-001',condition='staged-explore',pair=1)
            epoch=dict(campaign_config=live_pilot.reference(base/'config.json'),cases_per_assignment=1,
                assignments=[dict(cases=[assignment])],batch=str(base/'epoch/data'),
                runtime_by_task={'MS1-001':condition['runtime']['id']},settings={'model_id':condition['runtime']['model_id']},
                staged_inputs={'MS1-001':live_pilot.reference(base/'partition.json')})
            manifest=dict(run_id='R',run_instance_id='instance',task_id='MS1-001',assignment=assignment,prompt_sha256='input',condition_sha256='condition',
                acquisition=dict(epoch=live_pilot.reference(base/'epoch.json'),campaign_config=epoch['campaign_config']))
            dispatch=dict(run_id='R',run_instance_id='instance',input_sha256='input',condition_sha256='condition')
            # Upstream approval/genesis/epoch checking is exercised by the
            # campaign suite; here vary the final native dispatch binding.
            with patch.object(campaign_initialization,'verify_epoch',return_value=epoch),patch.object(pair_execution,'state',return_value={'dispatch':{'R':dispatch}}):
                staged_runtime.validate_authorization(REPO,root,manifest,controller,condition)
                dispatch['run_instance_id']='foreign'
                with self.assertRaisesRegex(ValueError,'admitted dispatch'):staged_runtime.validate_authorization(REPO,root,manifest,controller,condition)
                dispatch['run_instance_id']='instance'
                (base/'campaign').mkdir();(base/'campaign/STOP').write_text('synthetic stop')
                with self.assertRaisesRegex(ValueError,'open campaign'):staged_runtime.validate_authorization(REPO,root,manifest,controller,condition)
                (base/'campaign/STOP').unlink()
                controller.contract['boundary_contract']={**controller.contract['boundary_contract'],'transport':'unapproved'}
                with self.assertRaisesRegex(ValueError,'transport/partition'):staged_runtime.validate_authorization(REPO,root,manifest,controller,condition)
