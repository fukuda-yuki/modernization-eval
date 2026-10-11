"""Public toy requests only; no model, original Run or private prompt fixtures."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import support
from harness import gateway, live_usage, profiles, staged_input as staged, staged_runtime, util
import test_staged_runtime as boundary_fixtures
from test_staged_input import StageFixture


class PresentationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        for name in ('inputs','workspace','state'): (self.root/name).mkdir()
        self.plan=dict(kind='staged_request_partition_v1',request_sha256=util.sha256_bytes(b'first\nlater\n'),
            sections=[dict(id='a',text='first\n'),dict(id='b',text='later\n')],initial_ids=['a'],additional_ids=['b'],
            boundary_contract=dict(transport='opencode-server-response-barrier-v1',boundary_policy=staged.GROUP_BOUNDARY,
                observation_policy=staged.OBSERVATION_POLICY,snapshot_policy={'suffixes':['.cs'],'excluded_directories':[]}))
        self.plan['boundary_contract']['snapshot_policy_sha256']=staged.digest(self.plan['boundary_contract']['snapshot_policy'])

    def controller(self, control=True):
        initial,additional,partition=staged.request_partition('first\nlater\n',self.plan)
        if control: initial+=additional;additional=None
        partition['initial_sha256']=util.sha256_bytes(initial.encode())
        return staged.create(self.root/'controller',run_id='toy',run_instance_id='uuid',task='toy',
            condition='explore-observed' if control else 'staged-explore',workspace=self.root/'workspace',
            worker_roots=[self.root/'inputs',self.root/'state'],initial_prompt=initial,additional_prompt=additional,
            budget_seconds=30,boundary_contract=self.plan['boundary_contract'],partition=partition)

    def test_control_has_no_packet_and_rejects_send_and_gateway_arm(self):
        controller=self.controller();controller.audit_mounts()
        self.assertEqual('observed_presentation_v1',controller.contract['kind'])
        self.assertEqual(0,controller.contract['planned_additional_count'])
        self.assertFalse((controller.root/'additional.txt').exists())
        self.assertIsNone(controller.contract['additional_sha256'])
        with self.assertRaisesRegex(ValueError,'cannot dispatch'):controller.dispatch(Mock())
        g=gateway.Gateway(('127.0.0.1',0),self.root/'raw','toy','toy-model','synthetic-key',
            session_id='uuid',expected_prompt=controller.root/'initial.txt',staged_input=controller.root)
        self.addCleanup(g.server_close)
        with self.assertRaises(ValueError):g.arm_stage({})
        self.assertIsNone(g.stage['text'])

    def test_gateway_zero_action_identity_repeat_and_fake_ack_are_rejected(self):
        controller=self.controller()
        g=gateway.Gateway(('127.0.0.1',0),self.root/'raw','toy','toy-model','synthetic-key',
            session_id='uuid',expected_prompt=controller.root/'initial.txt',staged_input=controller.root)
        self.addCleanup(g.server_close)
        g.stage_barrier=dict(barrier_id='b',request_id='request',phase='before_resume')
        action=dict(run_id='toy',run_instance_id='uuid',contract_sha256=controller.contract_sha256,
            native_session_id='native',barrier_id='b',request_id='request',checkpoint_receipt_sha256='a'*64,
            action='no_input_by_design',native_message_id=None)
        for key,value in [('run_instance_id','foreign'),('barrier_id','other'),('action','additional_input_acknowledged'),
                          ('native_message_id','fabricated')]:
            with self.assertRaises(ValueError):g.release_stage_barrier('b',resume_action={**action,key:value})
        with self.assertRaises(ValueError):g.release_stage_barrier('b',native_message_id='fabricated')
        with self.assertRaises(ValueError):g.release_stage_barrier('b',capture_complete=True)
        g.release_stage_barrier('b',resume_action=action)
        self.assertEqual(g.stage_resume_action,action)
        self.assertIsNone(g.stage_native_message_id)
        with self.assertRaises(ValueError):g.release_stage_barrier('b',resume_action=action)
        g.stage_barrier=dict(barrier_id='next',request_id='post',phase='after_resume')
        g.stage_barrier_event.clear()
        with self.assertRaises(ValueError):g.release_stage_barrier('next',resume_action={**action,'barrier_id':'next','request_id':'post'})
        g.release_stage_barrier('next',capture_complete=True)
        self.assertTrue(g.stage_capture_complete)

    def test_native_inventory_proves_early_completion_without_fabricating_cli_events(self):
        controller=self.controller();controller.start();controller.bind_session('native')
        (self.root/'evidence').mkdir();(self.root/'evidence/agent.jsonl').write_text('')
        part=dict(id='step',type='step-finish',sessionID='native',messageID='assistant',reason='stop')
        messages=[dict(info=dict(id='assistant',sessionID='native',role='assistant'),parts=[part])]
        path=controller.root/'native-messages.json';util.write_new_json(path,messages)
        controller._append('native_message_inventory',dict(native_session_id='native',client_finished=True,sha256=util.sha256_file(path)))
        manifest=dict(staged_input=dict(contract_path=str(controller.root/'contract.json'),contract_sha256=controller.contract_sha256))
        proof=live_usage.native_completion(self.root,manifest)
        self.assertEqual(proof['source'],'native_message_inventory');self.assertEqual(proof['sessions'],['native'])
        self.assertEqual(proof['steps'],1);self.assertEqual(proof['errors'],[])
        self.assertEqual((self.root/'evidence/agent.jsonl').read_bytes(),b'')
        messages[0]['info']['sessionID']='foreign';util.write_json_atomic(path,messages)
        self.assertIn('native_message_inventory_unverified',live_usage.native_completion(self.root,manifest)['errors'])

    def test_missing_native_inventory_remains_unknown(self):
        controller=self.controller();controller.start();controller.bind_session('native')
        (self.root/'evidence').mkdir();(self.root/'evidence/agent.jsonl').write_text('')
        manifest=dict(staged_input=dict(contract_path=str(controller.root/'contract.json'),contract_sha256=controller.contract_sha256))
        self.assertIn('native_message_inventory_unverified',live_usage.native_completion(self.root,manifest)['errors'])

    def test_full_initial_is_byte_identical_to_explore_without_preload(self):
        repo=Path(__file__).resolve().parents[2]
        for task in ('MS1-CONT-A','MS1-CONT-B','CU1-ENR-C','CU1-ENR-D'):
            condition=profiles.resolve(repo,task,'explore-observed',
                'deepseek-music-observation-v1' if task.startswith('MS1') else 'deepseek-education-observation-v1',
                task_revision='evaluators-20261006')
            # Real profile/collection settings, exclusively toy request text.
            condition['migration_request']='first\nlater\n'
            prompt,context,additional,_=staged.prepare_presentation(condition,self.plan,self.root/'inputs')
            original=copy.deepcopy(condition);original['intervention']['method']='explore'
            self.assertEqual(prompt,profiles.prepare_prompt(original,self.root/'inputs')[0])
            self.assertIsNone(additional);self.assertEqual(context['blocks'],[])
            self.assertEqual('explore-observed',context['method'])

    def test_old_empty_partition_and_unknown_observation_policy_still_refused(self):
        for change in ({'additional_ids':[]},{'initial_ids':['a','b']}):
            with self.assertRaises(ValueError):staged.request_partition('first\nlater\n',{**self.plan,**change})
        for policy in ('unknown',None):
            plan=copy.deepcopy(self.plan);plan['boundary_contract']['observation_policy']=policy
            with self.assertRaises(ValueError):staged.request_partition('first\nlater\n',plan)
        controller=self.controller()
        (controller.root/'additional.txt').write_text('')
        with self.assertRaises(ValueError):staged.Controller(controller.root)


class ObservedBoundaryTests(StageFixture):
    setup_boundary=boundary_fixtures.BoundaryTests.setup_boundary
    setup_group=boundary_fixtures.GroupBoundaryTests.setup_group
    observe=boundary_fixtures.BoundaryTests.observe
    def observed_group(self,control):
        self.setup_group()
        self.controller.contract['boundary_contract']['observation_policy']=staged.OBSERVATION_POLICY
        self.controller.contract.update(presentation_mode='full_initial_no_additional' if control else 'staged_once')
        self.barrier.update(phase='before_resume',first_post_resume_request_id=None)

    def test_zero_input_action_once_before_release_then_first_change_ends_observation(self):
        self.observed_group(True)
        (self.root/'workspace/Program.cs').write_text('first')
        before,changed=self.observe();self.assertTrue(changed)
        self.transport.send.assert_not_called()
        action=self.controller.events()[-1]['action']
        self.assertEqual('no_input_by_design',action['action']);self.assertIsNone(action['native_message_id'])
        self.transport.release.assert_called_once_with('b',None,resume_action=action)
        with self.assertRaises(ValueError):self.controller.resume_action()
        barrier={**self.barrier,'barrier_id':'after','phase':'after_resume','first_post_resume_request_id':'post'}
        (self.root/'workspace/Program.cs').write_text('second')
        self.assertTrue(staged_runtime.observe_boundary(self.controller,self.transport,barrier,self.events,before,after_delivery=True)[1])
        self.transport.release.assert_called_with('after',None,capture_complete=True)
        self.transport.send.assert_not_called()
        self.assertFalse(any(e['kind'] in ('acknowledged','send_intent') for e in self.controller.events()))

    def test_staged_action_requires_real_ack_and_same_checkpoint_meaning(self):
        self.observed_group(False);(self.root/'workspace/Program.cs').write_text('first')
        self.assertTrue(self.observe()[1]);self.transport.send.assert_called_once()
        action=self.controller.events()[-1]['action']
        self.assertEqual('additional_input_acknowledged',action['action'])
        self.assertEqual('additional-native-message',action['native_message_id'])
        self.assertTrue((self.controller.root/'checkpoints/before-resume/receipt.json').exists())

    def test_control_stop_before_action_cannot_resume_or_send(self):
        self.observed_group(True);(self.root/'workspace/Program.cs').write_text('first')
        original=self.controller.checkpoint
        def stop(*args):
            receipt=original(*args);self.controller.stop('fixture STOP');return receipt
        self.controller.checkpoint=stop
        with self.assertRaises(ValueError):self.observe()
        self.transport.release.assert_not_called();self.transport.send.assert_not_called()
