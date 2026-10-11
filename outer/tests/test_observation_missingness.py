"""The review's 25-token fixture through ordinary collection and admission.

Every case has a new artificial Run directory. No saved research Run is read,
rescored, or repaired. Only prepared-profile validation is stubbed for admission.
"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import support
from harness import live_usage, staged_input, util
import test_recording_repair as recording
from research import live_pilot


class ObservationMissingnessTests(unittest.TestCase):
    def fixture(self, root, mode, *, cli=True, initial=True, extra=False):
        root.mkdir(parents=True)
        raw=recording.RecordingRepairTests().fixture(root)
        for name in ('workspace','state'):(root/name).mkdir()
        (root/'workspace/Program.cs').write_text('// toy implementation')
        policy=dict(suffixes=['.cs'],excluded_directories=[])
        _,_,partition=staged_input.split_sections([dict(id='a',text='req'),dict(id='b',text='uest')],['a'],['b'])
        partition['initial_sha256']=util.sha256_bytes(b'request')
        controller=staged_input.create(root/'_controller/staged-input',run_id='R',run_instance_id='I',task='toy',
            condition='explore-observed',workspace=root/'workspace',worker_roots=[root/'inputs',root/'state'],
            initial_prompt='request',additional_prompt=None,budget_seconds=30,partition=partition,
            boundary_contract=dict(transport='opencode-server-response-barrier-v1',boundary_policy=staged_input.GROUP_BOUNDARY,
                observation_policy=staged_input.OBSERVATION_POLICY,snapshot_policy=policy,snapshot_policy_sha256=staged_input.digest(policy)))
        manifest=util.read_json(root/'manifest.json')
        context=dict(method='explore-observed',blocks=[]);util.write_json_atomic(root/'context.json',context)
        util.write_new_json(root/'condition.json',dict(runtime={'model_id':'test-model'}))
        manifest.update(task_id='toy',condition_id='explore-observed',started_at=None,duration_seconds=1,
            prompt_sha256=util.sha256_file(root/'inputs/prompt.txt'),condition_sha256=util.sha256_file(root/'condition.json'),
            context_sha256=util.sha256_file(root/'context.json'))
        util.write_json_atomic(root/'manifest.json',manifest);controller.bind_run(root)
        controller.start();controller.bind_session('native');controller.checkpoint('initial',{'native_session_id':'native'})
        part=dict(id='step',type='step-finish',messageID='assistant',sessionID='native',reason='stop')
        (root/'evidence/agent.jsonl').write_text(json.dumps(dict(type='step_finish',sessionID='native',part=part))+'\n' if cli else '')
        user=dict(info=dict(id='user',role='user',sessionID='native'),parts=[dict(
            id='input',type='text',messageID='user',sessionID='native',text='request')])
        messages=[user,dict(info=dict(id='assistant',role='assistant',sessionID='native'),parts=[part])]
        if extra:
            other=copy.deepcopy(user);other['info']['id']='extra';other['parts'][0].update(id='extra-part',messageID='extra',text='Unexpected input')
            messages.append(other)
        if mode!='unavailable':
            path=controller.root/'native-messages.json';util.write_new_json(path,messages)
            controller._append('native_message_inventory',dict(native_session_id='native',
                client_finished=mode!='incomplete',sha256=util.sha256_file(path)))
            if mode=='missing-file':path.unlink()
            if mode=='changed':path.write_text('[]')
        else:controller._append('native_message_inventory_unavailable',{'error_type':'TimeoutError'})
        if not initial:util.write_json_atomic(raw/'0.request.json',{'messages':[dict(role='user',content='Different input')]})
        stamp='2026-10-11T00:00:00+00:00'
        for name in ('started','events'):
            rows=util.read_lines(raw/(name+'.jsonl'))
            for row in rows:
                row.update(request_sha256=util.sha256_file(raw/'0.request.json'),started_at=stamp,
                    send_evidence='observed_send',http_status=200,usage_complete=name=='events')
            (raw/(name+'.jsonl')).write_text(''.join(json.dumps(row)+'\n' for row in rows))
        identity={k:controller.contract[k] for k in ('run_id','run_instance_id','delivery_id')}
        identity['contract_sha256']=controller.contract_sha256
        util.append_line(raw/'staged-input.jsonl',dict(identity,kind='planned',additional_sha256=None))
        util.append_line(raw/'staged-input.jsonl',dict(identity,kind='request_checked',request_id='0',
            request_sha256=util.sha256_file(raw/'0.request.json'),native_session_id='native',at=stamp,
            receipt={'verified':False,'locations':[], 'not_applicable':'no_input_by_design'},rejection=None,
            after_resume=False,first_post_resume=False))
        controller.stop('operator_stop')
        controller.checkpoint('final',{'stop_confirmed':True});controller.finish('operator_stop',stop_confirmed=True)
        manifest=util.read_json(root/'manifest.json')
        cleanup=dict(run_id='R',run_instance_id='I',network='toy-network',network_id='toy-id',confirmed=True,status='removed')
        manifest.update(network_cleanup=cleanup,stop_confirmed=True);util.write_json_atomic(root/'manifest.json',manifest)
        util.write_new_json(root/'runtime.json',dict(run_id='R',run_instance_id='I',stop_confirmed=True,network='toy-network',network_id='toy-id'))
        util.write_new_json(root/'evidence/network-cleanup/a-result.json',cleanup)
        util.write_new_json(root/'snapshot.json',{})
        util.write_new_json(root/'implementation-receipt.json',dict(run_id='R',run_instance_id='I',stop_confirmed=True,
            raw=util.tree_hashes(raw),snapshot_sha256=util.sha256_file(root/'snapshot.json')))
        return controller,dict(run_id='R',run_instance_id='I',input_sha256=manifest['prompt_sha256'],condition_sha256=manifest['condition_sha256'])

    def admit(self,root,binding):
        with patch.object(live_pilot.profiles,'validate_run'):
            return live_pilot.validate_owned_terminal(root,binding,observe_resources=False)

    def test_review_a_b_c_and_missing_file_keep_known_25_tokens_initial_and_final(self):
        for mode in ('complete','unavailable','incomplete','missing-file'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)/'R';controller,binding=self.fixture(root,mode)
                originals=util.tree_hashes(root/'usage/raw');ledger=util.sha256_file(controller.root/'events.jsonl')
                value=live_usage.collect(root);stage=value['staged_input']
                self.assertEqual(value['observed_tokens'],25);self.assertEqual(value['total_tokens'],25)
                self.assertTrue(value['usage_complete'],value['inventory_issues']);self.assertTrue(value['input_reached'])
                self.assertTrue(util.read_json(root/'usage/context-evidence.json')['first_request_prompt_reached'])
                self.assertTrue(stage['checkpoints']['final']['archive']);self.assertEqual(stage['issues'],[])
                self.assertIsNone(stage['additional_input_reached'])
                self.assertIs(stage['zero_additional_verified'],True if mode=='complete' else None)
                self.assertEqual(bool(value['observation_issues']),mode!='complete')
                self.assertEqual(self.admit(root,binding)['native_session_id'],'native')
                self.assertEqual(originals,util.tree_hashes(root/'usage/raw'))
                self.assertEqual(ledger,util.sha256_file(controller.root/'events.jsonl'))
                self.assertEqual(len(list((root/'usage/derivations').glob('*/binding.json'))),1)

    def test_unknown_inventory_does_not_invent_missing_initial_input_or_native_completion(self):
        for missing in ('initial','cli'):
            with self.subTest(missing=missing),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)/'R';_,binding=self.fixture(root,'unavailable',initial=missing!='initial',cli=missing!='cli')
                value=live_usage.collect(root)
                self.assertEqual(value['observed_tokens'],25)
                self.assertIsNone(value['staged_input']['zero_additional_verified'])
                if missing=='initial':self.assertFalse(value['input_reached'])
                else:self.assertIn('native_agent_completion_unverified',value['inventory_issues'])
                with self.assertRaises(ValueError):self.admit(root,binding)

    def test_positive_extra_input_and_changed_original_still_reject(self):
        for mode,extra in (('complete',True),('incomplete',True),('changed',False)):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)/'R';_,binding=self.fixture(root,mode,extra=extra)
                value=live_usage.collect(root);self.assertEqual(value['observed_tokens'],25)
                self.assertFalse(value['input_reached']);self.assertTrue(value['inventory_issues'])
                if extra:
                    self.assertIn('stage_native_external_message_mismatch',value['staged_input']['issues'])
                    self.assertIs(value['staged_input']['zero_additional_verified'],False)
                with self.assertRaises(ValueError):self.admit(root,binding)
