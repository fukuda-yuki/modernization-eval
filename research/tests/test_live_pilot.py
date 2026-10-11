"""Finite, provider-free controls tests; no test represents a real Go call."""
import copy
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch, Mock
import subprocess
import sys
import threading
import time
import os

from outer.harness import run, util
from research import live_pilot as pilot


class PlanBoundaries(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(pilot.__file__).resolve().parents[2])
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'; self.repo.mkdir()
        self.protected = self.root / 'original'; self.protected.mkdir()
        self.plan = dict(schema_version=1, kind=pilot.KIND, phase_id=uuid.uuid4().hex,
            batch=str(self.root / 'pilot'), cohort='runs/live-pilot', source_commit='a'*40,
            source_pins={}, task_revision='evaluators-20261006',
            runtime_by_task={'MS1-CONT-A':'deepseek-music-repaired-v1',
                             'CU1-ENR-C':'deepseek-education-repaired-v1'},
            settings=dict(model_id='deepseek-v4.1-flash', provider='opencode-go',
                          use_balance=False, paid_fallback=False),
            bounds=dict(pilot.BOUNDS), pair_concurrency=2, require_fixed_instances=True,
            protected_roots=[str(self.protected)], thresholds=dict(pilot.DEFAULT_THRESHOLDS))
        for name in pilot.PIN_FILES:
            path = self.repo / name; path.parent.mkdir(parents=True, exist_ok=True)
            if name.startswith('research/protocols/'):
                path.write_bytes((Path(pilot.__file__).resolve().parents[1]/name).read_bytes())
            else: path.write_text(name, encoding='utf-8')
            self.plan['source_pins'][name] = util.sha256_file(path)
        self.plan['runtime_locks']={}
        for task,rid in self.plan['runtime_by_task'].items():
            family='music' if task.startswith('MS1-') else 'education'
            namespace='repaired-'+family
            runtimepath=self.repo/'outer/profiles/runtimes'/(rid+'.json')
            util.write_json_atomic(runtimepath,dict(id=rid,artifact_namespace=namespace))
            taskpath=self.repo/'outer/profiles/task-revisions/evaluators-20261006'/(task+'.json')
            util.write_json_atomic(taskpath,dict(task_id=task,canonical_task_id=task,revision_id='evaluators-20261006',
                evaluation=dict(evaluation_version='fixture-'+family,project=family+'.csproj')))
            lockpath=self.repo/'artifacts/runtime'/namespace/'lock.json'
            util.write_new_json(lockpath,dict(runtime_profile_id=rid,task_profile_revision='evaluators-20261006',
                evaluator_version='fixture-'+family,evaluator_project=family+'.csproj'))
            self.plan['runtime_locks'][rid]=pilot.reference(lockpath)
            for path in (runtimepath,taskpath):
                self.plan['source_pins'][path.relative_to(self.repo).as_posix()]=util.sha256_file(path)
        # Full accepted-runtime validator is a build/Git applicability leaf.
        # Namespace, lock hash and ledger coupling above remain real in fixtures.
        patcher=patch('research.repaired_runtime.validate_repaired_runtime_binding')
        self.runtime_leaf=patcher.start(); self.addCleanup(patcher.stop)
        self.actual_observer_collector=pilot.observer_terminal_receipt
        patcher=patch.object(pilot,'observer_terminal_receipt',side_effect=lambda m,p,f,ack:
            dict(observer_ack_verified=ack,evidence_files={},observer_exit_code=0 if ack else None))
        self.observer_leaf=patcher.start(); self.addCleanup(patcher.stop)
        for key, name in [('resource_monitor','wave_resource_monitor.py'),
                          ('resource_probe','wave_resource_probe.py'), ('browser_pin','browser.json')]:
            path = self.root / name; path.write_text('{}', encoding='utf-8')
            self.plan[key] = pilot.reference(path)
        self.plan['assignments'] = []
        for number, task in enumerate(('MS1-CONT-A','CU1-ENR-C'), 1):
            self.plan['assignments'].append({'pair':number, 'cases':[
                dict(pair=number,slot=slot,task=task,condition=arm,attempt=1,
                     run_id=run.run_id_for(task,arm,1),run_instance_id=uuid.uuid4().hex)
                for slot,arm in enumerate(('explore','preload'),1)]})

    def reject(self, change):
        plan = copy.deepcopy(self.plan); change(plan)
        before = util.tree_hashes(self.root)
        with self.assertRaises((ValueError, KeyError)):
            pilot.validate_plan(plan, self.repo)
        self.assertEqual(before, util.tree_hashes(self.root))
        self.assertFalse(Path(self.plan['batch']).exists())

    def test_fixed_plan_and_unsafe_bounds_rejected_without_writes(self):
        pilot.validate_plan(self.plan, self.repo)
        self.reject(lambda p:p['bounds'].update(max_runs=True))
        self.reject(lambda p:p['bounds'].update(max_runs=200))
        self.reject(lambda p:p.update(kind='nonmodel_resource_supervisor_fixture_v1'))
        self.reject(lambda p:p['settings'].update(paid_fallback=True))
        self.reject(lambda p:p['settings'].update(use_balance=True))
        self.reject(lambda p:p['thresholds'].update(cpu_percent_max=float('nan')))
        self.reject(lambda p:p['thresholds'].update(host_memory_available_min_bytes=1))

    def test_identity_and_path_collisions_rejected_without_writes(self):
        self.reject(lambda p:p['assignments'][1]['cases'][0].update(
            run_instance_id=p['assignments'][0]['cases'][0]['run_instance_id']))
        self.reject(lambda p:p['assignments'][0]['cases'][0].update(run_id='../original'))
        self.reject(lambda p:p.update(batch=str(self.protected / 'new')))
        self.reject(lambda p:p.update(batch=str(self.repo / 'runs/new')))
        self.reject(lambda p:p['assignments'][0]['cases'][0].update(task='CU1-ENR-C'))
        from research.acquisition_readiness import protected_instance_ids
        self.reject(lambda p:p['assignments'][0]['cases'][0].update(run_instance_id=next(iter(protected_instance_ids()))))

    @unittest.skipUnless(os.name=='nt','Windows native prefix/junction fixture')
    def test_native_prefix_normalized_and_actual_junction_rejected_before_mutations(self):
        from research.resource_supervisor import safe_environment
        base=Path(self.plan['batch']); base.mkdir(); link=base/'_control'
        self.assertEqual(pilot.safe_path('\\\\?\\'+str(base)),base.resolve())
        result=subprocess.run(['cmd','/c','mklink','/J',str(link),str(self.protected)],
            env=safe_environment(),capture_output=True,timeout=5)
        self.assertEqual(result.returncode,0); self.assertTrue(link.is_junction())
        before=util.tree_hashes(self.protected)
        with self.assertRaisesRegex(ValueError,'link/junction'): pilot.validate_plan(self.plan,self.repo)
        self.assertEqual(before,util.tree_hashes(self.protected))
        self.assertFalse((self.protected/'allocation.json').exists())

    def test_source_and_monitor_pins_are_mandatory(self):
        self.reject(lambda p:p['source_pins'].pop(pilot.PIN_FILES[0]))
        self.reject(lambda p:p['resource_probe'].update(sha256='f'*64))
        self.reject(lambda p:p['source_pins'].update({pilot.PIN_FILES[0]:'f'*64}))

    def test_runtime_lock_pin_namespace_ledger_and_full_validator_required(self):
        self.reject(lambda p:p['runtime_locks'].pop('deepseek-music-repaired-v1'))
        self.reject(lambda p:p['runtime_locks']['deepseek-music-repaired-v1'].update(sha256='f'*64))
        foreign=self.root/'foreign-lock.json'
        util.write_new_json(foreign,util.read_json(pilot.checked(self.plan['runtime_locks']['deepseek-music-repaired-v1'])))
        self.reject(lambda p:p['runtime_locks'].update({'deepseek-music-repaired-v1':pilot.reference(foreign)}))
        self.runtime_leaf.side_effect=ValueError('Accepted DLL applicability mismatch')
        self.reject(lambda p:None)

    def freeze(self):
        path=self.root/'plan.json'; util.write_new_json(path,self.plan)
        pilot.create(self.repo,path)
        return path

    def test_allocation_is_exclusive_and_derived_phase_cannot_change(self):
        path=self.freeze(); before=util.tree_hashes(self.root)
        with self.assertRaises(FileExistsError): pilot.create(self.repo,path)
        self.assertEqual(before,util.tree_hashes(self.root))
        phase=util.read_json(Path(self.plan['batch'])/'pair-1/phase.json')
        pilot.validate_observer_phase(phase,self.repo)
        for values in ({'runtime':'deepseek'},{'cohort':'runs/old'}, {'owner_kind':'nonmodel'}):
            with self.assertRaises(ValueError): pilot.validate_observer_phase({**phase,**values},self.repo)

    def watch(self):
        Path(self.plan['batch']).mkdir()
        watch=pilot.PilotWatch(self.plan,'a'*64)
        binding=self.plan['assignments'][0]['cases'][0]
        batch=Path(self.plan['batch'])/'pair-1'
        util.write_new_json(batch/binding['run_id']/'manifest.json',binding)
        watch.register(batch,binding)
        return watch,batch,binding

    def event(self,binding,index):
        return dict(run_id=binding['run_id'],session_id=binding['run_instance_id'],request_id=str(index),
            model_id='deepseek-v4.1-flash',response_model_id='deepseek-v4.1-flash',status='completed',
            usage_complete=True,send_evidence='observed_send',http_status=200,
            usage={'input_tokens':10,'output_tokens':3})

    def test_observed_request_token_identity_thresholds_and_partial_send(self):
        watch,batch,b=self.watch(); raw=batch/b['run_id']/'usage/raw'
        e=self.event(b,1); util.append_line(raw/'started.jsonl',e)
        self.assertEqual(watch.counters()['http_inflight'],1)
        util.append_line(raw/'events.jsonl',e)
        self.assertEqual(watch.counters()['observed_tokens'],13)
        with patch.dict(watch.plan['bounds'],{'request_count':1}):
            with self.assertRaisesRegex(RuntimeError,'requests'): watch.check()
        with patch.dict(watch.plan['bounds'],{'observed_tokens':13}):
            with self.assertRaisesRegex(RuntimeError,'observed_tokens'): watch.check()
        util.append_line(raw/'failure.jsonl',{'run_id':b['run_id']})
        with self.assertRaisesRegex(ValueError,'Provider'): watch.check()

    def test_unknown_or_foreign_send_evidence_cannot_pass(self):
        watch,batch,b=self.watch(); raw=batch/b['run_id']/'usage/raw'
        util.append_line(raw/'started.jsonl',{**self.event(b,1),'session_id':'foreign'})
        with self.assertRaises(ValueError): watch.check()

    def test_time_disk_and_observer_faults_latch_and_preserve_unsent(self):
        watch,batch,b=self.watch()
        watch.run_clocks[b['run_id']]=time.monotonic()-1801
        with self.assertRaisesRegex(RuntimeError,'longest_active'): watch.check()
        watch.run_clocks.clear(); watch.started=time.monotonic()-9001
        with self.assertRaisesRegex(RuntimeError,'wall_seconds'): watch.check()
        watch.started=time.monotonic()
        with patch.object(pilot.shutil,'disk_usage',return_value=Mock(free=0)):
            with self.assertRaisesRegex(RuntimeError,'Disk'): watch.check()
        monitor=Mock(); monitor.snapshot.return_value={'host_healthy':True,'resource_healthy':False}
        watch.monitors=[monitor]
        with self.assertRaisesRegex(RuntimeError,'observer'): watch.check()
        watch.monitors=[]; watch.latch('deliberate')
        self.assertTrue((Path(self.plan['batch'])/'_control/dispatch-stop.json').exists())
        self.assertTrue((batch/b['run_id']/'stop-request.json').exists())
        self.assertFalse((Path(self.plan['batch'])/'pair-2').exists())
        with self.assertRaises(RuntimeError): watch.check()

    def test_latch_publishes_stop_before_owned_wait_and_foreign_untouched(self):
        watch,batch,b=self.watch(); root=batch/b['run_id']
        util.write_new_json(root/'runtime.json',b)
        called=[]
        def stop(path):
            self.assertTrue((Path(self.plan['batch'])/'_control/dispatch-stop.json').exists())
            self.assertTrue((path/'stop-request.json').exists())
            called.append(path); return {'stop_confirmed':True}
        with patch.object(pilot.runtime,'request_stop',side_effect=stop): watch.latch('test')
        self.assertEqual(called,[root])
        watch.fault=None; util.write_json_atomic(root/'runtime.json',{**b,'run_instance_id':'f'*32})
        # A second receipt is exclusive; preserve it, while foreign resource is never stopped.
        with patch.object(pilot.runtime,'request_stop') as stop:
            with self.assertRaises(FileExistsError): watch.latch('foreign')
            stop.assert_not_called()

    def controls(self):
        """Mock provider/runtime/scorer leaves, retain real journal and controls."""
        owner=self
        class Monitor:
            stopped=0; admitted=[]; ack=True
            def __init__(self,phase,directory,scope):
                self.phase_path=phase; self.directory=directory/'test'; self.session='fixture-session'; self.scope=scope
                self.digest=util.sha256_file(phase); self.generation=1
            def start(self): pass
            def enroll_ready(self,scope): self.assert_scope=scope
            def admit(self,binding):
                if binding not in self.scope()['assignments']: raise ValueError('Unenrolled')
                self.admitted.append(binding['run_id'])
            def snapshot(self): return dict(host_healthy=True,resource_healthy=True,**pilot.DEFAULT_THRESHOLDS)
            def stop(self):
                Monitor.stopped+=1
                if Monitor.ack:
                    util.write_json_atomic(self.directory/'shutdown-ack.json',dict(session=self.session,
                        phase_sha256=self.digest,generation=self.generation,owned_resources_resolved=True,
                        monitor_stop_confirmed=True,fault_latched=False))
                return Monitor.ack and set(self.scope()['dispatched'])=={c['run_id'] for c in self.scope()['assignments']}
        def create(repo,batch,task,arm,attempt,runtime_id,**kw):
            b=kw['assignment']; root=batch/b['run_id']
            manifest={**b,'schema_version':1,'prompt_sha256':'b'*64,'condition_sha256':'c'*64,
                      'stop_confirmed':True,'assignment':b}
            util.write_new_json(root/'manifest.json',manifest); return manifest
        def condition(root):
            task='MS1-CONT-A' if root.parent.name=='pair-1' else 'CU1-ENR-C'
            return dict(task_profile_revision='evaluators-20261006',runtime=dict(id=owner.plan['runtime_by_task'][task],
                model_id='deepseek-v4.1-flash',timeout_seconds=1800,provider_timeout_seconds=600))
        return Monitor,create,condition

    def test_real_pair_journal_two_workers_overlap_and_product_fail_is_observation(self):
        path=self.freeze(); base=Path(self.plan['batch']); batch=base/'pair-1'
        watch=pilot.PilotWatch(self.plan,util.sha256_file(path))
        barrier=threading.Barrier(2); children=[]
        Monitor,create,condition=self.controls()
        def implement(repo,batch,rid):
            barrier.wait(3)
            child=subprocess.run([sys.executable,'-B','-c','print("finite nonmodel controls child")'],
                capture_output=True,timeout=5,env=__import__('research.resource_supervisor',fromlist=['safe_environment']).safe_environment())
            children.append(child.returncode)
            m=util.read_json(batch/rid/'manifest.json')
            return dict(run_id=rid,run_instance_id=m['run_instance_id'],stop_confirmed=True)
        with patch('research.resource_supervisor.ProcessMonitor',Monitor),\
             patch.object(pilot.profiles,'create',side_effect=create),\
             patch.object(pilot.profiles,'validate_run',side_effect=condition),\
             patch.object(pilot.next_phase_execution,'guarded_implementation',side_effect=implement),\
             patch.object(pilot,'validate_owned_terminal',side_effect=lambda root,b:dict(native_session_id=b['run_instance_id'])),\
             patch.object(pilot.next_phase_execution,'browser_postprocess',return_value=lambda *args:
                 {'scoring':{'state':'scored'},'quality':0,'verdict':'fail'}):
            result=pilot.execute_owned_pair_scope(self.repo,path,batch/'phase.json',budget_watch=watch)
        self.assertEqual(children,[0,0]); self.assertEqual(result['reason'],'pair_publication_restore_cleanup_required')
        current=pilot.pair_execution.state(batch/'_control/pair-journal.jsonl')
        self.assertEqual(len(current['dispatch']),2); self.assertEqual(len(current['results']),2)
        self.assertEqual(current['gates'],{})
        before=util.tree_hashes(base)
        # Existing immutable observer terminal receipt prevents silently replacing first evidence.
        with patch('research.resource_supervisor.ProcessMonitor',Monitor):
            with self.assertRaises(FileExistsError):
                pilot.execute_owned_pair_scope(self.repo,path,batch/'phase.json',budget_watch=watch)
        self.assertEqual(before,util.tree_hashes(base))

    def test_provider_fault_pauses_next_pair_and_second_execute_does_not_replay(self):
        path=self.freeze(); base=Path(self.plan['batch'])
        approval=self.root/'approval.json'; util.write_new_json(approval,dict(plan_sha256=util.sha256_file(path),
            authorized=True,approved_by='user',authorization_reference='finite fixture only'))
        Monitor,create,condition=self.controls(); calls=[]
        def failure(repo,batch,rid):
            calls.append(rid); raise RuntimeError('synthetic provider fault without HTTP')
        with patch('research.resource_supervisor.ProcessMonitor',Monitor),\
             patch.object(pilot.profiles,'create',side_effect=create),\
             patch.object(pilot.profiles,'validate_run',side_effect=condition),\
             patch('research.next_phase.git',side_effect=lambda repo,*args:self.plan['source_commit'] if args==('rev-parse','HEAD') else ''),\
             patch('research.catalog_environment.validate'),\
             patch.object(pilot.next_phase_execution,'guarded_implementation',side_effect=failure),\
             patch.object(pilot.next_phase_execution,'browser_postprocess',return_value=lambda *args:{}):
            result=pilot.execute(self.repo,path,approval)
            self.assertFalse(result['pilot_operational_complete'])
            self.assertIsNotNone(result['fault']); self.assertTrue(result['watcher_shutdown_verified'])
            self.assertEqual(len(calls),2)
            self.assertFalse((base/'pair-2/_control/pair-journal.jsonl').exists())
            before=util.tree_hashes(base)
            with self.assertRaises(FileExistsError): pilot.execute(self.repo,path,approval)
            self.assertEqual(before,util.tree_hashes(base)); self.assertEqual(len(calls),2)

    def test_single_campaign_scope_dispatches_one_run_and_defers_scoring(self):
        self.plan.update(kind=pilot.MAIN_KIND, cases_per_assignment=1)
        self.plan['assignments'] = self.plan['assignments'][:1]
        self.plan['assignments'][0]['cases'] = self.plan['assignments'][0]['cases'][:1]
        path = self.root/'single-plan.json'; util.write_new_json(path, self.plan)
        phase = pilot.observer_phase(self.plan, pilot.reference(path), 1)
        batch = Path(phase['batch']); util.write_new_json(batch/'phase.json', phase)
        watch = pilot.PilotWatch(self.plan, util.sha256_file(path))
        Monitor, create, condition = self.controls()
        calls = []
        def implement(repo, batch, rid):
            calls.append(rid)
            manifest = util.read_json(batch/rid/'manifest.json')
            return dict(run_id=rid, run_instance_id=manifest['run_instance_id'], stop_confirmed=True)
        # Approval/genesis validation has its own campaign tests. Exercise the
        # actual observer derivation, admission, pair journal and session counts.
        with patch.object(pilot, 'owner_plan', return_value=self.plan), \
             patch('research.resource_supervisor.ProcessMonitor', Monitor), \
             patch.object(pilot.profiles, 'create', side_effect=create), \
             patch.object(pilot.profiles, 'validate_run', side_effect=condition), \
             patch.object(pilot.next_phase_execution, 'guarded_implementation', side_effect=implement), \
             patch.object(pilot, 'validate_owned_terminal', side_effect=lambda root,b:dict(native_session_id=b['run_instance_id'])), \
             patch.object(pilot.next_phase_execution, 'browser_postprocess', return_value=lambda *a:self.fail('No intermediate scoring')):
            result = pilot.execute_owned_pair_scope(self.repo, path, batch/'phase.json',
                budget_watch=watch, defer_postprocess=True)
        self.assertEqual(result['reason'], 'evaluation_pending')
        self.assertEqual(len(calls), 1)
        current = pilot.pair_execution.state(batch/'_control/pair-journal.jsonl')
        self.assertEqual(len(current['dispatch']), 1)
        self.assertFalse(current['results'])
        self.assertEqual(len(watch.native_sessions), 1)
        self.assertIsNone(watch.fault)

    def test_unconfirmed_observer_ack_blocks_pilot_complete(self):
        path=self.freeze(); base=Path(self.plan['batch']); batch=base/'pair-1'
        watch=pilot.PilotWatch(self.plan,util.sha256_file(path)); Monitor,create,condition=self.controls()
        Monitor.ack=False
        def implement(repo,batch,rid):
            m=util.read_json(batch/rid/'manifest.json')
            return dict(run_id=rid,run_instance_id=m['run_instance_id'],stop_confirmed=True)
        with patch('research.resource_supervisor.ProcessMonitor',Monitor),\
             patch.object(pilot.profiles,'create',side_effect=create),\
             patch.object(pilot.profiles,'validate_run',side_effect=condition),\
             patch.object(pilot.next_phase_execution,'guarded_implementation',side_effect=implement),\
             patch.object(pilot,'validate_owned_terminal',side_effect=lambda root,b:dict(native_session_id=b['run_instance_id'])),\
             patch.object(pilot.next_phase_execution,'browser_postprocess',return_value=lambda *args:
                 {'scoring':{'state':'scored'},'quality':100}):
            pilot.execute_owned_pair_scope(self.repo,path,batch/'phase.json',budget_watch=watch)
        self.assertEqual(watch.fault['reason'],'observer_shutdown_unconfirmed')
        self.assertFalse(util.read_json(batch/'observer-terminal.json')['observer_ack_verified'])

    def terminal(self):
        watch,batch,b=self.watch(); root=batch/b['run_id']
        b={**b,'input_sha256':'b'*64,'condition_sha256':'c'*64}
        state={**b,'worker':'s2-worker-'+'1'*16,'gateway':'s2-gateway-'+'2'*16,
               'network':'s2-net-'+'3'*16,'network_id':'network-fixture','stop_confirmed':True}
        cleanup={**{k:b[k] for k in ('run_id','run_instance_id')},'network':state['network'],
                 'network_id':state['network_id'],'confirmed':True,'status':'removed'}
        manifest={**b,'schema_version':2,'stop_confirmed':True,'prompt_sha256':'b'*64,
                  'condition_sha256':'c'*64,'duration_seconds':4,'network_cleanup':cleanup}
        util.write_json_atomic(root/'manifest.json',manifest)
        util.write_new_json(root/'runtime.json',state); util.write_new_json(root/'condition.json',dict(runtime=dict(model_id='deepseek-v4.1-flash')))
        util.write_new_json(root/'evidence/network-cleanup/a-result.json',cleanup)
        util.write_new_json(root/'usage/normalized.json',dict(usage_complete=True,input_reached=True,run_instance_id=b['run_instance_id']))
        util.write_new_json(root/'usage/provenance.json',dict(run_id=b['run_id'],run_instance_id=b['run_instance_id'],
            expected_sessions=[b['run_instance_id']],inventory_complete=True,
            native_agent_session_ids=['ses_finite_fixture'],native_step_count=1))
        util.append_line(root/'evidence/agent.jsonl',dict(sessionID='ses_finite_fixture',type='step_finish'))
        util.append_line(root/'usage/raw/started.jsonl',self.event(b,1))
        util.append_line(root/'usage/raw/events.jsonl',self.event(b,1))
        util.write_new_json(root/'snapshot.json',{})
        util.write_new_json(root/'implementation-receipt.json',dict(run_id=b['run_id'],run_instance_id=b['run_instance_id'],
            stop_confirmed=True,raw=util.tree_hashes(root/'usage/raw'),snapshot_sha256=util.sha256_file(root/'snapshot.json')))
        return watch,batch,b,root

    def test_terminal_cleanup_session_hashes_and_readonly_docker_absence(self):
        watch,batch,b,root=self.terminal(); before=util.tree_hashes(root)
        def docker(*args,**kw):
            self.assertIn(args[0],('ps','network'))
            self.assertNotIn('rm',args); self.assertNotIn('stop',args)
            return subprocess.CompletedProcess(args,0,'','')
        with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker',side_effect=docker):
            proof=pilot.validate_owned_terminal(root,b)
        self.assertEqual(proof['resource_observations']['network'],'observed_absent')
        self.assertEqual(before,util.tree_hashes(root))
        for field,value in [('run_instance_id','f'*32),('stop_confirmed',False)]:
            path=root/'runtime.json'; original=util.read_json(path); util.write_json_atomic(path,{**original,field:value})
            with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker') as calls:
                with self.assertRaises(ValueError): pilot.validate_owned_terminal(root,b)
                calls.assert_not_called()
            util.write_json_atomic(path,original)
        path=root/'usage/provenance.json'; original=util.read_json(path)
        util.write_json_atomic(path,{**original,'native_agent_session_ids':['ses_one','ses_two']})
        with patch.object(pilot.profiles,'validate_run'):
            with self.assertRaisesRegex(ValueError,'Native/gateway'): pilot.validate_owned_terminal(root,b)
        util.write_json_atomic(path,original)
        with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker',return_value=subprocess.CompletedProcess([],1,'','')):
            with self.assertRaisesRegex(RuntimeError,'unobservable'): pilot.validate_owned_terminal(root,b)

    def test_staged_early_completion_is_retained_without_claiming_additional_delivery(self):
        _,_,binding,root=self.terminal()
        from outer.harness import staged_input
        for name in ('inputs','state','workspace'):(root/name).mkdir(exist_ok=True)
        initial,additional,partition=staged_input.split_sections(
            [dict(id='a',text='toy first'),dict(id='b',text='toy later')],['a'],['b'])
        controller=staged_input.create(root/'_controller/staged-input',run_id=binding['run_id'],
            run_instance_id=binding['run_instance_id'],task='toy',condition='staged-explore',
            workspace=root/'workspace',worker_roots=[root/'inputs',root/'state'],
            initial_prompt=initial,additional_prompt=additional,budget_seconds=30,
            boundary_contract={'snapshot_policy_sha256':'synthetic'},partition=partition)
        manifest=util.read_json(root/'manifest.json');manifest['staged_input']={
            'contract_path':str(controller.root/'contract.json'),'contract_sha256':controller.contract_sha256}
        util.write_json_atomic(root/'manifest.json',manifest)
        normalized=util.read_json(root/'usage/normalized.json')
        normalized.update(input_reached=False,staged_input=dict(issues=[],additional_input_reached=False,
            terminal=dict(stop_confirmed=True,reason='completed')))
        util.write_json_atomic(root/'usage/normalized.json',normalized)
        util.write_new_json(root/'usage/context-evidence.json',dict(first_request_prompt_reached=True))
        with patch.object(pilot.profiles,'validate_run'):
            proof=pilot.validate_owned_terminal(root,binding,observe_resources=False)
        self.assertEqual('ses_finite_fixture',proof['native_session_id'])
        self.assertFalse(util.read_json(root/'usage/normalized.json')['input_reached'])
        normalized['staged_input']['issues']=['synthetic_missing_evidence']
        util.write_json_atomic(root/'usage/normalized.json',normalized)
        with patch.object(pilot.profiles,'validate_run'):
            with self.assertRaises(ValueError):pilot.validate_owned_terminal(root,binding,observe_resources=False)

    def test_missing_native_input_and_tampered_raw_or_snapshot_rejected(self):
        watch,batch,b,root=self.terminal()
        for relative,change in [
            ('usage/normalized.json',lambda d:{**d,'input_reached':False}),
            ('snapshot.json',lambda d:{'tampered':True}),
            ('implementation-receipt.json',lambda d:{**d,'raw':{}})]:
            path=root/relative; original=path.read_bytes(); util.write_json_atomic(path,change(util.read_json(path)))
            with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker') as calls:
                with self.assertRaises(ValueError): pilot.validate_owned_terminal(root,b)
                calls.assert_not_called()
            path.write_bytes(original)
        native=root/'evidence/agent.jsonl'; native.unlink()
        with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker') as calls:
            with self.assertRaises(ValueError): pilot.validate_owned_terminal(root,b)
            calls.assert_not_called()

    def test_restore_history_counters_and_wall_origin_never_treat_unknown_as_complete(self):
        watch,batch,b,root=self.terminal()
        replay=pilot.PilotWatch(self.plan,'a'*64,wall_started_at='2026-01-01T00:00:00+00:00')
        with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker',return_value=subprocess.CompletedProcess([],0,'','')):
            replay.restore_completed(batch,[b])
        self.assertEqual(replay.counters()['accumulated_run_seconds'],4)
        with self.assertRaisesRegex(RuntimeError,'wall_seconds'): replay.check()
        before=util.tree_hashes(root)
        with patch.object(pilot.runtime,'request_stop') as stopped:
            replay.latch('finite new-scope fault')
            stopped.assert_not_called()
        self.assertEqual(before,util.tree_hashes(root))
        self.assertFalse((root/'stop-request.json').exists())
        m=util.read_json(root/'manifest.json'); util.write_json_atomic(root/'manifest.json',{**m,'stop_confirmed':False})
        replay=pilot.PilotWatch(self.plan,'a'*64)
        with patch.object(pilot.profiles,'validate_run'):
            with self.assertRaises(ValueError): replay.restore_completed(batch,[b])
        self.assertEqual(replay.bindings,[])

    def test_active_stop_targets_exclude_restored_history_inventory(self):
        watch,batch,b,root=self.terminal()
        replay=pilot.PilotWatch(self.plan,'a'*64)
        with patch.object(pilot.profiles,'validate_run'),patch.object(pilot.runtime,'docker',return_value=subprocess.CompletedProcess([],0,'','')):
            replay.restore_completed(batch,[b])
        active=self.plan['assignments'][0]['cases'][1]; active_root=batch/active['run_id']
        util.write_new_json(active_root/'manifest.json',active); util.write_new_json(active_root/'runtime.json',active)
        replay.register(batch,active); before=util.tree_hashes(root); targets=[]
        with patch.object(pilot.runtime,'request_stop',side_effect=lambda p:targets.append(p) or {'stop_confirmed':True}):
            replay.latch('active fault')
        self.assertEqual(targets,[active_root]); self.assertEqual(before,util.tree_hashes(root))
        self.assertTrue((active_root/'stop-request.json').exists())

    def test_real_supervisor_subprocess_new_kind_session_generation_and_ack(self):
        """Actual local observer process; synthetic probe, no Docker or Go."""
        from research.resource_supervisor import ProcessMonitor
        real_repo=Path(pilot.__file__).resolve().parents[1]
        self.plan['source_pins']={name:util.sha256_file(real_repo/name) for name in pilot.PIN_FILES}
        module=self.root/'wave_resource_monitor.py'
        module.write_text('''import time
from pathlib import Path
from outer.harness import util
CADENCE_SECONDS, STALE_SECONDS = 10,30
def probe_fn(roots): return {}
class ResourceMonitor:
    def __init__(self,directory,roots,*,probe_fn=probe_fn):
        self.probe_fn=probe_fn; self.last_tick=time.monotonic(); self.faults=set()
        self.directory=Path(directory); self.sequence=0
    def start(self): pass
    def stop(self): return True
    def set_wave(self,*args): pass
    def diagnostics(self): return {}
    def snapshot(self):
        self.last_tick=time.monotonic(); sample=self.probe_fn([]); self.sequence+=1
        path=self.directory/(str(self.sequence)+'.json'); util.write_new_json(path,{'sample':sample})
        return dict(host_healthy=True,resource_healthy=True,sampled_at=str(time.monotonic()),
            evidence_files={str(path):util.sha256_file(path)},escalation_healthy=False,host_memory_available_min_bytes=2000000000,
            docker_memory_available_min_bytes=2000000000,disk_free_min_bytes=10000000000,
            cpu_percent_max=1,http_inflight_max=0)
''',encoding='utf-8')
        self.plan['resource_monitor']=pilot.reference(module)
        path=self.root/'actual-observer-plan.json'; util.write_new_json(path,self.plan)
        with patch.object(pilot,'validate_runtime_locks'):
            pilot.create(real_repo,path)
        phase=Path(self.plan['batch'])/'pair-1/phase.json'
        monitor=ProcessMonitor(phase,self.root/'processes',lambda:dict(pairs=[],assignments=[],dispatched=[]))
        # Child uses the real newkind/pin/scope/session controller with only the
        # accepted-build leaf replaced; this is an explicit synthetic fixture.
        original_popen=subprocess.Popen
        def fixture_child(args,**kwargs):
            args=[args[0],'-B','-X','utf8','-c',
                'from research import live_pilot,resource_supervisor; live_pilot.validate_runtime_locks=lambda *a:None; '
                'raise SystemExit(resource_supervisor.main())',*args[6:]]
            return original_popen(args,**kwargs)
        try:
            with patch('research.resource_supervisor.subprocess.Popen',side_effect=fixture_child):
                monitor.start()
            monitor.enroll_ready(dict(pairs=[],assignments=[]))
            self.assertIsNone(monitor.process.poll()); self.assertTrue(monitor.snapshot()['resource_healthy'])
            self.assertTrue(monitor.stop()); self.assertEqual(monitor.process.returncode,0)
            ack=util.read_json(monitor.directory/'shutdown-ack.json')
            self.assertEqual(ack['session'],monitor.session)
            self.assertEqual(ack['phase_sha256'],util.sha256_file(phase))
            # Temporarily restore the actual collector, not the controls mock.
            terminal=self.actual_observer_collector(monitor,path,phase,True)
            self.assertTrue(terminal['observer_ack_verified']); self.assertEqual(terminal['observer_exit_code'],0)
            self.assertIn('config.json',terminal['evidence_files'])
            self.assertIn('shutdown-ack.json',terminal['evidence_files'])
            for name,digest in terminal['evidence_files'].items():
                self.assertEqual(util.sha256_file(pilot.safe_path(monitor.directory)/name),digest)
            util.write_json_atomic(monitor.directory/'shutdown-ack.json',{**ack,'session':'foreign'})
            self.assertFalse(self.actual_observer_collector(monitor,path,phase,True)['observer_ack_verified'])
        finally:
            if monitor.process is not None and monitor.process.poll() is None:
                # Exact child handle from this fixture only, never another PID.
                monitor.process.terminate(); monitor.process.wait(timeout=5)
            if hasattr(monitor,'log'): monitor.log.close()


if __name__ == '__main__': unittest.main()
