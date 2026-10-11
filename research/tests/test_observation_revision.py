"""Opaque builds and artificial Runs only; no saved research data or model calls.

Execution replaces Docker/browser actions, but uses real preparation, source,
spec, bundle, selection, restoration, and read-only revalidation code.
"""
import copy
from pathlib import Path
import shutil
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from outer.harness import browser_cleanup, profiles, preserve, util
from research import live_pilot, observation_revision as revision, repaired_runtime
from research import saved_reassessment as saved
from research.tests import test_saved_reassessment as saved_fixtures

REPO = Path(__file__).resolve().parents[2]


class ObservationRevisionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = saved_fixtures.SavedReassessmentTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root, self.source = self.fixture.root, self.fixture.source
        self.task = profiles.task_profile(REPO, 'CU1-ENR-C', 'evaluators-20261006')
        shutil.copyfile(REPO/self.task['evaluation']['spec_path'], self.source/'evaluation-assets/requirements.json')
        condition = util.read_json(self.source/'condition.json')
        condition.update(task_id='CU1-ENR-C')
        condition['evaluation'].update(evaluation_version='education-1.1.0',
            spec_sha256=self.task['evaluation']['spec_sha256'])
        util.write_json_atomic(self.source/'condition.json', condition)
        self.reseal_source()
        evaluation = self.source/'evaluations/old/evaluation.json'
        util.write_new_json(evaluation, self.output(self.source, 'same-finite-evaluation-id', complete=True))
        (self.source/'evaluations/index.jsonl').unlink()
        util.append_line(self.source/'evaluations/index.jsonl', {'directory':'evaluations/old',
            'evaluation_sha256':util.sha256_file(evaluation), 'adopted':True, 'verdict':'pass_auto'})
        families = {}
        # Exact project source hashes are public; binary bytes here are opaque.
        for name, family in repaired_runtime.FAMILIES.items():
            bundle = self.root/name; bundle.mkdir()
            (bundle/family['assembly']).write_bytes(('synthetic revised '+name).encode())
            sources = [p for p in (REPO/'inner/evaluator'/family['project']).rglob('*')
                       if p.is_file() and p.suffix in ('.cs', '.csproj') and not {'bin','obj'} & set(p.parts)]
            receipt = {'evaluation_version':family['version'], 'sdk':'8.0.425',
                'observation_revision':revision.REVISION, 'source_commit':repaired_runtime._git(REPO,'rev-parse','HEAD'),
                'source_worktree_clean_at_start':True, 'commands':['synthetic fixture; not an actual compiler'],
                'source_files':[{'path':p.relative_to(REPO).as_posix(),'sha256':util.sha256_file(p)} for p in sources],
                'global_json_sha256':util.sha256_file(REPO/'global.json'),
                'deployable_files':[{'path':p,**entry} for p,entry in util.tree_hashes(bundle).items()]}
            util.write_new_json(bundle/'build-receipt.json', receipt)
            families[name] = {'bundle':str(bundle),'build_receipt':live_pilot.reference(bundle/'build-receipt.json'),
                'evaluator_sha256':util.sha256_file(bundle/family['assembly']),
                'images':{key:'sha256:'+digit*64 for key,digit in [('worker','1'),('evaluator','2'),('gateway','3')]},
                'spec_sha256_by_task':{task:profiles.task_profile(REPO,task,'evaluators-20261006')['evaluation']['spec_sha256'] for task in family['tasks']}}
        self.revision_path = self.root/'revision.json'
        util.write_new_json(self.revision_path, {'kind':revision.KIND,'revision_id':revision.REVISION,'families':families})
        self.browser = self.root/'browser.json'; util.write_new_json(self.browser, {'synthetic':True,'node_path':'synthetic-node'})
        self.plan_path = self.root/'plan.json'
        self.plan = {'kind':revision.PLAN_KIND,'revision':live_pilot.reference(self.revision_path),
            'browser_pin':live_pilot.reference(self.browser),'selected_runs':[self.selection(self.source)]}
        self.save_plan()
        self.addCleanup(patch.stopall)
        patch.object(revision.catalog_environment,'validate',return_value={}).start()
        self.before = saved.byte_inventory(self.source)

    def reseal_source(self):
        manifest = util.read_json(self.source/'manifest.json')
        manifest.update(condition_sha256=util.sha256_file(self.source/'condition.json'),
                        assets_sha256=util.tree_hashes(self.source/'evaluation-assets'))
        util.write_json_atomic(self.source/'manifest.json', manifest)

    def output(self, source, eid, *, complete):
        condition = util.read_json(source/'condition.json')
        return {'evaluationId':eid,'taskId':condition['task_id'],'artifactPath':'/artifact',
            'artifactSha256':util.artifact_hash(source/'frozen'), 'specSha256':condition['evaluation']['spec_sha256'],
            'evaluationVersion':condition['evaluation']['evaluation_version'],'researchStatus':'complete' if complete else 'incomplete',
            'quality':100 if complete else None,'verdict':'pass_auto' if complete else 'blocked','evaluatorFaults':[],
            'uncheckedScope':[] if complete else ['synthetic missing browser observation']}

    def selection(self, source):
        manifest = util.read_json(source/'manifest.json')
        evaluation = source/'evaluations/old/evaluation.json'
        return {'source':str(source), 'run_id':manifest['run_id'],'run_instance_id':manifest['run_instance_id'],
            'manifest_sha256':util.sha256_file(source/'manifest.json'),
            'condition_sha256':util.sha256_file(source/'condition.json'),
            'artifact_sha256':util.artifact_hash(source/'frozen'), 'evaluation':live_pilot.reference(evaluation),
            'evaluation_id':util.read_json(evaluation)['evaluationId']}

    def save_plan(self):
        util.write_json_atomic(self.plan_path, self.plan)
        self.plan_ref = live_pilot.reference(self.plan_path)

    def separate_prior(self, source, *, kind='campaign_same_version_reassessment_v1', eid=None):
        """Artificial historical files with the existing producer/table schemas."""
        row=self.selection(source);condition=util.read_json(source/'condition.json')
        identity=f'{int(row["run_instance_id"],16) ^ (1 << 127):032x}'
        holder=self.root/'prior-assessments'/row['run_instance_id']
        stage=holder/'observation' if kind=='campaign_same_version_reassessment_v1' else holder
        a=dict(kind=kind,assessment_id=identity,source_run_id=row['run_id'],
            source_run_instance_id=row['run_instance_id'],source_path_private='/old-host/original',
            source_artifact_sha256=row['artifact_sha256'],source_condition_sha256=row['condition_sha256'],
            source_spec_sha256=condition['evaluation']['spec_sha256'],
            new_spec_sha256=condition['evaluation']['spec_sha256'],
            source_evaluation_version=condition['evaluation']['evaluation_version'],
            evaluation_version=condition['evaluation']['evaluation_version'],acquisition_count_increment=0,
            source_byte_inventory=saved.byte_inventory(source))
        util.write_new_json(stage/'assessment.json',a)
        output=self.output(source,eid,complete=True);output['reviewRunInstanceId']=identity
        util.write_new_json(stage/'output/evaluation.json',output)
        result=dict(assessment_id=identity,source_run_id=row['run_id'],source_run_instance_id=row['run_instance_id'],
            adopted=True,source_unchanged=True,quality=100,kind=kind,model_calls=0,acquisition_count_increment=0,
            quality_observed_complete=True,classification='already_evaluable',
            assessment=live_pilot.reference(stage/'assessment.json'),
            derivative_after_inventory={'observation/'+n:h for n,h in saved.byte_inventory(stage).items()},
            raw_result='output/evaluation.json',operation_status='evaluation_finished',cleanup_confirmed=True,errors=[])
        util.write_new_json(holder/'result.json',result)
        row.update(evaluation=live_pilot.reference(stage/'output/evaluation.json'),evaluation_id=eid)
        adoption=[dict(run_instance_id=row['run_instance_id'],assessment_id=identity,
            artifact_sha256=row['artifact_sha256'],condition_sha256=row['condition_sha256'],
            evaluator_version=a['evaluation_version'],evaluation_original_sha256=row['evaluation']['sha256'],
            evaluation_member_sha256=row['evaluation']['sha256'],source_reference_matches_original_sha256=True,
            evaluation_member='reassessments/'+row['run_instance_id']+'/output/evaluation.json',quality=100)]
        util.write_new_json(holder/'adopted-evaluations.json',adoption)
        row['prior_assessment']={key:live_pilot.reference(path) for key,path in dict(
            assessment=stage/'assessment.json',result=holder/'result.json',adoption=holder/'adopted-evaluations.json').items()}
        return row

    def prepare(self, **changes):
        entry = util.read_json(self.revision_path)['families']['education']
        args = dict(source=self.source, destination=self.root/'assessments',
            evaluator_bundle=entry['bundle'], assembly='Education.Evaluator.dll',
            spec=self.source/'evaluation-assets/requirements.json',evaluation_version='education-1.1.0',
            repair_contract=revision.REVISION,observation_plan=self.plan_ref)
        args.update(changes)
        return saved.prepare(**args)

    def cleanup(self, directory, instance):
        state = util.read_json(directory/'browser-resources.json')
        resources = [dict(item,id='e'*64,status='removed',confirmed=True) for item in state['resources']]
        receipt = {'confirmed':True,'status':'complete','owner':state['owner'],
                   'run_instance_id':instance,'resources':resources}
        util.write_json_atomic(directory/'browser-cleanup.json', receipt)
        return receipt

    def execute(self, stage, *, complete):
        a = util.read_json(stage/'assessment.json'); instance = a['assessment_id']
        def run(*args):
            out = stage/'output/http-only'
            util.write_new_json(out/'evaluation.json',self.output(self.source,a['source_evaluation_id'],complete=False))
            (out/'results.jsonl').write_text('')
            util.write_new_json(out/'evaluator-manifest.json',{'evaluatorSha256':a['evaluator_sha256']})
            return 0,False
        def compose(*args):
            out = stage/'output'; http = out/'http-only'
            result = self.output(self.source,a['source_evaluation_id'],complete=complete)
            result.update(reviewRunInstanceId=instance,baselineEvaluationSha256=util.sha256_file(http/'evaluation.json'),
                          baselineResultsSha256=util.sha256_file(http/'results.jsonl'))
            util.write_new_json(out/'evaluation.json',result)
            (out/'results.jsonl').write_text('')
            shutil.copy2(http/'evaluator-manifest.json',out/'evaluator-manifest.json')
            browser_cleanup.register(out,instance,'container','s2-score-'+'b'*32)
            self.cleanup(out,instance)
            return 0 if complete else 2
        with patch.object(saved.evaluate,'run_evaluator',side_effect=run), \
             patch.object(saved.runtime,'image_id',side_effect=lambda image:image), \
             patch.object(saved.runtime,'docker',return_value=SimpleNamespace(stdout='8.0.425')), \
             patch.object(saved.browser_cleanup,'cleanup',side_effect=lambda p,**k:self.cleanup(p,instance)), \
             patch.object(saved.browser_review,'complete_evaluation',side_effect=compose), \
             patch.object(saved.browser_review,'coverage_complete',return_value=True), \
             patch.object(saved.browser_review,'stored_coverage_complete',return_value=True):
            # Real scoring_command is exercised; only container/browser actions are replaced.
            result = saved.execute(stage,repo=REPO)
            validation = saved.revalidate(stage,self.root/(instance+'-validation.json'),repo=REPO)
        return result,validation

    def test_old_pass_prepare_execute_revalidate_and_no_original_write(self):
        stage = self.prepare(); result,validation = self.execute(stage,complete=True)
        self.assertTrue(result['adopted'],result)
        self.assertTrue(validation['adopted'],validation)
        self.assertEqual(validation['validation_status'],'complete_observation')
        self.assertEqual(saved.byte_inventory(self.source),self.before)
        self.assertEqual(util.read_json(stage/'output/evaluation.json')['evaluationId'],self.plan['selected_runs'][0]['evaluation_id'])
        self.assertNotEqual(result['assessment_id'],self.plan['selected_runs'][0]['run_instance_id'])
        with self.assertRaises(FileExistsError):self.prepare()
        with self.assertRaises(FileExistsError):saved.execute(stage,repo=REPO)

    def test_unknown_stays_null_after_execution_and_revalidation(self):
        stage=self.prepare();result,validation=self.execute(stage,complete=False)
        self.assertFalse(result['adopted']);self.assertIsNone(result['quality'])
        self.assertEqual(validation['validation_status'],'partial_observation',validation)
        self.assertFalse(validation['adopted']);self.assertIsNone(validation['quality'])
        self.assertEqual(util.read_json(stage/'output/evaluation.json')['uncheckedScope'],['synthetic missing browser observation'])

    def test_two_old_passes_with_same_evaluation_id_have_distinct_assessments(self):
        other=self.root/'second';shutil.copytree(self.source,other)
        manifest=util.read_json(other/'manifest.json');manifest['run_instance_id']='b'*32
        util.write_json_atomic(other/'manifest.json',manifest)
        self.plan['selected_runs'].append(self.selection(other));self.save_plan()
        result=revision.prepare_selected(REPO,self.plan_ref,self.root/'selected')
        self.assertEqual(len(result['stages']),2)
        records=[util.read_json(live_pilot.checked(row['assessment'])) for row in result['stages']]
        self.assertEqual(len({r['assessment_id'] for r in records}),2)
        self.assertEqual(len({r['source_evaluation_id'] for r in records}),1)
        with self.assertRaises(FileExistsError):revision.prepare_selected(REPO,self.plan_ref,self.root/'selected')

    def test_separately_adopted_prior_result_prepare_execute_revalidate(self):
        row=self.separate_prior(self.source)
        self.plan['selected_runs']=[row];self.save_plan()
        prior_root=Path(row['prior_assessment']['result']['path']).parent
        before=saved.byte_inventory(prior_root)
        stage=self.prepare();result,validation=self.execute(stage,complete=True)
        self.assertTrue(result['adopted'],result);self.assertTrue(validation['adopted'],validation)
        self.assertIsNone(util.read_json(stage/'assessment.json')['source_evaluation_id'])
        self.assertEqual(saved.byte_inventory(self.source),self.before)
        self.assertEqual(saved.byte_inventory(prior_root),before)

    def test_saved_adapter_prior_result_with_recorded_evaluation_id(self):
        row=self.separate_prior(self.source,kind='saved_artifact_reassessment',eid='historical-id')
        self.plan['selected_runs']=[row];self.save_plan()
        self.assertTrue(self.prepare().is_dir())

    def test_external_copy_without_prior_adoption_cannot_replace_original_index(self):
        path=self.root/'unbound/output/evaluation.json'
        path.parent.mkdir(parents=True);shutil.copyfile(self.source/'evaluations/old/evaluation.json',path)
        self.plan['selected_runs'][0]['evaluation']=live_pilot.reference(path);self.save_plan()
        with self.assertRaisesRegex(ValueError,'explicit prior assessment'):self.prepare()
        self.assertEqual(saved.byte_inventory(self.source),self.before)
        self.assertFalse((self.root/'assessments').exists())

    def test_prior_missing_altered_or_foreign_evidence_is_rejected(self):
        row=self.separate_prior(self.source);self.plan['selected_runs']=[row]
        original=copy.deepcopy(row)
        a_path=Path(row['prior_assessment']['assessment']['path'])
        result_path=Path(row['prior_assessment']['result']['path'])
        adoption_path=Path(row['prior_assessment']['adoption']['path'])
        originals={path:path.read_bytes() for path in (a_path,result_path,adoption_path)}
        mutations=[('assessment','source_run_instance_id','f'*32),
                   ('assessment','source_artifact_sha256','0'*64),
                   ('assessment','source_condition_sha256','0'*64),
                   ('assessment','source_spec_sha256','0'*64),
                   ('assessment','new_spec_sha256','0'*64),
                   ('assessment','source_byte_inventory',{}),
                   ('result','adopted',False),('result','assessment_id','f'*32),
                   ('result','derivative_after_inventory',{}),('result','quality',99),
                   ('adoption','assessment_id','f'*32),('adoption','evaluation_original_sha256','0'*64),
                   ('adoption','condition_sha256','0'*64),('adoption','evaluation_member_sha256','0'*64)]
        for name,key,value in mutations:
            with self.subTest(name=name,key=key):
                row=copy.deepcopy(original);self.plan['selected_runs']=[row]
                for path,data in originals.items():path.write_bytes(data)
                path=Path(row['prior_assessment'][name]['path']);data=util.read_json(path)
                (data[0] if name=='adoption' else data)[key]=value
                util.write_json_atomic(path,data)
                # Even explicitly repinning a foreign claim cannot bypass joins.
                row['prior_assessment'][name]=live_pilot.reference(path);self.save_plan()
                with self.assertRaises(ValueError):self.prepare()
        for path,data in originals.items():path.write_bytes(data)
        self.plan['selected_runs']=[copy.deepcopy(original)];self.save_plan()
        a_path.write_bytes(originals[a_path]+b'\n')
        with self.assertRaisesRegex(ValueError,'reference changed'):self.prepare()
        a_path.unlink()
        with self.assertRaises(FileNotFoundError):self.prepare()
        self.assertFalse((self.root/'assessments').exists())
        self.assertEqual(saved.byte_inventory(self.source),self.before)

    def test_prior_adoption_requires_unique_run_and_original_evaluation_id_gate_remains(self):
        row=self.separate_prior(self.source);self.plan['selected_runs']=[row]
        path=Path(row['prior_assessment']['adoption']['path']);data=util.read_json(path)
        util.write_json_atomic(path,data+data)
        row['prior_assessment']['adoption']=live_pilot.reference(path);self.save_plan()
        with self.assertRaisesRegex(ValueError,'one original Run'):self.prepare()
        self.plan['selected_runs']=[self.selection(self.source)]
        self.plan['selected_runs'][0]['evaluation_id']=None;self.save_plan()
        with self.assertRaisesRegex(ValueError,'evaluation ID'):self.prepare()

    def test_prior_evidence_is_checked_again_before_execution_and_revalidation(self):
        row=self.separate_prior(self.source);self.plan['selected_runs']=[row];self.save_plan()
        stage=self.prepare();self.execute(stage,complete=True)
        path=Path(row['prior_assessment']['result']['path'])
        path.write_bytes(path.read_bytes()+b'\n')
        with patch.object(saved.evaluate,'run_evaluator') as runner:
            with self.assertRaisesRegex(ValueError,'reference changed'):saved.execute(stage,repo=REPO)
        runner.assert_not_called()
        receipt=saved.revalidate(stage,self.root/'changed-prior-validation.json',repo=REPO)
        self.assertEqual(receipt['validation_status'],'invalid_evidence')
        self.assertFalse(receipt['adopted']);self.assertIsNone(receipt['quality'])
        self.assertEqual(saved.byte_inventory(self.source),self.before)

    def test_outside_selection_or_duplicate_uuid_refused(self):
        other=self.root/'foreign';shutil.copytree(self.source,other)
        with self.assertRaisesRegex(ValueError,'outside'):self.prepare(source=other)
        self.plan['selected_runs'].append(self.selection(other));self.save_plan()
        with self.assertRaisesRegex(ValueError,'unique'):self.prepare()
        self.assertFalse((self.root/'assessments').exists())

    def test_empty_selection_cannot_create_an_output(self):
        self.plan['selected_runs']=[];self.save_plan()
        with self.assertRaisesRegex(ValueError,'nonempty'):
            revision.prepare_selected(REPO,self.plan_ref,self.root/'empty')
        self.assertFalse((self.root/'empty').exists())

    def test_spec_mutation_or_old_dll_revision_refused(self):
        changed=self.root/'changed-spec.json';changed.write_bytes((self.source/'evaluation-assets/requirements.json').read_bytes()+b'\n')
        with self.assertRaisesRegex(ValueError,'unchanged spec'):self.prepare(spec=changed)
        data=util.read_json(self.revision_path)
        data['families']['education']['evaluator_sha256']=repaired_runtime.FAMILIES['education']['assembly_sha256']
        util.write_json_atomic(self.revision_path,data);self.plan['revision']=live_pilot.reference(self.revision_path);self.save_plan()
        with self.assertRaisesRegex(ValueError,'Historical DLL'):self.prepare()

    def test_changed_build_or_selection_after_prepare_blocks_execute_and_revalidate(self):
        stage=self.prepare();self.execute(stage,complete=True)
        entry=util.read_json(self.revision_path)['families']['education']
        (Path(entry['bundle'])/'Education.Evaluator.dll').write_bytes(b'changed')
        with patch.object(saved.evaluate,'run_evaluator') as runner:
            with self.assertRaises(ValueError):saved.execute(stage,repo=REPO)
        runner.assert_not_called()
        validation=saved.revalidate(stage,self.root/'changed-validation.json',repo=REPO)
        self.assertEqual(validation['validation_status'],'invalid_evidence')
        self.assertFalse(validation['adopted'])

    def test_restored_members_checked_separately_and_byte_difference_holds(self):
        archive=self.root/'archive'
        ref=preserve.pack(archive,'synthetic-original',{'run':self.source})
        restored=self.root/'restored';preserve.restore(archive,ref,restored)
        source=restored/'run'
        package=preserve.verify(archive,ref['package_id'],ref['sha256'])
        receipt={'restored_to':str(source),'original_manifest_sha256':util.sha256_file(source/'manifest.json'),
            'archive':str(archive),'files':{n.removeprefix('run/'):{'reference':ref,'member':n,'original_sha256':v['sha256']}
                                        for n,v in package['files'].items()}}
        path=self.root/'restoration.json';util.write_new_json(path,receipt)
        row=self.selection(source);row['restoration']=live_pilot.reference(path)
        self.plan['selected_runs']=[row];self.save_plan()
        stage=self.prepare(source=source,spec=source/'evaluation-assets/requirements.json')
        self.assertTrue(stage.is_dir())
        (source/'frozen/app.cs').write_text('restoration discrepancy')
        with self.assertRaisesRegex(ValueError,'Restoration byte mismatch'):
            revision.selected_source(REPO,self.plan_ref,source)
        self.assertEqual(saved.byte_inventory(self.source),self.before)

    def test_fixed_200_selection_includes_all_tasks_conditions_and_old_outcomes(self):
        self.plan['selected_runs']=[]
        for number in range(200):
            task_id=('MS1-CONT-A','MS1-CONT-B','CU1-ENR-C','CU1-ENR-D')[number//50]
            condition_id=('explore','preload')[number%50//25]
            task=profiles.task_profile(REPO,task_id,'evaluators-20261006')
            source=self.root/'finite-200'/str(number);shutil.copytree(self.source,source)
            shutil.copyfile(REPO/task['evaluation']['spec_path'],source/'evaluation-assets/requirements.json')
            config=util.read_json(source/'condition.json')
            config.update(task_id=task_id,condition_id=condition_id)
            config['evaluation'].update(evaluation_version=task['evaluation']['evaluation_version'],
                                        spec_sha256=task['evaluation']['spec_sha256'])
            util.write_json_atomic(source/'condition.json',config)
            manifest=util.read_json(source/'manifest.json')
            manifest.update(run_id=f'{task_id}-{condition_id}-{number:03}',run_instance_id=f'{number:032x}',
                condition_sha256=util.sha256_file(source/'condition.json'),assets_sha256=util.tree_hashes(source/'evaluation-assets'))
            util.write_json_atomic(source/'manifest.json',manifest)
            snapshot=util.read_json(source/'snapshot.json');snapshot['run_id']=manifest['run_id']
            util.write_json_atomic(source/'snapshot.json',snapshot)
            output=self.output(source,'opaque-evaluation-'+str(number),complete=number%3!=2)
            if number%3==1:output.update(verdict='fail_critical',quality=0)
            path=source/'evaluations/old/evaluation.json';util.write_json_atomic(path,output)
            (source/'evaluations/index.jsonl').unlink()
            util.append_line(source/'evaluations/index.jsonl',{'directory':'evaluations/old',
                'evaluation_sha256':util.sha256_file(path),'verdict':output['verdict']})
            self.plan['selected_runs'].append(self.separate_prior(source) if number in (150,175) else self.selection(source))
        self.save_plan()
        result=revision.prepare_selected(REPO,self.plan_ref,self.root/'prepared-200')
        self.assertEqual(len(result['stages']),200)
        records=[util.read_json(live_pilot.checked(row['assessment'])) for row in result['stages']]
        self.assertEqual(len({r['source_run_instance_id'] for r in records}),200)
        self.assertEqual(len({r['assessment_id'] for r in records}),200)
        self.assertEqual({r['source_evaluation']['sha256'] for r in records},
                         {r['evaluation']['sha256'] for r in self.plan['selected_runs']})
        self.assertFalse(result['executed'])
        self.assertEqual(sum('prior_assessment' in r for r in self.plan['selected_runs']),2)

    def test_wrong_sdk_and_missing_plan_do_not_launch_evaluator(self):
        stage=self.prepare()
        with patch.object(saved.runtime,'image_id',side_effect=lambda image:image), \
             patch.object(saved.runtime,'docker',return_value=SimpleNamespace(stdout='9.0.1')), \
             patch.object(saved.evaluate,'run_evaluator') as runner:
            with self.assertRaisesRegex(ValueError,'pinned SDK'):saved.execute(stage,repo=REPO)
        runner.assert_not_called();self.assertFalse((stage/'output').exists())
        a=util.read_json(stage/'assessment.json');a.pop('observation_plan')
        util.write_json_atomic(stage/'assessment.json',a)
        with self.assertRaisesRegex(ValueError,'selected plan'):saved.execute(stage,repo=REPO)

    def test_output_cannot_overlap_other_selected_run_or_build(self):
        other=self.root/'other';shutil.copytree(self.source,other)
        manifest=util.read_json(other/'manifest.json');manifest['run_instance_id']='b'*32
        util.write_json_atomic(other/'manifest.json',manifest)
        self.plan['selected_runs'].append(self.selection(other));self.save_plan()
        before=saved.byte_inventory(other)
        for destination in (other/'out',self.root/'education/out'):
            with self.subTest(destination=destination),self.assertRaisesRegex(ValueError,'overlaps'):
                self.prepare(destination=destination)
            self.assertFalse(destination.exists())
        self.assertEqual(saved.byte_inventory(other),before)

    def test_revalidation_receipts_protect_entire_selection_even_when_invalid(self):
        other=self.root/'other';shutil.copytree(self.source,other)
        manifest=util.read_json(other/'manifest.json');manifest['run_instance_id']='b'*32
        util.write_json_atomic(other/'manifest.json',manifest)
        self.plan['selected_runs'].append(self.selection(other));self.save_plan()
        stage=self.prepare()
        # No result exists, so ordinary revalidation would produce invalid_evidence.
        # Neither that receipt nor even a new parent directory may touch inputs.
        roots=(self.source,other,self.root/'music',self.root/'education',stage)
        before={root:saved.byte_inventory(root) for root in roots}
        for destination in (other/'new/receipt.json',self.root/'education/new/receipt.json',
                            self.root/'music/new/receipt.json',REPO/'new-observation-receipt.json',
                            self.plan_path,self.revision_path,self.browser):
            with self.subTest(destination=destination),self.assertRaisesRegex(ValueError,'overlaps'):
                saved.revalidate(stage,destination,repo=REPO)
        self.assertEqual({root:saved.byte_inventory(root) for root in roots},before)
        self.assertFalse((REPO/'new-observation-receipt.json').exists())
        receipt=saved.revalidate(stage,self.root/'safe-validation.json',repo=REPO)
        self.assertFalse(receipt['adopted'])
        self.assertEqual(receipt['validation_status'],'invalid_evidence')

    def test_revalidation_cannot_drop_plan_or_use_changed_plan_to_write_failure(self):
        stage=self.prepare()
        self.plan['selected_runs'].append(copy.deepcopy(self.plan['selected_runs'][0]))
        util.write_json_atomic(self.plan_path,self.plan)
        target=self.root/'unsafe-failure.json'
        with self.assertRaisesRegex(ValueError,'reference changed'):
            saved.revalidate(stage,target,repo=REPO)
        a=util.read_json(stage/'assessment.json');a.pop('observation_plan')
        util.write_json_atomic(stage/'assessment.json',a)
        with self.assertRaisesRegex(ValueError,'selected plan'):
            saved.revalidate(stage,target,repo=REPO)
        self.assertFalse(target.exists())

    def test_revalidation_protects_separate_old_assessment_and_adoption_evidence(self):
        row=self.separate_prior(self.source)
        adoption=self.root/'historical-processed/adopted-evaluations.json';adoption.parent.mkdir()
        shutil.copyfile(row['prior_assessment']['adoption']['path'],adoption)
        row['prior_assessment']['adoption']=live_pilot.reference(adoption)
        self.plan['selected_runs']=[row];self.save_plan()
        stage=self.prepare()
        prior_root=Path(row['prior_assessment']['result']['path']).parent
        before=saved.byte_inventory(prior_root)
        for target in (prior_root/'new/receipt.json',Path(row['evaluation']['path']).parent/'receipt.json',
                       adoption,adoption.parent/'new-receipt.json'):
            with self.subTest(target=target),self.assertRaisesRegex(ValueError,'overlaps'):
                saved.revalidate(stage,target,repo=REPO)
        self.assertEqual(saved.byte_inventory(prior_root),before)
        self.assertEqual(saved.byte_inventory(self.source),self.before)
        self.assertEqual(list(adoption.parent.iterdir()),[adoption])


if __name__ == '__main__':unittest.main()
