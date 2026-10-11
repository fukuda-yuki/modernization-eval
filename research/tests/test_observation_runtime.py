"""Exercise new runtime preparation/admission with synthetic accepted bytes.

Only Git commit lookup and Docker version probes are replaced. Bundle, receipt,
revision, image, profile, source/spec/input and current lock checks remain real.
"""
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from outer.harness import profiles, util
from research import live_pilot, observation_revision as revision, repaired_runtime as subject
from research.tests import test_repaired_runtime as fixtures


class ObservationRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.RepairedRuntimeTests(); self.f.setUp(); self.addCleanup(self.f.doCleanups)
        f = self.f
        self.original_family = dict(f.family, assembly_sha256='f'*64)
        f.write('global.json', {'sdk':{'version':'8.0.425','rollForward':'disable'}})
        f.write('research/observation_revision.py','synthetic revision implementation')
        self.runtime_id='deepseek-music-observation-v1'
        f.write('outer/profiles/runtimes/'+self.runtime_id+'.json',dict(f.profile,
            id=self.runtime_id,artifact_namespace='music-observation-runtime-20261011'))
        f.receipt.update(observation_revision=revision.REVISION,source_worktree_clean_at_start=True,
                         global_json_sha256=util.sha256_file(f.repo/'global.json'))
        util.write_json_atomic(f.receipt_path,f.receipt)
        entry={'bundle':str(f.bundle),'build_receipt':live_pilot.reference(f.receipt_path),
            'evaluator_sha256':f.family['assembly_sha256'],'images':f.old['images'],
            'spec_sha256_by_task':{f.task_id:f.task['evaluation']['spec_sha256']}}
        self.revision_path=f.base/'revision.json'
        util.write_new_json(self.revision_path,{'kind':revision.KIND,'revision_id':revision.REVISION,
            'families':{'music':entry,'education':{}}})
        self.ref=live_pilot.reference(self.revision_path)
        self.options=dict(f.options,runtime_id=self.runtime_id,observation_revision=self.ref,
                          accepted_receipt_sha256=util.sha256_file(f.receipt_path))
        self.root=f.repo/'artifacts/runtime/music-observation-runtime-20261011'
        stack=ExitStack();self.addCleanup(stack.close)
        stack.enter_context(patch.object(subject,'FAMILIES',{'music':self.original_family}))
        stack.enter_context(patch.object(subject,'_git',side_effect=lambda repo,*args:f.commit if args==('rev-parse','HEAD') else ''))
        stack.enter_context(patch.object(subject,'_committed_files'))
        stack.enter_context(patch.object(subject.runtime,'image_id',side_effect=lambda value:value))
        stack.enter_context(patch.object(subject.runtime,'docker',side_effect=lambda *args,**kwargs:
            SimpleNamespace(stdout='8.0.425' if args[-2]=='dotnet' else '1.17.11')))

    def prepare(self):
        return subject.prepare_repaired_runtime(self.f.repo,**self.options)

    def test_prepare_and_validate_fresh_revision_runtime_preserve_old_namespace(self):
        old_root=self.f.root;old_root.mkdir(parents=True);(old_root/'protected').write_bytes(b'historical')
        before=util.tree_hashes(old_root)
        lock=self.prepare()
        self.assertTrue(subject.validate_repaired_runtime_binding(self.f.repo,lock))
        self.assertEqual(lock['repaired_runtime_binding']['observation_revision'],self.ref)
        self.assertEqual(lock['evaluator_build']['observation_revision'],self.ref)
        self.assertEqual(lock['evaluator_sha256'],self.f.family['assembly_sha256'])
        self.assertEqual(profiles.runtime_root(self.f.repo,self.f.task_id,
            profiles.read(self.f.repo,'runtimes',self.runtime_id)),self.root)
        self.assertEqual(util.tree_hashes(old_root),before)
        with self.assertRaises(FileExistsError):self.prepare()

    def test_old_namespace_or_missing_revision_refused(self):
        self.options['runtime_id']=self.original_family['runtime_id']
        with self.assertRaisesRegex(ValueError,'fresh runtime'):self.prepare()
        self.options['runtime_id']=self.runtime_id;self.options['observation_revision']=None
        with self.assertRaisesRegex(ValueError,'Unsupported'):self.prepare()

    def test_image_mismatch_and_revision_change_refused(self):
        lock=self.prepare()
        data=util.read_json(self.revision_path)
        data['families']['music']['images']['evaluator']='sha256:'+'9'*64
        util.write_json_atomic(self.revision_path,data)
        with self.assertRaisesRegex(ValueError,'reference changed'):
            subject.validate_repaired_runtime_binding(self.f.repo,lock)
        self.options['observation_revision']=live_pilot.reference(self.revision_path)
        # Retain the original output; a second location is not implicitly made.
        with self.assertRaises((ValueError,FileExistsError)):self.prepare()

    def test_historical_binary_hash_cannot_be_revision_pin(self):
        data=util.read_json(self.revision_path)
        data['families']['music']['evaluator_sha256']=self.original_family['assembly_sha256']
        util.write_json_atomic(self.revision_path,data)
        self.options['observation_revision']=live_pilot.reference(self.revision_path)
        with self.assertRaisesRegex(ValueError,'Historical DLL'):self.prepare()
        self.assertFalse(self.root.exists())


if __name__=='__main__':unittest.main()
