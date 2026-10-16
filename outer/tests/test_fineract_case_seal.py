import json
from pathlib import Path
import tempfile
import unittest

from outer.fineract.java_api import seal_cases
from outer.harness import util


class CaseSealTests(unittest.TestCase):
    def test_seal_binds_exact_run_and_partitions_tasks_without_replacing_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);harness=root/'harness';harness.mkdir();(harness/'private').mkdir()
            (harness/'private/spec.json').write_text('{}')
            (harness/'snapshot.json').write_text('{}')
            util.write_new_json(harness/'result.json',{'expected_suites':['A','B'],'success':True})
            original=(harness/'result.json').read_bytes()
            run=root/'run';run.mkdir()
            util.write_new_json(run/'started.json',{'harness_result_sha256':util.sha256_file(harness/'result.json')})
            report={'junit':{'status':'passed','cases':[
                {'suite':'A','case':'one','status':'passed'},{'suite':'B','case':'two','status':'passed'}]},
                'environment_failure':False,'state':{'ExitCode':0}}
            util.write_new_json(run/'result.json',report)
            mapping=root/'tasks.json'
            mapping.write_text(json.dumps({'task-a':{'expected_suites':['A'],'selectors':['A']}}))
            with self.assertRaises(ValueError):seal_cases(harness,run,mapping)
            mapping.write_text(json.dumps({'task-a':{'expected_suites':['A'],'selectors':['A']},
                                           'task-b':{'expected_suites':['B'],'selectors':['B']}}))
            result=seal_cases(harness,run,mapping)
            self.assertEqual(result['expected_cases'],2)
            sealed=util.read_json(Path(result['record'])/'result.json')
            self.assertEqual(sealed['tasks']['task-a']['expected_cases'],[{'suite':'A','case':'one'}])
            self.assertEqual((harness/'result.json').read_bytes(),original)
            util.write_json_atomic(run/'started.json',{'harness_result_sha256':'0'*64})
            with self.assertRaises(ValueError):seal_cases(harness,run,mapping)
