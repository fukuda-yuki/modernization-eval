import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from outer.fineract.artifacts import extract_artifact,files,junit_result,pack_source


class FineractArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)

    def test_restore_preserves_bytes_and_rejects_replacement(self):
        source=self.root/'source';source.mkdir()
        (source/'A.java').write_bytes(b'package a;\r\n// submitted bytes\n')
        (source/'build').mkdir();(source/'build/forged.xml').write_text('not evidence')
        archive=self.root/'artifact.tar'
        receipt=pack_source(source,archive)
        restored=extract_artifact(archive,self.root/'restored')
        self.assertEqual(restored,receipt['files'])
        self.assertEqual(set(restored),{'A.java'})
        with self.assertRaises(FileExistsError):pack_source(source,archive)
        with self.assertRaises(ValueError):extract_artifact(archive,self.root/'restored')

    def test_tar_link_traversal_duplicate_and_hidden_rejected(self):
        for index,name in enumerate(('../escape','/absolute','.git/config','same','link')):
            archive=self.root/f'{index}.tar'
            with tarfile.open(archive,'w') as tar:
                info=tarfile.TarInfo(name);info.size=1
                if name=='link':info.type=tarfile.SYMTYPE;info.linkname='/outside';info.size=0
                tar.addfile(info,io.BytesIO(b'x'))
                if name=='same':tar.addfile(info,io.BytesIO(b'x'))
            with self.subTest(name=name),self.assertRaises(ValueError):
                extract_artifact(archive,self.root/f'out-{index}')

    def test_zero_skip_missing_and_failure_are_not_pass(self):
        reports=self.root/'reports';reports.mkdir()
        self.assertEqual(junit_result(reports,['Suite'])['status'],'incomplete')
        path=reports/'TEST-Suite.xml'
        for inner,expected in (('<skipped/>','incomplete'),('<failure/>','failed'),('','passed')):
            path.write_text(f'<testsuite><testcase classname="Suite" name="one">{inner}</testcase></testsuite>')
            self.assertEqual(junit_result(reports,['Suite'])['status'],expected)
        self.assertEqual(junit_result(reports,['Other'])['status'],'incomplete')
        (reports/'TEST-copy.xml').write_bytes(path.read_bytes())
        with self.assertRaises(ValueError):junit_result(reports,['Suite'])
