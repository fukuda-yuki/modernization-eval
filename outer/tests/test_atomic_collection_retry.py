"""Bounded Windows replacement faults; no provider or saved Run involved."""
from pathlib import Path
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import support
from harness import util, run
import test_collection_contract as collection_fixture


def denied(code=5):
    error = PermissionError(13, 'synthetic replacement denial')
    error.winerror = code
    return error


class AtomicCollectionRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.target = Path(self.tmp.name)/'record.json'
        util.write_new_json(self.target, {'old': True})

    def test_same_temp_eventually_replaces_original_atomically(self):
        for code in (5, 32):
            attempts = []
            original = Path.replace
            def replace(temp, target):
                attempts.append((temp, temp.read_bytes()))
                if len(attempts) < 3:
                    self.assertEqual(util.read_json(target), {'old': True})
                    raise denied(code)
                return original(temp, target)
            util.write_json_atomic(self.target, {'old': True})
            with patch.object(Path, 'replace', replace), util.collection_atomic_retries(lambda: None):
                util.write_json_atomic(self.target, {'new': True})
            self.assertEqual(len(attempts), 3)
            self.assertEqual(len(set(attempts)), 1)
            self.assertEqual(util.read_json(self.target), {'new': True})

    def test_exhaustion_preserves_original_and_temp_and_raises(self):
        with patch.object(Path, 'replace', side_effect=denied()) as replace:
            with util.collection_atomic_retries(lambda: None), self.assertRaises(PermissionError):
                util.write_json_atomic(self.target, {'new': True})
        self.assertEqual(replace.call_count, 5)
        self.assertEqual(util.read_json(self.target), {'old': True})
        self.assertEqual(len(list(self.target.parent.glob('*.tmp'))), 1)

    def test_other_errors_and_unscoped_writes_are_not_retried(self):
        for error in (PermissionError(13, 'POSIX'), denied(19), OSError('disk fault')):
            with self.subTest(error=error), patch.object(Path, 'replace', side_effect=error) as replace:
                with util.collection_atomic_retries(lambda: None), self.assertRaises(type(error)):
                    util.write_json_atomic(self.target, {})
                self.assertEqual(replace.call_count, 1)
        with patch.object(Path, 'replace', side_effect=denied()) as replace:
            with self.assertRaises(PermissionError):
                util.write_json_atomic(self.target, {})
            self.assertEqual(replace.call_count, 1)

    def test_stop_or_deadline_during_backoff_prevents_second_replace(self):
        for error in (InterruptedError('STOP'), TimeoutError('expired')):
            calls = []
            def check():
                calls.append(True)
                if len(calls) >= 3:
                    raise error
            with patch.object(Path, 'replace', side_effect=denied()) as replace:
                with util.collection_atomic_retries(check), self.assertRaises(type(error)):
                    util.write_json_atomic(self.target, {})
                self.assertEqual(replace.call_count, 1)

    def test_ordinary_collection_retries_manifest_without_recollecting(self):
        fixture = collection_fixture.CollectionContractTests(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        runs, root, manifest = fixture.run_fixture()
        original = Path.replace
        calls = []
        def replace(temp, target):
            if Path(target) == root/'manifest.json':
                calls.append(temp)
                if len(calls) == 1:
                    raise denied()
            return original(temp, target)
        with patch.object(Path, 'replace', replace), patch.object(util, 'collect', wraps=util.collect) as collect:
            fixed, snapshot = run.collect_run(runs, manifest['run_id'])
        self.assertTrue(fixed['submission_fixed'])
        self.assertEqual(collect.call_count, 1)
        self.assertEqual(len(set(calls)), 1)
        self.assertEqual(util.artifact_hash(root/'frozen'), snapshot['artifact_sha256'])

    def test_collection_guard_honors_existing_stop_and_caller_deadline(self):
        for mode in ('stop', 'deadline'):
            fixture = collection_fixture.CollectionContractTests(); fixture.setUp()
            try:
                runs, root, manifest = fixture.run_fixture()
                if mode == 'stop': (root/'STOP').touch()
                with patch.object(Path, 'replace', side_effect=denied()) as replace:
                    with self.assertRaises(InterruptedError if mode == 'stop' else TimeoutError):
                        run.collect_run(runs, manifest['run_id'], retry_deadline=time.monotonic()-1
                                        if mode == 'deadline' else None)
                    self.assertEqual(replace.call_count, 1)
                self.assertTrue((root/'snapshot.json').is_file())
            finally:
                fixture.doCleanups()

    @unittest.skipUnless(os.name == 'nt', 'Real Windows read-handle sharing semantics')
    def test_windows_read_handle_release_succeeds_without_chmod(self):
        stream = self.target.open('rb')
        probe = self.target.with_suffix('.probe')
        probe.write_bytes(b'{}')
        try:
            with self.assertRaises(PermissionError) as caught:
                probe.replace(self.target)
            self.assertIn(caught.exception.winerror, (5, 32))
        except BaseException:
            stream.close()
            raise
        timer = threading.Timer(0.12, stream.close)
        timer.start()
        try:
            with util.collection_atomic_retries(lambda: None):
                util.write_json_atomic(self.target, {'new': True})
        finally:
            stream.close(); timer.join()
        self.assertEqual(util.read_json(self.target), {'new': True})


if __name__ == '__main__':
    unittest.main()
