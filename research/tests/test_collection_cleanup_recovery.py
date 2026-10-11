"""Fictional cleanup transitions; no saved research JSON or trace is used."""
from contextlib import contextmanager
import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from outer.harness import evaluate, util
from research import collection_recovery as subject
from research.tests import test_collection_recovery as fixtures


class CleanupManifestRecoveryTests(unittest.TestCase):
    @contextmanager
    def fixture(self):
        fixture = fixtures.CollectionRecoveryTests()
        fixture.setUp()
        try:
            root = fixture.source
            old = util.read_json(root/'manifest.json')
            old['stop_evidence'] = '合成fixtureの停止確認 🧪'
            old['network_cleanup'].update(started_at='2000-01-01T00:00:00+00:00',
                ended_at='2000-01-01T00:00:01+00:00', duration_seconds=1.0, containers_retained=True)
            cleanup = old['network_cleanup']
            util.write_json_atomic(root/'evidence/network-cleanup/toy-result.json', cleanup)
            intent = {k: cleanup[k] for k in ('run_id', 'run_instance_id', 'network', 'network_id', 'started_at')}
            intent.update(confirmed=False, status='failed', containers=[], network_snapshot=dict(
                Name=cleanup['network'], Id=cleanup['network_id'], Internal=True, Driver='bridge', Containers={},
                Labels={'sample2.run':cleanup['run_id'], 'sample2.instance':cleanup['run_instance_id']}))
            util.write_new_json(root/'evidence/network-cleanup/toy-intent.json', intent)
            util.write_json_atomic(root/'manifest.json', old)
            temp = root/('manifest.json.'+'a'*32+'.tmp')
            util.write_new_json(temp, {**old, 'submission_fixed':True})
            receipt = {**fixture.receipt, 'at':'2000-01-01T00:00:02+00:00',
                       'manifest_sha256':util.sha256_file(root/'manifest.json')}
            util.write_json_atomic(root/'implementation-receipt.json', receipt)
            current = copy.deepcopy(old)
            current['network_cleanup'].update(status='absent', started_at='2000-01-01T00:00:03+00:00',
                ended_at='2000-01-01T00:00:04+00:00', duration_seconds=0.75)
            util.write_json_atomic(root/'manifest.json', current)
            util.write_new_json(root/'evidence/network-cleanup/later-result.json', current['network_cleanup'])
            yield fixture, temp, old, current, receipt
        finally:
            fixture.doCleanups()

    def refused(self, fixture):
        before = util.tree_hashes(fixture.source)
        with patch.object(evaluate, 'score_run') as score:
            with self.assertRaises((ValueError, FileNotFoundError)):
                fixture.recover(score=True)
        score.assert_not_called()
        self.assertFalse(fixture.destination.exists())
        self.assertEqual(before, util.tree_hashes(fixture.source))

    def test_exact_projection_and_confirmed_cleanup_transition_recover_without_mutation(self):
        with self.fixture() as (fixture, temp, old, current, receipt):
            root = fixture.source
            before = util.tree_hashes(root)
            self.assertNotEqual(receipt['manifest_sha256'], util.sha256_file(root/'manifest.json'))
            self.assertFalse((root/'evidence/network-cleanup/later-intent.json').exists())
            path, result = fixture.recover()
            self.assertTrue(result['recovery_complete'])
            self.assertTrue(result['source_unchanged'])
            self.assertEqual(before, util.tree_hashes(root))
            self.assertEqual(util.read_json(root/'implementation-receipt.json'), receipt)
            binding = util.read_json(path.parent/'intent.json')['manifest_binding']
            self.assertEqual(binding['kind'], 'confirmed_cleanup_manifest_transition_v1')
            projection = path.parent/binding['derivation']['output']
            self.assertEqual(util.sha256_file(projection), receipt['manifest_sha256'])
            self.assertEqual(util.read_json(projection), old)
            self.assertEqual(binding['derivation']['source'], subject.live_pilot.reference(temp))
            regenerated = json.loads(temp.read_text(encoding='utf-8'))
            regenerated['submission_fixed'] = False
            blob = (json.dumps(regenerated, ensure_ascii=False, indent=2)+'\n').encode('utf-8')
            self.assertEqual(blob, projection.read_bytes())
            transition = binding['transition']
            self.assertEqual(transition['allowed_changed_fields'], ['network_cleanup'])
            self.assertEqual(transition['cleanup_changed_fields'],
                ['duration_seconds', 'ended_at', 'started_at', 'status'])
            self.assertFalse(transition['current_intent_required'])
            self.assertEqual(util.read_json(path.parent/'run/R/manifest.json'),
                             {**current, 'submission_fixed':True})
            self.assertEqual(result['acquisition_count_increment'], 0)
            self.assertEqual(result['usage_count_increment'], 0)

    def test_current_cleanup_wrong_identity_unconfirmed_or_extra_changes_refused(self):
        changes = [dict(run_id='foreign'), dict(run_instance_id='2'*32), dict(network='foreign'),
            dict(network_id='foreign'), dict(confirmed=False), dict(confirmed=1),
            dict(containers_retained=False), dict(status='removed'), dict(duration_seconds=-1),
            dict(error={'type':'failure'}), dict(unexpected=True),
            dict(container_observation_retries=[{'attempt':1, 'at':'2000-01-01T00:00:03+00:00'}]),
            dict(started_at='1999-01-01T00:00:00+00:00'), dict(started_at='2000-01-01T00:00:03')]
        for change in changes:
            with self.subTest(change=change), self.fixture() as (fixture, temp, old, current, receipt):
                current['network_cleanup'].update(change)
                util.write_json_atomic(fixture.source/'manifest.json', current)
                # Matching JSON alone must not establish a valid transition.
                util.write_json_atomic(fixture.source/'evidence/network-cleanup/later-result.json',
                                       current['network_cleanup'])
                self.refused(fixture)

    def test_old_cleanup_wrong_identity_or_unconfirmed_refused_even_with_matching_old_hash(self):
        for change in (dict(run_instance_id='2'*32), dict(network_id='foreign'), dict(confirmed=False)):
            with self.subTest(change=change), self.fixture() as (fixture, temp, old, current, receipt):
                old['network_cleanup'].update(change)
                util.write_json_atomic(temp, {**old, 'submission_fixed':True})
                receipt['manifest_sha256'] = hashlib.sha256(
                    (json.dumps(old, ensure_ascii=False, indent=2)+'\n').encode('utf-8')).hexdigest()
                util.write_json_atomic(fixture.source/'implementation-receipt.json', receipt)
                util.write_json_atomic(fixture.source/'evidence/network-cleanup/toy-result.json',
                                       old['network_cleanup'])
                self.refused(fixture)

    def test_exact_current_hash_keeps_the_existing_route_without_a_temp(self):
        fixture = fixtures.CollectionRecoveryTests(); fixture.setUp()
        try:
            before = util.tree_hashes(fixture.source)
            path, result = fixture.recover()
            self.assertTrue(result['recovery_complete'])
            binding = util.read_json(path.parent/'intent.json')['manifest_binding']
            self.assertEqual(binding['kind'], 'exact_manifest_hash_v1')
            self.assertEqual(binding['current']['sha256'], binding['receipt_manifest_sha256'])
            self.assertFalse((path.parent/'receipt-manifest-projection.json').exists())
            self.assertEqual(before, util.tree_hashes(fixture.source))
        finally:
            fixture.doCleanups()

    def test_other_manifest_fields_and_submission_fixation_are_not_allowed_changes(self):
        for change in (dict(duration_seconds=99), dict(prompt_sha256='f'*64),
                       dict(submission_fixed=True), dict(extra_field='forbidden')):
            with self.subTest(change=change), self.fixture() as (fixture, temp, old, current, receipt):
                current.update(change)
                util.write_json_atomic(fixture.source/'manifest.json', current)
                self.refused(fixture)

    def test_runtime_identity_must_match_both_cleanup_results(self):
        for field in ('run_id', 'run_instance_id', 'network', 'network_id', 'stop_confirmed'):
            with self.subTest(field=field), self.fixture() as (fixture, temp, old, current, receipt):
                state = util.read_json(fixture.source/'runtime.json')
                state[field] = False if field == 'stop_confirmed' else 'foreign'
                util.write_json_atomic(fixture.source/'runtime.json', state)
                self.refused(fixture)

    def test_missing_conflicting_and_ambiguous_evidence_refused_before_output(self):
        modes = ('old_hash', 'temp_missing', 'temp_duplicate', 'temp_field', 'temp_format',
                 'old_result', 'current_result', 'duplicate_result', 'old_intent',
                 'intent_network', 'intent_uuid', 'failure_order')
        for mode in modes:
            with self.subTest(mode=mode), self.fixture() as (fixture, temp, old, current, receipt):
                root = fixture.source
                if mode == 'old_hash':
                    receipt['manifest_sha256'] = 'f'*64
                    util.write_json_atomic(root/'implementation-receipt.json', receipt)
                elif mode == 'temp_missing': temp.unlink()
                elif mode == 'temp_duplicate':
                    (root/('manifest.json.'+'b'*32+'.tmp')).write_bytes(temp.read_bytes())
                elif mode == 'temp_field':
                    util.write_json_atomic(temp, {**old, 'submission_fixed':True, 'extra':'forbidden'})
                elif mode == 'temp_format': temp.write_text(json.dumps(util.read_json(temp)))
                elif mode == 'old_result': (root/'evidence/network-cleanup/toy-result.json').unlink()
                elif mode == 'current_result': (root/'evidence/network-cleanup/later-result.json').unlink()
                elif mode == 'duplicate_result':
                    util.write_new_json(root/'evidence/network-cleanup/duplicate-result.json', current['network_cleanup'])
                elif mode == 'old_intent': (root/'evidence/network-cleanup/toy-intent.json').unlink()
                elif mode.startswith('intent_'):
                    path = root/'evidence/network-cleanup/toy-intent.json'; intent = util.read_json(path)
                    if mode == 'intent_network': intent['network_snapshot']['Id'] = 'foreign'
                    else: intent['network_snapshot']['Labels']['sample2.instance'] = '2'*32
                    util.write_json_atomic(path, intent)
                elif mode == 'failure_order':
                    receipt['at'] = '2000-01-01T00:00:05+00:00'
                    util.write_json_atomic(root/'implementation-receipt.json', receipt)
                self.refused(fixture)


if __name__ == '__main__': unittest.main()
