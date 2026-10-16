import copy
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from tools.fineract_corpus import producer as p

ROOT = Path(__file__).resolve().parents[2]
TASKS = ROOT / 'docs/fineract-corpus/public/tasks.json'


class MaterialUnitTests(unittest.TestCase):
    def test_dependency_membership_does_not_imply_target_relevance(self):
        units = [
            {'id': 'fee', 'path': 'a', 'range_bytes': [0, 1],
             'feature_ids': ['charges', 'savings-deposits'], 'selection_feature_ids': ['charges']},
            {'id': 'interest', 'path': 'z', 'range_bytes': [0, 1],
             'feature_ids': ['savings-deposits'], 'selection_feature_ids': ['savings-deposits']},
        ]
        task = {'target_features': ['savings-deposits'], 'common_features': ['business-date-cob']}
        self.assertEqual([(u['id'], rank) for u, rank in p.ordered_units(units, task)],
                         [('interest', 0), ('fee', 2)])

    def test_public_requests_contain_business_contracts_without_solution_locations(self):
        config = p.read_json(TASKS)
        forbidden = ('SavingsHelper', 'IntegrationTests', 'ValidationTest', '.java', '.adoc',
                     'gold patch', 'hidden case', '公開資料は', '公開要求→元資料', '研究者')
        for task in config['tasks']:
            body = (TASKS.parent / task['request_file']).read_text(encoding='utf-8')
            for value in forbidden:
                with self.subTest(task=task['id'], value=value):
                    self.assertNotIn(value, body)
        self.assertIn('HTTP400', (TASKS.parent / 'FINERACT-CHARGE-TEMPLATE.md').read_text(encoding='utf-8'))
        self.assertIn('min(E,R−1)', (TASKS.parent / 'FINERACT-DELINQUENCY-RESUME.md').read_text(encoding='utf-8'))

    def test_sections_preserve_utf8_tables_and_code_exactly(self):
        data = ('= 文書\n\n== First\n\n|===\n|値\n== not a section\n|===\n'
                '\n[source]\n----\n== code\n----\n\n== Second\n本文\n').encode()
        spans, status = p.sections(data, '.adoc')
        self.assertEqual(status, 'complete-sections')
        self.assertEqual([s[2] for s in spans], ['文書', 'First', 'Second'])
        self.assertEqual(b''.join(data[a:b] for a, b, _ in spans), data)
        for a, b, _ in spans:
            data[a:b].decode('utf-8')

    def test_unbalanced_block_is_whole_and_reported(self):
        data = b'= Title\n== First\n----\n== Inside\n'
        spans, status = p.sections(data, '.adoc')
        self.assertEqual(spans, [(0, len(data), 'Title')])
        self.assertEqual(status, 'whole-file-unbalanced-delimiter')

    def test_budget_stops_at_next_section_without_skipping(self):
        units = [({'id': str(i), 'bytes': size}, 0) for i, size in enumerate([4, 9, 2, 8])]
        actual = p.prefixes(units, [8, 16, 24])
        self.assertEqual([v['ids'] for v in actual], [['0'], ['0', '1', '2'], ['0', '1', '2', '3']])
        self.assertEqual(actual[0]['next_unit']['id'], '1')
        with self.assertRaisesRegex(ValueError, 'strictly increasing'):
            p.prefixes(units, [8, 10, 24])

    def test_archive_traversal_and_symlinks_are_rejected_even_with_matching_archive_hash(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for n, name in enumerate(['fineract-test/../../escape', 'fineract-test/link']):
                archive = root / f'{n}.tar.gz'
                with tarfile.open(archive, 'w:gz') as tar:
                    item = tarfile.TarInfo(name)
                    if n:
                        item.type = tarfile.SYMTYPE
                        item.linkname = '/etc/passwd'
                        tar.addfile(item)
                    else:
                        item.size = 1
                        tar.addfile(item, io.BytesIO(b'x'))
                with self.assertRaises(ValueError):
                    p.unpack(archive, root / f'out-{n}', {'archive_sha256': p.sha(archive.read_bytes()), 'commit': 'test'})
            self.assertFalse((root / 'escape').exists())

    def test_source_archive_hash_cannot_be_silently_refreshed(self):
        with tempfile.TemporaryDirectory() as td:
            archive = Path(td) / 'archive'
            archive.write_bytes(b'not pinned')
            with self.assertRaisesRegex(ValueError, 'digest mismatch'):
                p.unpack(archive, Path(td) / 'out', p.read_json(p.DATA / 'source.lock.json'))

    def test_requests_require_exact_bytes_opaque_spec_hash_and_frozen_priorities(self):
        policy = p.read_json(p.DATA / 'selection-policy.json')
        config = p.read_json(TASKS)
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for task in config['tasks']:
                (root / task['request_file']).write_bytes((TASKS.parent / task['request_file']).read_bytes())
            def check_change(key, value, pattern):
                changed = copy.deepcopy(config)
                changed['tasks'][0][key] = value
                p.write_json(root / 'tasks.json', changed)
                with self.assertRaisesRegex(ValueError, pattern):
                    p.load_tasks(root / 'tasks.json', policy)
            check_change('request_sha256', '0' * 64, 'digest mismatch')
            check_change('evaluation_spec_sha256', '/private/evaluator', 'invalid')
            check_change('target_features', ['charges'], 'priority mismatch')
            check_change('request_file', '../private', 'Unsafe')
            p.write_json(root / 'tasks.json', config)
            self.assertEqual(len(p.load_tasks(root / 'tasks.json', policy)), 3)


@unittest.skipUnless(os.environ.get('FINERACT_SOURCE_ARCHIVE'), 'Set FINERACT_SOURCE_ARCHIVE to the pinned archive for full-corpus tests')
class FullCorpusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.bundle = cls.root / 'first'
        cls.archive = Path(os.environ['FINERACT_SOURCE_ARCHIVE'])
        p.generate(cls.archive, TASKS, cls.bundle)
        cls.manifest = p.read_json(cls.bundle / 'manifest.json')

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_two_offline_generations_are_byte_identical(self):
        other = self.root / 'second'
        with patch('urllib.request.urlopen', side_effect=AssertionError('Generation must be offline')):
            p.generate(self.archive, TASKS, other)
        self.assertEqual(p.inventory(self.bundle), p.inventory(other))
        self.assertEqual(p.verify(other), {'verified': True, 'conditions': 21, 'source_files': 7772})

    def test_feature_sections_have_body_bound_roles_and_mixed_roles_where_needed(self):
        units = [u for u in self.manifest['document_units'] if '/chapters/features/' in u['path']]
        records = p.read_json(p.DATA / 'section-roles.json')['sections']
        self.assertEqual(len(units), 275)
        self.assertEqual({u['id'] for u in units}, set(records))
        for unit in units:
            self.assertEqual(unit['information_roles'], [e['role'] for e in unit['information_role_evidence']])
            body = p.unit_body(self.bundle / 'corpus', unit).decode('utf-8')
            for evidence in unit['information_role_evidence']:
                self.assertIn(evidence['anchor'], body)
        pause = {u['original_heading']: u['information_roles'] for u in units if u['path'].endswith('/pause-delinquency.adoc')}
        self.assertEqual(pause['Database Design'], ['data-model'])
        self.assertEqual(pause['Validation Rules'], ['validation-rules'])
        self.assertEqual(pause['Example Scenarios'], ['worked-examples'])
        savings = next(u for u in units if u['path'].endswith('/savings-interest-posting.adoc') and u['original_heading'] == 'How Anniversary Posting Works')
        self.assertEqual(savings['information_roles'], ['calculation-rules', 'worked-examples'])

    def test_savings_small_is_interest_material_and_fee_document_is_rest(self):
        tid = 'FINERACT-SAVINGS-MONTH-END'
        units = {u['id']: u for u in self.manifest['document_units']}
        selection = self.manifest['selections'][tid]
        small = [units[uid] for uid in selection['prefixes']['small']['ids']]
        self.assertTrue(any(u['path'] == 'reconstructed/savings-detail.md' for u in small))
        self.assertTrue(all('savings-deposits' in u['selection_feature_ids'] for u in small))
        fees = next(u for u in units.values() if u['path'] == 'reconstructed/charges-detail.md')
        self.assertIn('savings-deposits', fees['feature_ids'])
        self.assertEqual(next(r['rank'] for r in selection['ranked_units'] if r['id'] == fees['id']), 2)

    def test_seal_candidate_is_reproducible_and_does_not_replace_active_seal(self):
        active = (p.DATA / 'selection-seal.json').read_bytes()
        candidate = self.root / 'candidate.json'
        p.propose_seal(self.archive, candidate)
        self.assertEqual(candidate.read_bytes(), active)
        self.assertEqual((p.DATA / 'selection-seal.json').read_bytes(), active)
        with self.assertRaisesRegex(ValueError, 'already exists'):
            p.propose_seal(self.archive, candidate)

    def test_metadata_and_navigation_are_bound_to_selection_seal(self):
        for key in ('document_units', 'navigation_sha256'):
            changed = copy.deepcopy(self.manifest)
            if key == 'document_units':
                changed[key][0]['information_roles'] = ['unrequested-hint']
            else:
                changed[key] = '0' * 64
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'Sealed document metadata|Sealed navigation'):
                p.check_seal(changed)

    def test_seven_conditions_and_original_corpus_preserved(self):
        m = self.manifest
        self.assertEqual(len(m['conditions']), 21)
        source = self.bundle / 'corpus/source'
        feature_docs = list((source / 'fineract-doc/src/docs/en/chapters/features').glob('*.adoc'))
        self.assertEqual(len([f for f in feature_docs if f.name != 'index.adoc']), 34)
        self.assertEqual(len(p.inventory(source)), 7772)
        self.assertFalse((source / '.git').exists())
        for license_path in m['source_lock']['license_paths']:
            self.assertTrue((source / license_path).is_file())
        # Every whole original adoc/md document is reconstructed exactly by its disjoint unit ranges.
        parents = {}
        for unit in m['document_units']:
            parents.setdefault(unit['path'], []).append(unit)
        for path, units in parents.items():
            self.assertEqual(b''.join(p.unit_body(self.bundle / 'corpus', u) for u in units),
                             (self.bundle / 'corpus' / path).read_bytes())
        for tid, selection in m['selections'].items():
            rows = [r for r in m['conditions'] if r['task'] == tid]
            for key in ('request_sha256', 'source_inventory_sha256', 'semantic_corpus_sha256',
                        'evaluation_spec_sha256', 'common_authority_sha256'):
                self.assertEqual(len({r[key] for r in rows}), 1)
            a, b, c = [selection['prefixes'][x]['ids'] for x in ('small', 'medium', 'large')]
            self.assertTrue(set(a) < set(b) < set(c))
            for dose in ('explore', 'medium'):
                pair = [r for r in rows if r['dose'] == dose]
                self.assertEqual(pair[0]['body_sha256'], pair[1]['body_sha256'])
                self.assertEqual(pair[0]['selection_index_sha256'], pair[1]['selection_index_sha256'])
            dosed = [r for r in rows if r['dose'] != 'explore']
            self.assertEqual(len({r['selection_index_sha256'] for r in dosed}), 1)

    def test_existing_bundle_is_never_overwritten(self):
        with self.assertRaisesRegex(ValueError, 'already exists'):
            p.generate(self.archive, TASKS, self.bundle)
        self.assertEqual(p.read_json(self.bundle / 'manifest.json'), self.manifest)

    def test_tampered_prompt_cannot_be_resealed_by_updating_output_hashes(self):
        mp = self.bundle / 'manifest.json'
        row = self.manifest['conditions'][0]
        prompt = self.bundle / row['initial_path']
        original_prompt, original_manifest = prompt.read_bytes(), mp.read_bytes()
        try:
            prompt.write_bytes(original_prompt + b'\nUnrequested hidden hint\n')
            tampered = copy.deepcopy(self.manifest)
            tampered['conditions'][0]['initial_sha256'] = p.sha(prompt.read_bytes())
            tampered['conditions'][0]['initial_bytes'] = prompt.stat().st_size
            tampered['output_inventory'] = [x for x in p.inventory(self.bundle) if x['path'] != 'manifest.json']
            p.write_json(mp, tampered)
            with self.assertRaisesRegex(ValueError, 'regenerated row|regenerated prompt'):
                p.verify(self.bundle)
        finally:
            prompt.write_bytes(original_prompt)
            mp.write_bytes(original_manifest)

    def test_added_private_or_host_file_fails_verification(self):
        extra = self.bundle / 'corpus/.agents/SKILL.md'
        try:
            p.write(extra, 'not part of fixed source')
            with self.assertRaisesRegex(ValueError, 'files changed'):
                p.verify(self.bundle)
        finally:
            extra.unlink()
            extra.parent.rmdir()

    def test_manifest_metadata_and_selection_tampering_rejected(self):
        mp = self.bundle / 'manifest.json'
        original = mp.read_bytes()
        try:
            for key, value in [('navigation_bytes', 0), ('body_bytes', 1),
                               ('evaluation_spec_sha256', 'f' * 64), ('worker_mounts', ['.'])]:
                changed = copy.deepcopy(self.manifest)
                changed['conditions'][0][key] = value
                p.write_json(mp, changed)
                with self.subTest(key=key), self.assertRaises(ValueError):
                    p.verify(self.bundle)
            changed = copy.deepcopy(self.manifest)
            first = next(iter(changed['selections'].values()))
            first['ranked_units'].reverse()
            p.write_json(mp, changed)
            with self.assertRaisesRegex(ValueError, 'Sealed order'):
                p.verify(self.bundle)
        finally:
            mp.write_bytes(original)


if __name__ == '__main__':
    unittest.main()
