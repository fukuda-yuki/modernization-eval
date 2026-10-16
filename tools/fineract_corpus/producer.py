"""Offline material producer. No model, evaluator, or build execution."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import tempfile
import urllib.request

DATA = Path(__file__).resolve().parent
SCHEMA = 1
CONDITIONS = (
    ('explore-indexed', 'explore', True),
    ('selection-indexed', 'selection', True),
    ('small-indexed', 'small', True),
    ('medium-indexed', 'medium', True),
    ('large-indexed', 'large', True),
    ('explore-original', 'explore', False),
    ('medium-original', 'medium', False),
)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def digest(value):
    return sha(canonical(value))


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data.encode('utf-8') if isinstance(data, str) else data)


def write_json(path, value):
    write(path, json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n')


def safe_relative(path):
    p = PurePosixPath(path)
    require(path and not p.is_absolute() and all(x not in ('..', '.') for x in p.parts)
            and '\\' not in path and ':' not in path, f'Unsafe relative path: {path}')
    return p


def inventory(root):
    result = []
    for path in sorted(root.rglob('*')):
        require(not path.is_symlink(), f'Symlink forbidden: {path}')
        if path.is_file():
            result.append({'path': path.relative_to(root).as_posix(), 'sha256': sha(path.read_bytes()),
                           'bytes': path.stat().st_size, 'executable': bool(path.stat().st_mode & 0o111)})
    return result


def producer_inventory():
    return [entry for entry in inventory(DATA)
            if '__pycache__' not in PurePosixPath(entry['path']).parts
            and not entry['path'].endswith('.pyc')]


def unpack(archive, target, lock):
    require(sha(archive.read_bytes()) == lock['archive_sha256'], 'Source archive digest mismatch')
    target.mkdir(parents=True)
    prefix = 'fineract-' + lock['commit']
    with tarfile.open(archive, 'r:gz') as tar:
        seen = set()
        for member in tar:
            parts = safe_relative(member.name).parts
            require(parts[0] == prefix, 'Unexpected archive root')
            require(member.isdir() or member.isfile(), 'Archive links/devices forbidden')
            if member.isdir():
                continue
            relative = '/'.join(parts[1:])
            require(relative and relative not in seen, 'Duplicate/empty archive member')
            require(not {'.git', '.agents', '.codex'}.intersection(parts[1:]), 'Host/management material in archive')
            seen.add(relative)
            dest = target / relative
            with tar.extractfile(member) as stream:
                write(dest, stream.read())
            dest.chmod(0o755 if member.mode & 0o111 else 0o644)
    entries = inventory(target)
    require(len(entries) == lock['file_count'] and digest(entries) == lock['inventory_sha256'],
            'Pinned source inventory mismatch')
    return entries


def sections(data, suffix):
    """Disjoint complete level-2 sections; never split tables or delimited blocks.

    Keep all bytes including preamble. Markdown files are indivisible because a
    generic Markdown splitter cannot safely infer tables/front matter/HTML blocks.
    AsciiDoc includes are kept as original text, never expanded or fetched.
    """
    text = data.decode('utf-8')
    lines = text.splitlines(keepends=True)
    starts, offset, stack = [0], 0, []
    title = ''
    headings = {}
    for line in lines:
        stripped = line.strip()
        is_delimiter = bool(re.fullmatch(r'(?:[-.=*_+]{4,}|[|!,;]===|////)', stripped))
        if suffix == '.adoc' and is_delimiter:
            if stack and stack[-1] == stripped:
                stack.pop()
            elif not stack or stack[-1] not in ('----', '....', '++++', '////'):
                stack.append(stripped)
        if not stack:
            if re.match(r'^(?:=|#) [^\s]', line) and not title:
                title = line[2:].strip()
            if suffix == '.adoc' and re.match(r'^== [^\s]', line):
                if offset:
                    starts.append(offset)
                headings[offset] = line[3:].strip()
        offset += len(line.encode('utf-8'))
    # Unbalanced delimiters: preserve whole file; report rather than improvise.
    if stack:
        return [(0, len(data), title)], 'whole-file-unbalanced-delimiter'
    starts = sorted(set(starts))
    return [(start, end, headings.get(start, title)) for start, end in
            zip(starts, starts[1:] + [len(data)]) if end > start], 'complete-sections'


def proxy(n):
    return (n + 3) // 4


def roles(path):
    if '/features/' in path:
        return ['business-overview', 'specification', 'design']
    if '/architecture/' in path:
        return ['architecture', 'design']
    if '/testing/' in path:
        return ['public-test-guidance']
    return ['reference']


def document_units(corpus, features, recon_meta, lock):
    tags = {}
    for feature in features:
        for path in feature['documents']:
            tags.setdefault('source/' + path, []).append(feature['id'])
    units, omissions = [], []
    for path in sorted(corpus.rglob('*')):
        relative = path.relative_to(corpus).as_posix()
        if not path.is_file() or path.suffix.lower() not in ('.adoc', '.md') or relative.startswith('common/'):
            continue
        data = path.read_bytes()
        try:
            spans, split_status = sections(data, path.suffix.lower())
        except UnicodeDecodeError:
            omissions.append({'path': relative, 'reason': 'non-UTF-8; original retained for exploration'})
            continue
        reconstructed = relative.startswith('reconstructed/')
        meta = recon_meta.get(path.name, {}) if reconstructed else {}
        for number, (start, end, heading) in enumerate(spans, 1):
            body = data[start:end]
            unit_id = 'D' + sha(relative.encode())[:12] + '-' + str(number).zfill(3)
            units.append({
                'id': unit_id, 'path': relative, 'range_bytes': [start, end],
                'range_lines': [data[:start].count(b'\n') + 1, data[:end].count(b'\n') + (not data[:end].endswith(b'\n'))],
                'sha256': sha(body), 'source_file_sha256': sha(data), 'original_heading': heading or path.name,
                'feature_ids': sorted(meta.get('feature_ids', tags.get(relative, []))),
                'information_roles': meta.get('information_roles', roles(relative)),
                'representation': 'researcher-reconstruction' if reconstructed else ('original' if start == 0 and end == len(data) else 'verbatim-extract'),
                'origin': {'repository': 'modernization-eval' if reconstructed else lock['repository'],
                           'commit': None if reconstructed else lock['commit'],
                           'path': relative.removeprefix('source/'),
                           'basis': meta.get('evidence_ids', [])},
                'version_date_basis': 'Researcher-created 2026-10-16 from pinned baseline; not historical process records' if reconstructed else 'Present at pinned SHA; original creation year unverified. Copyright/example dates are not creation dates.',
                'rights': 'Apache-2.0; see source/LICENSE_SOURCE and source/NOTICE_SOURCE; researcher additions marked' if reconstructed else 'See source/LICENSE_SOURCE, source/NOTICE_SOURCE and file-specific notices retained in original',
                'bytes': len(body), 'proxy_tokens_bytes_div_4_ceil': proxy(len(body)),
                'duplicate_group': sha(body), 'parent_document': relative,
                'split_status': split_status,
            })
    return units, omissions


def evidence_ledger(source, features, evidence):
    by_path = {}
    for item in evidence:
        path = item['path']
        data = (source / path).read_text(encoding='utf-8')
        lines = data.splitlines()
        start, end = item['lines']
        excerpt = '\n'.join(lines[start - 1:end])
        require(0 < start <= end <= len(lines), 'Evidence line range invalid')
        require(sha((source / path).read_bytes()) == item['source_sha256'], 'Evidence file changed')
        require(item['anchor'] in excerpt, f'Evidence anchor absent: {item["id"]}')
        by_path.setdefault(path, []).append(item['id'])
    ledger = []
    for f in features:
        row = dict(f)
        row['links'] = []
        for kind in ('documents', 'code', 'public_tests'):
            for path in f[kind]:
                require((source / path).is_file(), f'Missing ledger source: {path}')
                row['links'].append({'kind': kind, 'path': 'source/' + path,
                                     'confirmation': '本文確認' if path in by_path else 'ファイル名のみ',
                                     'evidence_ids': by_path.get(path, [])})
        row['reconstructed_documents'] = [
            {'path': 'reconstructed/' + name, 'evidence_ids': meta['evidence_ids'],
             'information_roles': meta['information_roles'], 'representation': 'researcher-reconstruction'}
            for name, meta in read_json(DATA / 'reconstructed.json').items() if f['id'] in meta['feature_ids']]
        row['gaps'] = [
            'Original overview/detail/PT/ST deliverables are not established by file counts.',
            'Public tests have not been executed by this producer; runtime coverage is 未確認.',
        ]
        if not f['documents']:
            row['gaps'].append('Dedicated chapter documentation not mapped; consult API and shared researcher overview.')
        if not f['public_tests']:
            row['gaps'].append('Meaningful public-test mapping remains 未確認; complete source tests remain available.')
        ledger.append(row)
    return ledger


def ordered_units(units, task):
    def rank(unit):
        tags = set(unit['feature_ids'])
        if tags.intersection(task['target_features']):
            return 0
        if tags.intersection(task['common_features']):
            return 1
        return 2
    return [(u, rank(u)) for u in sorted(units, key=lambda u: (rank(u), u['path'], u['range_bytes'][0]))]


def prefixes(ordered, budgets):
    result = []
    for budget in budgets:
        ids, used, stop = [], 0, None
        for unit, rank in ordered:
            if used + unit['bytes'] > budget:
                stop = {'id': unit['id'], 'bytes': unit['bytes'], 'reason': 'next complete section exceeds remaining budget'}
                break
            used += unit['bytes']
            ids.append(unit['id'])
        result.append({'ids': ids, 'bytes': used, 'budget_bytes': budget, 'next_unit': stop})
    require(0 < len(result[0]['ids']) < len(result[1]['ids']) < len(result[2]['ids']),
            'Actual body sets must be nonempty and strictly increasing; do not skip/reorder to rescue them')
    return result


def unit_body(corpus, unit):
    start, end = unit['range_bytes']
    return (corpus / unit['path']).read_bytes()[start:end]


def ref(unit):
    a, b = unit['range_lines']
    return f'{unit["original_heading"]} — {unit["path"]}:L{a}-L{b}'


def navigation(units):
    # Only original headings and references; tags/roles are organization keys.
    groups = {}
    for unit in units:
        for tag in unit['feature_ids'] or ['unmapped']:
            for role in unit['information_roles']:
                groups.setdefault((tag, role), []).append(unit)
    return ''.join(f'## {tag} / {role}\n\n' + ''.join(f'- {ref(u)}\n' for u in values) + '\n'
                   for (tag, role), values in sorted(groups.items())).encode('utf-8')


def load_tasks(path, policy):
    config = read_json(path)
    require(config.get('schema_version') == SCHEMA, 'Unsupported tasks schema')
    require(set(config) == {'schema_version', 'tasks'}, 'Unknown tasks config fields')
    require(isinstance(config.get('tasks'), list) and config['tasks'], 'Empty task list')
    tasks, seen = [], set()
    for task in config['tasks']:
        require(set(task) == {'id', 'request_file', 'request_sha256', 'evaluation_spec_sha256',
                              'evaluation_spec_kind', 'target_features', 'common_features'}, 'Unknown task fields')
        tid = task['id']
        require(tid in policy['tasks'] and tid not in seen, 'Unknown/duplicate task ID')
        seen.add(tid)
        for key in ('target_features', 'common_features'):
            require(task[key] == policy['tasks'][tid][key], f'Frozen priority mismatch: {tid}/{key}')
        for key in ('request_sha256', 'evaluation_spec_sha256'):
            require(re.fullmatch('[0-9a-f]{64}', task[key]) is not None, f'Missing/invalid {key}')
        require(task['evaluation_spec_kind'] in ('public-contract', 'execution-spec'), 'Unknown evaluation binding kind')
        rp = path.parent / safe_relative(task['request_file'])
        require(rp.resolve().is_relative_to(path.parent.resolve()) and not rp.is_symlink(), 'Request path escapes input directory')
        body = rp.read_bytes()
        require(body.strip() and sha(body) == task['request_sha256'], 'Public request digest mismatch/empty request')
        body.decode('utf-8')
        tasks.append((task, body))
    return sorted(tasks, key=lambda x: x[0]['id'])


def make_table(rows):
    fields = [('task', 'task'), ('condition', 'condition'), ('initial document IDs', 'initial_document_ids'),
              ('body bytes', 'body_bytes'), ('selection index bytes', 'selection_index_bytes'),
              ('navigation bytes', 'navigation_bytes'), ('relevance bytes target/common/rest', 'relevance_bytes'),
              ('request SHA256', 'request_sha256'), ('source SHA256', 'source_inventory_sha256'),
              ('explorable semantic SHA256', 'semantic_corpus_sha256'), ('evaluation spec SHA256', 'evaluation_spec_sha256'),
              ('organization', 'organization')]
    lines = ['# Seven-condition material sets', '', '| ' + ' | '.join(x[0] for x in fields) + ' |',
             '| ' + ' | '.join('---' for _ in fields) + ' |']
    for row in rows:
        cells = []
        for _, key in fields:
            value = row[key]
            cells.append(json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else str(value))
        lines.append('| ' + ' | '.join(cells) + ' |')
    return '\n'.join(lines) + '\n'


def render_condition(corpus, task, request, units, selection_record, nav, authority,
                     cid, dose, indexed, source_hash, semantic_hash):
    tid = task['id']
    by_id = {u['id']: u for u in units}
    ordered = ordered_units(units, task)
    selection = ''.join(ref(by_id[uid]) + '\n' for uid in selection_record['prefixes']['large']['ids']).encode('utf-8')
    selected = selection_record['prefixes'].get(dose, {'ids': [], 'bytes': 0})
    body = b''.join(unit_body(corpus, by_id[uid]) for uid in selected['ids'])
    selected_index = selection if dose != 'explore' else b''
    intro = ('Complete baseline materials are in source/, reconstructed/, common/.\n'
             'The final change request follows and is also in request.md.\n'
             + ('Global navigation: navigation/index.md.\n' if indexed else '')).encode('utf-8')
    initial = intro + b'\n' + authority + b'\n# Final change request\n\n' + request
    if selected_index:
        initial += b'\n# Selection index\n\n' + selected_index
    if selected['ids']:
        initial += b'\n# Verbatim material sections\n\n'
        for uid in selected['ids']:
            unit = by_id[uid]
            initial += ('\n[' + uid + '] ' + ref(unit) + '\n\n').encode('utf-8') + unit_body(corpus, unit)
    initial_path = f'conditions/{tid}/{cid}/initial.md'
    counts = {'target': 0, 'common': 0, 'rest': 0}
    ranks = {u['id']: rank for u, rank in ordered}
    for uid in selected['ids']:
        counts[('target', 'common', 'rest')[ranks[uid]]] += by_id[uid]['bytes']
    row = {'task': tid, 'condition': cid, 'dose': dose,
                 'organization': 'indexed' if indexed else 'original',
                 'initial_document_ids': selected['ids'], 'body_bytes': len(body), 'body_sha256': sha(body),
                 'selection_index_bytes': len(selected_index), 'selection_index_sha256': sha(selected_index),
                 'navigation_bytes': len(nav) if indexed else 0, 'navigation_sha256': sha(nav) if indexed else sha(b''),
                 'common_authority_sha256': sha(authority), 'common_authority_bytes': len(authority),
                 'request_sha256': sha(request), 'source_inventory_sha256': source_hash,
                 'semantic_corpus_sha256': semantic_hash,
                 'evaluation_spec_sha256': task['evaluation_spec_sha256'], 'evaluation_spec_kind': task['evaluation_spec_kind'],
                 'initial_path': initial_path, 'initial_sha256': sha(initial), 'initial_bytes': len(initial),
                 'proxy_tokens_bytes_div_4_ceil': proxy(len(initial)), 'relevance_bytes': counts,
                 'worker_mounts': ['corpus/', f'tasks/{tid}/request.md'] + (['navigation/index.md'] if indexed else [])}
    return row, initial


def check_seal(manifest):
    seal = read_json(DATA / 'selection-seal.json')
    for key in ('source_inventory_sha256', 'semantic_corpus_sha256'):
        require(manifest[key] == seal[key], f'Sealed {key} changed')
    require(digest(manifest['selection_policy']) == seal['selection_policy_sha256'], 'Sealed policy changed')
    require(bool(manifest['selections']), 'No selections')
    for tid, selection in manifest['selections'].items():
        require(tid in seal['selections'], 'Unsealed task')
        require(digest(selection['ranked_units']) == seal['selections'][tid]['ranked_units_sha256'], 'Sealed order changed')
        require(selection['prefixes'] == seal['selections'][tid]['prefixes'], 'Sealed dose sets changed')
        require(selection['selection_policy_sha256'] == seal['selection_policy_sha256'], 'Task policy hash changed')
        for key in ('target_features', 'common_features'):
            require(selection['task'][key] == manifest['selection_policy']['tasks'][tid][key], 'Task priorities changed')


def generate(archive, tasks_path, output):
    require(not output.exists(), 'Output already exists; preservation requires a fresh path')
    lock = read_json(DATA / 'source.lock.json')
    policy = read_json(DATA / 'selection-policy.json')
    features = read_json(DATA / 'features.json')['features']
    evidence = read_json(DATA / 'evidence.json')['evidence']
    recon_meta = read_json(DATA / 'reconstructed.json')
    tasks = load_tasks(tasks_path, policy)
    output.parent.mkdir(parents=True, exist_ok=True)
    # Build atomically; an error never leaves a half-generated bundle for admission.
    with tempfile.TemporaryDirectory(prefix='.fineract-corpus-', dir=output.parent) as temp:
        root = Path(temp) / 'bundle'
        corpus = root / 'corpus'
        source_entries = unpack(archive, corpus / 'source', lock)
        shutil.copytree(DATA / 'reconstructed', corpus / 'reconstructed')
        write(corpus / 'common/authority.md', (DATA / 'authority.md').read_bytes())
        units, omissions = document_units(corpus, features, recon_meta, lock)
        ledger = evidence_ledger(corpus / 'source', features, evidence)
        nav = navigation(units)
        write(root / 'navigation/index.md', nav)
        semantic_entries = inventory(corpus)
        semantic_hash = digest(semantic_entries)
        authority = (corpus / 'common/authority.md').read_bytes()
        rows, selections = [], {}
        for task, request in tasks:
            tid = task['id']
            write(root / 'tasks' / tid / 'request.md', request)
            ordered = ordered_units(units, task)
            chosen = prefixes(ordered, policy['budgets_bytes'])
            selections[tid] = {'task': task, 'ranked_units': [{'id': u['id'], 'rank': rank} for u, rank in ordered],
                               'prefixes': dict(zip(('small', 'medium', 'large'), chosen)),
                               'selection_policy_sha256': digest(policy)}
            for cid, dose, indexed in CONDITIONS:
                row, initial = render_condition(corpus, task, request, units, selections[tid], nav, authority,
                                                cid, dose, indexed, digest(source_entries), semantic_hash)
                write(root / row['initial_path'], initial)
                rows.append(row)
        manifest = {'schema_version': SCHEMA, 'source_lock': lock, 'selection_policy': policy,
                    'producer_inputs_sha256': digest(producer_inventory()),
                    'source_inventory_sha256': digest(source_entries), 'semantic_corpus_sha256': semantic_hash,
                    'semantic_inventory': semantic_entries, 'document_units': units, 'document_omissions': omissions,
                    'feature_ledger': ledger, 'evidence': evidence, 'selections': selections,
                    'navigation_sha256': sha(nav), 'conditions': rows,
                    'token_estimate_method': 'ceil(UTF-8 bytes/4); proxy only, not model tokens or context-admission evidence',
                    'public_tests_executed': False, 'private_evaluator_accessed': False}
        check_seal(manifest)
        write(root / 'conditions.md', make_table(rows))
        # Complete output inventory detects missing/extra files, including worker contamination.
        manifest['output_inventory'] = inventory(root)
        write_json(root / 'manifest.json', manifest)
        verify(root)
        root.rename(output)
    return {'bundle': str(output), 'conditions': len(rows), 'semantic_corpus_sha256': semantic_hash}


def verify(bundle):
    manifest = read_json(bundle / 'manifest.json')
    require(manifest['schema_version'] == SCHEMA, 'Unsupported bundle schema')
    check_seal(manifest)
    actual = [e for e in inventory(bundle) if e['path'] != 'manifest.json']
    require(actual == manifest['output_inventory'], 'Output files changed, missing, or added')
    corpus = bundle / 'corpus'
    entries = inventory(corpus)
    require(entries == manifest['semantic_inventory'] and digest(entries) == manifest['semantic_corpus_sha256'],
            'Semantic corpus mismatch')
    lock = read_json(DATA / 'source.lock.json')
    require(manifest['source_lock'] == lock, 'Source lock differs from producer')
    require(digest(inventory(corpus / 'source')) == lock['inventory_sha256'], 'Source does not match pinned baseline')
    require(manifest['selection_policy'] == read_json(DATA / 'selection-policy.json'), 'Selection policy changed')
    require(manifest['producer_inputs_sha256'] == digest(producer_inventory()), 'Producer code/material revision changed')
    features = read_json(DATA / 'features.json')['features']
    units, omissions = document_units(corpus, features, read_json(DATA / 'reconstructed.json'), lock)
    require(units == manifest['document_units'] and omissions == manifest['document_omissions'], 'Document provenance mismatch')
    require(manifest['evidence'] == read_json(DATA / 'evidence.json')['evidence'], 'Evidence records changed')
    require(evidence_ledger(corpus / 'source', features, manifest['evidence']) == manifest['feature_ledger'], 'Feature ledger mismatch')
    nav = navigation(units)
    require((bundle / 'navigation/index.md').read_bytes() == nav and sha(nav) == manifest['navigation_sha256'], 'Navigation differs from headings/references')
    require(len(manifest['conditions']) == 7 * len(manifest['selections']), 'Expected seven conditions per task')
    for tid, selection in manifest['selections'].items():
        task = selection['task']
        ordered = ordered_units(units, task)
        require(selection['ranked_units'] == [{'id': u['id'], 'rank': rank} for u, rank in ordered], 'Selection order changed')
        expected = prefixes(ordered, manifest['selection_policy']['budgets_bytes'])
        require(selection['prefixes'] == dict(zip(('small', 'medium', 'large'), expected)), 'Prefix selection changed')
        rows = [r for r in manifest['conditions'] if r['task'] == tid]
        require({r['condition'] for r in rows} == {x[0] for x in CONDITIONS} and len(rows) == 7, 'Condition set mismatch')
        request = (bundle / 'tasks' / tid / 'request.md').read_bytes()
        require(sha(request) == task['request_sha256'], 'Final requirement changed')
        for row, (cid, dose, indexed) in zip(rows, CONDITIONS):
            expected_row, initial = render_condition(
                corpus, task, request, units, selection, nav,
                (corpus / 'common/authority.md').read_bytes(), cid, dose, indexed,
                lock['inventory_sha256'], manifest['semantic_corpus_sha256'])
            require(row == expected_row, 'Condition metadata differs from regenerated row')
            require((bundle / row['initial_path']).read_bytes() == initial, 'Initial message differs from complete regenerated prompt')
        for dose in ('explore', 'medium'):
            pair = [r for r in rows if r['dose'] == dose]
            require(len({r['body_sha256'] for r in pair}) == 1 and len({r['selection_index_sha256'] for r in pair}) == 1,
                    'Organization comparison changed body/selection index')
    require((bundle / 'conditions.md').read_text(encoding='utf-8') == make_table(manifest['conditions']), 'Set table mismatch')
    return {'verified': True, 'conditions': len(manifest['conditions']), 'source_files': lock['file_count']}


def fetch(output):
    require(not output.exists(), 'Archive output already exists')
    lock = read_json(DATA / 'source.lock.json')
    output.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(lock['archive_url'], timeout=120) as response:
        data = response.read()
    require(sha(data) == lock['archive_sha256'], 'Fetched archive differs from lock; do not silently refresh lock')
    with output.open('xb') as stream:
        stream.write(data)
    return {'archive': str(output), 'sha256': sha(data)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    p = subs.add_parser('fetch'); p.add_argument('--output', type=Path, required=True)
    p = subs.add_parser('generate')
    p.add_argument('--source-archive', type=Path, required=True)
    p.add_argument('--tasks', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p = subs.add_parser('verify'); p.add_argument('--bundle', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'fetch':
            result = fetch(args.output)
        elif args.command == 'generate':
            result = generate(args.source_archive, args.tasks, args.output)
        else:
            result = verify(args.bundle)
    except (ValueError, KeyError, OSError, UnicodeError, tarfile.TarError) as exc:
        parser.exit(2, f'fineract-corpus: {exc}\n')
    print(json.dumps(result, sort_keys=True))
