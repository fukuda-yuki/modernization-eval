"""Shared primitives for the outer harness.

Only standard library. No model client, no network access.
"""
import hashlib
import json
import os
import uuid
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

# Collection excludes. Frozen artifact identity separately follows the SDK.
EXCLUDED_DIRECTORIES = ('bin', 'obj', '.git', '.vs', 'node_modules', 'testresults')
EXCLUDED_SUFFIXES = ('.sqlite', '.sqlite3', '.db', '.db-shm', '.db-wal', '.user')
ARTIFACT_EXCLUDED_DIRECTORIES = ('bin', 'obj', '.git')
LEGACY_COLLECTION_POLICY = 'legacy-v1'
STATIC_DB_COLLECTION_POLICY = 'workspace-static-db-v2'
DATABASE_SUFFIXES = ('.sqlite', '.sqlite3', '.db')
DATABASE_SIDECARS = ('-wal', '-shm', '-journal')

# Text files whose line endings and BOM are fixed at collection time.
# The evaluation ID depends on the artifact bytes, so the bytes must not
# depend on the checkout's line-ending configuration.
NORMALIZED_SUFFIXES = (
    '.cs', '.csproj', '.cshtml', '.config', '.json', '.jsonl', '.md', '.txt',
    '.css', '.js', '.html', '.xml', '.razor', '.sln', '.props', '.targets',
    '.yml', '.yaml', '.csv', '.ps1', '.sh',
)

BOM = b'\xef\xbb\xbf'
_ATOMIC_RETRY_CHECK = ContextVar('collection_atomic_retry_check', default=None)


@contextmanager
def collection_atomic_retries(check):
    """Opt in only stopped collection writes; caller supplies STOP/deadline checks."""
    token = _ATOMIC_RETRY_CHECK.set(check)
    try:
        yield
    finally:
        _ATOMIC_RETRY_CHECK.reset(token)


def _replace_json(temp, path):
    check = _ATOMIC_RETRY_CHECK.get()
    delays = iter((0.05, 0.1, 0.2, 0.4))
    while True:
        try:
            temp.replace(path)
            return
        except PermissionError as exc:
            # Windows access denied / sharing violation only. Never chmod,
            # delete the destination, rewrite the temp, or swallow another error.
            if check is None or getattr(exc, 'winerror', None) not in (5, 32):
                raise
            delay = next(delays, None)
            if delay is None:
                raise
            check()
            # Poll STOP/deadline during the bounded backoff, not just afterwards.
            until = time.monotonic() + delay
            while time.monotonic() < until:
                check()
                time.sleep(min(0.025, max(0, until - time.monotonic())))
            check()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def write_new_json(path, value):
    """Exclusive create. Never overwrites an existing record."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def write_json_atomic(path, value):
    """Replace a mutable record (the run manifest). Not used for packages."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with temp.open('w', encoding='utf-8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    _replace_json(temp, path)


def append_line(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8', newline='\n') as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def read_lines(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def is_excluded(relative_parts):
    return any(part.lower() in EXCLUDED_DIRECTORIES for part in relative_parts)


def artifact_files(root):
    """Files that the evaluator counts, in the evaluator's order.

    The evaluator orders by the Windows relative path (backslash separated)
    with StringComparer.Ordinal. Sorting the forward-slash form instead would
    reorder paths that differ only around '/' versus a character between
    '/' and '\\' (digits, ':', uppercase letters, '['), so the backslash form
    is what gets sorted here too.

    Frozen identity counts every regular file except bin/obj/.git, exactly as
    both SDK evaluators do. Workspace collection selection is a separate API.
    Existing frozen trees without excluded database files keep their hashes.
    """
    root = Path(root)
    reject_links(root)
    files = []
    for path in root.rglob('*'):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part.lower() in ARTIFACT_EXCLUDED_DIRECTORIES for part in relative.parts):
            continue
        files.append(str(relative))
    return sorted(files, key=lambda name: name.replace('/', '\\').encode('utf-16-be'))


def reject_links(root):
    root = Path(root)
    for path in [root, *root.rglob('*')]:
        if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
            raise ValueError('Links are not allowed in collected artifacts')


def _files_hash(root, files):
    builder = []
    for relative in files:
        builder.extend((relative.replace('\\', '/'), ' ', sha256_file(Path(root) / relative), '\n'))
    return sha256_bytes(''.join(builder).encode('utf-8'))


def artifact_hash(root):
    """Byte identity of an artifact directory.

    Same algorithm as the evaluator's Sha256Directory: for every file not under
    bin/obj/.git, append "<forward-slash relative path> <sha256>\n" in the
    evaluator's order, then hash the UTF-8 bytes.
    """
    root = Path(root)
    if not root.is_dir():
        raise FileNotFoundError(root)
    return _files_hash(root, artifact_files(root))


def resolve_collection_policy(policy=None):
    policy = LEGACY_COLLECTION_POLICY if policy is None else policy
    if policy not in (LEGACY_COLLECTION_POLICY, STATIC_DB_COLLECTION_POLICY):
        raise ValueError('Unknown submission collection policy')
    return policy


def collection_contract(policy=None):
    policy = resolve_collection_policy(policy)
    return {'version': 2 if policy == STATIC_DB_COLLECTION_POLICY else 1,
        'policy': policy, 'excluded_directories': list(EXCLUDED_DIRECTORIES),
        'database_assets_retained': policy == STATIC_DB_COLLECTION_POLICY,
        'nonempty_database_sidecars': 'hold_without_checkpoint' if policy == STATIC_DB_COLLECTION_POLICY else 'legacy_suffix_selection',
        'frozen_identity': 'all_regular_files_except_bin_obj_git'}


def _database_sidecar(path):
    name = path.name.lower()
    return any(name.endswith(suffix + sidecar) for suffix in DATABASE_SUFFIXES for sidecar in DATABASE_SIDECARS)


def _collection_excluded(path, policy):
    if policy == LEGACY_COLLECTION_POLICY:
        return path.suffix.lower() in EXCLUDED_SUFFIXES
    return path.suffix.lower() == '.user' or _database_sidecar(path)


def collection_files(root, *, policy=None):
    """Select workspace submission bytes, never alter or checkpoint databases."""
    policy = resolve_collection_policy(policy)
    root = Path(root)
    reject_links(root)
    if not root.is_dir():
        raise FileNotFoundError(root)
    candidates = [p for p in root.rglob('*') if p.is_file()
        and not is_excluded(p.relative_to(root).parts)]
    if policy == STATIC_DB_COLLECTION_POLICY:
        by_name = {p.relative_to(root).as_posix().lower(): p for p in candidates}
        for path in candidates:
            if path.suffix.lower() not in DATABASE_SUFFIXES:
                continue
            for ending in DATABASE_SIDECARS:
                sidecar = by_name.get(path.relative_to(root).as_posix().lower() + ending)
                if sidecar is not None and sidecar.stat().st_size:
                    raise ValueError('Unsealed submitted database: ' + path.relative_to(root).as_posix()
                        + '; nonempty sidecar ' + sidecar.relative_to(root).as_posix()
                        + '; collection held without checkpoint')
    names = [str(p.relative_to(root)) for p in candidates if not _collection_excluded(p, policy)]
    return sorted(names, key=lambda name: name.replace('/', '\\').encode('utf-16-be'))


def collection_hash(root, *, policy=None):
    """Hash exactly the selected workspace files before text normalization."""
    return _files_hash(root, collection_files(root, policy=policy))


def normalize_text(data):
    """Return (fixed_bytes, kinds). Kinds describe what was changed."""
    kinds = []
    if data.startswith(BOM):
        data = data[len(BOM):]
        kinds.append('bom_removed')
    if b'\r' in data:
        data = data.replace(b'\r\n', b'\n').replace(b'\r', b'\n')
        kinds.append('crlf_to_lf')
    return data, kinds


def collect(workspace, frozen, *, normalize=True, policy=None):
    """Copy workspace to frozen, excluding generated files and fixing text bytes."""
    policy = resolve_collection_policy(policy)
    workspace, frozen = Path(workspace), Path(frozen)
    reject_links(workspace)
    if frozen.exists():
        raise FileExistsError(frozen)
    if not workspace.is_dir():
        raise FileNotFoundError(workspace)
    selected = set(collection_files(workspace, policy=policy))
    frozen.mkdir(parents=True)
    collected, fixed, normalized = {}, {}, []
    excluded, excluded_evidence = [], {}
    for path in sorted(workspace.rglob('*'), key=lambda p: str(p.relative_to(workspace))):
        relative = path.relative_to(workspace)
        if is_excluded(relative.parts):
            continue
        name = str(relative).replace('\\', '/')
        if path.is_dir():
            (frozen / relative).mkdir(parents=True, exist_ok=True)
            continue
        if not path.is_file():
            raise ValueError('Special files are not collectable: ' + name)
        if str(relative) not in selected:
            excluded.append(name)
            if policy == STATIC_DB_COLLECTION_POLICY:
                excluded_evidence[name] = {'sha256': sha256_file(path), 'bytes': path.stat().st_size,
                    'reason': 'database_sidecar_not_submitted' if _database_sidecar(path) else 'user_configuration'}
            continue
        data = path.read_bytes()
        collected[name] = {'sha256': sha256_bytes(data), 'bytes': len(data)}
        kinds = []
        if normalize and path.suffix.lower() in NORMALIZED_SUFFIXES:
            data, kinds = normalize_text(data)
        (frozen / relative).parent.mkdir(parents=True, exist_ok=True)
        with (frozen / relative).open('wb') as stream:
            stream.write(data)
        fixed[name] = {'sha256': sha256_bytes(data), 'bytes': len(data)}
        if kinds:
            normalized.append({'path': name, 'changed': kinds})
    return {'collected': collected, 'frozen': fixed, 'normalized': normalized,
            'excluded_files': excluded,
            'excluded_directories': list(EXCLUDED_DIRECTORIES),
            'excluded_suffixes': list(EXCLUDED_SUFFIXES) if policy == LEGACY_COLLECTION_POLICY
                else ['.user', *[suffix + sidecar for suffix in DATABASE_SUFFIXES for sidecar in DATABASE_SIDECARS]],
            'collection_policy': policy, 'collection_contract': collection_contract(policy),
            'excluded_file_evidence': excluded_evidence,
            'database_assets': {name: entry for name, entry in fixed.items()
                if Path(name).suffix.lower() in DATABASE_SUFFIXES}}


def tree_hashes(root):
    """Relative path -> {sha256, bytes} for every regular file under root."""
    root = Path(root)
    reject_links(root)
    entries = {}
    for path in sorted(root.rglob('*'), key=lambda p: str(p.relative_to(root))):
        if path.is_file():
            entries[str(path.relative_to(root)).replace('\\', '/')] = {
                'sha256': sha256_file(path), 'bytes': path.stat().st_size}
    return entries
