"""Private, hash-bound technical acceptance resources in the same global budget.

The shared acquisition collector retains each known input/output component even
when its counterpart is missing. This is accounting only: no adopted slots,
retry reservations or authorization are inherited. The actual origin and closed
Run inventory must be supplied.
"""
from datetime import datetime, timezone
from pathlib import Path
from outer.harness import util
from research import live_pilot


def verify_undispatched(row, identities):
    """Accept only sealed preparation failures with no possible model dispatch."""
    from outer.harness import live_usage
    if set(row) != {'manifest', 'acquisition', 'journal'}:
        raise ValueError('Explicit undispatched attempt evidence required')
    path = live_pilot.checked(row['manifest'])
    manifest = util.read_json(path)
    batch = path.parent.parent
    acquisition_path = live_pilot.checked(row['acquisition'])
    journal_path = live_pilot.checked(row['journal'])
    if (path.name != 'manifest.json' or acquisition_path != batch/'pipeline-acquisition.json'
            or journal_path != batch/'_control/pair-journal.jsonl'):
        raise ValueError('Foreign undispatched attempt evidence')
    acquisition = util.read_json(acquisition_path)
    journal, errors = live_usage.journal(journal_path)
    staged, staged_errors = live_usage.journal(path.parent/'_controller/staged-input/events.jsonl')
    identity = manifest['run_instance_id']
    matches = [r for r in acquisition.get('runs', []) if r.get('run_instance_id') == identity]
    if (identity in identities or manifest.get('started_at') or manifest.get('ended_at')
            or manifest.get('model_called') is True
            or manifest.get('native_session_id') or (path.parent/'runtime.json').exists()
            or util.tree_hashes(path.parent/'usage/raw')
            or any((path.parent/'snapshots').glob('*'))
            or errors or len(journal) != 1 or journal[0].get('kind') != 'pair_reserved'
            or not any(b.get('run_instance_id') == identity for b in journal[0].get('assignments', []))
            or staged_errors or any(e.get('kind') != 'planned' for e in staged)
            or acquisition.get('acquired') is not False or len(matches) != 1
            or matches[0].get('dispatch') is not None
            or matches[0].get('manifest', {}).get('sha256_at_acquisition') != row['manifest']['sha256']
            or acquisition.get('usage', {}).get('requests') != 0
            or acquisition.get('usage', {}).get('dispatched_runs') != 0
            or acquisition.get('usage', {}).get('observed_tokens', 0) != 0
            or acquisition.get('usage', {}).get('unknown_usage_requests')):
        raise ValueError('Undispatched attempt changed, duplicated or possibly sent; no resend')
    identities.add(identity)
    acquisition_binding = manifest.get('acquisition')
    config_ref = (acquisition_binding.get('campaign_config')
                  if isinstance(acquisition_binding, dict) else None)
    if not isinstance(config_ref, dict):
        raise ValueError('Original campaign clock reference required')
    config = util.read_json(live_pilot.checked(config_ref))
    if not isinstance(config, dict) or not isinstance(config.get('started_at'), str):
        raise ValueError('Original campaign clock missing')
    return config['started_at']


def read(reference):
    from research.acquisition_pipeline import usage_for_manifests
    receipt=util.read_json(live_pilot.checked(reference))
    legacy = (set(receipt)=={'kind','started_at','runs'}
        and receipt['kind']=='technical_acceptance_resources_v1' and bool(receipt['runs']))
    recovery = (set(receipt)=={'kind','started_at','runs','undispatched_attempts'}
        and receipt['kind']=='technical_acceptance_resources_v2' and bool(receipt['undispatched_attempts']))
    if not (legacy or recovery):
        raise ValueError('Explicit technical resource origin and Run inventory required')
    origin=datetime.fromisoformat(receipt['started_at'])
    if origin.tzinfo is None or origin>datetime.now(timezone.utc):raise ValueError('Invalid shared resource clock origin')
    paths=[];identities=set()
    for row in receipt.get('undispatched_attempts', []):
        if verify_undispatched(row, identities) != receipt['started_at']:
            raise ValueError('Resource origin differs from the original campaign clock')
    for row in receipt['runs']:
        if set(row)!={'manifest','raw_usage'}:raise ValueError('Explicit sealed technical Run evidence required')
        path=live_pilot.checked(row['manifest']);manifest=util.read_json(path)
        identity=manifest['run_instance_id']
        if (path.name!='manifest.json' or identity in identities
                or manifest.get('stop_confirmed') is not True
                or (manifest.get('network_cleanup') or {}).get('confirmed') is not True
                or not manifest.get('ended_at') or not manifest.get('started_at')
                or datetime.fromisoformat(manifest['started_at'])<origin
                or row['raw_usage']!=util.tree_hashes(path.parent/'usage/raw')):
            raise ValueError('Technical Run changed, duplicated, unstopped or outside shared clock')
        identities.add(identity);paths.append(path)
    return receipt['started_at'],usage_for_manifests(paths)
