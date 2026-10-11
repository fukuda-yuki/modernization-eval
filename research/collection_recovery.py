"""One-shot, same-Run recovery of a stopped PermissionError collection fault.

No dispatcher, worker start, campaign ledger writer or acquisition counter is
called here. Originals and their failure receipt stay unchanged. The ordinary
collector, terminal verifier and saved-scoring API own their existing checks.
"""
import argparse
from contextlib import nullcontext
import json
import math
from pathlib import Path
import re
import shutil
import time

from outer.harness import browser_review, evaluate, profiles, run, util
from research import catalog_environment, live_pilot
from research.campaign_reassessment import _existing_lock

KIND = 'same_run_collection_recovery_v1'


def _inventory(root):
    return util.tree_hashes(root)


def _source(source):
    manifest = util.read_json(source/'manifest.json')
    condition = profiles.validate_run(source)
    receipt = util.read_json(source/'implementation-receipt.json')
    snapshot = util.read_json(source/'snapshot.json')
    identity = {k: manifest[k] for k in ('run_id', 'run_instance_id')}
    if (manifest.get('schema_version') != 2 or condition.get('schema_version') != 2
            or manifest.get('stop_confirmed') is not True
            or manifest.get('end_reason') != 'completed' or source.name != identity['run_id']
            or not re.fullmatch('[0-9a-f]{32}', identity['run_instance_id'])
            or receipt.get('collection_status') != 'collection_fault'
            or receipt.get('collection_error_type') != 'PermissionError'
            or receipt.get('stop_confirmed') is not True
            or any(receipt.get(k) != v for k, v in identity.items())
            or receipt.get('manifest_sha256') != util.sha256_file(source/'manifest.json')
            or receipt.get('snapshot_sha256') != util.sha256_file(source/'snapshot.json')
            or receipt.get('raw') != _inventory(source/'usage/raw')
            or snapshot.get('run_id') != identity['run_id']
            or snapshot.get('artifact_state') != 'fixed'
            or snapshot.get('artifact_sha256') != util.artifact_hash(source/'frozen')
            or snapshot.get('frozen') != _inventory(source/'frozen')):
        raise ValueError('Exact stopped PermissionError collection and fixed snapshot required')
    if evaluate.used_sequences(source):
        raise ValueError('Existing scoring needs its own reconciliation; never automatically repeat')
    return manifest, snapshot


def recover(*, repo, source, destination, recovery_source_repo=None, score=False,
            timeout=1800, stop_file=None):
    """Explicit local postprocessing, with a fresh bounded budget, never model time.

    Destination must be outside the acquisition batch/repositories. One directory
    per source UUID prevents replay into the same destination, including after a
    crash. A new destination is an explicit new *technical* recovery attempt.
    """
    if type(timeout) is not int or not 1 <= timeout <= 1800:
        raise ValueError('Finite postprocessing timeout of 1..1800 seconds required')
    repo, source, destination = map(live_pilot.safe_path, (repo, source, destination))
    original_repo = live_pilot.safe_path(recovery_source_repo) if recovery_source_repo else repo
    for protected in (repo, original_repo, source.parent):
        if (destination == protected or destination.is_relative_to(protected)
                or protected.is_relative_to(destination)):
            raise ValueError('Separate recovery destination outside acquisition/repository required')
    deadline = time.monotonic() + timeout
    # Read-only ownership probe; release the Windows byte lock before inventory.
    with _existing_lock(source/'controller.lock'):
        pass
    manifest, old_snapshot = _source(source)
    config_ref = (manifest.get('acquisition') or {}).get('campaign_config')
    if config_ref is not None:
        config = util.read_json(live_pilot.checked(config_ref))
        campaign_root = live_pilot.safe_path(config['root'])
        if (not source.is_relative_to(campaign_root) or destination == campaign_root
                or destination.is_relative_to(campaign_root) or campaign_root.is_relative_to(destination)):
            raise ValueError('Recovery destination must be outside the bound acquisition campaign')
    before = _inventory(source)
    holder = destination/manifest['run_instance_id']
    def check():
        if (holder/'STOP').exists() or (stop_file is not None and Path(stop_file).exists()):
            raise InterruptedError('Collection recovery STOP requested')
        if time.monotonic() >= deadline:
            raise TimeoutError('Collection recovery postprocessing budget expired')
    check()
    holder.mkdir(parents=True, exist_ok=False)
    root = holder/'run'/manifest['run_id']
    intent = dict(schema_version=1, kind=KIND, source_path_private=str(source),
        source_run_id=manifest['run_id'], source_run_instance_id=manifest['run_instance_id'],
        source_inventory=before, source_condition_sha256=manifest['condition_sha256'],
        source_failure_sha256=util.sha256_file(source/'implementation-receipt.json'),
        recovery_controller_sha256=util.sha256_file(Path(__file__)),
        model_calls=0, acquisition_count_increment=0, usage_count_increment=0,
        usage_accounting='reference original acquisition once; never sum derivative usage',
        score_requested=score, timeout_seconds=timeout)
    util.write_new_json(holder/'intent.json', intent)
    terminal, scoring, error, phase = None, None, None, 'copy'
    try:
        shutil.copytree(source, root)
        if _inventory(root) != before or _inventory(source) != before:
            raise ValueError('Source/copy changed before recovery')
        check()
        # Retain the old collection bytes, including its original timestamp.
        (root/'snapshot.json').rename(holder/'saved-snapshot.json')
        (root/'frozen').rename(holder/'saved-frozen')
        phase = 'collection'
        _, snapshot = run.collect_run(root.parent, root.name,
            retry_deadline=deadline, retry_check=check)
        if ({k: v for k, v in snapshot.items() if k != 'collected_at'}
                != {k: v for k, v in old_snapshot.items() if k != 'collected_at'}
                or _inventory(root/'frozen') != _inventory(holder/'saved-frozen')):
            raise ValueError('Recollection differs from the saved snapshot/frozen evidence')
        (root/'snapshot.json').rename(holder/'recollected-snapshot.json')
        shutil.copyfile(holder/'saved-snapshot.json', root/'snapshot.json')
        # Original failure receipt stays intact. Its snapshot/raw bindings are
        # checked by the existing terminal verifier, never replaced by success.
        binding = dict(run_id=manifest['run_id'], run_instance_id=manifest['run_instance_id'],
            input_sha256=manifest['prompt_sha256'], condition_sha256=manifest['condition_sha256'])
        check()
        phase = 'terminal_validation'
        terminal = live_pilot.validate_owned_terminal(root, binding)
        usage = util.read_json(root/'usage/normalized.json')
        prior_usage = source/'usage/normalized.json'
        if prior_usage.exists():
            prior = util.read_json(prior_usage)
            if prior.get('usage_complete') is True and any(prior.get(k) != usage.get(k)
                    for k in ('total_tokens', 'input_tokens', 'output_tokens', 'request_count')):
                raise ValueError('Recollection conflicts with complete original usage')
        if score:
            check()
            if _inventory(source) != before:
                raise ValueError('Source changed before scoring; retain without execution')
            phase = 'scoring_environment'
            condition = util.read_json(root/'condition.json')
            environment = nullcontext()
            if browser_review.required(condition.get('evaluation', {}).get('evaluation_version', '')):
                epoch = util.read_json(live_pilot.checked(manifest['acquisition']['epoch']))
                browser = util.read_json(live_pilot.checked(epoch['browser_pin']))
                environment = catalog_environment.activated(browser, repo)
            with environment:
                check()
                remaining = math.floor(deadline - time.monotonic())
                if remaining < 1:
                    raise TimeoutError('No remaining scoring budget')
                phase = 'scoring'
                scoring = evaluate.score_run(repo, root.parent, root.name, timeout=remaining,
                    recovery_source_repo=original_repo)
        check()
        phase = 'preservation_validation'
        # Only manifest fixation and derived usage/scoring may change in the copy.
        expected_manifest = {**manifest, 'submission_fixed': True}
        if util.read_json(root/'manifest.json') != expected_manifest:
            raise ValueError('Recovery changed acquisition manifest fields')
        derived = ('usage/normalized.json', 'usage/events.jsonl', 'usage/context-evidence.json',
                   'usage/provenance.json', 'usage/staged-input-evidence.json', 'usage/derivations/')
        after = _inventory(root)
        for name, value in before.items():
            if name != 'manifest.json' and not name.startswith(derived) and after.get(name) != value:
                raise ValueError('Copied acquisition evidence changed: ' + name)
    except (Exception, KeyboardInterrupt) as exc:
        error = type(exc).__name__
    unchanged = _inventory(source) == before
    result = dict(schema_version=1, kind=KIND, intent=live_pilot.reference(holder/'intent.json'),
        source_run_id=manifest['run_id'], source_run_instance_id=manifest['run_instance_id'],
        source_unchanged=unchanged, model_calls=0, acquisition_count_increment=0, usage_count_increment=0,
        usage_accounting=intent['usage_accounting'], resend_authorized=False,
        collection_recovered=unchanged and error is None and terminal is not None,
        source_terminal=terminal, scoring=scoring, error_type=error, error_stage=phase if error else None,
        derivative_inventory=_inventory(root),
        usage_reference=live_pilot.reference(root/'usage/normalized.json')
            if (root/'usage/normalized.json').exists() else None)
    result['recovery_complete'] = bool(result['collection_recovered'] and (not score or
        scoring is not None and scoring.get('scoring_state') == 'scored'
        and scoring.get('operation_status') == 'complete' and not scoring.get('mismatches')))
    util.write_new_json(holder/'result.json', result)
    return live_pilot.reference(holder/'result.json')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('repo', 'source', 'destination'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--recovery-source-repo', type=Path)
    parser.add_argument('--score', action='store_true')
    parser.add_argument('--timeout', type=int, default=1800)
    parser.add_argument('--stop-file', type=Path)
    ref = recover(**vars(parser.parse_args()))
    print(json.dumps(ref))
    result = util.read_json(ref['path'])
    return 0 if result['recovery_complete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
