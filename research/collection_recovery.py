"""One-shot, same-Run recovery of a stopped PermissionError collection fault.

No dispatcher, worker start, campaign ledger writer or acquisition counter is
called here. Originals and their failure receipt stay unchanged. The ordinary
collector, terminal verifier and saved-scoring API own their existing checks.
"""
import argparse
from contextlib import nullcontext
from datetime import datetime
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


def _manifest_bytes(value):
    """The existing atomic writer's exact UTF-8/indent/LF serialization."""
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode('utf-8')


def _timestamp(value):
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None:
        raise ValueError('Cleanup transition requires timezone-bound timestamps')
    return instant


def _cleanup_transition(source, previous, current, receipt):
    """Bind the sole allowed change to two retained successful cleanup receipts."""
    old, new = previous.get('network_cleanup'), current.get('network_cleanup')
    state = util.read_json(source/'runtime.json')
    identity = {k: current[k] for k in ('run_id', 'run_instance_id')}
    identity.update({k: state.get(k) for k in ('network', 'network_id')})
    if (state.get('stop_confirmed') is not True
            or any(state.get(k) != current[k] for k in ('run_id', 'run_instance_id'))
            or any(not isinstance(v, str) or not v for v in identity.values())):
        raise ValueError('Cleanup transition runtime identity/stop mismatch')
    required = {*identity, 'started_at', 'ended_at', 'duration_seconds',
                'confirmed', 'status', 'containers_retained'}
    references = []
    times = []
    for cleanup, status in ((old, 'removed'), (new, 'absent')):
        if (not isinstance(cleanup, dict) or not required <= cleanup.keys()
                or cleanup.keys() - required - {'container_observation_retries'}
                or cleanup['confirmed'] is not True or cleanup['containers_retained'] is not True
                or cleanup['status'] != status or any(cleanup[k] != v for k, v in identity.items())
                or type(cleanup['duration_seconds']) not in (int, float)
                or not math.isfinite(cleanup['duration_seconds']) or cleanup['duration_seconds'] < 0):
            raise ValueError('Only bound confirmed removed-to-absent cleanup is recoverable')
        start, end = map(_timestamp, (cleanup['started_at'], cleanup['ended_at']))
        if start > end:
            raise ValueError('Cleanup receipt time order mismatch')
        retries = cleanup.get('container_observation_retries', [])
        if not isinstance(retries, list):
            raise ValueError('Invalid cleanup observation retries')
        for index, retry in enumerate(retries, 1):
            if (not isinstance(retry, dict) or set(retry) != {'at', 'attempt'}
                    or type(retry['attempt']) is not int or retry['attempt'] != index or index > 3
                    or not start <= _timestamp(retry['at']) <= end):
                raise ValueError('Invalid cleanup observation retry evidence')
        matches = [p for p in (source/'evidence/network-cleanup').glob('*-result.json')
                   if live_pilot.safe_path(p).read_bytes() == _manifest_bytes(cleanup)]
        if len(matches) != 1:
            raise ValueError('Unique retained cleanup result required for both states')
        references.append(live_pilot.reference(matches[0]))
        times.append((start, end))
    if not times[0][1] <= _timestamp(receipt['at']) <= times[1][0]:
        raise ValueError('Cleanup transition must follow the retained failure receipt')
    allowed = {'status', 'started_at', 'ended_at', 'duration_seconds'}
    if (_manifest_bytes({k: v for k, v in old.items() if k not in allowed})
            != _manifest_bytes({k: v for k, v in new.items() if k not in allowed})):
        raise ValueError('Cleanup changed outside status and timing fields')
    # A removed result has a preceding removal intent; an absent confirmation
    # deliberately does not. Do not invent an intent for the later observation.
    intent_path = Path(references[0]['path']).with_name(
        Path(references[0]['path']).name.removesuffix('-result.json') + '-intent.json')
    intent = util.read_json(live_pilot.safe_path(intent_path))
    network = intent.get('network_snapshot') or {}
    labels = network.get('Labels') or {}
    if (any(intent.get(k) != v for k, v in identity.items())
            or intent.get('started_at') != old['started_at']
            or intent.get('confirmed') is not False or intent.get('status') != 'failed'
            or network.get('Name') != identity['network'] or network.get('Id') != identity['network_id']
            or labels.get('sample2.run') != identity['run_id']
            or labels.get('sample2.instance') != identity['run_instance_id']
            or network.get('Internal') is not True or network.get('Driver') != 'bridge'
            or network.get('Containers') != {}):
        raise ValueError('Previous removal intent network/Run identity mismatch')
    return dict(previous=references[0], current=references[1], runtime=live_pilot.reference(source/'runtime.json'),
                previous_intent=live_pilot.reference(intent_path), current_intent_required=False,
                identity=identity, allowed_changed_fields=['network_cleanup'],
                allowed_cleanup_changed_fields=sorted(allowed),
                cleanup_changed_fields=[k for k in sorted(old.keys() | new.keys())
                                        if _manifest_bytes(old.get(k)) != _manifest_bytes(new.get(k))])


def _manifest_binding(source, manifest, receipt):
    current = live_pilot.reference(source/'manifest.json')
    binding = dict(kind='exact_manifest_hash_v1', current=current,
                   failure_receipt=live_pilot.reference(source/'implementation-receipt.json'),
                   receipt_manifest_sha256=receipt.get('manifest_sha256'))
    if receipt.get('manifest_sha256') == current['sha256']:
        return binding, None
    # This fallback covers only the failed submission-fix write followed by a
    # successful idempotent cleanup. It never reconstructs arbitrary old fields.
    if manifest.get('submission_fixed') is not False or receipt.get('submission_fixed') is not False:
        raise ValueError('Cleanup-only recovery requires the original unfixed submission')
    matches = []
    for candidate in sorted(source.glob('manifest.json.*.tmp')):
        if not re.fullmatch(r'manifest\.json\.[0-9a-f]{32}\.tmp', candidate.name):
            continue
        raw = live_pilot.safe_path(candidate).read_bytes()
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            continue
        if (not isinstance(value, dict) or value.get('submission_fixed') is not True
                or _manifest_bytes(value) != raw):
            continue
        previous = {**value, 'submission_fixed': False}
        derived = _manifest_bytes(previous)
        if util.sha256_bytes(derived) == receipt.get('manifest_sha256'):
            matches.append((candidate, previous, derived))
    if len(matches) != 1:
        raise ValueError('Unique retained atomic temp must reproduce the exact receipt manifest hash')
    candidate, previous, derived = matches[0]
    expected = {**previous, 'network_cleanup': manifest.get('network_cleanup')}
    if _manifest_bytes(expected) != (source/'manifest.json').read_bytes():
        raise ValueError('Manifest changed outside the permitted cleanup receipt replacement')
    transition = _cleanup_transition(source, previous, manifest, receipt)
    binding.update(kind='confirmed_cleanup_manifest_transition_v1', transition=transition,
        derivation=dict(source=live_pilot.reference(candidate), field='submission_fixed',
                        from_value=True, to_value=False,
                        serialization='json.ensure_ascii_false.indent_2.utf8.lf',
                        output='receipt-manifest-projection.json', sha256=util.sha256_bytes(derived)))
    return binding, derived


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
            or receipt.get('snapshot_sha256') != util.sha256_file(source/'snapshot.json')
            or receipt.get('raw') != _inventory(source/'usage/raw')
            or snapshot.get('run_id') != identity['run_id']
            or snapshot.get('artifact_state') != 'fixed'
            or snapshot.get('artifact_sha256') != util.artifact_hash(source/'frozen')
            or snapshot.get('frozen') != _inventory(source/'frozen')):
        raise ValueError('Exact stopped PermissionError collection and fixed snapshot required')
    if evaluate.used_sequences(source):
        raise ValueError('Existing scoring needs its own reconciliation; never automatically repeat')
    binding, projection = _manifest_binding(source, manifest, receipt)
    return manifest, snapshot, binding, projection


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
    manifest, old_snapshot, manifest_binding, projection = _source(source)
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
    if projection is not None:
        projection_path = holder/'receipt-manifest-projection.json'
        util.write_new_json(projection_path, json.loads(projection))
        if util.sha256_file(projection_path) != manifest_binding['receipt_manifest_sha256']:
            raise ValueError('Saved projection differs from the receipt manifest hash')
    root = holder/'run'/manifest['run_id']
    intent = dict(schema_version=1, kind=KIND, source_path_private=str(source),
        source_run_id=manifest['run_id'], source_run_instance_id=manifest['run_instance_id'],
        source_inventory=before, source_condition_sha256=manifest['condition_sha256'],
        source_failure_sha256=util.sha256_file(source/'implementation-receipt.json'),
        manifest_binding=manifest_binding,
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
