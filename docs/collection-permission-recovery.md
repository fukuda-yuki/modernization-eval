# Stopped collection PermissionError recovery

The collector can finish `snapshot.json` and `frozen/` before a mutable JSON
replacement fails. A retained snapshot alone does not establish successful
collection, and the original `implementation-receipt.json` remains a fault.
Windows may deny replacement while a normal read handle is open. The original
handle owner cannot be inferred from the exception; permanent access failures
must remain failures.

`run.collect_run` now enables retries only for atomic JSON replacement during
that collection call. Only `PermissionError` with Windows error 5 or 32 is
eligible: at most five replacement attempts, with backoffs of 50, 100, 200 and
400 ms. Each retry uses the same already flushed temporary file. No destination
deletion, permission change, content rewrite or whole-collection retry occurs.
Other exceptions propagate immediately. Exhaustion retains the old target and
temporary file. Backoff polls STOP and an optional caller monotonic deadline.
STOP permits the first preservation write but prevents waiting and retrying.
Other atomic-write callers retain their previous single-attempt behavior.

For a stopped, completed schema-2 Run with a bound `PermissionError` collection
receipt and a complete matching snapshot/frozen tree, the separate adapter is:

```sh
python -m research.collection_recovery \
  --repo /absolute/clean-repair-checkout \
  --source /absolute/acquisition-batch/pair-N/RUN \
  --destination /absolute/separate-recovery-output \
  --recovery-source-repo /absolute/original-fixed-checkout \
  --score --timeout 1800 --stop-file /absolute/recovery-STOP
```

Omit `--score` for collection verification only. The source checkout is used by
the existing saved-scoring provenance validator; the repair checkout must be
committed and clean for scoring. The frozen condition, profiles, evaluator,
specification and image bindings are unchanged. This command does not prepare
new runtime assets, start a worker, send a model request or update a campaign
ledger. The command is an implementation interface, not acquisition permission.
Do not switch a running campaign to this code. Any future acquisition adoption
requires its normal source/lock pinning outside the active campaign.
Mandatory browser scoring uses the source epoch's hash-bound browser pin through
the existing validated browser-environment adapter. Missing or changed pins hold
the recovery rather than falling back to whatever Node/browser is installed.
The caller must serialize `--score` with the existing evaluator lane, as for the
saved-scoring API. The standalone adapter does not acquire the campaign-wide
evaluator lease or pause an active campaign; use its existing owner or an
explicitly reconciled quiet postprocessing window.

The adapter checks an existing source controller lock without writing it, copies
the complete Run outside its acquisition directory, and retains the prior
snapshot/frozen tree beside the working derivative. It uses the ordinary
collector and requires exact snapshot equality apart from collection time,
including the DB and file inventories. Both the original and recollected
snapshot are retained; the working derivative keeps the original timestamp and
snapshot bytes after this comparison. The original collection-fault receipt is
also kept unchanged. The ordinary terminal verifier then checks gateway/native
session evidence, usage, input and stopped/absent owned resources before scoring.
Missing evidence or a mismatch blocks scoring; a quality failure cannot trigger
a new model attempt.

The failure receipt's manifest hash normally must match the current manifest
exactly. One narrow exception handles a later, successful cleanup confirmation:
the failed atomic writer's uniquely matching `manifest.json.<uuid>.tmp` must
contain `submission_fixed=true`. Changing only that field to `false` in memory,
then serializing with the original key order, `ensure_ascii=False`, `indent=2`,
UTF-8 and one LF, must reproduce the receipt's original SHA-256 exactly. Both
the failure receipt and current manifest must still say `submission_fixed=false`.
This is a verified derivation of the receipt-bound bytes, not a relaxed hash check.

Replacing only that derived manifest's `network_cleanup` must then reproduce the
current manifest bytes exactly. Within cleanup, only `status`, `started_at`,
`ended_at` and `duration_seconds` may differ. The transition is strictly
`removed` to `absent`; both receipts must be confirmed and retain containers,
with identical Run ID/UUID and network name/ID bound to the stopped runtime.
Each cleanup must exactly match one retained result file. The old removal also
requires its corresponding intent and matching owned, unused internal bridge
snapshot. The later absence confirmation does not require a removal intent.
Timezone-bound timestamps must place the failure receipt after the old cleanup
and before the new one. Other changes, ambiguous evidence or an unreproducible
old hash are rejected before creating the recovery destination.

Only the separate output receives `receipt-manifest-projection.json`, whose hash
is the old receipt hash. `intent.json.manifest_binding` records its source temp,
exact derivation recipe, current/failure/cleanup/intent/runtime references and
the allowed and observed differences. No source manifest, receipt, temp or
cleanup record is rewritten. The complete source inventory is still checked
before and after recovery.

The derivative keeps the source Run UUID. Its receipt records zero model calls,
zero new acquisitions and zero additional acquisition usage. Its normalized
usage is a reference to the original acquisition, never another row to sum.
The original implementation duration and budget fields remain unchanged. The
explicit timeout is for this local postprocessing operation; the scorer receives
only its remaining time. STOP is checked between phases and during replacement
backoff; the existing scorer retains its own timeout/cleanup behavior. A STOP
arriving during scoring is checked again on return and does not make that
attempt successful.

One exclusive output directory per source UUID prevents implicit replay into the
same destination after success, failure or interruption. Prior scoring traces
are refused rather than repeated. Partial outputs are retained. `result.json`
distinguishes collection evidence, scoring state, source preservation and overall
recovery completion. An evaluator fault, mismatch, incomplete observation or
cleanup failure is not a completed scoring recovery. This adapter does not
replace the original campaign result or automatically adopt the derivative;
that remains an explicit downstream evidence reconciliation.

Tests use public toy requirements, synthetic raw requests and a small SQLite
database. Windows error injection runs on all platforms; an actual Windows
read-handle test is separately guarded and must run on Windows. The suite also
exercises the ordinary terminal verifier and a real local stub-evaluator process
whose deliberately wrong artifact path is rejected. No saved research Run,
production evaluator workload or provider request is used by these tests.
