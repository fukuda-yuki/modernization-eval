# Pipeline owner loss and technical-attempt exclusion

The acquisition pipeline's heartbeat is a progress projection. Its per-Run
resource observers do not independently own full worker shutdown when the
controller disappears. A separate process in the same tool Job is not proof of
an independent lifetime.

`research/pipeline_guard.py` is a narrow outer adapter for a frozen acquisition
checkout. The original config, genesis, plans, model/prompt/allocation, total
budgets and time origin stay unchanged. Original runtime, evaluator and helper
imports continue to use that checkout. The current adapter's small
`acquisition_pipeline.run_pipeline(..., attempt_policy=...)` control change reads
explicit technical exclusions; it is loaded separately and its actual code hash
is recorded with the guard and policy hashes. It is not represented as the old
controller's bytes. New epochs retain the original measurement-input/runtime pins.
No epoch migration framework or alternate acquisition engine is introduced.

The implementation reuses existing owner locks, cumulative usage collection,
launcher child-handle termination, observer closure and runtime fence/stop/network
cleanup APIs. Historical direct commands retain their default behavior; use the
guarded entrypoint for this supervised execution and exclusion policy.

## Windows launch contract

Use the ordinary, installed Windows Terminal application from the execution PC.
The operational acceptance must verify this exact adapter build, frozen checkout,
and launch route without a provider. For example (replace all paths locally):

```powershell
wt.exe -w new new-tab python.exe -B -X utf8 C:\guard-checkout\research\pipeline_guard.py launch C:\campaign\config.json --repo C:\frozen-checkout
```

The launch parent must report `IsProcessInJob=False`. It starts the supervisor
with the existing `CREATE_NO_WINDOW` method, and the supervisor checks its own
Job non-membership before creating the controller. The controller retains a
read-only handle to that supervisor; the supervisor retains its child handle.
There is **no production breakaway fallback** and no Job-limit change, service,
Task Scheduler registration, authentication change or security-setting change.
An in-Job invocation fails before starting a controller. On Linux the equivalent
synthetic acceptance uses a new session; it is not Windows acceptance.

The invoker may exit after the hash-bound `ready.json` appears. Registration
records the starter/supervisor/controller identities, the original config
reference, adapter hashes and separation observation. Readiness requires a
registered controller, a bound `controller-owned.json` published strictly after
that child acquires the campaign lock, an observed held lease and a successful
independent budget sample. The peer-loss watcher also starts only inside that
ownership scope and stops before its lock is released. A failed pre-owner launch
can terminate only its own child; it cannot emit campaign STOP or touch another
owner's attempts. After an owned child exits, cleanup reacquires the campaign lock
and leaves a different owner untouched if that acquisition loses a race.
Logs and receipts are under `_supervision/<invocation-uuid>/`.
Failure to observe registration does not justify killing an ambiguous process;
inspect that invocation's logs and result first. Guarded launches are serialized
by the supervisor lock. A contender for a direct controller's campaign lock gains
no stop authority until it actually acquires that lock.

Job non-membership plus a synthetic parent/Job-exit test is scoped evidence. It
does not guarantee arbitrary long uptime, survive a machine outage or stop
containers if every owner process is deliberately killed at once. Do not label
the implementation live-accepted until the PC runs the actual adapter acceptance.

## Ownership and stopping

Process checks bind PID **and OS creation identity**. Windows checks retain the
opened process handle; Linux checks use boot identity and process start ticks.
No process is killed from a saved PID. Only `Popen` handles created by this
invocation are eligible for launcher termination. A missed/late heartbeat alone
is never a stop reason. The existing OS owner locks are also checked.

The controller-side adapter detects supervisor loss. The supervisor detects
controller loss, owner-lease loss, STOP, original campaign wall time, Run time,
cumulative observed request/token/run-time bounds, and stale independent budget
sampling. Brief incomplete JSON writes get a bounded observation retry; the
existing time origin is never reset. Resource observers remain the resource
threshold authorities while both sides supervise their liveness through the
original controller's existing checks.

Guarded preparation has two additive owner records. The controller writes a
hash-bound `pipeline-preparation.json` after reserving the attempt. The existing
watcher's `register` callback writes `pipeline-prepared/<uuid>.json` after the
frozen profile creator finishes, before pair reservation and dispatch. This
does not change the frozen profile/runtime/evaluator helpers or their pins.
`run.create_run` legitimately saves a manifest before `profiles.create` adds its
UUID; an active bound preparation may therefore lack that key. The guard records
this as pending usage with a null measured duration, while retaining all earlier
observed usage and the original wall clock. It does not invent a completed Run
or replace missing measurements with zero. A prepared receipt, pair reservation,
dispatch, runtime, raw usage or prior complete observation ends that exception.
Wrong identities and later disappearance of observed manifest/runtime/usage or
dispatch evidence remain faults; the last successful budget observation remains
in the supervisor result. There is no preparation-specific budget reset.

Historical incomplete attempts without this owner evidence are not retroactively
declared to be preparing. The existing exclusion command can instead seal their
confirmed pre-dispatch interruption as described below. It never manufactures a
manifest UUID, completes profile creation, or retrofits preparation receipts.

On a fault the guard publishes pipeline STOP first, then the exact phase/Run stop
markers. Bounded stop helpers call the ordinary `stop_owned` (gateway admission
fence, then owned worker/gateway stop) before lease-aware `request_stop` records
terminal evidence and network cleanup. Thus physical stop does not depend on a
hung controller releasing its Run lease. All stop helpers start before blocking
Docker inspection. After controller exit the scope is scanned again for late
allocation. Ownership mismatch, Docker outage, absent ACK, evidence-write failure
or a retained live lease stays explicit; it is not successful cleanup.

Observer ACK/exit evidence is reported separately from worker/network closure.
The old observer closer runs in a bounded helper and cannot hold the safety
timer indefinitely. A dead historical observer without an ACK is not retroactively
given one. Investigate unresolved closure before another invocation. STOP is
never automatically removed, and a failure never automatically restarts a Run.

## Non-adoption and a fresh-UUID retry

An interrupted or individually failed technical attempt is **excluded from
adoption**, not rescued by another score. No original is deleted or rewritten.
There is no derivative-result import or collection/scoring command in this change.
PR33 and its originals are not modified by this path.

After independently confirmed worker/gateway stop and network cleanup, the PC can
record an explicitly authorized technical exclusion:

```powershell
python -B -X utf8 C:\guard-checkout\research\pipeline_retry.py --repo C:\frozen-checkout --config C:\campaign\config.json --record C:\campaign\epochs\001\data\pair-1\pipeline-attempt.json --reason controller_interrupted --authorization-reference USER_INSTRUCTION_REFERENCE
```

`--reason technical_failure` instead requires existing provider/infrastructure
classification, a bound original collection-fault receipt, or the explicitly
adjudicated infrastructure evidence described below. Ordinary low
quality, product/build failure, or an already adopted slot is not retry authority.
The user-instruction reference identifies authorization that already exists; the
command does not manufacture approval. It is limited to single-condition attempts
using the existing shared reserved-attempt budget.

The command acquires the existing pipeline/Run locks, binds the original
reservation/UUID, checks physical closure, and stores `_exclusions/<uuid>.json`
and an `attempt_excluded` event. It creates a missing acquisition receipt if
needed; an existing receipt and every original Run/scoring file remain unchanged.
The inventory excludes only the Run root's `controller.lock`, explicitly recorded
as `inventory_excluded_paths`. That OS lock remains held, without reading its
locked bytes through a second handle. Lock liveness probes likewise use file-size
metadata and the OS lock operation. Nested files with the same name remain hashed.
A repeated identical exclusion is idempotent. Original source changes, duplicate
selection, partial journals, competing owners or conflicting exclusions are held.

### Explicit infrastructure adjudication

Acquisition, persistence and evaluation infrastructure faults use the same
`--reason technical_failure --adjudication C:\private\ruling.json` operation.
The private ruling supplies explicit attribution; the existing original failure
records establish what occurred. New exception classes or OS error numbers do
not require new eligibility branches. The operation does not invent an approval,
repair data, run an evaluator, change a score, or dispatch a replacement.

The old `--evaluation-adjudication` spelling remains an alias. Existing
`evaluation_infrastructure_exclusion_adjudication_v1` rulings with classification
`evaluation_infrastructure_fault` retain their evaluation-only checks and receipt
format; they need not be rewritten. New rulings use this common contract:

Prepare the private ruling **outside the original Run**, from the retained
original evidence and an explicit adjudication. Do not edit any acquisition,
evaluation, manifest or event ledger to make it eligible. Its JSON contract is:

```json
{
  "kind": "infrastructure_exclusion_adjudication_v1",
  "run_id": "ORIGINAL_RUN_ID",
  "run_instance_id": "ORIGINAL_UUID",
  "authorization_reference": "EXISTING_USER_INSTRUCTION_REFERENCE",
  "decision": {
    "classification": "infrastructure_fault",
    "low_level_cause": "unknown",
    "product_defect": false,
    "quality_selection": false,
    "ordinary_timeout": false,
    "retry_requires_new_uuid": true,
    "basis": "Existing explicit adjudication and its evidence basis"
  },
  "config": {"path": "ABSOLUTE_CONFIG_PATH", "sha256": "ACTUAL_SHA256"},
  "attempt": {"path": "ABSOLUTE_ATTEMPT_PATH", "sha256": "ACTUAL_SHA256"},
  "acquisition": {"path": "ABSOLUTE_ACQUISITION_PATH", "sha256": "ACTUAL_SHA256"},
  "pipeline_evaluation": null,
  "manifest": {"path": "ABSOLUTE_MANIFEST_PATH", "sha256": "ACTUAL_SHA256"},
  "runtime": {"path": "ABSOLUTE_RUNTIME_PATH", "sha256": "ACTUAL_SHA256"},
  "pair_journal": {"path": "ABSOLUTE_PAIR_JOURNAL_PATH", "sha256": "ACTUAL_SHA256"},
  "snapshot": null,
  "evaluation_index": null,
  "evaluation_record": null,
  "fault_evidence": [{"path": "ABSOLUTE_ORIGINAL_FAULT_EVIDENCE_PATH", "sha256": "ACTUAL_SHA256"}]
}
```

Each reference binds the actual existing file and hash. `authorization_reference`
must match the CLI argument. The common ruling binds the exact campaign, reserved
attempt and UUID, original acquisition receipt, manifest, runtime identity and
single-Run dispatch journal. The journal reader also verifies its referenced
implementation receipts. Acquisition usage must still equal the original raw
observations and manifest measurements; missing earlier costs cannot become zero.
All old reservations, observed token components and unknown totals remain counted.
A dispatched Run with no requests retains its dispatch and elapsed time.

`snapshot`, `evaluation_index` and `pipeline_evaluation` reference their original
files when present, or must explicitly be null when absent. Later appearance,
loss or mutation is rejected. `evaluation_record` references the latest original
record when an evaluation exists; before evaluation it is null. Unindexed
scoring artifacts stay held. A failed acquisition need not have a snapshot or a
score: the exclusion neither fabricates those files nor changes `acquired`.

Before evaluation, `fault_evidence` must include a fixed acquisition error/fault
receipt or the fixed journal/implementation receipt recording the failure. Extra
original controller evidence may come from the Run's `evidence` or `_controller`
directory or the batch's `pipeline-evaluation.log`; arbitrary external files or
product files do not supply fault authority. A bare exception string or an error
number is insufficient. The common branch rejects retained product failures,
recorded timeouts/budget stops and explicit timeout exception records regardless
of the ruling. Its classification is never inferred from a new exception name.

The common branch also checks the original campaign/epoch Run ceilings (and
condition wall budget, when present) against the saved duration, Run end,
watcher-fault time and Run stop-request time. Reaching a ceiling blocks exclusion
even when the saved end reason is `operator_stop`. Missing bounds, invalid times,
foreign stop identity or an operator stop without its original marker stay held.
The frozen watcher may retain only `RuntimeError` for a ceiling, cumulative budget
or explicit STOP. Such generic exception records remain ambiguous and held even
below the Run ceiling; an adjudication cannot restore the lost distinction.
This checks retained evidence without changing the producer, its limits or clocks.

When an evaluation exists, the same ruling uses the evaluation checks below.
Fault evidence must then belong to the selected evaluation directory, its
sequence's original stdout/stderr logs, or the original `pipeline-evaluation.log`.
The ruling and all evidence remain private and unchanged for resume validation.

The saved scoring must be an unadopted `evaluator_fault`, exit code 2, explicitly
not timed out, without mismatches or numeric quality. Output, when produced, must
be an identity-bound error for the fixed artifact/spec/evaluator. Missing output
is eligible only when the original record never claimed an output hash.
The original missing-output writer omits `directory` in its record and index.
That format is resolved through the adjudicated record reference only when it is
the unique matching `evaluations/fault-SEQUENCE-UUID/record.json` for the latest
index entry; neither original file is rewritten. Scored
results, quality failures, product-prerequisite failures and retained partial
product failures (including earlier or HTTP-only results) block this branch.
An ordinary timeout, exception prose or SQLite error number alone never permits
exclusion. The explicit ruling is required in addition to these checks; its
`low_level_cause` remains unknown.

When evaluation occurred, its recorded closure must be confirmed: an owned browser-resource
receipt is checked with the existing saved-cleanup validator; a no-browser-resource
receipt must bind the Run, artifact/spec and removed evaluator container. Missing
or ambiguous closure stays held. The command also holds the existing pair lock to
exclude concurrent evaluation, in addition to the campaign/Run locks and existing
worker/gateway physical-stop checks.

The same `_exclusions` receipt and `attempt_excluded` event retain the original
acquisition/evaluation bytes, all costs and missingness, shared reservation count
and campaign clock. Resume revalidates the ruling and its fixed evidence, skips
only that attempt, and uses ordinary same-allocation dispatch with a fresh UUID.
There is no automatic conversion of every exception into retry authority.

Deploy this control-adapter change only after the current initial assignments
finish and the campaign/evaluator are safely stopped, then use the existing retry
stage. Do not replace running code or modify frozen protocol/runtime/evaluator
pins. The initial Windows/cloud acceptance of the base owner adapter does not
constitute execution-PC acceptance of the common adjudication extension.

### Interrupted preparations

The same `--reason controller_interrupted` command has a preparation-only branch
for a partial or absent manifest and a Run which never dispatched. Its identity
comes from the unique reservation event, attempt and hash-bound fixed phase.
Under the supervisor, campaign and pair locks (and the Run lock if present), it
requires the retained campaign STOP and checks for any dispatch, runtime, raw
request, native/staged-send or completed-implementation evidence. Such evidence
or an ambiguous journal prevents an unsent classification. A missing Run directory
or `controller.lock` does not itself prevent closing an unstarted reservation.

For an entered pair scope, the existing observer closer must confirm the exact
phase's startup/scope/shutdown bindings, an empty dispatched set, ACK and process
exit. A reservation that never entered its pair scope can have no observer or Run.
Docker must successfully report no containers or networks bearing the planned
instance UUID; even a stopped allocated resource prevents the unsent classification.
The exclusion seals those observations and their evidence references outside the
original Run. Only that complete proof permits zero dispatched/request/token
counts; absent/null measured duration stays absent/null. Repeated exclusion is
idempotent. The controller and guard validate the sealed preparation exclusion,
so the original incomplete manifest no longer blocks ordinary new-UUID selection.

A refused preparation exclusion appends `_exclusion-holds/<uuid>/<receipt>.json`
with its reason, raw evidence hashes, known token components and unknown totals.
It writes no successful exclusion/acquisition receipt and authorizes no retry.
Previously observed execution in those holds cannot become an unsent zero-cost
attempt merely because later evidence disappeared. Genuine dispatched attempts
continue to use the existing complete-Run technical-exclusion requirements.

The receipt retains factual failure evidence, all observed usage, missing request
terminals and unknown totals. Controller disappearance with no proven cause is
recorded as `cause=unknown`, not a model/provider failure. Measured null durations
remain null; start-to-confirmed-cleanup time is a separate conservative budget-only
charge. A future controller reconstructs costs once per original attempt, and the
independent guard also reads those costs from originals. Original reservations
continue to consume the same shared attempt ceiling. Nothing resets campaign time.

When the PC later resumes through the guarded entrypoint, the adapter removes only
explicitly excluded attempts from candidate/evaluation selection. It does not adopt
or re-score their artifacts. All their reservations and costs remain counted.
The ordinary epoch/dispatch path assigns a **new UUID** to a replacement, preserving
the fixed task/condition/model/prompt allocation. An excluded receipt does not
exclude a subsequent attempt: another failure needs its own applicable existing
policy or explicit exclusion. Successful low-quality replacements remain eligible
for ordinary adoption; quality never controls technical retry.

This command does not remove STOP, launch a model, or approve scientific execution.
Actual model restart remains a separate PC operation after runtime acceptance and
reconciliation. It does not imply that a passed synthetic test is live acceptance.
The execution PC must seal each interrupted reservation before its existing
authorized resume procedure, retaining the original STOP bytes as evidence.
All original reservations still consume the same ceiling (including a ceiling of
150); exclusion never refunds attempts or resets prior costs or the time origin.

## Providerless validation

```text
python -B -m unittest discover -s research/tests -p "test_pipeline*.py" -v
python -B -m unittest discover -s outer/tests -v
python -B -m unittest discover -s research/tests -v
```

Windows tests create and terminate **only their own synthetic Job objects**.
They test detached flags, breakaway denial, and survival of a test Job separately
from membership in an outer Job; they never change an existing Job. The positive
production route must additionally be run from the verified Windows Terminal
entrypoint on the PC. Record adapter hashes, process creation identities, parent
termination versus Job termination, child survival, STOP/limit response, actual
owned closure, and graceful completion. Report unsupported tests as skipped.

An opt-in Linux/Windows Docker stop fixture uses an already installed
`python:3.12-slim-bookworm` image ID, two internal-network toy containers and an
actual killed synthetic controller. Set `SAMPLE2_PIPELINE_DOCKER_FIXTURE=1` and run
`test_pipeline_owned_stop.py`. It performs no pull or provider request, verifies
admission/stop/cleanup receipts, and removes only its own synthetic containers.
Neither this fixture nor ordinary unit tests constitute live model acceptance.

`test_pipeline_preparation.py` additionally runs the actual guard and controller
processes with the ordinary four-lane pipeline, watcher, pair execution and
initial Run writer. Synthetic profile/resource/model/evaluator boundaries hold
four UUID-less manifests across several independent samples, then allow normal
preparation, dispatch and completion. A mixed two-ready/two-preparing case and
post-dispatch manifest loss/identity mismatch exercise the same composed path.
This is providerless control-stack coverage, not a real observer/Docker or model
acceptance claim; those PC checks remain separate for every changed adapter head.

The preparation retry composition additionally interrupts two prepared and two
UUID-less lanes before any request, uses the real exclusion API, and resumes the
same slots with four new UUIDs against a local artificial HTTP provider. It checks
original-byte preservation, retained STOP, zero requests before exclusion, one
request per replacement, cumulative prior usage and the unchanged ceiling. A
second case exhausts 150 reservations and refuses replacement requests despite
successful exclusions. Observer/Docker observations in this composition are
explicit synthetic boundaries; the separate real Docker fixture and execution-PC
observer acceptance cover those physical interfaces.
