# Observed zero/one-message presentation

`explore-observed` supplies the original full explore request once, with no source
preload and no later external user message. `staged-explore` supplies the approved
partition in two messages. Both use the existing fixed OpenCode 1.17.11
`serve` / `run --attach` adapter, original source/DB mounts, native session,
workspace, Run UUID, and continuous original Run deadline. No model, CLI,
permissions, compaction settings, evaluator, or resource ceiling is changed by
selecting this interface.

The existing staged v1 contract remains available. To explicitly select the new
observation meaning, the private request partition's `boundary_contract` requires
both `boundary_policy: response_tool_group_pause_v1` and
`observation_policy: resumption_first_change_v1`. The resulting controller
contract is `observed_presentation_v1`, with `presentation_mode` equal to
`staged_once` or `full_initial_no_additional`, and an explicit
`planned_additional_count` of 1 or 0. A missing or empty additional message is
still invalid for staged delivery. The control has no `additional.txt`; its
additional payload hash is null. Its initial prompt uses the same original
request, common prefix, path substitution and collection suffix as explore.

## Observation and evidence

The first completed response tool group with a net change in the frozen
implementation scope establishes the boundary. The controller pauses only the
owned worker and descendants for a stable observation, attempts a private
checkpoint, then unpauses. Build/scorer success does not select this boundary.
This observes the completed group, not the exact instant or individual tool
responsible for the first write.

- Staged delivery uses the existing intent, native API send, native ACK and
  independent gateway request-containment evidence. An unknown send is not retried.
- The control records `no_input_by_design` without calling the native prompt API.
  The gateway rejects an input arm or a fabricated native ACK for this mode.
- A durable `resumption_action` binds the contract, Run/session, held response,
  request and pre-resume checkpoint receipt. The gateway's release receipt
  records that action separately from an input ACK.
- The first observed request after release is recorded separately from the first
  request containing the additional input. Additional delivery/ACK are not
  applicable to the control, including when the boundary is unreached.

Both modes have `initial`, `before-resume`, `after-resume-first-change` and `final`
checkpoint labels. After the first post-resume implementation change group is
observed and its checkpoint attempted, extra response barriers stop. Model work
continues until its ordinary end, STOP or the original deadline. The final
checkpoint is taken only after owned shutdown. These are private artifacts;
intermediate checkpoints do not collect a final submission or run a scorer.
An unreachable observation and a failed archive have distinct null reasons.
Neither is silently replaced with a later snapshot. Ownership, session, send or
proof ambiguity still fails closed. All requests and pause/copy time remain in
the original accounting scope.

The new contract also retains the native server's full, unpaginated message
inventory in the private controller directory. Its hash and session are bound
in the ledger. It verifies the number, IDs and exact text hashes of external
user messages, distinguishing native synthetic messages. Missing evidence means
unknown, never proof of zero sends. The fixed attach CLI can omit JSONL events
when a response finishes immediately; completion can instead be proved by native
`step-finish` parts in that inventory. The proof source is explicit in provenance;
CLI events are never manufactured. Foreign or contradictory native records are
rejected. Legacy contracts retain their original evidence path.

## Frozen mixed allocation

`approved_observed_presentation_campaign_v1` is a narrow single-case campaign
kind. `research.presentation_allocation.generate(seed, tasks)` returns an
allocation and 100 assignments for exactly four task IDs. The algorithm is
`sha256_rank_five_strata_25_blocks_v1`: five consecutive strata of five blocks;
one control block per stratum; all four tasks once per block. The smallest
SHA-256 rank selects the control block and orders tasks inside each block.
Ranking encodes `seed + NUL + domain + NUL + value`, with `control:<stratum>`
and `task:<block>` domains and a text-value tie breaker. Tasks are initially
sorted. Each task has 20 staged and 5 control assignments.

The exact allocation, seed, algorithm, hash and assignment identities are
validated together and pinned in the approved plan and every epoch. No seed,
budget, output location or execution approval is invented by this helper.
Historical two-case and single-condition schemas are retained. A separate
single-condition campaign can exercise either mode for technical acceptance.
Its resource origin must use the existing shared-budget receipts; acceptance
does not silently start a new clock or become part of the scientific allocation.

The existing dispatcher admits fixed pending order into at most four active Run
slots, prioritizes unattempted assignments over technical retries, and retains
the same condition/task on a fresh-UUID retry. Evaluator concurrency remains one.
Block completion is not a dispatch barrier. Join the pinned allocation's
position/block/stratum to assignment, dispatch, runtime and resource-monitor
records to inspect actual start order and concurrent load. Scheduled-position
dispersion does not guarantee balanced wall-clock intervals. All attempts and
the original shared caps are retained; quality and delivery success do not
select replacements.

## Verification and handoff

The public tests use toy request text/files. The opt-in Docker probe uses the
already acquired fixed CLI and a container-local canned provider:

```sh
python outer/verify/probe-staged-docker.py \
  --opencode /private/path/to/opencode \
  --output /private/new-fixture-output \
  --presentation-matrix --concurrency 4
```

It exercises all four task bindings in both modes, overlapping four owned Runs,
post-observation model continuation, early completion, no later change and STOP.
The fixture worker contains Python/bash and the fixed CLI, not the production
.NET workload. Ordinary tests cover allocation mutation, duplicate/foreign
actions, zero-message proof, legacy regressions and existing budget/ownership
paths. Runtime/gateway images must be rebuilt and pinned for the final committed
controller. Fixture success does not establish real-provider acceptance or
production CPU/RAM/disk capacity.

Keep the exact research plan, prompt partitions, approval, raw native/gateway
records, generated app/DB and individual results in the approved private storage.
Freeze a new plan/allocation hash for a changed cohort; a prior all-staged plan
or acceptance receipt does not describe the mixed cohort. This implementation
neither starts acquisition nor rewrites original Runs/scores. Main merge,
real-provider acceptance and acquisition remain separate operations.
