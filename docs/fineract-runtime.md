# Fineract runtime and acceptance preparation

This implementation is a preparation and calibration path for issues #37–#39.
The finite controller is implemented; **no real-model campaign has run and the
revised evaluator still requires independent review**.
The historical .NET runtime, E1/E2/E3 artifacts and frozen plans remain separate.

## Fixed inputs and boundaries

- Apache Fineract 1.15.0: `d5636847ac556c30b437254c353f05526d172b97`.
- Verified source archive, canonical build scripts and public fixtures; no Git history.
- Temurin Java 21 and upstream Gradle 8.14.5; PostgreSQL 18.3. Dockerfile/base
  digests are pinned. Preparation records resolved platform image IDs, actual
  versions and source/recipe hashes; apt dependencies are resolved at build time.
- OpenCode 1.17.11 tarball is hash checked. No automatic update, model catalog
  fetch or default plugin import. Host credentials/settings are never copied to
  workers. The gateway is the only intended provider credential holder.
- Public corpus seal: `130fbd2a49f14a6c89bf2636f5a1f593d7d3e749d286c02c67a2a457db951634`;
  semantic corpus: `00eb6ec1fa1384ab535c0da5910559f02261ef154c3ea086c20816963e97acaf`.
  Independent material review accepted commit `5bb53ea070d9ee01cb52ec0f5175cd181d056fde`.

## Entry points

Run these from the repository root. All records are local ignored artifacts.
Commands create new UUID resources and exclusive records; failed attempts remain.

```text
python -m outer.fineract.prepare begin --repo . --archive <verified-source.tar.gz>
python -m outer.fineract.prepare collect --record <preparation>
python -m outer.fineract.seed create --preparation <preparation>
python -m outer.fineract.seed probe-four --preparation <preparation> --seed <seed> --worker-image <image>
python -m outer.fineract.service start-baseline --preparation <preparation>
python -m outer.fineract.service inspect --record <service>
python -m outer.fineract.service api-probe --record <service>
python -m outer.fineract.evaluation begin --preparation <preparation> --seed <seed> --private <private-spec> [--submission <exact-source.tar>]
python -m outer.fineract.evaluation collect --record <evaluation>
python -m outer.fineract.service start-evaluation --record <evaluation>
python -m outer.fineract.api_check begin --service <service> --private <private-client> --image <api-tests-image> --mode verify
python -m outer.fineract.api_check collect --record <api-check>
python -m outer.fineract.java_api prepare --preparation <preparation> --private <private-java-spec>
python -m outer.fineract.java_api collect-preparation --record <harness>
python -m outer.fineract.java_api begin --harness <harness> --service <service>
python -m outer.fineract.java_api collect --record <api-run>
python -m outer.fineract.service stop --record <service>
```

`seed` contains only wrapper distributions and downloaded dependency modules.
It excludes compiled project outputs, build caches, transforms, daemon state and
lock files. Four workers receive independent copies, volumes, HOME/state and
internal networks. Preparation uses internet access for official dependencies;
workers and canonical evaluation builds use offline dependencies.

`evaluation` restores the fixed build/test configuration and overlays submitted
`src/main` code into a fresh canonical tree. Exact submitted bytes and excluded
artifact paths are recorded. Private checks and submitted reports are never used
as a model's inputs. A successful build alone never assigns business quality.

The service executes only the JAR directory. It cannot read private unit-test
sources in the build volume. HTTP/DB tests run in a separate trusted client. The
Java API harness is compiled from fixed public SDK/fixtures plus private tests;
its dependency snapshot is hashed, copied and verified before each run. Unlike
worker seeds, that evaluator-only snapshot includes compiled canonical fixtures.
Old reports are excluded and each API run creates new JUnit evidence. Final
bindings can require an exact case inventory and task-specific selectors; missing
or substituted cases cannot pass. Each task evaluation requires a fresh DB, so
loan/savings fixture-created accounting metadata cannot alter charge baselines. The client
shares its owned app's network namespace so the public localhost TLS certificate
works without changing host TLS/security settings. No ports are published.

`stop` checks ownership, retains logs, stops the owned app/database, and preserves
containers and volumes. There is no prune or historical-network cleanup. New
internal networks use non-overlapping /28 ranges from `10.239.0.0/16`; Docker
rejects concurrent allocation collisions. Check host routes before using this
pool on another machine. It does not reconfigure Docker's global address pools.

## Resource observations and limits

The local Docker VM exposes about 30.2 GiB and 16 CPUs. Core/provider builds use
one Gradle worker, a 2 GiB Gradle heap, 768 MiB test/compiler heaps and a 6 GiB
container limit. The public OpenAPI client generation required an 8 GiB Gradle
heap and 12 GiB preparation container; failed 2/4 GiB attempts are retained.
This larger preparation is separate from the four-worker setting. The approved
common runtime prepares both public Java clients serially from the pinned source
and dependency-only seed. It exports only SDK JARs, classes, resources and a hashed
inventory. Each 5 GiB worker gets an independent copy. The Gradle adapter consumes
these outputs for the two client modules; production compilation and all test
tasks remain enabled. Private evaluator snapshots and opaque project Gradle
history never enter worker inputs. A fresh offline production/test build and
a real public API test passed in one 5 GiB worker, with 3,936 production and 626
test classes compiled. Measured peak memory was 5,168,979,968 bytes, below the
5,368,709,120-byte cap, with zero OOM events. The dual-SDK export contains 3,852
files (32,869,727 bytes). Earlier single-client and adapter failures are retained.

Run evaluations serially after a worker wave releases memory. Four worker
containers, their databases, SDK preparation and evaluation must not all run at
once. The synthetic four-worker probe exercises each dedicated PostgreSQL database
and gateway, rejects cross-worker and internet connections, and retains stopped
source artifacts. Its local SSE replies and usage are explicitly synthetic;
this is infrastructure proof and makes no provider request.

## Shared finite admission contract

`campaign.py` constructs the explicit 21-row matrix: 15 scheduled assignments
in waves 4/4/4/3 and six `not_run` rows. It binds exact initial messages, requests,
source/semantic inventories, actual execution specs, runtime and evaluator hashes.
A public-contract-only corpus cannot authorize model execution.

`campaign_budget.py` uses a shared SQLite transaction before each upstream send.
It bounds 1,700 total requests, the campaign
wall deadline and 90 million observed input+output tokens. Reservations survive
crashes, duplicate request IDs are rejected and closed admission never reopens.
Missing usage remains null; known usage is a lower bound. In-flight responses can
cross the observed-token threshold; it is not a prepaid monetary guarantee.
The optional gateway integration leaves historical callers unchanged.

`controller.py` enforces four concurrent Runs, 15 normal runs plus at most two
technical retries, 1,800 seconds/run, 600 seconds/provider request, 30,600 total
run seconds and 14,400 wall seconds. Model errors, business failures, normal build
failures and normal timeout are not technical-retry reasons. Durable allocation
intents precede container creation. Recovery closes campaign admission, fences
each gateway before stopping workers, and collects exact submissions. It never
replays model attempts. Frozen submissions can be scored again with new result
identities. A stopped campaign, exact artifact hashes and private execution
specifications are required. A report with a nonzero client exit cannot pass.

Readiness fails closed until the exact material review, four-worker runtime
proof, independently approved calibrated Java/API/DB/COB evaluator and a recent workspace-bound read-only
proof of Go `Use balance` OFF are present. A key, Go URL or usage response does
not prove paid fallback is disabled. Auth/billing settings must not be changed.
The evaluator approval must bind its exact hash; material approval cannot stand
in for evaluator approval, and calibration's own passing result is not that review.

## Evidence as of 2026-10-16

- Independent corpus check: 18 tests, no skips; two builds reproduced all files.
- Linux controller CI at `76d2ec9`: 370 tests, nine existing optional skips, no errors.
  The subsequent immutable-image binding check also passed in the focused suite.
  Windows full-suite behavior is not claimed; retained newline-sensitive tests
  require the Linux path. Node is installed only in the controller-test image.
- Public Java helper checks: 2 delinquency and 9 savings tests passed.
- Real public HTTP/API/COB: 3 tests passed against a disposable PostgreSQL service.
- Draft private Java calibration: baseline and mutant each 5 failed/3 passed;
  candidate 8 passed. All three fresh offline builds produced a real boot JAR.
- Private charge HTTP/DB calibration: baseline 13 failed/9 passed; candidate
  22 passed; mutant 2 failed/20 passed, using the same hashed client bundle.
  These are calibration checks, not model results.
- Delinquency and savings/fixed-deposit reference: 47 API cases passed and 40
  direct PostgreSQL checks passed. The private fixture base preserves post-test
  loan state for SQL checks; inherited public loan cleanup otherwise repays
  loans and removes active delinquency tags before inspection. Earlier attempts
  are retained. Independent review required stronger filtered READ authorization,
  all-frequency ordinary-date preservation, and repeated-post monetary-state
  checks. The replacement reference passes 53 API cases and 28 charge checks;
  direct PostgreSQL checks also pass 46/46. Targeted regression replay and the
  new frozen release remain under validation.
  Exact baseline/mutant and review bindings remain private.
- Four concurrent dual-SDK workers passed dedicated DB/gateway, cross-worker,
  internet, credential/mount and resource-isolation checks. A separate synthetic
  interruption recovered all four original UUIDs and exact source artifacts,
  with gateway fencing and no replay. Public corpus recovery verified all 7,804
  output files in a new volume.
- Model calls: **0**. Current Go usage GET succeeds but does not expose the
  workspace paid-fallback setting. That proof remains unavailable.

Private tests, candidate/mutant sources, detailed service logs, credentials and
raw responses stay out of Git and worker inputs. An eventual evaluator release
must freeze exact expected case identities, distinguish fixture/environment
failure from application failure, calibrate meaningful mutants, cover all
preservation requirements, and treat observable API behavior as authoritative
rather than require one internal helper implementation.


## Finite execution and recovery commands

```text
python -m outer.fineract.sdk begin --preparation <preparation> --seed <public-seed>
python -m outer.fineract.sdk collect --record <public-sdk>
python -m outer.fineract.execution bind --preparation <preparation> --release <private-release.json> --output <new-public-binding>
python -m outer.fineract.public_probe begin --settings <settings.json> --plan <plan.json> --output <new-public-proof>
python -m outer.fineract.public_probe collect --record <public-proof>
python -m outer.fineract.controller create-probe --settings <settings.json> --plan <plan.json> --output <new-synthetic-campaign>
python -m outer.fineract.controller run --record <synthetic-campaign>
python -m outer.fineract.controller create --settings <settings.json> --plan <plan.json> --approval <approval.json> --readiness <readiness.json> --output <new-live-campaign>
python -m outer.fineract.controller recover --record <interrupted-campaign>
python -m outer.fineract.controller score --record <stopped-live-campaign>
python -m outer.fineract.execution restore-bundle --binding <public-binding> --output <new-recovery-proof>
```

These commands preserve prior records, containers and volumes. Output paths must
be new. The synthetic interruption option `run --halt-after-launch` is rejected
for live campaigns. Keep the PC and Docker running while a local build or
campaign is active; sleep or shutdown interrupts execution and requires recovery.
