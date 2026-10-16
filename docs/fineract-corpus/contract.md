# Issue #36 producer contract (v1)

Scope: fixed materials only; consumers in #37–39 own execution, dependencies,
private scoring, and admission. No model call is performed here.

```
python -m tools.fineract_corpus fetch --output /absolute/cache/fineract.tar.gz
python -m tools.fineract_corpus generate \
  --source-archive /absolute/cache/fineract.tar.gz \
  --tasks /absolute/public/tasks.json --output /absolute/new-bundle
python -m tools.fineract_corpus verify --bundle /absolute/new-bundle
```

The validated environment is CPython 3.11/3.12 on Linux/POSIX. Preserve executable
mode bits in the corpus filesystem; on Windows, use a Linux container/WSL filesystem.
Native Windows generation is not validated. `generate` is offline, Python standard
library only, and refuses an existing output.
The archive must match the repository/SHA/archive digest in `source.lock.json`.
`tasks.json` schema:

```json
{
  "schema_version": 1,
  "tasks": [{
    "id": "FINERACT-DELINQUENCY-RESUME",
    "request_file": "requests/resume.md",
    "request_sha256": "<64 lowercase hex characters>",
    "evaluation_spec_sha256": "<64 lowercase hex characters>",
    "evaluation_spec_kind": "public-contract",
    "target_features": ["loan-delinquency"],
    "common_features": ["business-date-cob", "events", "persistence"]
  }]
}
```

Relative request paths resolve under the tasks.json directory. Input is public
requests plus opaque evaluation-spec hashes; NEVER provide private evaluator paths,
gold patches, result directories or raw model logs. Supported task IDs and feature
priorities are frozen in `selection-policy.json`; consumers may bind the final
public request bytes and real evaluation specification hash, not change priorities.
A changed public request must be reviewed against that policy before use.

Outputs:

- `corpus/source/`: all 7,772 pinned source files, including public tests, docs,
  build resources, LICENSE_SOURCE/NOTICE_SOURCE. No git history or host config.
- `corpus/reconstructed/`: clearly marked researcher-created baseline materials.
- `corpus/common/authority.md`: provenance/currentness decisions, common to all.
- `navigation/index.md`: global feature/role navigation; headings and references only.
- `tasks/<task>/request.md`: exact final requirement bytes, common to all conditions.
- `conditions/<task>/<condition>/initial.md`: exact UTF-8 initial message.
- `manifest.json`: source inventory hash, semantic corpus inventory/hash, document
  units with byte ranges and provenance, feature/evidence ledger, fixed selection
  order and omissions, seven condition rows per task, prompt/body/navigation/request/
  evaluation-spec hashes, byte sizes and explicitly labelled proxy token estimates.
- `conditions.md`: primary readable seven-condition set table per task.

Conditions: `explore-indexed`, `selection-indexed`, `small-indexed`,
`medium-indexed`, `large-indexed`, `explore-original`, `medium-original`.
Small/medium/large are strict prefixes of complete source sections under
8/32/128 KiB of body bytes. A unit that does not fit stops the prefix: no skipping,
no truncation. Generation fails if the actual sets are not strictly nested.
The same selection index is in all index-only and body messages (including
medium-original); it is distinct from global navigation. Both exploration messages
have no selection index or body. The global navigation file is only mounted for
indexed conditions. Original source paths and all semantic bytes stay identical.

Consumer workspace: mount ONLY `corpus/` contents, the task's `request.md`, and,
for indexed rows, `navigation/index.md`. Deliver `initial.md` as the initial message.
Do not mount the whole bundle, manifest, tools, management checkout, `.agents`,
other task requirements, condition comparison table or private evaluation material.
No initial message may be truncated to fit a context window: reject admission.
The common authority note is included verbatim in every initial message.

Consumers must bind the row's request/source/semantic/evaluation hashes into run
identity, keep private evaluator artifacts outside the workspace, and supply a
separate pinned Java/Gradle/DB/dependency environment. Bundled build definitions
are not an offline dependency cache or evidence of successful Fineract execution.
`public-contract` binds a public specification only; it does not attest that an
implementation of that evaluator exists. No model run is authorized by this bundle.
