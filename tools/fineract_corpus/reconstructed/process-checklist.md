# Cross-domain verification plan and missing process artifacts

Researcher-created retrospective material, 2026-10-16. Apache-2.0.
Derived from Apache Fineract 1.15.0, commit `d5636847ac556c30b437254c353f05526d172b97`.
This is not an original project overview, design, PT/ST deliverable or executed test report.
Creation resources: one Codex implementation session, pinned public source and public tests; no new model experiment.
No gold patch, hidden evaluator or successful maintenance run was consulted. Static observations below do not establish runtime correctness.

## Trace and observation protocol

This is a retrospective verification plan grounded in public entry points. It is neither an executed report nor an original historical PT/ST specification. Record preconditions, operation, observable output, preserved state and evidence location separately. An API result, a helper result, a database record and an accounting posting are different observations. A test runner reporting zero tests cannot support a pass claim.

| Stage | Public evidence to inspect | Observation to retain | Current status |
| --- | --- | --- | --- |
| Environment | baseline build definitions and public testing chapters | Java/Gradle/DB versions, dependency lock and test discovery | unexecuted; supplied by execution owner |
| API boundary | annotated endpoint and validator | request identity, permission, public validation result | static mapping only |
| Business rule | baseline helper/service and final request | date/enum inputs and derived state | static mapping only |
| Persistence | audit action or posting path | stored history, absence of unintended mutation | unexecuted |
| Integration | API, business date, COB, accounting | consistency across layers | unexecuted |
| Regression | existing public tests and explicit changed-rule exceptions | discovered/executed/skipped/failed counts | unexecuted |

## Date and transaction discipline

Business date is a logical organization date; system-clock timestamps used for audit do not replace it. Preserve distinction among the start of a pause, a resume, a posting boundary and creation time. The historical business-date design text is useful for rationale but cannot establish the absence of current business-date APIs or COB. All conditions receive the same authority correction.

## Missing artifacts and resource accounting

The testing index has a TBD body and includes separate testing guidance. Neither four testing chapters nor many public test files establishes complete per-capability PT/ST documents. The researcher overview, detailed rule explanations, option matrix and this checklist fill explicit documentation roles; they are not newly observed business functions or independent samples. The creation resource is one implementation session using pinned public source; no Fineract test run or model acceptance run was performed here. Execution environment readiness, actual test outcomes and generalization remain separate work.

## Evidence and limits

- BD-DOC: `source/fineract-doc/src/docs/en/chapters/architecture/business-date.adoc` lines 1–63. Opening is a historical design-state claim, contradicted as current inventory by the pinned API and public BDD.
- BD-API: `source/fineract-core/src/main/java/org/apache/fineract/infrastructure/businessdate/api/BusinessDateApiResource.java` lines 45–84. Business date GET and POST exist; update is dispatched through a command.
- BD-TEST: `source/fineract-e2e-tests-runner/src/test/resources/features/BusinessDate.feature` lines 1–49. Public BDD business-date scenarios exist; execution not performed.
- PAUSE-TEST: `source/integration-tests/src/test/java/org/apache/fineract/integrationtests/DelinquencyActionIntegrationTests.java` lines 135–210. Public API integration preserves PAUSE and RESUME records and expects pause active on resume day.
- SAVINGS-TEST: `source/fineract-core/src/test/java/org/apache/fineract/portfolio/savings/domain/SavingsHelperAnniversaryPostingTest.java` lines 36–202. JUnit specifies aligned and short-month periods, not full API or DB coverage.
- CHARGE-API: `source/fineract-charge/src/main/java/org/apache/fineract/portfolio/charge/api/ChargesApiResource.java` lines 113–135. Template API is GET, checks READ permission and delegates query parameters.

Source copyright and notices remain in `source/LICENSE_SOURCE`, `source/NOTICE_SOURCE` and the original files. Researcher prose is an addition; source files are unchanged. Original document creation dates and comprehensive per-feature PT/ST coverage remain unverified.
