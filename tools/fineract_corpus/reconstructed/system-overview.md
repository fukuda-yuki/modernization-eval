# Business capabilities and cross-domain operations

Researcher-created retrospective material, 2026-10-16. Apache-2.0.
Derived from Apache Fineract 1.15.0, commit `d5636847ac556c30b437254c353f05526d172b97`.
This is not an original project overview, design, PT/ST deliverable or executed test report.
Creation resources: one Codex implementation session, pinned public source and public tests; no new model experiment.
No gold patch, hidden evaluator or successful maintenance run was consulted. Static observations below do not establish runtime correctness.

## Scope and actors

The baseline contains customer and group management, loan accounts, savings and deposit accounts, account transfers, charge definitions, journal entries, organizational offices and roles. The identified API entry points are distinct business responsibilities, not a count of source files. A teller's account operation, an administrator's role change and an accounting entry are different operations even when they share infrastructure.

Client and group APIs support identification and grouping. Deposit APIs accept applications and expose account state. Transfers move funds between accounts. Journal-entry APIs expose balanced entries. Organization and role APIs define the administrative context. These observations establish breadth; they do not establish that every combination works in a deployed system.

## Cross-domain process to inspect

For a customer account operation, distinguish the customer identity, product/account state, transaction date, charge eligibility, balance change and accounting effect. The transfer and journal APIs are separate entry points: a successful account response alone is not proof of reconciled accounting. The financial operation also has an access boundary; the caller's permissions are part of the context, not an optional UI check.

Loans and deposits share business-date concerns while retaining their own lifecycle and interest rules. A future maintenance exercise must describe which boundary it changes and which other responsibilities remain invariant. Do not infer a new API, a new numeric posting type or changed authority from a convenience template.

## Information completeness

The feature chapter is uneven: some operations have dedicated prose while customer, group, transfer and organizational capabilities are clearer in annotated API source. The whole baseline remains searchable. Absence from this overview is not evidence of absence from Fineract. The feature ledger records representative entry points, filename-only links and unmapped test coverage separately.

## Evidence and limits

- CUSTOMER-API: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/client/api/ClientsApiResource.java` lines 134–196. API operation body/annotations establish this business entry point; not execution evidence.
- GROUP-API: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/group/api/GroupsApiResource.java` lines 181–240. API operation body/annotations establish this business entry point; not execution evidence.
- FIXED-API: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/savings/api/FixedDepositAccountsApiResource.java` lines 145–205. API operation body/annotations establish this business entry point; not execution evidence.
- RECURRING-API: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/savings/api/RecurringDepositAccountsApiResource.java` lines 132–184. API operation body/annotations establish this business entry point; not execution evidence.
- TRANSFER-API: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/account/api/AccountTransfersApiResource.java` lines 65–133. API operation body/annotations establish this business entry point; not execution evidence.
- ACCOUNTING-API: `source/fineract-provider/src/main/java/org/apache/fineract/accounting/journalentry/api/JournalEntriesApiResource.java` lines 170–222. API operation body/annotations establish this business entry point; not execution evidence.
- OFFICE-API: `source/fineract-provider/src/main/java/org/apache/fineract/organisation/office/api/OfficesApiResource.java` lines 100–150. API operation body/annotations establish this business entry point; not execution evidence.
- ROLE-API: `source/fineract-provider/src/main/java/org/apache/fineract/useradministration/api/RolesApiResource.java` lines 100–134. API operation body/annotations establish this business entry point; not execution evidence.
- BD-API: `source/fineract-core/src/main/java/org/apache/fineract/infrastructure/businessdate/api/BusinessDateApiResource.java` lines 45–84. Business date GET and POST exist; update is dispatched through a command.

Source copyright and notices remain in `source/LICENSE_SOURCE`, `source/NOTICE_SOURCE` and the original files. Researcher prose is an addition; source files are unchanged. Original document creation dates and comprehensive per-feature PT/ST coverage remain unverified.
