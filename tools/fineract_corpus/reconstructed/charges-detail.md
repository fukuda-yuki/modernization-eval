# Charge templates: baseline option matrix and API contract

Researcher-created retrospective material, 2026-10-16. Apache-2.0.
Derived from Apache Fineract 1.15.0, commit `d5636847ac556c30b437254c353f05526d172b97`.
This is not an original project overview, design, PT/ST deliverable or executed test report.
Creation resources: one Codex implementation session, pinned public source and public tests; no new model experiment.
No gold patch, hidden evaluator or successful maintenance run was consulted. Static observations below do not establish runtime correctness.

## Entry point and baseline branching

GET /charges/template accepts numeric chargeAppliesTo and chargeTimeType and validates READ permission before delegation. The pinned service branches for WORKING_CAPITAL_LOAN (5); other inputs fall back to the unfiltered template. That fallback is existing behavior, not evidence that loan/savings filtering already works. LOAN=1 and SAVINGS=2 exist in the public enum.

The unfiltered response supplies generic options and separate loan/savings options, currency options, payment options, account mapping and tax-related fields. A filtered UI convenience response must be evaluated independently from creating a charge. This producer does not run either operation.

## Existing specialized template sets

These are reconstructed from the pinned dropdown service and enums, not a future patch or hidden expected output. Order is not an asserted contract.

| Entity | Specialized time options (numeric ID) | Specialized calculation options (numeric ID) |
| --- | --- | --- |
| Loan | DISBURSEMENT (1), SPECIFIED_DUE_DATE (2), INSTALMENT_FEE (8), OVERDUE_INSTALLMENT (9), TRANCHE_DISBURSEMENT (12) | FLAT (1), PERCENT_OF_AMOUNT (2), PERCENT_OF_AMOUNT_AND_INTEREST (3), PERCENT_OF_INTEREST (4), PERCENT_OF_DISBURSEMENT_AMOUNT (5) |
| Savings | SPECIFIED_DUE_DATE (2), SAVINGS_ACTIVATION (3), WITHDRAWAL_FEE (5), ANNUAL_FEE (6), MONTHLY_FEE (7), OVERDRAFT_FEE (10), WEEKLY_FEE (11), SAVINGS_NOACTIVITY_FEE (16) | FLAT (1), PERCENT_OF_AMOUNT (2) |
| Working capital | SPECIFIED_DUE_DATE (2) | FLAT (1), also for the specified-due-date second level |

SAVINGS_CLOSURE (4) exists in the enum but is commented out in the specialized savings template dropdown. Do not replace the observed dropdown set with every enum value or every charge-creation eligibility rule.

## PT/ST-style checks and gaps

Checklist (not executed): compare selected-entity generic options to the corresponding specialized unfiltered options; inspect currency/account/tax metadata; compare the unfiltered response before/after a change; preserve WCP first/second-level behavior; exercise authentication and READ permission; confirm a GET has no business update; distinguish invalid enum/date parsing from allowed-option filtering. The final request defines any new rejection rules.

A dedicated baseline template-filter integration test was not found by searching public integration/provider/e2e tests for charges/template and the API operation names. This is a scoped search gap, not proof that no indirect coverage exists. LoanChargeTaxIntegrationTest is related charge/tax coverage, not claimed as direct template filtering coverage. The independent evaluation work must supply executable coverage without relabeling this checklist as a passed original ST suite.

## Evidence and limits

- CHARGE-DOC: `source/fineract-doc/src/docs/en/chapters/features/charges-template-filters.adoc` lines 1–51. Documented hierarchical template filtering is WCP-specific.
- CHARGE-API: `source/fineract-charge/src/main/java/org/apache/fineract/portfolio/charge/api/ChargesApiResource.java` lines 113–135. Template API is GET, checks READ permission and delegates query parameters.
- CHARGE-SERVICE: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/charge/service/ChargeReadPlatformServiceImpl.java` lines 117–178. WCP branch returns filtered template; other inputs fall back to unfiltered fields with currency, account and tax metadata.
- CHARGE-OPTIONS: `source/fineract-charge/src/main/java/org/apache/fineract/portfolio/charge/service/ChargeDropdownReadPlatformServiceImpl.java` lines 75–157. Loan and savings specialized option sets are already available; savings closure is commented out of the template set.
- CHARGE-TIME: `source/fineract-core/src/main/java/org/apache/fineract/portfolio/charge/domain/ChargeTimeType.java` lines 23–43. Numeric time enum identifiers; do not equate every enum value with template eligibility.
- CHARGE-CALC: `source/fineract-charge/src/main/java/org/apache/fineract/portfolio/charge/domain/ChargeCalculationType.java` lines 23–45. Calculation identifiers and WCP flat-only sets.
- CHARGE-APPLIES: `source/fineract-charge/src/main/java/org/apache/fineract/portfolio/charge/domain/ChargeAppliesTo.java` lines 21–30. Loan=1, savings=2, WCP=5 are existing enum values.

Source copyright and notices remain in `source/LICENSE_SOURCE`, `source/NOTICE_SOURCE` and the original files. Researcher prose is an addition; source files are unchanged. Original document creation dates and comprehensive per-feature PT/ST coverage remain unverified.
