# Savings and deposits: posting boundaries and public-test explanation

Researcher-created retrospective material, 2026-10-16. Apache-2.0.
Derived from Apache Fineract 1.15.0, commit `d5636847ac556c30b437254c353f05526d172b97`.
This is not an original project overview, design, PT/ST deliverable or executed test report.
Creation resources: one Codex implementation session, pinned public source and public tests; no new model experiment.
No gold patch, hidden evaluator or successful maintenance run was consulted. Static observations below do not establish runtime correctness.

## Baseline schedule and configuration

Interest posting type is product/account configuration, and period construction is distinct from actual ledger posting. The documented standard codes are 1/4/5/6/7; anniversary codes are 8/9/10/11 for 1/3/6/12-month intervals. The baseline SavingsHelper stores the initial day-of-month, adds the corresponding month interval and clamps the day to the destination month's length. Anniversary branches do not use financial-year alignment.

The interval endpoint is the calculated posting date minus one day. Manual post-interest-as-on dates can split an interval; the helper carries state for that split. The documented downstream PostingPeriod handles the effective calculation cutoff. Thus a boundary helper may return a period extending beyond the requested calculation date without establishing a duplicate financial posting.

## Validation and accounting responsibilities

SavingsAccountAnniversaryValidationTest checks recognition across compounding combinations. FixedDepositProductAnniversaryValidationTest also checks compounding longer than posting. These tests do not establish the actual credit to an account or its journal entry. Retain the difference between configuration acceptance, boundary construction, balance accrual and posting persistence when reviewing behavior.

## Public test and PT/ST-style checks

SavingsHelperAnniversaryPostingTest contains non-month-end anchors, short-month clamping and leap-year annual cases. These are baseline expectations. A new month-end-origin request must be read as a new rule; the existence of January 31 coverage alone does not establish it. Existing integration tests remain searchable under integration-tests; broader API/DB execution is unverified in this material producer.

Checklist (not executed): classify the original start date before stepping through short months; inspect each of the four anniversary intervals; compare a non-month-end origin before and after February; inspect leap/non-leap transitions; preserve ordinary posting types and financial-year treatment; inspect a manual split and calculation cutoff; observe posting persistence and accounting without double credit. Use public requirement values for changed behavior and record environment/build failures separately from product results.

## Evidence and limits

- SAVINGS-DOC: `source/fineract-doc/src/docs/en/chapters/features/savings-interest-posting.adoc` lines 1–113. Posting types, anchor, financial-year separation and manual posting are documented.
- SAVINGS-HELPER: `source/fineract-core/src/main/java/org/apache/fineract/portfolio/savings/domain/SavingsHelper.java` lines 50–197. Baseline keeps initial numeric day and clamps in short months; manual posting can split periods.
- SAVINGS-TEST: `source/fineract-core/src/test/java/org/apache/fineract/portfolio/savings/domain/SavingsHelperAnniversaryPostingTest.java` lines 36–202. JUnit specifies aligned and short-month periods, not full API or DB coverage.
- SAVINGS-VALIDATION: `source/fineract-savings/src/test/java/org/apache/fineract/portfolio/savings/domain/SavingsAccountAnniversaryValidationTest.java` lines 45–83. Parameterized savings validation recognizes anniversary types.
- FIXED-VALIDATION: `source/fineract-savings/src/test/java/org/apache/fineract/portfolio/savings/domain/FixedDepositProductAnniversaryValidationTest.java` lines 45–88. Fixed-deposit validation distinguishes valid compounding and longer-than-posting errors.
- FIXED-API: `source/fineract-provider/src/main/java/org/apache/fineract/portfolio/savings/api/FixedDepositAccountsApiResource.java` lines 145–205. API operation body/annotations establish this business entry point; not execution evidence.

Source copyright and notices remain in `source/LICENSE_SOURCE`, `source/NOTICE_SOURCE` and the original files. Researcher prose is an addition; source files are unchanged. Original document creation dates and comprehensive per-feature PT/ST coverage remain unverified.
