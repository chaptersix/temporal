# Schedule Matching Times Property Findings

## Status

Plans 1 and 2 are complete for the test-only analysis harness. Plan 3 has not started.
Reviewed evidence is stored under campaign `20260710-plan1-validity-hardening-0e0638d11`
and the four Plan 2 campaigns listed below.

## Iteration Definition

- Version: `v1`
- Budget: cumulative across a complete matching-times request
- Categories: next-time calls, inclusion checks, calendar search steps, interval
  checks, exclusion checks, excluded-candidate retries, and result-loop steps
- Validator version: `v1`
- Validation budget: separate from matching work; component checks, civil days,
  calendar tuples, interval occurrences, effective days, and exclusion checks

## Plan 1 Reviewed Findings

### OBSERVATION: The Revised Invalidity Contract Is Enforced

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Cases: `plan1-empty-inclusion`, `plan1-february-30`,
  `plan1-fully-excluded`, `plan1-valid-empty-query`
- Empty inclusions and impossible inclusion components classify as
  `invalid-unsatisfiable-component`.
- Complete exclusion classifies as `invalid-empty-effective-set`.
- A globally satisfiable schedule whose finite query range has no match returns
  `valid-empty-query-result`.
- Every valid classification carries a witness accepted by the independent semantic
  model.

### OBSERVATION: Validation-Budget Exhaustion Remains Indeterminate

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Case: `plan1-fully-excluded`
- A validation budget of 100 exhausts before proof and returns
  `indeterminate-validation-budget`.
- With a sufficient proof budget, the same spec classifies as
  `invalid-empty-effective-set`.
- Matching work remains zero for both outcomes, so neither indeterminacy nor proven
  invalidity enters the matching loop.

### OBSERVATION: Normalized Midnight Arithmetic Can Skip A Civil Boundary

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Case: `plan1-apia-end-boundary`
- Minimized failfile:
  `testdata/rapid/TestExperimentBudgetMatrix/TestExperimentBudgetMatrix-20260710214418-23034.fail`
- Repeatedly adding a day to a timezone-normalized midnight retained a shifted hour
  across later days and omitted an occurrence exactly at the schedule end in
  `Pacific/Apia`.
- The validator now advances year/month/day fields independently and reconstructs each
  civil day. `TestValidationAcceptsTimezoneCalendarAtScheduleEnd` retains the case.

### OBSERVATION: Exclusion Satisfiability Is Not Inclusion-Bound Satisfiability

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Retained failfile:
  `testdata/rapid/TestPropertyBudgetBoundary/TestPropertyBudgetBoundary-20260710213955-22355.fail`
- A witness-first generator initially required an exclusion calendar to have a witness
  inside schedule-level inclusion bounds. Plan 1 requires exclusion calendars to be
  structurally and globally satisfiable, but only inclusion components must have a
  timestamp inside schedule-level bounds.
- Classification: generator defect. The generator and validator were corrected; the
  minimized case is retained.

### OBSERVATION: Raw Nil Calendar Elements Must Be Checked Before Protobuf Cloning

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Retained failfile:
  `testdata/rapid/TestPropertyStructuralInvalidClassification/TestPropertyStructuralInvalidClassification-20260710215624-24833.fail`
- Protobuf cloning normalizes a nil legacy calendar element into an empty message,
  which then acquires valid default calendar semantics.
- The analysis canonicalizer now rejects raw nil inclusion and exclusion calendar
  elements before cloning. Classification: implementation defect in the copied
  validator, fixed and retained as a regression.

### OBSERVATION: Lord Howe's Repeated Half Hour Exposes Two Distinct Behaviors

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Case: `plan1-lord-howe-repeated-half-hour`
- Minimized failfiles:
  `testdata/rapid/TestPropertySmallWindowOracle/TestPropertySmallWindowOracle-20260710215807-24987.fail`
  and
  `testdata/rapid/TestPropertyRangeDecomposition/TestPropertyRangeDecomposition-20260710215809-24987.fail`
- The first failfile shows the independent oracle matching the first occurrence of
  local 01:30 during a 30-minute rollback while the copied and production calculators
  return no result. The copied calculator remains at production parity; production is
  intentionally unchanged by this exercise.
- The second failfile showed a copied-validator defect: validation considered only the
  absolute occurrence preferred by `time.Date` and could reject schedule bounds that
  selected the other occurrence. Validation now accepts either absolute occurrence.
- `TestCopiedCalculatorLordHoweRepeatedHalfHourDivergence` preserves the matching
  divergence; `TestValidationAcceptsEitherRepeatedHourOccurrence` preserves the fixed
  validity behavior.

### NEGATIVE EVIDENCE: Reduced-Domain Differential Search Found No Mismatch

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Population: constructed valid and fully excluded schedules over 1-120 second
  horizons.
- Checks: 50,000.
- No optimized-validator versus brute-force classification mismatch, unsound witness,
  or false empty-set proof was observed in this finite population.

### NEGATIVE EVIDENCE: Full Property Campaign Found No Counterexample

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Population: witness-first valid, structural-invalid, component-invalid,
  effective-empty, representation, oracle, parity, and budget properties.
- Checks: 10,000 per Rapid property.
- No new counterexample was observed after retained failfiles replayed successfully.

### NEGATIVE EVIDENCE: Bounded Fuzz Campaigns Completed

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- `FuzzScheduleValidation`: 30 seconds, 9,712 executions, 2 new interesting inputs
  after a 28-input baseline. Throughput plateaued during the finite run.
- `FuzzMatchingTimesOracle`: 30 seconds, 1,207 executions, no new interesting inputs
  after a 33-input baseline. Throughput plateaued during the finite run, so this is
  weak negative evidence rather than an exhaustive claim.

## Mutation Kill Matrix

All switches are test-only fields on the copied validator/calculator. Each row is
killed by the named semantic property in `TestMutationKillMatrix`.

| Mutation | Property ID | Result |
| --- | --- | --- |
| Make query start inclusive | `PROP-ORDERING-START-EXCLUSIVE` | killed |
| Ignore all exclusions | `PROP-EXCLUSION-SUBTRACTION` | killed |
| Stop validating exclusions | `PROP-INVALID-EXCLUSION-STRUCTURE` | killed |
| Accept empty structured inclusion | `PROP-EMPTY-STRUCTURED-INCLUSION` | killed |
| Treat February 30 as satisfiable | `PROP-REDUCED-COMPLETENESS-FEBRUARY-30` | killed |
| Ignore day of week | `PROP-WITNESS-SOUNDNESS-DAY-OF-WEEK` | killed |
| Permit schedule start after end | `PROP-INVERTED-BOUNDS-CLASSIFICATION` | killed |
| Return empty on matching-budget exhaustion | `PROP-MATCHING-BUDGET-TAXONOMY` | killed |
| Allow duplicate union results | `PROP-STRICT-ORDERING-NO-DUPLICATES` | killed |
| Let jitter cross the next nominal time | `PROP-JITTER-DOES-NOT-CROSS-NEXT-NOMINAL` | killed |
| Treat validation indeterminate as invalid | `PROP-VALIDATION-BUDGET-IS-INDETERMINATE` | killed |

### INFERENCE: The Property Set Detects The Planned Representative Defects

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Eleven of eleven planned mutation switches changed the outcome checked by their
  assigned property.
- This demonstrates sensitivity to the planned fault set; it does not establish
  completeness against unmodeled defects.

## Plan 1 Property Lifecycle

- `PROP-VALID-WITNESS`: revised valid generation from independent fields to
  witness-first construction; every inclusion is independently satisfiable and
  exclusions preserve an effective witness.
- `PROP-QUERY-SEPARATION`: revised the old successful-empty assumption to distinguish
  global satisfiability from query-local emptiness.
- `PROP-VALIDATION-BUDGET`: added an independent validation budget and exact `N` versus
  `N-1` boundary property.
- `PROP-REDUCED-DIFFERENTIAL`: added a brute-force reference for finite reduced
  horizons and ran 50,000 checks.
- `PROP-SMALL-WINDOW-ORACLE`: a valid generator may still exercise DST transitions,
  but it no longer selects the non-preferred absolute occurrence of an ambiguous local
  tuple for generic copied-calculator properties. The known production-parity
  divergence is retained as a dedicated regression instead of silently weakening the
  oracle.
- `PROP-PARITY`: retained legacy parity only for generated cases classified valid by
  the stricter exercise contract. Empty, impossible, inverted, and fully excluded
  cases are intentional analysis-only divergences.

## Plan 1 Verification

### OBSERVATION: Required Packages Remain Passing

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Property package: default, 50,000-check reduced differential, 10,000-check full
  campaign, and both required fuzz targets passed.
- Legacy scheduler package: passed unchanged.
- CHASM scheduler package: passed unchanged.
- Repository-wide lint was not run, as explicitly deferred for this plan.

## Plan 2 Reviewed Findings

### OBSERVATION: Every Computational Attack Track Completed

- Campaign: `20260710-plan2-computational-red-team-872e129a8-adversarial`
- Retained valid cases: `plan2-dense-sparse-seed-g00-m00-g01-m00`,
  `plan2-exclusion-seed-g00-m00-g01-m00`,
  `plan2-sparse-first-seed-g00-m00-g01-m00`, `plan2-result-count-seed`,
  `plan2-horizon-seed-g00-m00`, and `plan2-input-size-seed`.
- Six fixed-seed searches covered dense result plus sparse union, exclusion
  amplification, sparse first result, result-count amplification, horizon
  amplification, and input-size amplification. Each search evaluated eight bounded
  candidates over two generations and retained Pareto elites.
- Every retained valid-cost case passed validator-v1 and carried a witness accepted by
  the independent semantic model. The separately reviewed validation-abuse case never
  entered matching computation.
- The checked-in replay corpus is grouped by track under `testdata/redteam/v1/` and
  validates against `candidate-v1.schema.json`.

### OBSERVATION: The Full Budget Matrix Completed Through Ten Million

- Campaign: `20260710-plan2-computational-red-team-872e129a8-adversarial`
- The matrix contains 96 retained-case/profile/result-limit combinations: six cases,
  four query profiles, and result limits 1, 10, 100, and 1,000.
- Every matrix recorded all tiers from 10 through 10,000,000, partial result counts,
  the largest failing and smallest successful tier, exact required work, and successful
  `N` versus matching exhaustion at `N-1`.
- Case `plan2-input-size-seed` had the largest reviewed exact matching requirement:
  9,552,072 work for realistic and boundary profiles at result limit 1,000.
- The same case retains a deterministic 150,054-byte synthetic UTC TZif payload and 24
  redundant calendar components; its bounded replay JSON is 222,053 bytes.
- Case `plan2-dense-sparse-seed-g00-m00-g01-m00` required 618,000 work for realistic
  and boundary profiles at result limit 1,000.
- No lower operational ceiling replaced the required 10,000,000 tier.

### NEGATIVE EVIDENCE: Larger Budgets Did Not Change Completed Results

- Campaign: `20260710-plan2-computational-red-team-872e129a8-adversarial`
- Finite population: 96 matrices over six reviewed adversarial candidates.
- Every larger budget after the exact success boundary returned identical times and an
  identical work vector. No retained case exceeded 10,000,000 matching work, panicked,
  lost witness soundness, or reported a partial response as complete.

### OBSERVATION: Validation Abuse Has A Separate Exact Boundary

- Campaign: `20260710-plan2-computational-red-team-872e129a8-invalid-abuse`
- Case: `plan2-validation-abuse-seed`
- Two overlapping exclusions collectively remove a seven-day dense interval without
  either exclusion individually covering the complete horizon.
- Validation is indeterminate through budget 500,000, classifies the effective set as
  empty at exact work 777,917, succeeds at `N`, and remains indeterminate at `N-1`.
- Matching work is zero at every validation tier. This population is excluded from all
  valid matching-cost distributions.

### OBSERVATION: Uniform And Adversarial Frequencies Remain Separate

- Campaign: `20260710-plan2-uniform-distribution-872e129a8`
- A finite uniform grid enumerated 72 seed/mutation pairs. Seventy-one passed Plan 1
  validity and one invalid or unsupported mutation was excluded from valid-cost
  distributions.
- Matching-work distribution in this grid: P50 12,000, P90 9,551,810, and P99 and
  maximum 9,949,825. Validation-work distribution: P50 35,271, P90 222,698, and P99
  and maximum 259,352.
- These are generator frequencies, not customer frequencies. Curated benchmark,
  objective-driven adversarial, uniform-generated, and invalid-abuse evidence remain
  separate in their manifests and summaries.

### OBSERVATION: Work Categories Have Different Host Costs

- Campaign: `20260710-plan2-work-calibration-872e129a8`
- All nine required microbenchmark categories recorded three repetitions of `ns/op`,
  `B/op`, and `allocs/op`. Five candidate benchmarks cover low, median,
  budget-transition, P99-like, and maximum-work cases and retain raw iteration-v1
  vectors.
- On the recorded Apple M4 Max host, interval modular calculation was approximately
  3.7-3.9 ns/op, calendar outer search approximately 56-57 ns/op, excluded-candidate
  retry approximately 2.0 microseconds/op with one allocation, and canonicalization
  plus validation approximately 6.0-6.1 milliseconds/op for the input-size case.
- Across the five candidate classes, Pearson correlations with the recorded CPU proxy
  were 0.999 for total matching work, 0.999 for calendar search, -0.207 for excluded
  retries, and 0.773 for validation work.

### INFERENCE: Raw Work Must Remain Available Beside CPU Calibration

- Campaign: `20260710-plan2-work-calibration-872e129a8`
- The correlation sample is small and host-specific. It shows that iteration-v1
  categories are not interchangeable CPU units; it does not justify replacing the
  deterministic budget with elapsed time.
- Weighted model `cpu-correlation-v1` is versioned separately from iteration definition
  `v1` and is not selected or enforced as a production model.

### OBSERVATION: Static Guard Proxies Reject Valid Cases

- Campaign: `20260710-plan2-computational-red-team-872e129a8-adversarial`
- At illustrative, non-selected thresholds, a 10,000 cumulative matching-work cap
  rejected four of six reviewed valid cases; a 100,000 validation-work cap rejected
  four; a 30-day query-window cap rejected two; a result-count cap of 100 rejected
  three; and component/range caps rejected the valid input-size case.
- A combined 4,096-byte plus 10,000-work example threshold rejected the valid bounded
  timezone-data/input-size case, while leaving five other reviewed valid cases
  unaffected; this is a false positive and incomplete coverage rather than safety.
- The durable `DECISIONS.md` records valid false positives, observed CPU/allocation
  avoided at each illustrative threshold, retry semantics, client error information,
  and the Plan 3 tenfold-load gap for every evaluated guard.

### RECOMMENDATION: Defer Dynamic Guards And Reject Static Cost Substitutes

- Deferred for Plan 3 evidence: cumulative matching work, separate validation work,
  maximum result count, combined serialized-size/work, and cancellation/deadline
  checks at work ticks.
- Rejected as Plan 2 production candidates: maximum query window, component and range
  caps, per-next-time work, per-calendar work, elapsed wall-clock budget, and estimates
  based only on the densest or sparsest source.
- No production budget or guard was selected or enforced.

### NEGATIVE EVIDENCE: Known Lord Howe Divergence Was Preserved

- Campaigns: all four reviewed Plan 2 campaigns.
- Case: `plan1-lord-howe-repeated-half-hour`
- Plan 2 made no production scheduler changes and did not weaken Plan 1 validity or
  oracle semantics. The dedicated copied/production parity divergence remains a
  regression and production follow-up, not a computational-guard fix.

## Production Implementation Handoff

This section is a handoff for a separate production implementation project after the
analysis exercise is complete. Plans 1 through 3 remain analysis-only and must not use
these notes as authorization to change production behavior.

### OBSERVATION: Repeated-Hour Matching Assumes A One-Hour Rollback

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Case: `plan1-lord-howe-repeated-half-hour`
- The shared legacy `SpecBuilder` calendar search omits the first repeated local 01:30
  occurrence during Lord Howe's 30-minute rollback. CHASM receives that same builder,
  so the defect can affect both legacy and CHASM matching paths.

### RECOMMENDATION: Fix Repeated-Time Search Using The Actual Offset Transition

- Add the retained Lord Howe case as a production regression before changing the
  algorithm.
- Derive the alternative occurrence from the timezone's actual UTC-offset change; do
  not assume all rollbacks are one hour.
- Verify legacy and CHASM list-matching behavior with 30-minute and one-hour repeated
  periods, schedule bounds selecting either occurrence, and query boundaries around
  both occurrences.

### OBSERVATION: Production Canonicalization Has Validation Gaps

- Campaign: `20260710-plan1-validity-hardening-0e0638d11`
- Structured inclusion calendars are field-validated, while structured exclusions are
  not validated by the same production canonicalization loop.
- Negative jitter is later clamped to zero rather than rejected, which silently changes
  caller input semantics.
- The analysis also found that raw nil repeated-message elements must be rejected before
  protobuf cloning normalizes them into empty messages.

### RECOMMENDATION: Audit Production Validation As One Compatibility-Sensitive Change

- Evaluate symmetric inclusion/exclusion validation, negative and malformed duration
  rejection, invalid timestamp rejection, and raw nil element handling.
- Add create, update, replay, legacy, and CHASM compatibility tests before enforcement.
- Decide how existing stored schedules that violate the proposed rules are read,
  listed, updated, or migrated. Do not introduce stricter rejection without an explicit
  compatibility policy.

### OPEN QUESTION: Should The Exercise's Global Validity Contract Become Production Policy?

- Empty inclusion sets, impossible components, inverted bounds, and globally empty
  effective sets are invalid by exercise policy, but production currently accepts some
  of these shapes.
- A production change would affect schedule creation/update validation and possibly
  existing persisted schedules. The final decision requires compatibility evidence
  from the complete exercise and a migration design.
- Query-local empty success must remain distinct from global unsatisfiability regardless
  of the policy decision.

### RECOMMENDATION: Preserve Separate Validation And Matching Outcomes

- Any production design should distinguish structural invalidity, proven global
  unsatisfiability, validation-budget indeterminacy, matching-budget exhaustion, and a
  valid query-local empty result.
- Budget exhaustion must not be returned as successful emptiness or guessed
  invalidity. Error mapping must tell clients whether changing the query, result limit,
  or schedule can help and whether retry is meaningful.
- Bound diagnostic strings and timezone information in errors and telemetry.

### RECOMMENDATION: Do Not Select A Production Work Limit Before Plans 2 And 3

- Existing evidence already shows that 10,000 matching-work units reject legitimate
  dense-plus-sparse schedules.
- Plan 2 must calibrate deterministic work against CPU and allocations and evaluate
  valid false positives. Plan 3 must add cancellation, concurrency, and tenfold-load
  evidence.
- A later implementation should prefer cumulative request accounting and ensure both
  legacy and CHASM use the same work definition and client-visible outcome.

### RECOMMENDATION: Create A Separate Production Decision Record After Plan 3

- Carry forward the Lord Howe defect, validation audit, compatibility policy, error
  taxonomy, selected or rejected guards, cancellation placement, and migration plan.
- Reference reviewed cases and campaigns rather than generator frequency, and preserve
  rejected guards with their valid counterexamples.

## Historical Pre-Plan-1 Context

The following observations predate the Plan 4 bundle contract. They remain as context
and regression-test provenance, but are not reviewed Plan 1 evidence and are not used
for a Plan 1 recommendation.

### OBSERVATION: Dense Interval Plus Sparse Calendar Amplifies Work

- Classification: `guard-evidence`
- Minimized by: `TestPropertyRangeDecomposition`
- Query: `(2020-01-01T00:00:00Z, 2020-01-01T01:00:00Z]`
- Inclusion 1: one-second interval
- Inclusion 2: January 1 at midnight with a constrained day-of-week range
- Observation: listing dense interval results repeatedly recomputes the next sparse
  calendar result. A 5,000-result analysis exhausted 1,000,000 work units around the
  forty-first query minute.
- Consequence: query-window length alone does not predict work. Result limit and the
  cost of every union member must be considered together.
- Retention: `TestDenseIntervalAndSparseCalendarBudgetTransition`

### OBSERVATION: Ten-Thousand Budget Is Frequently Crossed In Generated Mixed Specs

- Classification: `guard-evidence`
- Campaign: 1,000 deterministic Rapid cases using seed `5937846176933676086`
- Profile: realistic query bounds, result limits selected from 1, 10, 100, and 1,000
- Observation: 119 of 1,000 generated cases failed at 10,000 and succeeded at a higher
  budget tier.
- Examples: minimized or early retained cases required approximately 14,000, 15,000,
  17,000, 20,000, and 20,167 work units.
- Common shape: a one- or two-second interval combined with other intervals or sparse
  calendar union members.
- Caveat: this is generator coverage frequency, not an estimate of customer schedule
  frequency. The generator intentionally over-samples edge conditions.

### OBSERVATION: Sparse Union Members Dominate Dense Result Cost

- Classification: `guard-evidence`
- Minimized shapes:
  - one-second interval plus two sparse calendars exhausted 1,000,000 work units after
    approximately sixteen query minutes;
  - one-second interval plus a calendar representing impossible February/April day-31
    combinations exhausted 1,000,000 work units after 87 dense results.
- Observation: `rawNextTime` evaluates every union member for both the selected nominal
  time and the following nominal time used to bound jitter. A sparse or impossible
  calendar search is therefore repeated for every dense interval result.
- Consequence: the work budget must account for cross-product behavior between result
  density and union-member search cost.

### OBSERVATION: Dense All-Excluded Matching Needed A Termination Budget Before Plan 1

- Classification: `superseded-baseline-behavior`
- Shape: one-second interval with an exclusion calendar matching every civil second.
- Historical observation: the syntax-only baseline entered matching and reached the
  10,000-work budget through repeated excluded-candidate retries.
- Plan 1 result: the same spec is invalid because its effective set is empty. Low
  validation budgets remain indeterminate, and matching never begins.
- Retention: `TestAllExcludedDenseIntervalValidationClassification`

### OBSERVATION: Structured Exclusion Calendars Are Not Validated Like Inclusions

- Classification: `possible-production-defect`
- Minimized mutation: an exclusion calendar containing month 13
- Observation: inclusion structured calendars pass through
  `validateStructuredCalendar`, but exclusion structured calendars do not. The copied
  baseline accepted the malformed exclusion and returned success.
- Analysis behavior: the copied calculator now validates exclusions with the same
  field rules and returns `ErrInvalidSpec`.
- Production behavior: intentionally unchanged by this exercise.
- Follow-up: evaluate adding exclusion validation to production schedule
  canonicalization in the separate implementation project.

### OBSERVATION: Negative Jitter Is Silently Treated As Zero

- Classification: `possible-production-defect`
- Minimized mutation: `jitter = -1s`
- Observation: the copied baseline accepted negative jitter; `addJitter` later clamps a
  negative maximum to zero. The invalid request therefore appears successful with
  different semantics.
- Analysis behavior: reject negative jitter, invalid duration protobufs, and invalid
  schedule start/end timestamp protobufs as `ErrInvalidSpec`.
- Production behavior: intentionally unchanged by this exercise.

## Property Revisions

- Range decomposition is semantic only when all component queries complete. The first
  version used a one-hour range and 5,000 results under a 1,000,000-work analysis cap.
  Rapid correctly found a valid mixed schedule that exhausted the cap. The property was
  narrowed to a one-minute range; budget exhaustion remains covered separately.
- The initial oracle and jitter properties used up to one hour and 100-1,000 results.
  High-work mixed schedules exhausted the analysis cap before the semantic assertion.
  The oracle now uses a one-minute horizon and jitter comparison uses ten results;
  larger horizons remain in the budget experiments.
- Nil structured-calendar and range elements are normalized by protobuf cloning into
  empty messages. Under the revised exercise contract, an empty structured inclusion
  calendar is unsatisfiable and therefore invalid. An empty range message still carries
  field-specific default semantics and must be classified using the containing field.

## Guard Evidence

- A cumulative 10,000-work cap rejects a valid one-second interval when it is unioned
  with a sparse yearly calendar, even for a one-hour query. A higher cap can produce the
  requested results. This is evidence that 10,000 cannot be justified as universally
  sufficient.
- Some structurally valid all-excluded schedules cannot prove exhaustion within any
  practical low budget. They are invalid under the revised exercise contract, but a
  validator that exhausts its proof budget must report indeterminate/budget exhaustion
  rather than claiming it has proved invalidity.

## Do Not Add

- Do not treat a successful empty result for one query range as proof that the schedule
  spec is globally unsatisfiable.
- Do not derive a production query-window guard from generator profile bounds.
- Do not use elapsed wall-clock time as the deterministic work counter.
- Do not estimate query cost from the densest or sparsest inclusion alone. Mixed union
  members can multiply work because every next-time search evaluates each member.
- Do not reject a satisfiable schedule merely because its current query range contains
  no matches. Empty structured inclusions, globally impossible civil-date conjunctions,
  inverted schedule-level bounds, and globally empty effective sets are invalid by
  explicit exercise policy and are enforced by the Plan 1 validator.
