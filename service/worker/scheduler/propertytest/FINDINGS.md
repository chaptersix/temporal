# Schedule Matching Times Property Findings

## Status

The analysis harness is under construction. Findings below are retained when a
property run produces a minimized counterexample or evidence about a work-budget
transition.

## Iteration Definition

- Version: `v1`
- Budget: cumulative across a complete matching-times request
- Categories: next-time calls, inclusion checks, calendar search steps, interval
  checks, exclusion checks, excluded-candidate retries, and result-loop steps

## Confirmed Findings

### Dense Interval Plus Sparse Calendar Amplifies Work

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

### Ten-Thousand Budget Is Frequently Crossed In Generated Mixed Specs

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

### Sparse Union Members Dominate Dense Result Cost

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

### Dense All-Excluded Schedule Needs A Termination Budget

- Classification: `guard-evidence`
- Shape: one-second interval with an exclusion calendar matching every civil second.
- Observation: the spec is structurally valid, produces no result, and reaches the
  10,000-work budget through repeated excluded-candidate retries.
- Consequence: returning empty at the cap would falsely claim the no-match search was
  complete. Budget exhaustion must remain distinguishable from empty success.
- Retention: `TestAllExcludedDenseIntervalReachesBudget`

### Structured Exclusion Calendars Are Not Validated Like Inclusions

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

### Negative Jitter Is Silently Treated As Zero

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
  explicit exercise policy and will be migrated in the next plan.
