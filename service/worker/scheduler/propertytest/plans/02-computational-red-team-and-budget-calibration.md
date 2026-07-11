# Plan 2: Computational Red Team And Budget Calibration

## Prerequisite

Plan 1 must be complete. All red-team candidates in the valid-cost track must pass the
new satisfiability validator. Invalid specs are tested in a separate validation-abuse
track and must not contaminate valid-spec cost distributions.

## Objective

Search systematically for valid schedule specs that maximize deterministic work, CPU
time, allocations, work per result, and work required to prove a query-local empty
result. Use those cases to evaluate iteration budgets and candidate guards without
selecting or implementing a production limit.

## Threat Model

Assume a caller can choose:

- Inclusion calendars, intervals, and cron forms.
- Exclusion calendars.
- Timezone name or timezone data within request-size limits.
- Schedule start/end bounds.
- Jitter.
- List query start/end range.
- A schedule shape that is valid but intentionally expensive.
- Repeated or concurrent matching-time queries.

The red team should optimize small serialized inputs that cause disproportionate work.

## Attack Tracks

### Dense Result Plus Sparse Union

- One-second or low-second interval.
- One or more yearly or rare calendars.
- Multiple sparse calendars with different next occurrences.
- Impossible calendars are excluded from this valid track by Plan 1 validation.
- Vary jitter because following-nominal computation evaluates union members again.

### Exclusion Amplification

- Dense inclusion with exclusions rejecting 50%, 90%, 99%, and nearly all candidates.
- Multiple overlapping exclusions.
- Exclusions that reject long runs before leaving one valid witness.
- Timezone-dependent exclusions across DST transitions.

Fully excluded effective sets belong to validation red teaming, not matching-cost
calibration, because Plan 1 classifies them invalid.

### Sparse First Result

- Explicit far-future years.
- Leap-day schedules.
- Rare day-of-month/day-of-week conjunctions that remain satisfiable.
- Schedule bounds narrowly surrounding the only witness.
- Multiple union members whose first results lie far apart.

### Result-Count Amplification

- Compare result limits 1, 10, 100, and 1,000.
- Hold query range constant while varying result limit.
- Hold result limit constant while varying density.
- Measure work needed for first result separately from marginal work per additional
  result.

### Horizon Amplification

- Query windows from one minute through the supported calendar horizon.
- Absolute earliest/latest end controls from the existing profiles.
- Query-local empty ranges before, between, and after valid witnesses.
- Finite schedules already exhausted for the requested query.

### Input-Size Amplification

- Maximum reachable number of calendars, intervals, exclusions, and ranges.
- Overlapping and redundant components.
- Equivalent serialized forms with different parser costs.
- Timezone data near accepted request-size limits.

### Validation Abuse

- Invalid specs whose invalidity is expensive to prove.
- Fully excluded dense intervals.
- Nearly unsatisfiable components with one last-horizon witness.
- Cases that transition from indeterminate to invalid only at high validation budgets.

Report validation and matching budgets independently.

## Objective-Driven Search Runner

Rapid remains responsible for structured generation and shrinking. Add a deterministic
evolutionary runner for maximizing cost because Go fuzzing optimizes code coverage, not
work count.

### Candidate Representation

Use the semantic `ScheduleModel` plus:

```go
type RedTeamCandidate struct {
    Model       ScheduleModel
    QueryStart  time.Time
    QueryEnd    time.Time
    ResultLimit int
    JitterSeed  string
}
```

Candidates must be serializable to bounded JSON for replay.

### Mutations

- Add, remove, or duplicate a union member.
- Add, remove, widen, narrow, or shift a range.
- Change interval and phase while preserving validity.
- Move a witness later or earlier.
- Increase or decrease exclusion coverage.
- Move query start/end.
- Change result limit.
- Switch timezone among the fixed corpus.
- Switch equivalent representation.

Every mutation in the valid track must retain validator success and a witness.

### Fitness Vector

Do not collapse all behavior into one score. Retain elites for:

```text
total matching work
calendar search work
excluded-candidate retries
work before first result
work per returned result
work before query-local empty success
CPU nanoseconds
allocations and bytes allocated
serialized-input bytes to work ratio
validation proof work
```

Use Pareto selection so a case dominating one category is not lost because another case
has a larger total.

### Search Controls

- Fixed seed.
- Configurable population and generation counts.
- Elite retention.
- Deterministic mutation order.
- Per-candidate hard work and wall-clock safety limits.
- Periodic checkpoint outside the repository.
- Final minimization using focused Rapid generators or deterministic delta reduction.

## Budget Matrix

Start with:

```text
10
100
1,000
10,000
100,000
500,000
1,000,000
5,000,000
10,000,000
```

For every retained candidate:

1. Determine the largest failing and smallest successful budget.
2. Binary search the exact required deterministic work.
3. Verify exact success at `N` and failure at `N-1`.
4. Verify all larger budgets return identical results and work.
5. Record partial result count at every failing tier.
6. Record whether failure occurred in validation or matching computation.

Run the matrix across result limits 1, 10, 100, and 1,000 and all query-range profiles.

## Cost Calibration

The current work model assigns one unit to every category. Calibrate rather than assume
those units correspond equally to CPU.

### Microbenchmarks

Add benchmarks isolating:

- Calendar outer search step.
- Calendar field advancement.
- Inclusion source check.
- Interval modular calculation.
- Exclusion match.
- Excluded-candidate retry.
- Jitter following-nominal calculation.
- Timezone conversion and DST handling.
- Canonicalization and validation.

Record `ns/op`, `B/op`, and `allocs/op`.

### Candidate Benchmarks

Benchmark reviewed low, median, P99-like, budget-transition, and maximum-work cases.
Run enough repetitions for stable benchstat comparison.

### Work Model Evaluation

- Correlate raw work categories with measured CPU.
- Preserve the raw category vector even if a weighted score is introduced.
- Version any weighted work definition separately from iteration definition `v1`.
- Do not tune weights to only the current top examples.

## Property Checks During Red Teaming

Every candidate execution still enforces:

- Determinism.
- Ordering, uniqueness, and query bounds.
- Independent model membership.
- Exact budget behavior.
- Validation success for the valid track.
- No partial response reported as complete after exhaustion.
- Representation equivalence where applicable.

Cost optimization must not bypass semantic assertions.

## Distribution Reporting

Produce separate summaries for:

- Curated realistic profiles.
- Uniform property generators.
- Objective-driven adversarial search.
- Validation-abuse cases.

Never describe generator or adversarial frequency as customer frequency.

For each profile and result limit report:

- P50, P90, P99, and maximum work.
- CPU and allocation distributions.
- Count crossing each budget.
- Exact transition examples.
- Work per result.
- Time to first result.
- Query-local empty proof cost.
- Serialized size to work amplification.

## Guard Evaluation

Evaluate but do not implement:

- Cumulative matching-work budget.
- Separate validation-work budget.
- Maximum query window.
- Maximum result count.
- Maximum number of union/exclusion components.
- Maximum total ranges.
- Combined serialized-size and work guard.
- Cancellation/deadline checks at work ticks.

For each candidate guard record:

- Valid cases rejected.
- Invalid cases stopped earlier.
- CPU and memory reduction.
- Whether changing query range or result count can make the request succeed.
- Retry semantics.
- Error information needed by clients.
- Tenfold-load behavior, completed in Plan 3.

## Deliverables

- `redteam` opt-in test/benchmark runner under `propertytest`.
- Versioned candidate JSON schema.
- Reviewed minimized corpus grouped by fitness objective.
- Budget-transition table.
- Benchmark results and work-category correlation report.
- `FINDINGS.md` updates for every new edge case and rejected property.
- A guard evidence table containing both candidate and `Do Not Add` guards.

## Acceptance Criteria

- At least one deterministic search campaign completes for every attack track.
- Every retained valid candidate passes Plan 1 validation with a witness.
- Every retained invalid candidate has a reproducible validation classification.
- Exact `N`/`N-1` transitions are recorded for retained cases.
- Budget results cover through at least 10,000,000 work units or a documented lower
  operational ceiling.
- CPU and allocation benchmarks exist for each work category.
- Generator, realistic, and adversarial distributions are never conflated.
- No production budget is selected or enforced.

## Failure Modes And Safety

- The evolutionary runner itself must have hard process-time and work limits.
- Candidate panics are captured as failures and minimized.
- A hung candidate must be terminable through context and outer test timeout.
- Checkpoints must be atomic and bounded.
- High-work cases must remain opt-in to ordinary tests.
- Red-team output must not contain unbounded timezone data or secrets.
