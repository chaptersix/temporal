# Schedule Matching Times Property Analysis Specification

## Status

This document is the execution specification for an analysis-only investigation of
Temporal schedule matching-time computation. It is intentionally separate from any
production integration or replacement project.

The work has two milestones:

1. Build an isolated, copied computation and the tooling required to generate,
   minimize, replay, instrument, and compare schedule cases.
2. Use that tooling in an iterative property-analysis campaign to discover edge cases,
   useful invariants, inappropriate invariants, and evidence for or against computation
   guards.

Milestone 2 begins after Milestone 1 is complete. Findings from Milestone 2 may motivate
a later implementation proposal, but that proposal is not part of this specification.

## Scope

### In Scope

- Copy the current schedule matching-time computation into a test-only analysis area.
- Give the copy an analysis-oriented API that accepts a raw `ScheduleSpec` and query
  range and returns matching times, work counts, and errors.
- Add deterministic work-budget instrumentation to the copied computation.
- Make the maximum work budget configurable.
- Evaluate a ladder of work budgets rather than assuming 10,000 is correct.
- Make generated query start and end bounds configurable.
- Add structured property-based generators using Rapid.
- Add deterministic experiment profiles representing common, boundary, sparse,
  exclusion-heavy, invalid, and adversarial schedules.
- Add parity checks between the untouched production computation and the copied
  computation where their contracts overlap.
- Iteratively develop and refine properties using minimized counterexamples.
- Record schedules that fail at one work budget and succeed at a larger budget.
- Record candidate guards and evidence showing where they would reject legitimate
  schedules.

### Out of Scope

- Wiring the copied computation into the legacy schedule workflow query.
- Wiring the copied computation into the CHASM scheduler.
- Replacing, refactoring, or promoting the copied implementation into production.
- Changing workflowservice protobuf request or response messages.
- Selecting a production work limit.
- Enforcing a production query-window limit.
- Changing schedule execution, workflow determinism, or replay behavior.
- Changing schedule creation or update validation.
- Adding production metrics, logging, or dynamic configuration.

There is no final integration step in this exercise. Integration will be considered as
a separate project after the analysis produces evidence.

## Questions This Analysis Must Answer

1. Which valid schedule shapes require the most work to produce their first result?
2. Which valid schedule shapes require the most work to prove that no result exists?
3. How much additional work do exclusion calendars introduce?
4. How does requested range length affect work for dense and sparse schedules?
5. Which schedules fail with budgets of 100, 1,000, 10,000, 100,000, and 1,000,000?
6. For each budget, which cases fail there but succeed at the next larger budget?
7. Does a cumulative request budget behave more predictably than a per-result or
   per-calendar budget?
8. Is the work count stable and deterministic for identical inputs?
9. Which empty results are legitimate, and which are caused by invalid input or budget
   exhaustion?
10. Which proposed properties are true schedule semantics, and which are false
    assumptions exposed by DST, jitter, exclusions, finite years, or query boundaries?
11. Which guards would protect computation without rejecting plausible customer
    schedules?
12. Which guards appear attractive but should not be implemented?

## Existing Computation Baseline

The analysis starts from these current paths:

- `service/worker/scheduler/spec.go`
  - `SpecBuilder.NewCompiledSpec`
  - `CompiledSpec.GetNextTime`
  - `CompiledSpec.rawNextTime`
  - interval and exclusion handling
- `service/worker/scheduler/calendar.go`
  - calendar parsing and canonicalization helpers
  - `compiledCalendar.matches`
  - `compiledCalendar.next`
- `service/worker/scheduler/workflow.go`
  - `handleListMatchingTimesQuery`
- `chasm/lib/scheduler/scheduler.go`
  - `Scheduler.ListMatchingTimes`
- `service/worker/scheduler/calendar_test.go`
  - existing example tests and `FuzzCalendar`
- `service/worker/scheduler/spec_test.go`
  - existing interval, calendar, exclusion, start/end, and jitter examples

The legacy and CHASM API loops are reference behavior only. They will not be modified
by this work.

## Analysis Code Layout

Create a test-only directory:

```text
service/worker/scheduler/propertytest/
    calculator_test.go
    calendar_test.go
    canonicalize_test.go
    generators_test.go
    model_test.go
    oracle_test.go
    parity_test.go
    properties_test.go
    budget_properties_test.go
    experiment_test.go
    testdata/
        rapid/
```

All copied calculator and generator files must end in `_test.go`. The analysis code must
not be importable by production packages and must not add a production binary surface.

The copied types and functions will use an `analysis` prefix where necessary to avoid
confusion with the production implementation.

## Copy Strategy

1. Copy the minimum complete computation required to accept a raw `ScheduleSpec` and
   produce matching times.
2. Preserve current behavior before adding instrumentation.
3. Copy parsing, canonicalization, calendar matching, interval matching, exclusions,
   start/end handling, jitter, and the list loop when they are needed for the copied
   path to stand alone.
4. Do not improve or simplify algorithms during the initial copy.
5. Add parity tests using the existing example cases before introducing behavioral
   changes.
6. Commit conceptual changes to the copy in small stages: error propagation first,
   instrumentation second, configurable limits third.
7. When parity intentionally diverges, document the divergence in the test and in the
   findings log rather than weakening the comparison silently.

Copying the computation creates freedom to change signatures and collect diagnostics.
It does not make the copy an independent correctness oracle. Semantic properties must
therefore use a separate model, brute-force oracle, or metamorphic relationship.

## Calculator Contract

The analysis calculator will expose a contract equivalent to:

```go
type ComputeOptions struct {
    MaxResults    int
    MaxIterations int
}

type WorkBreakdown struct {
    Total                    int
    NextTimeCalls            int
    InclusionSourceChecks    int
    CalendarSearchSteps      int
    IntervalChecks           int
    ExclusionChecks          int
    ExcludedCandidateRetries int
    ResultLoopSteps          int
}

type ComputeResult struct {
    Times []time.Time
    Work  WorkBreakdown
}

func ComputeMatchingTimes(
    spec *schedulepb.ScheduleSpec,
    start time.Time,
    end time.Time,
    jitterSeed string,
    options ComputeOptions,
) (ComputeResult, error)
```

The exact Go names may change while implementing the harness, but the observable
contract must retain all of the information above.

### Contract Rules

- Query start is exclusive, matching current list behavior.
- Query end is inclusive, matching current list behavior.
- `MaxResults` and `MaxIterations` must be positive.
- `MaxIterations` applies cumulatively to the entire request.
- A successful result always reports its complete work breakdown.
- An error result reports work consumed before the error.
- Budget exhaustion is distinguishable from invalid input.
- The public-facing list of times is never treated as complete after budget exhaustion.
- A valid, globally satisfiable schedule with no matches in the requested query range
  returns success with an empty list.
- Invalid schedule syntax or structure returns an error rather than a successful empty
  list.
- A schedule spec whose effective timestamp set is empty over its supported schedule
  horizon is invalid for this exercise. This includes an empty inclusion set, an empty
  structured inclusion calendar, an impossible civil-date conjunction, inverted
  schedule-level bounds, and a schedule whose inclusions are completely removed by its
  exclusions.
- Every supplied inclusion and exclusion component must be structurally valid. Every
  supplied inclusion component must also be independently satisfiable, even when
  another union member could produce matches.

## Iteration Definition

The requested iteration count will be implemented as deterministic budget ticks. Wall
clock duration, allocations, and CPU samples are useful secondary observations but are
not suitable as the correctness budget.

A budget tick is consumed for each repeated unit of search work:

- Entering a next-time computation.
- Evaluating an inclusion calendar or interval source.
- Advancing a calendar search field or candidate date/time.
- Evaluating an exclusion calendar against a candidate.
- Rejecting an excluded candidate and searching again.
- Advancing the outer result-producing loop.

The implementation must not increment the counter based on elapsed time, goroutine
scheduling, map iteration order, logging, or other nondeterministic behavior.

`WorkBreakdown.Total` must equal the sum of the budgeted categories. If some diagnostic
categories are intentionally non-budgeted, they must be named separately and excluded
from this equality.

The exact placement of ticks is part of the experiment. Any change to tick placement
must update a version string or constant in the experiment output so results from two
definitions are not compared as if they were identical.

## Error Taxonomy

Define errors that can be detected with `errors.Is` or `errors.As`:

```text
ErrInvalidSpec
ErrUnsatisfiableSpec
ErrInvalidQueryRange
ErrInvalidOptions
ErrIterationLimit
```

Required distinctions:

| Situation | Result |
| --- | --- |
| Malformed calendar range | `ErrInvalidSpec` |
| Invalid interval or phase | `ErrInvalidSpec` |
| Invalid or conflicting timezone | `ErrInvalidSpec` |
| Empty inclusion set or empty structured inclusion | `ErrUnsatisfiableSpec` wrapping `ErrInvalidSpec` |
| Impossible inclusion calendar | `ErrUnsatisfiableSpec` wrapping `ErrInvalidSpec` |
| Schedule start after schedule end | `ErrUnsatisfiableSpec` wrapping `ErrInvalidSpec` |
| Effective inclusions fully removed by exclusions | `ErrUnsatisfiableSpec` wrapping `ErrInvalidSpec` |
| End before start | `ErrInvalidQueryRange` |
| Non-positive result or iteration limit | `ErrInvalidOptions` |
| Work budget exhausted | `ErrIterationLimit` |
| Satisfiable schedule with no match in query range | Empty success |
| Satisfiable finite schedule already exhausted for this query | Empty success |

The limit error must include the configured budget, consumed work, last search time,
and work breakdown. It must not embed raw timezone data or an unbounded serialization of
the schedule.

## Property Testing Library

Use `pgregory.net/rapid` v1.3.x unless repository dependency or license review rejects
MPL-2.0. Rapid is selected for:

- Type-safe generic generators.
- Dependent generation through `rapid.Custom`.
- Integrated shrinking without handwritten shrink trees.
- Persistence and replay of minimized counterexamples.
- State-machine support for later metamorphic query sequences.
- `rapid.MakeFuzz`, which allows the same property to run under Go's coverage-guided
  fuzzer.
- No transitive dependencies outside the Go standard library.

If Rapid is rejected, use `github.com/leanovate/gopter` and implement explicit custom
shrinkers for the semantic schedule model. Do not add both libraries.

Primary references:

- <https://pkg.go.dev/pgregory.net/rapid>
- <https://github.com/flyingmutant/rapid>
- <https://pkg.go.dev/github.com/leanovate/gopter>

## Semantic Model

Generators must create a semantic model first and render it into protobuf forms. This
keeps generation valid by construction and supplies an oracle independent from the
copied parser representation.

The model must represent:

```go
type ScheduleModel struct {
    Calendars  []CalendarModel
    Intervals  []IntervalModel
    Exclusions []CalendarModel
    StartTime  *time.Time
    EndTime    *time.Time
    Jitter     time.Duration
    Timezone   TimezoneModel
}
```

`CalendarModel` should store concrete allowed values or normalized ranges for second,
minute, hour, day of month, month, year, and day of week. `IntervalModel` should store
whole-second interval and phase values.

The model must support renderers for:

- Structured calendar protobufs.
- Calendar string protobufs.
- Cron strings when the generated model is representable in cron syntax.
- Mixed schedule specs containing multiple representation types.

The model must also support a direct `MatchesNominal(time.Time)` operation for oracle
checks without invoking copied calendar search code.

## Generator Rules

### Valid Generation

- Construct valid values directly instead of generating arbitrary protobufs and
  discarding most cases.
- Avoid `Filter` and `SkipNow` for normal validity constraints.
- Draw range start first, then draw range end from `[start, fieldMax]`.
- Draw a positive step appropriate for the selected range.
- Populate every required structured-calendar field except year, whose empty form means
  all years.
- Draw interval first, then draw phase from `[0, interval)`.
- Generate query end as a duration after query start.
- Clamp generated times to the supported calendar horizon.
- Keep all randomness inside Rapid draws so shrinking and replay remain deterministic.

### Invalid Generation

Generate a valid base model and apply exactly one labeled mutation, such as:

- Out-of-range field start.
- End before start in a calendar range.
- Negative or zero step where invalid.
- Nil range element.
- Nil interval element.
- Interval below one second.
- Negative phase.
- Phase equal to or greater than interval.
- Conflicting cron timezones.
- Invalid timezone name or timezone data.
- Excessively long calendar comment.
- Invalid timestamp or duration protobuf representation.

One mutation per case makes the expected error unambiguous and improves shrinking.

### Timezone Generation

Use a fixed checked-in list so generated cases do not depend on enumerating host
timezone files. Include at least:

- `UTC`
- `America/Chicago`
- `America/Los_Angeles`
- `Europe/London`
- `Australia/Lord_Howe`
- `Pacific/Apia`

Create separate fixed examples around DST gaps, repeated hours, half-hour transitions,
and skipped civil dates. Random named-zone generation must not replace these examples.

## Query Range Profiles

Query bounds are analysis parameters, not proposed API guards. The experiment runner
must make both absolute time bounds and start-to-end window bounds configurable.

```go
type QueryRangeProfile struct {
    EarliestStart  time.Time
    EarliestEnd    time.Time
    LatestEnd      time.Time
    MinWindow      time.Duration
    MaxWindow      time.Duration
    WindowBuckets  []time.Duration
}
```

Generate query end within both sets of constraints:

```text
end >= EarliestEnd
end <= LatestEnd
end >= start + MinWindow
end <= start + MaxWindow
```

Profiles with incompatible constraints are invalid configuration and must fail before
case generation. This permits experiments that hold the start distribution constant
while moving only the minimum or maximum allowed end timestamp.

### Default Realistic Profile

- Earliest start: `2020-01-01T00:00:00Z`
- Earliest end: `2020-01-01T00:01:00Z`
- Latest end: `2035-12-31T23:59:59Z`
- Minimum window: one minute
- Maximum window: one year
- Weighted window buckets: one hour, one day, seven days, thirty days, ninety days, and
  one year

This profile is intended to concentrate cases around ranges likely to be requested by
people inspecting upcoming schedule times.

### Short Profile

- Minimum window: one second
- Maximum window: seven days
- Dense weighting at one minute, one hour, one day, and seven days

### Long-Horizon Profile

- Earliest start: calendar year 2000
- Earliest end: calendar year 2001
- Latest end: calendar year 2100
- Minimum window: one year
- Maximum window: the remaining supported calendar horizon
- Buckets: one year, five years, ten years, twenty-five years, fifty years, and full
  remaining horizon

### Boundary Profile

Generate ranges centered around:

- Leap days.
- End-of-month boundaries.
- Year boundaries.
- DST transitions for the selected fixed zones.
- The minimum and maximum supported calendar years.
- Schedule-level start and end times.

The profile name and concrete bounds must be included in failure output and experiment
records.

## Iteration Budget Matrix

The initial budget ladder is:

```text
10
100
1,000
10,000
100,000
1,000,000
```

The experiment runner must accept an override list. The ladder may be expanded when two
adjacent values do not localize a transition adequately.

For every generated valid case:

1. Run with each budget from smallest to largest until the case succeeds or reaches the
   maximum analysis budget.
2. Record the smallest successful budget.
3. Record every lower budget that returned `ErrIterationLimit`.
4. When practical, binary search between the largest failing and smallest successful
   budgets to determine the exact required work count.
5. Verify that all larger budgets produce the same times and same consumed work as the
   smallest successful run.
6. If the case still fails at 1,000,000, preserve it as a high-work counterexample and
   do not assume it is invalid.

The 10,000 budget is one row in this matrix. It is not an acceptance criterion or a
recommended production default.

Also vary `MaxResults` independently across:

```text
1
10
100
1,000
```

This distinguishes work required to find the first match from work required to fill a
large response.

## Experiment Profiles

### Common

- One to three inclusions.
- Zero to two exclusions.
- Common minute, hourly, daily, weekly, and monthly patterns.
- Realistic query range profile.

### Dense

- Per-second and per-minute intervals.
- Calendars matching many values in each field.
- Result limits from 1 through 1,000.

### Sparse

- Explicit years.
- Leap-day schedules.
- Rare day-of-month and day-of-week conjunctions.
- Single months and days.
- Long-horizon query profile.

### Exclusion Heavy

- Dense inclusion plus broad exclusions.
- Exclusions rejecting none, some, most, or all candidates.
- Multiple overlapping exclusions.

### Boundary And Timezone

- Query and schedule start/end equality.
- Nanosecond-bearing input timestamps.
- DST gaps and repetitions.
- Leap years and non-leap century years.
- Calendar-year limits.

### Invalid

- Exactly one known invalid mutation per generated case.
- All expected error categories represented.

### Adversarial

- Many inclusion and exclusion entries.
- Many ranges per field.
- Sparse inclusions combined with broad exclusions.
- Impossible civil-date combinations that remain structurally valid.
- Long query horizons.

Adversarial cases must remain within existing request and field-size constraints when
the purpose is to study computation reachable by real API input. Separate tests may
intentionally exceed those constraints to study local algorithm behavior, but must be
labeled as unreachable through validated production input.

## Initial Property Catalog

Properties are hypotheses. A failing property must first be evaluated as a possibly
incorrect hypothesis before being treated as an implementation bug.

### Safety And Contract

- The calculator never panics for generated input.
- Every execution terminates at or before its configured work budget.
- Invalid specs return `ErrInvalidSpec` rather than empty success.
- Budget exhaustion returns `ErrIterationLimit` rather than empty success.
- Structurally valid but globally unsatisfiable specs return `ErrUnsatisfiableSpec`.
- Satisfiable specs with no match in the requested query range return empty success.
- Work accounting is internally consistent.

### Determinism

- Identical inputs produce identical times, errors, and work breakdowns.
- A replayed Rapid counterexample produces the same outcome.
- Work counts do not depend on test order.

### Ordering And Bounds

- Returned times are strictly increasing.
- Returned times are unique.
- Returned times are after the exclusive query start.
- Returned times are not after the inclusive query end.
- Result length is no greater than `MaxResults`.

### Budget Relationships

- Raising the budget cannot change a previously successful result.
- Raising the budget cannot increase consumed work for an identical successful result.
- A case requiring `N` work units succeeds at `N` and fails at `N-1`.
- Lower-budget failure followed by higher-budget success is reported as a budget
  transition, not invalid input.

### Range Relationships

- Raising `MaxResults` preserves the smaller result as a prefix.
- Moving query start to a returned time produces the remaining suffix.
- Splitting a range and concatenating its results is equivalent to the unsplit range
  when boundary semantics and result caps are accounted for.
- Extending query end cannot alter earlier results.

### Semantic Matching

- Every nominal result matches at least one inclusion in the independent model.
- No nominal result matches an exclusion in the independent model.
- Interval results satisfy epoch-plus-phase modular arithmetic.
- Schedule-level start and end constraints are respected.

### Jitter

- Jitter is deterministic for an identical seed.
- Jittered time is not before nominal time.
- Jitter does not exceed the configured maximum.
- Jitter does not cross the following nominal time according to current semantics.
- Properties explicitly distinguish nominal ordering from jittered ordering.

### Representation

- Equivalent calendar, structured-calendar, and cron renderings produce equal results.
- Canonicalization is idempotent.
- Rendering and re-canonicalizing a semantic model preserves its represented match set.

### Oracle

- For small UTC windows with zero jitter, results equal an independent second-by-second
  enumeration of the semantic model.
- For small timezone-aware windows, enumerating absolute seconds and evaluating local
  calendar fields produces the same set, including repeated and skipped civil times.

### Parity

- Before intentional changes, the copied calculator matches current production output
  for the existing spec and calendar fixtures.
- Divergences are minimized and classified as copy defect, production defect, contract
  clarification, or intentional analysis behavior.

## Properties That Must Not Be Assumed

Do not encode any of these as guards or invariants without evidence:

- An empty query result implies an invalid schedule; the spec may be satisfiable outside
  the requested query range.
- A date absent from one year is globally impossible; leap days and day-of-week
  conjunctions require evaluation across the component's effective year range.
- Matching times have a bounded maximum gap.
- A schedule must match at least once inside a realistic query window.
- Local wall-clock times are unique during DST transitions.
- Jittered results obey properties that only nominal times guarantee.
- More calendar entries always require more work.
- A longer query range always requires more work.
- A 10,000-work budget is sufficient because typical examples fit within it.
- A schedule exceeding an analysis budget should be rejected at creation time.
- A public API end-time limit should match the generator's `MaxWindow`.
- Wall-clock timeout is a deterministic substitute for a work budget.

When a proposed property fails, preserve the minimized case and decide whether the
property, model, copy, or production algorithm is wrong.

## Guard Evaluation Framework

Candidate guards will be recorded but not implemented. Each candidate must answer:

1. What resource or correctness failure does the guard prevent?
2. Is the guard deterministic?
3. Can it be evaluated at spec validation time, query time, or only during search?
4. Which common, sparse, boundary, and adversarial cases would it reject?
5. Does it reject a valid schedule or merely bound one query?
6. Is the failure retryable with a different range or limit?
7. Can the client distinguish partial results from complete results?
8. How would the guard interact with legacy and CHASM behavior?
9. Is the guard supported by observed distributions rather than a single example?
10. What happens under a tenfold increase in concurrent query load?

Potential guards to analyze include:

- Cumulative request work budget.
- Per-next-time work budget.
- Per-calendar search budget.
- Maximum query window.
- Maximum number of inclusion or exclusion entries.
- Maximum total ranges across a spec.
- Maximum result count.

The report must also contain a `Do Not Add` section for guards shown to reject plausible
valid schedules or to depend on nondeterministic timing.

## Rapid Test Organization

Use separate `rapid.Check` tests for distinct properties so shrinking targets one
failure at a time. Do not place the entire catalog in one property callback.

Representative organization:

```text
TestPropertySafety
TestPropertyOrderingAndBounds
TestPropertyDeterminism
TestPropertyBudgetBoundary
TestPropertyRangeDecomposition
TestPropertyRepresentationEquivalence
TestPropertyOracleUTC
TestPropertyOracleTimezone
TestPropertyInvalidSpecsReturnErrors
TestPropertyProductionParity
```

Expose the highest-value callbacks through `rapid.MakeFuzz`:

```text
FuzzMatchingTimesSafety
FuzzMatchingTimesOracle
FuzzMatchingTimesBudget
FuzzMatchingTimesParity
```

Avoid fuzzing all expensive long-horizon profiles in normal unit-test runs. Long-horizon
and million-budget campaigns must be separately selectable.

## Experiment Output

Each experiment record must include:

- Stable case seed or Rapid replay identifier.
- Experiment and query-range profile names.
- Iteration-definition version.
- Canonical schedule spec in bounded protobuf JSON form.
- Query start, query end, and window duration.
- Jitter seed and result limit.
- Attempted budget.
- Status: success, empty success, invalid, or budget exhausted.
- Work breakdown.
- Result count.
- First and last result when present.
- Smallest successful budget when found.
- Largest failing budget when found.
- Whether the case is reachable through currently validated API input.

Aggregate summaries must group by profile and include:

- Number of cases.
- Success, empty, invalid, and exhausted counts.
- P50, P90, P99, and maximum work among successful cases.
- Count blocked at each budget.
- Count that succeeds at the next budget.
- Result-count distribution.
- Work-breakdown distribution.
- The top minimized examples by total work and by each work category.

Generated reports should be written outside the repository by default. Minimized
regression cases selected for permanent retention belong in test fixtures, not in a
bulk generated corpus committed automatically.

## Findings Log

Create `service/worker/scheduler/propertytest/FINDINGS.md` when Milestone 2 begins.
Every material counterexample must record:

- Property or hypothesis.
- Minimized schedule and query range.
- Observed result and work breakdown.
- Classification.
- Whether the property was retained, narrowed, or rejected.
- Whether the case suggests a production bug, documentation issue, or guard candidate.
- Follow-up property or generator change.

Classifications are:

```text
copy-defect
model-defect
generator-defect
property-too-strong
expected-edge-case
possible-production-defect
guard-evidence
do-not-guard-evidence
```

## Execution Phases

### Milestone 1: Build The Analysis Harness

#### Phase 1: Baseline And Dependency

- Add Rapid as a test dependency after dependency-policy review.
- Create the test-only analysis directory.
- Capture current example fixtures used for parity.
- Record exact commands and iteration-definition version.

#### Phase 2: Copy Current Computation

- Copy canonicalization, parsing, calendar matching, intervals, exclusions, jitter, and
  list computation.
- Keep behavior unchanged.
- Add deterministic parity tests for existing fixtures.
- Correct copy defects until parity passes.

#### Phase 3: Add Errors And Work Instrumentation

- Add the error taxonomy.
- Add the shared cumulative work counter and breakdown.
- Add configurable result and work limits.
- Add exact boundary tests for `N` versus `N-1` budgets.
- Confirm all executions terminate within the configured budget.

#### Phase 4: Add Model, Generators, And Oracle

- Implement the semantic model.
- Implement valid, invalid, sparse, exclusion, timezone, and range-profile generators.
- Implement renderers.
- Implement small-window independent oracles.
- Verify that Rapid minimizes a deliberately injected failure to a readable case.

#### Phase 5: Add Experiment Runner

- Implement budget and result-limit matrices.
- Implement configurable query-range profiles.
- Emit per-case records and aggregate summaries.
- Add selectors so expensive profiles do not run in the default unit suite.

Milestone 1 is complete when the acceptance criteria below pass. No production wiring
or implementation decision follows from completion.

### Milestone 2: Iterative Property Analysis

#### Phase 6: Establish Baseline Properties

- Implement safety, ordering, bounds, determinism, error, and accounting properties.
- Run all query-range profiles across the initial budget matrix.
- Start `FINDINGS.md` with discovered counterexamples.

#### Phase 7: Add Semantic And Metamorphic Properties

- Add independent oracle checks.
- Add range decomposition, suffix, prefix, and representation properties.
- Add jitter, exclusion, interval, and timezone properties.
- Narrow or reject incorrect properties based on minimized cases.

#### Phase 8: Search Work-Budget Transitions

- Concentrate generation on cases near 100, 1,000, 10,000, 100,000, and 1,000,000.
- Minimize examples that fail one budget and succeed at the next.
- Separate first-result cost from many-result cost.
- Separate no-match proof cost from successful-search cost.

#### Phase 9: Evaluate Guard Hypotheses

- Apply each candidate guard to the retained generated corpus.
- Measure rejected cases by profile.
- Record guards that reject plausible valid schedules in `Do Not Add` findings.
- Produce evidence, not a production recommendation, for the separate follow-up project.

Milestone 2 is iterative. New edge cases should lead to new focused generators and
properties rather than only example tests.

## Acceptance Criteria For Milestone 1

- The copied computation is entirely test-only.
- No production list-matching or schedule-execution path is modified.
- Existing representative examples pass parity checks.
- Invalid specs produce typed errors in the copy.
- Budget exhaustion is distinguishable from empty success.
- Work counts and breakdowns are deterministic.
- `N` versus `N-1` budget behavior has focused tests.
- Query range bounds and window buckets are configurable.
- Budget values are configurable and include the initial matrix.
- Valid, invalid, sparse, exclusion-heavy, timezone, and adversarial generators exist.
- A small-window independent oracle exists.
- Rapid shrinking produces replayable, readable counterexamples.
- Expensive experiment profiles are opt-in.
- Default tests complete within the repository's normal unit-test expectations.

## Acceptance Criteria For Milestone 2

- Every property in use has a focused generator or documented coverage profile.
- Incorrect properties discovered during the campaign are documented rather than
  silently weakened.
- Cases blocked by 10,000 but successful at higher budgets are retained and summarized.
- Cases still blocked at the maximum analysis budget are retained separately.
- Work distributions are reported per profile and query-window bucket.
- Empty query results from satisfiable schedules are distinguished from unsatisfiable,
  structurally invalid, and budget-exhausted schedules.
- Candidate guards and `Do Not Add` guards are supported by minimized examples.
- Findings cover intervals, calendars, mixed specs, exclusions, jitter, finite years,
  DST, leap behavior, range boundaries, and representation equivalence.

## Verification Commands

Use `-tags test_dep` for all repository Go tests.

```bash
go test -tags test_dep ./service/worker/scheduler/propertytest
go test -tags test_dep ./service/worker/scheduler/propertytest -rapid.checks=10000
go test -tags test_dep ./service/worker/scheduler/propertytest \
  -run='^$' -fuzz='^FuzzMatchingTimes' -fuzztime=10m
go test -tags test_dep ./service/worker/scheduler
go test -tags test_dep ./chasm/lib/scheduler
make lint-code
```

Long-horizon and million-budget experiments must use explicit test names or flags and
must not be included accidentally in the ordinary `go test` path.

## Implementation Discipline

- Keep the production worktree changes limited to test-only analysis files, the Rapid
  dependency, and this specification.
- Do not refactor production code while copying it.
- Do not change an observed behavior merely to make a property pass.
- Preserve minimized failures before changing a generator or property.
- Prefer generators that construct valid schedules over rejection-heavy generation.
- Keep random choices deterministic and under Rapid control.
- Version the iteration definition and experiment profiles.
- Treat 10,000 as an experimental data point.
- Treat min/max query end settings as generator controls, not API recommendations.
- Defer every production integration and guard decision to a separate follow-up.
