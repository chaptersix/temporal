# Plan 1: Validity And Property Hardening

## Objective

Replace the harness's current empty-set semantics with the exercise's stricter validity
contract, build an independently testable satisfiability classifier, and demonstrate
that the property suite detects representative defects through mutation testing.

## Current State To Change

The current analysis harness intentionally mirrors several production behaviors that no
longer match the exercise contract:

- `TestValidEmptyScheduleShapesReturnEmptySuccess` treats empty inclusions, empty
  structured calendars, impossible dates, and inverted schedule bounds as successful
  empty sets.
- `scheduleCaseGenerator` can produce schedule-level start after end.
- The valid generator can create impossible calendar conjunctions.
- The independent oracle reports match sets but does not classify global
  satisfiability.
- All-excluded schedules are treated as valid high-work computations.

These are migration inputs, not accepted final behavior.

## Validity Model

### Structural Invalidity

Return `ErrInvalidSpec` for:

- Invalid protobuf timestamps or durations.
- Negative jitter.
- Invalid timezone data or conflicting timezone declarations.
- Nil or malformed interval definitions after protobuf normalization.
- Out-of-range calendar fields.
- Range end before start.
- Invalid range step.
- Invalid inclusion or exclusion calendar fields.
- Schedule start after schedule end.

### Component Unsatisfiability

Return `ErrUnsatisfiableSpec`, wrapping `ErrInvalidSpec`, when:

- The inclusion set is empty.
- A structured inclusion calendar lacks any required non-year field.
- A supplied inclusion calendar has no civil timestamp satisfying all its fields over
  its effective year range.
- An inclusion component has no timestamp inside the schedule-level start/end bounds.

Every supplied inclusion is checked independently before union evaluation.

Impossible exclusion calendars are also invalid. Although they subtract nothing, they
are caller-supplied components that fail the exercise's component-validity rule.

### Effective-Set Unsatisfiability

Return `ErrUnsatisfiableSpec` when at least one inclusion is independently satisfiable
but the union, after schedule bounds and exclusions, contains no timestamp in the
supported schedule horizon.

This includes fully excluded interval and calendar schedules.

### Query-Local Empty Result

Return successful empty matching times when:

- The schedule is globally satisfiable.
- The requested query range is valid.
- No schedule timestamp lies inside that query range.

Validation must not depend on the ListScheduleMatchingTimes query range.

### Indeterminate Validation

Satisfiability proof work receives a separate configurable validation budget. If the
validator cannot prove satisfiable or unsatisfiable within that budget, return a typed
`ErrValidationLimit` outcome.

Never map validation-budget exhaustion to `ErrUnsatisfiableSpec`.

## Validator Architecture

Implement the validator in the test-only analysis package with these stages:

1. Canonicalize and perform protobuf/field validation.
2. Compute the effective supported schedule horizon.
3. Validate each inclusion and exclusion component independently.
4. Determine whether the complete effective set contains at least one timestamp.
5. Return a structured validation result with proof work and witness information.

Suggested result:

```go
type ValidationStatus int

const (
    ValidationValid ValidationStatus = iota
    ValidationInvalidStructural
    ValidationInvalidComponentUnsatisfiable
    ValidationInvalidEffectiveSetEmpty
    ValidationIndeterminateBudget
)

type ValidationResult struct {
    Status       ValidationStatus
    Work         ValidationWorkBreakdown
    Witness      time.Time
    Component    string
    Reason       string
}
```

A valid result must include a concrete witness timestamp. An unsatisfiable result must
identify whether the proof concerns one component or the full effective set.

## Calendar Satisfiability Algorithm

Use finite civil-date enumeration rather than calling the copied `next` algorithm as
the oracle:

1. Determine the component's effective years, intersected with 2000 through 2100 and
   schedule-level bounds.
2. Enumerate civil dates in the component timezone.
3. Check year, month, day of month, and day of week predicates.
4. Determine whether at least one hour/minute/second tuple is permitted.
5. Convert permitted local tuples to absolute timestamps.
6. Reject tuples normalized by Go into a different local time because of DST gaps.
7. Accept either absolute occurrence during repeated hours.
8. Stop on the first witness.

An impossible date such as February 30 produces no witness and is invalid. February 29
must be evaluated across all effective years before being classified.

## Effective-Set Satisfiability Algorithm

The validator must not enumerate every absolute second across a century. Implement a
day-at-a-time existence check:

1. Enumerate effective civil days in the schedule timezone.
2. Represent possible seconds of the day as a fixed 86,400-bit set.
3. Add inclusion-calendar seconds permitted on that date.
4. Add interval occurrences intersecting that absolute-day span using modular
   arithmetic.
5. Remove exclusion-calendar seconds.
6. Apply exact schedule start/end boundaries.
7. Return the first remaining absolute timestamp as the witness.

Only one day's bitsets need to be retained. Reuse buffers to keep allocation stable.
Handle 23-, 24-, and 25-hour local days by mapping local tuples to absolute timestamps
rather than assuming every day contains exactly 86,400 absolute seconds.

The implementation may begin with a slower reference algorithm for reduced horizons.
The optimized validator must be differentially checked against that reference before it
is trusted on the full supported horizon.

## Generator Changes

Create four explicit generator families:

```text
validSatisfiableScheduleCaseGenerator
invalidStructuralScheduleCaseGenerator
invalidComponentScheduleCaseGenerator
invalidEffectiveSetScheduleCaseGenerator
```

### Valid Satisfiable Generator

- Generate a witness timestamp first.
- Construct every inclusion calendar so the witness or another retained witness matches.
- Generate interval phase from a witness modulo the interval.
- Generate schedule bounds around at least one witness.
- Generate exclusions that preserve at least one effective witness.
- Store the witness in the generated semantic model for assertion.

### Invalid Component Generator

Start with a valid component and apply one labeled mutation:

- Remove a required field.
- Select February 30.
- Select April 31.
- Select February 29 only in non-leap explicit years.
- Create an impossible day-of-month/day-of-week/year conjunction.
- Move schedule bounds outside the component's only possible occurrence.

### Invalid Effective-Set Generator

- Generate a satisfiable inclusion.
- Generate exclusions that cover every inclusion witness over the effective horizon.
- Include interval cases, calendar cases, and mixed unions.
- Include small horizons where the independent reference can prove emptiness cheaply.

Avoid rejection-heavy generation. Construct each classification by design.

## Property Set

### Classification

- Every valid generated spec returns `ValidationValid` with a witness.
- The independent model confirms every returned witness.
- Every structural mutation returns structural invalidity.
- Every impossible component returns component unsatisfiability.
- Every fully excluded effective set returns effective-set unsatisfiability.
- Raising validation budget never changes a completed classification.
- A validation result completed with work `N` completes at `N` and is indeterminate at
  `N-1`.

### Query Separation

- Changing only the query range does not change spec validation.
- A valid schedule may produce empty results for a query range excluding its witness.
- Expanding a query to include a witness produces at least one result.

### Representation

- Equivalent Calendar, StructuredCalendar, and Cron renderings have equal validity.
- Canonicalization preserves validity and a valid witness.
- Invalid exclusions are rejected in every supported representation.

### Soundness And Completeness On Reduced Domains

For generated horizons of at most one year:

- The optimized validator and brute-force reference return the same classification.
- Every valid classification has a brute-force witness.
- Every unsatisfiable classification has no brute-force witness.

For full horizons, require sound witnesses and differential agreement on cases where
the reference completes within its budget.

## Existing Test Migration

Replace `TestValidEmptyScheduleShapesReturnEmptySuccess` with:

- `TestInvalidEmptyInclusionSet`
- `TestInvalidEmptyStructuredInclusion`
- `TestInvalidImpossibleCivilDate`
- `TestInvalidInvertedScheduleBounds`
- `TestValidScheduleCanHaveEmptyQueryResult`

Update `TestAllExcludedDenseIntervalReachesBudget` to distinguish:

- validation proof budget exhaustion at low budgets;
- effective-set unsatisfiability when a sufficient proof budget is supplied;
- matching computation must not run after validation proves invalidity.

Update generators used by all semantic properties so their `valid` cases are guaranteed
globally satisfiable.

## Mutation Testing Plan

Add test-only fault switches to the copied validator and calculator. Faults must not be
available to production code.

Required mutations:

| Mutation | Property Expected To Kill It |
| --- | --- |
| Make query start inclusive | ordering/bounds or range decomposition |
| Ignore all exclusions | exclusion subtraction or oracle |
| Stop validating exclusions | invalid structural classification |
| Accept empty structured inclusion | invalid component classification |
| Treat February 30 as satisfiable | reduced-domain completeness |
| Ignore day of week | oracle or witness soundness |
| Permit schedule start after end | invalid bounds classification |
| Return empty on budget exhaustion | safety/error taxonomy |
| Allow duplicate union results | strict ordering/union property |
| Let jitter cross next nominal | jitter property |
| Treat validation indeterminate as invalid | budget monotonicity/error taxonomy |

Create a mutation kill matrix in `FINDINGS.md`. Each mutation must be caught by a named
property without relying on an unrelated panic or timeout.

## Campaigns

Run:

```bash
go test -tags test_dep ./service/worker/scheduler/propertytest -rapid.checks=10000
go test -tags test_dep ./service/worker/scheduler/propertytest \
  -run='^$' -fuzz='^FuzzScheduleValidation$' -fuzztime=30s -parallel=1
go test -tags test_dep ./service/worker/scheduler/propertytest \
  -run='^$' -fuzz='^FuzzMatchingTimesOracle$' -fuzztime=30s -parallel=1
```

Retain minimized examples for every property revision and validation defect.

## Acceptance Criteria

- The revised invalidity contract is encoded in tests and documentation.
- Empty, impossible, inverted-bound, and fully excluded effective sets are invalid.
- Query-local empty results remain successful for globally satisfiable specs.
- Validation returns a witness for every valid classification.
- Reduced-domain brute-force differential properties pass at least 50,000 checks.
- Every planned mutation is killed by a named property.
- Validation-budget exhaustion remains distinct from invalidity.
- Existing production parity is retained only for cases where the exercise contract has
  not intentionally diverged.
- Legacy and CHASM package tests remain unchanged and passing.

## Risks And Tradeoffs

- Effective-set satisfiability with intervals, exclusions, and timezones is materially
  more complex than structural validation.
- Full-horizon proof can itself become a denial-of-service vector, so validation needs a
  separate deterministic budget.
- A stricter exercise contract intentionally diverges from current production behavior.
- Day bitsets provide an analyzable reference but may not be the eventual production
  implementation.
- An indeterminate result means the exercise cannot always synchronously decide whether
  a spec is invalid; it must not guess.
