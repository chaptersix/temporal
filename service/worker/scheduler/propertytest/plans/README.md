# Schedule Property Analysis Execution Plans

## Purpose

These plans extend the test-only schedule matching-time analysis harness. They do not
modify the production legacy scheduler, CHASM scheduler, public protobuf API, or
workflow replay behavior.

The plans must be executed in order because each produces inputs and guarantees needed
by the next:

1. [Validity And Property Hardening](01-validity-and-property-hardening.md)
2. [Computational Red Team And Budget Calibration](02-computational-red-team-and-budget-calibration.md)
3. [Operational Red Team](03-operational-red-team.md)
4. [Results Processing And Decision Record](04-results-processing-and-decision-record.md)

## Fixed Decisions

For this exercise, a schedule spec is invalid when any of the following is true:

- It has no inclusion calendar or interval.
- It contains an empty structured inclusion calendar.
- An inclusion calendar is impossible over its effective supported year range.
- Schedule-level start time is after schedule-level end time.
- Any inclusion or exclusion component is structurally malformed.
- Its complete effective timestamp set is empty after schedule bounds and exclusions.

Every supplied inclusion component must be independently satisfiable. A valid interval
does not excuse an impossible calendar in the same union.

A valid, globally satisfiable schedule may still return an empty result for a particular
ListScheduleMatchingTimes query range. Query-local emptiness is not invalidity.

## Shared Result Vocabulary

Every validator or calculator outcome must be one of:

```text
valid-success
valid-empty-query-result
invalid-structural
invalid-unsatisfiable-component
invalid-empty-effective-set
indeterminate-validation-budget
matching-computation-budget-exhausted
cancelled
deadline-exceeded
```

`indeterminate-validation-budget` must never be silently converted into invalidity or a
successful empty result.

## Shared Artifacts

The plans operate on and extend:

- `schedule-matching-times-property-analysis-spec.md`
- `service/worker/scheduler/propertytest/FINDINGS.md`
- `service/worker/scheduler/propertytest/testdata/rapid/`
- The copied calculator and semantic model under `propertytest/`

New generated bulk data must go to a configurable output directory outside the
repository. Only minimized, reviewed regression fixtures should be committed.

## Execution Gates

Plan 2 must not begin until Plan 1's validity classifier is deterministic, independently
checked on reduced domains, and mutation-tested.

Plan 3 must not begin until Plan 2 has produced a reviewed corpus containing low-cost,
budget-transition, maximum-work, all-excluded, timezone, and malformed cases.

No plan or campaign is complete until Plan 4's result-bundle requirements are met. This
gate applies continuously: each campaign records its evidence before the next campaign
changes generators, properties, work definitions, or environment.

No production guard or integration proposal is part of these plans. Each plan produces
evidence for a separate implementation decision.

## Verification Baseline

All Go tests use `-tags test_dep`. Long and stress campaigns remain opt-in.

```bash
go test -tags test_dep ./service/worker/scheduler/propertytest
go test -tags test_dep ./service/worker/scheduler/propertytest -rapid.checks=10000
go test -tags test_dep ./service/worker/scheduler
go test -tags test_dep ./chasm/lib/scheduler
```

Repository-wide lint is deferred until explicitly requested.

## Operational Context Check Order

Plan 3 retains iteration definition `v1` and validator definition `v1`. Context polling
is separately versioned as `context-check-v1` and does not contribute to either work
total. The analysis API bounds result-slice capacity at the documented 10,000-result
analysis maximum; the reviewed Plan 2/3 replay corpus remains bounded to 1,000 results.
This is a test-harness allocation bound, not a proposed production guard.

Plan 3 campaign manifests use the composite work definition
`v1/validator-v1/context-check-v1`. Comparisons with Plan 2 use a documented projection
that removes the new non-budgeted cancellation-check fields, and the complete Plan 2
corpus is rerun under Plan 3. No aggregate silently mixes the composite and Plan 2
definitions.

After static option and query-range checks, each existing validation or matching work
boundary performs these operations in order:

1. Invoke the deterministic test hook, when configured.
2. Increment the separately reported cancellation-check count.
3. Return `context.Canceled` or `context.DeadlineExceeded` when the context is done.
4. Check the active validation or matching budget.
5. Consume the existing `v1` work tick.

Validation completes before matching begins, so a validation budget can win only in
the validation phase and a matching budget can win only after valid classification.
An observable cancellation or deadline therefore stops with zero additional budgeted
ticks. Error results may expose a stable prefix for analysis, but `Complete` remains
false and the prefix is never presented as a complete response.
