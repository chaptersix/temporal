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
