# Plan 3 Guard And Retry Decisions

## PLAN3-GUARD-01: cumulative-matching-work

- Status: deferred
- OBSERVATION: Campaign 20260711-plan3-operational-red-team-b960841d9-concurrency completed the mandatory tenfold-load matrix without changing iteration-v1 work vectors.
- INFERENCE: Cumulative work remains measurable but a production limit is not selected.
- RECOMMENDATION: Preserve the Plan 2 deferral; do not enforce a production guard.

## PLAN3-GUARD-02: separate-validation-work

- Status: deferred
- OBSERVATION: The maximum-validation case retained zero matching work at every concurrency tier.
- INFERENCE: Combining validation and matching would erase an actionable phase distinction.
- RECOMMENDATION: Keep the budgets and typed outcomes separate.

## PLAN3-GUARD-03: cancellation-deadline-ticks

- Status: deferred
- OBSERVATION: Deterministic polling stops on the first observable boundary and is accounted separately from iteration-v1 work.
- INFERENCE: Poll placement is operational evidence, not a production choice.
- RECOMMENDATION: Carry the explicit check order into a separate production proposal; enforce nothing in Plan 3.

## PLAN3-RETRY-01: deterministic-budget-exhaustion

- Status: accepted-analysis-policy
- OBSERVATION: Immediate identical retries reproduce the same work vector and exhaustion point.
- INFERENCE: Identical retry is non-progressing unless the query, result limit, or configured analysis budget changes.
- RECOMMENDATION: Mark smaller windows, fewer results, or larger phase-appropriate budgets as potentially actionable; never report a partial prefix as complete.

## PLAN3-STATIC-GUARDS

- Status: rejected
- OBSERVATION: The Plan 2 query-window, component-count, range-count, per-next-time, per-calendar, elapsed-time, and single-source estimates retain their valid false positives or incomplete coverage under tenfold load.
- RECOMMENDATION: Preserve all Plan 2 rejections. No production guard is selected.

## Plan 2 Decision Tenfold-Load Follow-Up

| Plan 2 decision | Status after Plan 3 | Tenfold-load evidence |
| --- | --- | --- |
| PLAN2-GUARD-01 cumulative matching work | deferred | OBSERVATION: one and ten workers retained identical matching vectors; no limit was selected. |
| PLAN2-GUARD-02 separate validation work | deferred | OBSERVATION: maximum validation work remained separate with zero matching work. |
| PLAN2-GUARD-03 maximum query window | rejected | OBSERVATION: load did not repair the valid false positives or make window length a complete cost proxy. |
| PLAN2-GUARD-04 maximum result count | deferred | OBSERVATION: smaller result limits remained actionable, but no production maximum was selected. |
| PLAN2-GUARD-05 maximum components | rejected | OBSERVATION: tenfold load did not make component count a sufficient cost predictor. |
| PLAN2-GUARD-06 maximum total ranges | rejected | OBSERVATION: tenfold load did not make range count a sufficient cost predictor. |
| PLAN2-GUARD-07 serialized size plus work | deferred | OBSERVATION: the bounded large-timezone case completed at one and ten workers with deterministic work. |
| PLAN2-GUARD-08 cancellation/deadline ticks | deferred | OBSERVATION: test-only polling stopped with zero post-observation ticks; production placement remains undecided. |
| PLAN2-GUARD-09 per-next-time work | rejected | OBSERVATION: concurrency did not add the omitted cumulative-request coverage. |
| PLAN2-GUARD-10 per-calendar work | rejected | OBSERVATION: concurrency did not add the omitted validation, interval, exclusion, or result-loop coverage. |
| PLAN2-GUARD-11 elapsed wall-clock budget | rejected | OBSERVATION: latency changed with worker count while deterministic work did not. |
| PLAN2-GUARD-12 densest/sparsest estimate | rejected | OBSERVATION: mixed workloads retained the Plan 2 cross-product limitation. |

RECOMMENDATION: Treat this table as the Plan 3 resolution of every Plan 2 tenfold-load open question. It changes no production guard status and selects no production guard.
