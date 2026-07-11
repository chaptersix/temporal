# Plan 2 Guard Decision Record

Campaign: `20260710-plan2-computational-red-team-872e129a8-adversarial`

## PLAN2-GUARD-01: cumulative-matching-work

- Status: deferred
- OBSERVATION: example-threshold valid rejections=4; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: retry with a smaller result limit or range may succeed.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-02: separate-validation-work

- Status: deferred
- OBSERVATION: example-threshold valid rejections=4; invalid cases stopped earlier=1.
- INFERENCE: Retry semantics: retrying unchanged is not meaningful without a larger proof budget.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-03: maximum-query-window

- Status: rejected
- OBSERVATION: example-threshold valid rejections=2; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: range reduction is actionable but window alone poorly predicts mixed-union work.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-04: maximum-result-count

- Status: deferred
- OBSERVATION: example-threshold valid rejections=3; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: a smaller result limit is actionable.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-05: maximum-components

- Status: rejected
- OBSERVATION: example-threshold valid rejections=1; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: removing equivalent components changes the spec rather than the query.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-06: maximum-total-ranges

- Status: rejected
- OBSERVATION: example-threshold valid rejections=1; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: range count is an incomplete cost proxy.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-07: serialized-size-plus-work

- Status: deferred
- OBSERVATION: example-threshold valid rejections=1; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: query changes can reduce dynamic work but not serialized size.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-08: cancellation-deadline-ticks

- Status: deferred
- OBSERVATION: example-threshold valid rejections=0; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: clients may retry after reducing work or extending a deadline.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-09: per-next-time-work

- Status: rejected
- OBSERVATION: example-threshold valid rejections=0; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: later results can be expensive even when each prior call completed.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-10: per-calendar-search-work

- Status: rejected
- OBSERVATION: example-threshold valid rejections=0; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: it omits interval, exclusion, validation, and result-loop work.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-11: elapsed-wall-clock-budget

- Status: rejected
- OBSERVATION: example-threshold valid rejections=0; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: timing is nondeterministic and host-dependent.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

## PLAN2-GUARD-12: densest-or-sparsest-source-estimate

- Status: rejected
- OBSERVATION: example-threshold valid rejections=0; invalid cases stopped earlier=0.
- INFERENCE: Retry semantics: mixed union members create cross-product cost.
- OPEN QUESTION: Tenfold-load evidence is deferred to Plan 3.
- RECOMMENDATION: Preserve phase, consumed work, limit, and actionable query changes in any future client error; select no production guard in Plan 2.

