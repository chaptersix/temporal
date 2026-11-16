# WI-007 independent bounded overlap-effect model

Evidence ID: E-EFFECTS-MODEL-001. Contract source: WI-007 D39–D42 at
`d8a84beffd32cf0451bc6c9db56993c28fe162b4`. The specification imports no production code.

```sh
python3 docs/development/scheduler2/model/effects/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-effects-model-witness-fixed
```

The runner uses pinned FizzBee v0.5.4 at 100 and 200 actions, one atomic action
at a time, for each of three factored specifications, then six one-fault mutants.
Each case has a 300-second timeout;
process-group termination also kills wrapper descendants. Baselines require the
success marker and zero exit status. Mutants require their particular named
invariant, not a parser error, timeout or unrelated failure.

## Fixture and abstraction

The protocol specification (`effects.fizz`) has one committed start intent (target
1), initially unknown chain, an abstract successor obligation, and at most two
accepted overlap demands independently choosing cancel or terminate. It preserves
all binding-time, demand, result, update and clock ordering of the original product
model. The separate selection specification (`selection.fizz`) starts with accepted
BUFFER waiters (2,3), and admits at most two overlap entries (4,5) into capacity4.
An overflow event creates no demand or debit. Initial admissions and general queue
ingress remain assumed/covered separately in WI-005.
At most two effect records exist, keyed `1/cancel` and `1/terminate`, distinct from
the target start identity. Demands coalesce by kind. Desired unbound effects have
no request and cannot issue. Acknowledgment supplies chain-1 and freezes namespace,
workflow, first-run fence, empty RunId, effect identity, bound time, 60-second
horizon and one-second backoff. A definition update cannot rewrite snapshots.

The binding fields are symbolic request data. The identity field represents a
local effect identity; only cancel has a corresponding remote RequestId in the
concrete API. No remote terminate deduplication or exactly-once RPC claim follows.

Issuance selects one effect and records its immutable request in an observation
journal. One permit limits simultaneous in-flight issuance. At most two attempts
per kind are allowed. Clock values are 0,1,60; there is no rollback. Retry issuance
requires RetryAt and a strictly unexpired deadline; observations independently
check at least one second between attempts. This is a finite retry-timing subset,
not verification of arbitrary error sequences, rollback or every deadline value.
A request bound later may have a deadline beyond the explored clock horizon.

Terminate demand suppresses every subsequent cancel issuance. An in-flight cancel
response after terminate is ignored for scheduler state and cannot suppress the
terminate effect. The model represents supersession using the terminate-demand
guard rather than a separate persisted Superseded enum; an unused cancel request
may remain bound in this abstraction even if the adapter avoids binding it.

Success settles Accepted and NotFound settles NoTarget, separately. Other results
retry while within the horizon and otherwise retain Ambiguous. Late success or
NotFound can settle an ambiguous request. Neither outcome releases occupancy,
changes started count/carryover, debits budget, or creates successor admission.
Matching target completion alone counts an early start if necessary, changes
symbolic carryover, clears effects and arms queued drain. Old effect results after
that completion or a successor admission stutter.

## Factorization and queue selection

The projection boundary is accepted demand plus matching target completion. Queue
selection consumes no effect transport state; effect results cannot release its
blocker. The protocol checks that boundary while selection checks its consumers.
The factored models omit the Cartesian product of eligibility/selection choices
with transport histories. This is a manually argued decomposition, not a machine-
checked composition/refinement theorem. Concrete adapter tests must exercise the
combined transitions, especially demand publication and completion-created drain.

`binding.fizz` complements the protocol clock horizon with a delayed-binding
scenario: an unknown chain waits until tick60, binds deadline120, can issue/retry
at60/61, expires at120 and accepts late settlement. Early binding at0 remains an
alternative. This checks that the horizon starts at binding, not original demand,
without multiplying every queue/transport ordering by another global time range.

## Queue selection and observations

Current eligibility excludes no entry, entry 4, or entry 5. Free drain filters
ineligibility first, then scans eligible entries: the first is selected; each
later cancel/terminate replaces only that unadmitted selection. Intervening BUFFER
entries remain ordered. A declarative oracle independently chooses the last
eligible overlap entry, or the first eligible entry when no overlap remains,
and computes remaining/superseded identities. Later cancel can replace earlier
terminate: effect issuance dominance does not govern candidate selection.
Only the selected admission debits the budget; committed target 1 is immutable.

The protocol checks seven safety assertions and six witnesses; selection checks
three safety assertions and four witnesses; delayed binding checks two safety
assertions and four witnesses. These cover completion-owned occupancy, immutable
fencing and retry timing, no effect accounting/carryover, supersession, accepted
queue demand, filtered replacement, overflow and delayed-horizon recovery.

| Mutation | Required invariant failure |
| --- | --- |
| `EFFECT_RELEASES_BLOCKER` | `OnlyCompletionReleasesTarget` or `CompletionCreatesDrain` |
| `CANCEL_AFTER_TERMINATE` | `TerminateDominatesFutureIssuance` |
| `RETRY_FENCE_DRIFTS` | `FencedImmutableDistinctRequests` |
| `FIRST_EFFECT_WINS` | `SelectionAfterEligibility` |
| `OVERFLOW_CREATES_DEMAND` | `AcceptedDemandOnly` |
| `HORIZON_FROM_DEMAND` | `BindingStartsHorizon` |

Failed publication, stale physical task validation and restart are explicit
stuttering boundaries, not a proof of the database/adapter implementation. Exact
incarnation matching, RPC transport, payload separation/selective loading,
serialization, CHASM task publication and concrete mutex ownership need actual
implementation tests. Absence/null/failure carryover semantics remain WI-003;
this model tracks only whether effect results can change a symbolic carryover.
No automatic Go refinement proof, liveness assertions or fairness assumptions
are present. FizzBee's `IsLive: true` output is vacuous for this safety model.

## Exploration history

The first exploration was stopped before acceptance to add RetryAt guards and
explicit clock advancement, rather than checking only frozen backoff metadata.
The second product exploration reached its actual 300-second timeout with last
reported 380000 visited nodes and 478218 queued. Its timeout log is preserved; it
is not a passing result. The redundant second bound was stopped. Factoring keeps
protocol-critical bind/result ordering and moves independent selection to its own
checked specification; delayed binding adds complementary full-horizon coverage.
The runner now writes results incrementally and stops immediately on any baseline
failure or timeout. Only complete successful baselines and named mutation failures
qualify as evidence.

## Manual adapter mapping

Inspected after model authoring: integration `6175ef959`,
`chasm/lib/scheduler2/internal/decision/effects.go`. Model Desired maps to
AwaitingTarget; Ready/Retry/InFlight project to EffectPending, with InFlight/busy
being transport state rather than an additional persisted enum. Accepted,
NoTarget and Ambiguous map to EffectAccepted, EffectAbsent and EffectAmbiguous.
Terminate-demand suppression projects unfinished cancel to EffectSuperseded.
The model may retain a suppressed cancel that the concrete highest-demand
reconciler never creates; its absence of further issuance is the shared behavior.
Frozen snapshots map to separate effect request data; the model does not inspect
or verify DATA loading. Matching completion's clear maps to the blocker-dependent
reconciliation that removes target effects.

The model's simple `now+1` retry scheduling checks the one-second minimum in its
finite clock/attempt domain. It does not reproduce repeated failure delivery's
concrete `max(now, previous RetryAt)` advancement or rollback clamping; deterministic
implementation regressions must cover those exact physical deadline identities.
Neither this mapping nor the factorization is an automated correspondence proof.

## Independent witness review

Review found three existential flags could conflate events in reverse order. The
corrected witnesses record a stale response only when a successor has already
been admitted; a cancel response only when that cancel is actually in flight and
terminate demand exists; and an updated retry at the retry Issue transition itself.
The entire factored suite was rerun after these changes. Prior factored results
remain local exploration evidence, not the final accepted specification hash.
The selection fixture always retains two eligible initial BUFFER entries; an
all-ineligible queue is outside this model and remains a concrete-test obligation.

## Accepted results

`/private/tmp/scheduler2-effects-model-witness-fixed` completed all six baselines and
six named sensitivity checks. Each specification exhausted the same graph at both
100 and 200 actions, with zero queued nodes:

| Specification | Visited nodes | Valid nodes | Unique states |
| --- | ---: | ---: | ---: |
| Effect protocol | 90058 | 72728 | 18168 |
| Queue selection | 121 | 121 | 99 |
| Delayed binding | 176 | 176 | 138 |

All 12 safety assertions and 14 existential witnesses passed; every mutant failed
its named invariant. Exact per-case commands, hashes and outcomes, compact logs
and mutant counterexamples are in `evidence/`; full graphs stay at recorded local
paths. The actual product-model timeout and abandoned first exploration log are
retained separately and are not included as passing results. Python syntax and
owned-file whitespace checks passed. No production tests were run for this
independent specification-only contribution.
