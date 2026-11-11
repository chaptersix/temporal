# Independent WI-010 finite lifecycle specification

This is a factored executable specification of reviewed D52–D54, not generated
from the Go implementation and not a refinement proof. D51 exact-run deletion is
covered separately by component/History tests; these models do not prove it.

## Reproduce and evidence

Pinned tool: local FizzBee v0.5.4.

```sh
python3 docs/development/scheduler2/model/lifecycle/run.py \
  --fizz /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz \
  --output /private/tmp/scheduler2-lifecycle-model-reviewed
```

The runner executes both factors at max_actions 100 and 200, then five named
mutants. Timeout is 300 seconds per process group; a timeout cannot count as a
mutant detection. Baseline failure stops subsequent exploration. Exact transformed
sources, hashes, commands, exit codes and counts are in the output results.json,
with per-case run.log, compiled AST and graph/counterexamples. Both bounds exhaust
the same graph for each factor:

| Factor | Visited nodes | Valid nodes | Unique states |
| --- | --- | --- | --- |
| Activity evidence | 584 | 422 | 146 |
| Idle scheduling | 1958 | 1813 | 1104 |

All baseline assertions/witnesses passed; idle reports `IsLive: true` for
`StableIdleEventuallyCloses`. Compiled idle AST identifies StabilizeEnvironment,
AdvanceTime and DeliverCurrent as `FAIRNESS_LEVEL_WEAK`.

| Mutation | Required named counterexample |
| --- | --- |
| ANCHOR_REGRESSES | AnchorMatchesAcceptedEvidence |
| DUPLICATE_ACK_REFRESH | FirstObservationImmutable or AnchorMatchesAcceptedEvidence |
| CLOSE_WHILE_PENDING | NoUnauthorizedClose |
| LOST_REARM | StableIdleEventuallyCloses (temporal lasso) |
| STALE_TIMER_CLOSE | NoUnauthorizedClose |

All five were detected. The first exploratory run detected lost rearm through
unreachable positive witnesses before reaching liveness; it was **not** accepted
as temporal evidence. The runner now omits positive existential witnesses for only
that mutant, retaining its transitions, safety and temporal assertion. The reviewed
run explicitly reports `IsLive: false` and `FAILED: Liveness check failed` with the
named property and a counterexample. Baseline witnesses remain enabled unchanged.

## Activity factor and mapping

Two retained operations start Pending; first successful observations are independently
chosen fixed times 3 and 5, creation is 1, optional control evidence is 4. Completion
may precede acknowledgment and supplies no clock. First acknowledgment can fill
metadata in Completed without changing Completed to Started. Duplicates do not
refresh evidence; terminal eviction can occur in either order. `expected` is a
separate ghost maximum of accepted creation/control/first observations, independent
of retained history. Observed immutable per-operation metadata is checked against
first evidence. Witnesses require early completion followed by acknowledgment,
eviction of the record containing the anchor, and out-of-order retention while the
older record remains.

| Model | Intended implementation correspondence |
| --- | --- |
| anchor | Persistent root activity timestamp, preserved by root reconstruction |
| first/observed | First successful-start timestamp plus explicit presence marker |
| Completion | First matching completion; no fabricated callback clock |
| Acknowledge after completion | Narrow terminal metadata merge; no phase reopening |
| Evict | Bounded terminal metadata/request eviction; root evidence survives |
| ControlUpdate | One accepted activity-producing control update |

Only matching incarnation/operation/chain evidence is admitted into this factor.
Chain validation, accounting/carryover, timestamp conversion and exact request bytes
are trusted adapter obligations checked separately. Evicted/old/malformed results
are represented by stutter, not separately enumerated identities. Retention capacity
is two, sufficient for the explicit reordered-eviction counterexample, not a cap4
state-space proof. No assumption that every operation eventually receives an ack.

## Idle factor and genuine conditional L3

Initial eligible state has anchor 0, positive idle duration 2 and one persisted
physical obligation. An optional new anchor 2 replaces the token/deadline; old
physical envelopes remain deliverable. One optional hold invalidates the current
logical obligation. Hold labels cover pause, manual, backfill, queue, pending,
ambiguous, unsettled effect, Active/Yield source, uninitialized state and disabled
idle duration. Labels represent the already-computed eligibility predicate; this
factor does not mechanically derive it from concrete counts/phases.

`StabilizeEnvironment` is an explicit weak-fair environmental premise: eventually
lifecycle setup is initialized, idle duration is positive, all holds resolve, the
anchor stops changing and reconciliation commits. It is **not** a scheduler API
or a claim that ambiguous work always resolves. Once stable, new updates/holds are
disabled. `AdvanceTime` is weak-fair and monotonically reaches 6, beyond every
possible deadline. `DeliverCurrent` is weak-fair, requires the exact current token
and deadline physically present and elapsed, then closes. Thus the temporal
assertion checks eventual close under explicit eventual eligibility/stability,
clock advancement, successful publication and delivery assumptions. Close is
absorbing. Permanent external waits and zero-idle forever are outside the premise.

Old envelope delivery is not fair or required for progress, but can occur arbitrarily
and must not close. NoUnauthorizedClose records validation at the actual closing
transition, including eligibility, current token and not-before deadline. Witnesses
cover successful rearm plus stale delivery rejection and eventual close after
pending/ambiguous holds resolve. They do not prove every hold label was individually
witnessed. Started-only and terminal history are represented by the no-hold state.

Failed publication and duplicate delivery are stuttering atomic transitions; they
may occur arbitrarily but cannot prevent a continuously enabled fair successful
transition forever. This abstracts rollback semantics; it does not prove storage
atomicity. Concrete tests must discriminate partial publication and stale timers.

## Composition obligations and limits

- The factors are not checked as a Cartesian product. Mapping accepted evidence
  into idle rearming, summary changes and token updates remains a Go integration
  obligation. The synthetic anchor value represents validated evidence, not a
  proof of wall/virtual clock conversion or causal ordering of physical clocks.
- Root unconfirmed-start count and unsettled effect/source predicate must match
  concrete operations without loading unrelated DATA partitions. No count oracle
  is built into the idle factor; full-state Go tests must check this correspondence.
- No task identity collision, numeric overflow, revision change, close/recreate,
  request dedup retention, mixed overlap queue, RPC or database is modeled.
- Successful atomic reconciliation on activity/eligibility change is a premise;
  lost-rearm sensitivity shows why it matters. Physical delivery fairness is not
  established by the mere presence of a task.
- This finite evidence neither proves arbitrary unbounded histories nor closes
  remaining M2 implementation/mapping gates or M3 proofs/correspondence.

Reviewed source SHA256:

- anchor.fizz: ba6c95b99a922ae2b61c98181e108e5e6231ebdb279015d50e2db1f4df41972b
- idle.fizz: fb213bbaf5b71de58634158df598b167cd7f1a6e650167b54d078df12b0579ef
- run.py: 0d96e73b442c3847532169d75a68fc9118f0759227ba956c5997e9df8ac9e965
