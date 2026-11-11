# WI-009 model composition and implementation audit

Status: independent review of the coordinator worktree at base `403e778e1` plus its
uncommitted WI-009 changes. This document records a manual argument and remaining
acceptance obligations, not a refinement proof or a claim that tests passed.
The reviewed independent models are `model/backfill/arbitration.fizz` and
`model/backfill/cursor.fizz` in the backfill-model worktree. Their README and runner
results define the actual finite exploration bounds and checked source hashes.

## What the checked models establish

The arbitration model uses queue capacity four, **one** contended operation slot,
two initially ready backfill requests with one candidate each, fixed policy modes,
and sustained manual/automatic producers. Its `always eventually
FiniteBackfillsProgress` assertion is an actual liveness assertion. Since the two
request resolutions are permanent, it requires eventual resolution of both.
The compiled FizzBee input marks the eight service/release actions with
`FAIRNESS_LEVEL_WEAK`. This is stronger evidence than a finite recovery witness,
but remains conditional on the modeled actions and assumptions.

The cursor model separately checks a three-nominal stream, one possible definition
update, exclusion, an actual-time range endpoint, and publication before disposition.
It contains no liveness claim. The model runner detects the named naive-arbitration
liveness failure separately from safety failures; a timeout is not accepted as a
counterexample or successful exploration. The README explicitly disclaims automatic
lifting to four concurrent operations or composition with real enumeration.

## Manual mapping to Go

| Model concept | Concrete owner and transition | Obligation not supplied by the model |
| --- | --- | --- |
| Queue turn | `Coordination.NextQueue`, `cyclicClass`, `planWork` | Enum ordering and skipped classes must preserve cyclic priority. |
| Concrete vacancy lease | `OwnerClass` plus `OwnerKey` | All queue insertions must honor the same key; request/candidate changes must revoke ownership explicitly. |
| Pinned backfill | `BackfillFocus`, `selectedBackfill`, `LastBackfill` | Four requests, new arrivals, revision changes and exclusion-only scans are not the two-ready-request fixture. |
| Operation turn | `Coordination.NextOperation`, `workDemand.operation`, `WorkHandler.Execute` | `Unfinished` must equal the count of unreclaimable operation records; only actual admission advances the turn. |
| One slot occupied | Concrete `Unfinished` and `BlockingOperationId` | Concrete tracked occupancy and other ALLOW_ALL intents coexist; the model has no such simultaneous identities. |
| CPU/service action | `NextWork` and the selected `WorkTask` quantum | Fair delivery of one physical task must imply eventual service of every persistently runnable class. |
| External tracked completion | Keyed completion through `Apply` | Authentication, chain fencing, first-result semantics and durable task publication are separate implementation obligations. |
| External ALLOW settlement | Keyed acknowledgment/early completion | Concurrent results, stale/evicted identities and exact summary maintenance are outside this model. |
| Source peek | `scanBackfill`, `BeginRange`, `PeekRange(..., 8)` | Calendar/jitter evaluation and bounded work are supplied by occurrence tests, not the symbolic source. |
| Pending source candidate | `BackfillState.Continuation.Pending` | Nominal, actual, revision and definition fingerprint must survive serialization and denied admission unchanged. |
| Publication | `transferBackfill` followed by `finishBackfill`/`DisposeRange` | Their mutations and external-start task must commit in the same CHASM transaction. |
| Definition update | `backfillNeedsScan`, `RebaseRange` | A leased old candidate must be revoked before rescan; original range end remains immutable. |
| Request completion | Removal from `RootState.Backfills` on terminal scan | This means handoff/disposition finished, not that generated workflows completed. |

Public `Apply` rejects Due, Drain, DispatchManual and DispatchBackfill. Only the
coordinator calls the internal admission transition. Legacy generator/manual/drain
handlers must remain invalid/no-op; otherwise they bypass resource entitlement.
Callbacks and configuration changes reconcile work but do not admit successors.
The package-private boundary is a convention checked by source review and tests,
not a type-level capability or formal proof.

The Go planner separates resource contention from runnable work. Manual/backfill
heads require a vacancy lease even for direct ALLOW_ALL or policy-only disposal.
Automatic direct ALLOW_ALL does not require that vacancy, but needs its operation
turn. Automatic capacity drops and scan quanta do not advance resource turns.
`NextWork` advances for a successful quantum; `NextQueue` advances when a leased
handoff/disposition completes; `NextOperation` advances when Unfinished increases.
The operation identities and nominal cursor are independent of these turns.

## Conditional argument for four operation records

A one-slot model is **not automatically a worst-case abstraction** of four slots.
With four records, a tracked blocker can coexist with spare capacity and ALLOW_ALL
starts, several results may arrive in different orders, and extra admissions advance
the operation turn. Simply replacing “occupied” with `Unfinished == 4` does not
produce a trace refinement. The concrete blocking-owner predicate must remain
separate from capacity availability.

A possible lifting argument uses cyclic rank rather than one-slot state projection:

1. Fix a ready candidate whose definition/identity is no longer invalidated. Once
   granted, its vacancy lease persists through operation-capacity waits. No other
   insertion consumes that reserved position. Full capacity changes executability,
   not the leaseholder's operation demand.
2. For a continuously operation-contending class, cyclic distance from NextOperation
   decreases on grants to preceding contenders. A grant cannot jump past a present
   entitled class. Scans, drops, denied attempts and failed publication do not move
   that cursor. Extra free slots permit additional grants but do not remove this
   ordering property. This requires the concrete planner's predicates to stay valid
   until service or legitimate disposition.
3. Given recurring usable operation capacity and eventual successful coordinator
   service, the entitled class eventually admits. If it instead becomes eligible
   for queue insertion or policy disposition, that action releases its lease without
   needing an operation slot. Drain requires only an operation slot and can create
   the vacancy needed by accepted producers; it does not acquire resources in the
   reverse order.
4. Once each leaseholder eventually resolves, QueueTurn gives each continuously
   contending producer class a turn. CPU NextWork must also give every runnable scan
   class service. Within backfill, ready candidates pin focus until disposition;
   handoff, terminal scan and exclusion-only Yield rotate request selection. New
   sequences cannot repeatedly reset LastBackfill ahead of an existing request.
5. For a finite range under eventually stable definition, each scan quantum has
   bounded work and eventually either produces a candidate or finishes the range.
   Each committed exclusion/handoff strictly advances disposed nominal time. Combine
   this with eventual service of ready candidates to argue finite-request completion.

These are review obligations, not discharged mathematical lemmas. In particular,
steps 2, 4 and 5 need a concrete invariant/rank argument or a richer verified model
before describing the entire cap4 implementation as proved live. There is no universal
sixteen-grant or elapsed-time bound. Current finite regression traces cannot prove
infinite-execution L2 by themselves.

## Assumptions and exclusions

- Correct persisted operation summaries, queue bounds, concrete candidate ownership
  and atomic state/task publication are safety prerequisites.
- Coordinator tasks receive fair service; publication eventually succeeds. Tokens,
  sequences and execution timestamps remain representable. Permanent payload errors,
  exhausted counters and endlessly failed commits are outside this progress argument.
- Required tracked completions and ALLOW_ALL settlement recur sufficiently to make
  operation capacity usable. Merely delivering tasks to four permanently ambiguous
  operations cannot provide progress.
- Definition/policy updates eventually permit stable finite enumeration. Repeated
  revision invalidation can prevent any particular candidate from receiving service.
- Accepted requests may resolve through legitimate overlap disposition. Completion
  of a finite backfill is not a promise to execute every nominal occurrence.
- A leased ALLOW_ALL command can delay a pending cancellation command while all
  operations remain unresolved. The argument assumes the needed external resolution;
  it does not claim unconditional deadlock freedom for that dependency.
- Framework request-ID retention is bounded and separate from operation retention.
  The model trusts that boundary and does not prove actual History leases or storage.

## Concrete regression audit

The following assessment is from reading test source, not running these tests.

| Scenario | Current evidence / remaining obligation |
| --- | --- |
| Historical handoff with failed publication and reload | `TestCoordinatorHistoricalBackfillAtomicHandoff` checks pending retention, physical task nonpublication, manual bypass and request completion before workflow completion. |
| Four active requests and ordered rotation | `TestCoordinatorBackfillCapAndRequestRotation` rejects request five without changing root and observes four request IDs in order; `TestBackfillPublicDedupAndPublication` also accepts the rejected fifth request after a completed request frees capacity. |
| Continuous class competition | `TestCoordinatorSustainedManualAutomaticBackfillCompetition` provides a finite recovery trace; its automatic source is finite and it acknowledges each new operation immediately, so it is not cap4 saturation or sustained-infinite-ingress liveness evidence. |
| Full operation capacity and preserved lease | `TestCoordinatorCapacityLeaseSurvivesNewIngress` reaches four ALLOW_ALL intents, preserves manual ownership through new ingress, and services it after one acknowledgment. |
| Full queue plus cap4 mixed occupancy | Require the original starvation schedule: tracked blocker plus three unresolved ALLOW_ALL intents, drain-created vacancy, leased accepted ALLOW_ALL, and competing tracked refills. Assert reservation survives until a later operation release. |
| Revision invalidates a ready leased backfill | `TestCoordinatorBackfillRevisionInvalidatesLeaseAndRebasesNominal` checks revocation, historical restart after disposed nominal, immutable end and old admitted request. Add failed publication at invalidation/rebase and multiple requests competing during replacement. |
| Queue handoff completes request before workflow | `TestCoordinatorBackfillOverridePreservesAcceptedWaiter` preserves queued backfill identity/policy after request removal. |
| Jitter/end/exclusion/fragmented bounds | Occurrence backfill tests cover pure helpers. Add coordinator-level actual>end completion with no admission and exclusion-Yield rotation between multiple requests. |
| Work token and per-quantum atomicity | Generation tests now separate scan and handoff commits and cover WorkTask rollback/stale tokens. Backfill-specific lease/cursor/task atomicity must remain checked across each publication boundary. |
| Public Backfill request-ID behavior | `TestBackfillPublicDedupAndPublication` covers changed invalid body duplicate, empty-ID independence, failed-publication retry, and capacity rejection without consuming ID followed by acceptance after removal. Duplicate after the original request completes is not asserted explicitly. |
| Admission validation/ownership | `TestBackfillRejectedRequestDoesNotPublish` covers nil/unconfigured/missing bounds, bad nanos, reversed range, before2000/end2101, unknown policy and sequence exhaustion with full committed-state comparison. The public test checks caller timestamp mutation; `TestBackfillAcceptanceAvoidsPayloadAndRejectsExhaustedToken` covers token exhaustion rollback. Invalid-body public request-ID retry is not individually asserted. |
| Metadata-only acceptance | `TestBackfillAcceptanceAvoidsPayloadAndRejectsExhaustedToken` poisons definition and diagnostics, commits acceptance, and validates WorkTask while proving the definition remains poisoned. Request/carryover partitions are not specifically poisoned by that test. |

Backfill request-ID tests should reuse actual Node request-ID methods and separately
record the mock-engine boundary, as WI-008 does. They are not a substitute for the
existing History-engine request-ID checks. The new backfill tests exercise the wrapper directly. `TestCoordinatorRejectsAdmissionBypass` and `TestCoordinatorLegacyTasksCannotAdmit` additionally check public transition rejection and disabled legacy handler validation/execution. These close the earlier wrapper and bypass coverage findings; the remaining cases above are narrower composition obligations.

This audit neither changes the local-only authorization nor closes the remaining
M2 specification/mapping gates, M3 correspondence/API work, or M4 state-growth and
migration-boundary evidence.


## Subsequent concrete checks

Parent execution passed `TestCoordinatorFullQueueVacancySurvivesOperationContention`:
the fixture explicitly reaches a tracked blocker plus three ALLOW intents and four
queued backfill candidates through accepted ingress. A drain consumes the released
operation slot, grants a vacancy to manual ALLOW, and preserves it across new tracked
ingress and failed publication. A later ALLOW settlement admits the leaseholder;
tracked backfill subsequently fills the vacancy. This is the previously missing
resource-composition regression, not infinite-execution proof.

`TestCoordinatorBackfillJitterBeyondEndCompletesWithoutAdmission` and
`TestCoordinatorBackfillExclusionYieldRotatesRequests` pass with real Node reloads.
They check actual-time range termination, exclusion-only scan rotation, and failed
configuration publication retaining both historical continuations. Public request
tests also now retry an invalid body with the same ID and deduplicate after range
completion. The larger cap4 model is supplemental; its exact scope/results belong
to `model/backfill/CAPACITY.md` once complete.
