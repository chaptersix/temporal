# Current M2 coverage audit

Read-only source audit at WI-011 prerequisite `4e0b5d4d4`; no new tests were run
for this document. This is a coverage map, not acceptance of an implementation
under development. Test names below are in `chasm/lib/scheduler2/` unless a different
path is stated. Accepted execution manifests are linked from [the ledger](../evidence/README.md)
(E17–E66); historical results retain their original source revision.

## Initial historical-regression inventory

[WI-000 inventory](../investigation.md#historical-defects) owns fixed revisions,
baseline inclusion and original reproduction. These are intended-behavior mappings,
not a claim that scheduler2 copied those patches.

| Defect | Current discriminating evidence | Remaining boundary |
| --- | --- | --- |
| F1 jitter gap loses candidate | occurrence `TestPendingJitterBeyondNominalEnd`, `TestSavedPendingJitterReplay`, `TestScanOracleDetectsDroppedPendingCandidate`; `TestGenerationJitterUpdateSupersedesPending` | Current coordinator mapping must retain pending nominal/actual and distinguish scan from disposition. |
| F2 empty carryover forwarded | `TestCallbackCarryoverMatrix`, `TestCarryoverFrozenAtAdmission`; WI-003 empty-vs-null mutant | Public workflow request correspondence remains M3. |
| F3 no-result success clears prior result/failure, explicit null survives | Same matrix plus completion saved JSON and sequential carryover tests | Do not equate zero payload bytes with absent payload metadata. |
| F4 replayed CHASM request mutates twice | `TestManualPublicDedupAtomicityAndChangedBody`, backfill request tests; framework dedup evidence WI-008 | DATA-node mock and actual History framework cases have distinct scope; bounded dedup retention is not indefinite. |
| F5 duplicate update/patch | Existing revision-stutter tests and Trigger/Backfill dedup cover separate mechanisms | New atomic local update needs its own duplicate/stale contract and regression. API conflict tokens/UpdateSchedule retry response are M3. |
| F6 execution virtual time | `TestActivityStartObservationUsesSnapshotOffset`, generation rollback/token tests, activity oracle | Successful RPC wall time plus captured virtual offset differs from execution merge time; no cross-host clock guarantee. |
| F7 final automatic start lost to idle close | `TestIdleAmbiguousWaitAndObservedStartedEligibility`, generation atomic handoff, root unconfirmed-summary oracle | Add/identify one final automatic-source exhaustion → admitted Pending → idle-enabled trace; manual-only pending evidence plus separate generator tests is composition, not that exact regression. |
| F8 outstanding work/overdue idle reconciliation | `TestIdleObligationsManualQueueAndUpdate`, backfill/effect obligation tests, pause/resume expired-obligation test, saved late-observation replay | Stable close is conditional on hold removal, advancing clock, delivery and successful publication. |

## Implemented event and replay coverage

`internal/decision/decision.go` currently declares fifteen event kinds. Coverage is
factored, not one generator exploring every product. Old M1 `Due` injection is a
privileged test transition; public Apply no longer grants coordinator admission.
A replay name does not imply it exercises the current WorkHandler path.

| Events/boundary | Generated evidence | Saved or directed real adapter evidence |
| --- | --- | --- |
| Due, Update, Acknowledged, RetryableFailure, Complete, Expire | decision event sequences, sequential/buffer/ALLOW properties; activity Node properties | M1 replay/persistence and start-task tests; generation and ALLOW saved traces. M1 update means metadata revision, not full definition replacement. |
| Callback outcomes | decision completion properties | completion JSON; malformed/resource/first-terminal/carryover matrix; actual Node failed publication |
| Drain | buffer and ALLOW pure properties | buffer/effect saved traces and current coordinator contention tests; distinguish helper-granted Drain from WorkHandler selection |
| EffectSucceeded, EffectNotFound, EffectFailed, EffectExpired | independent effect properties | effect tasks deadline/retry/permit tests; effects JSON includes failure/NotFound; expiry/success covered directed, not every replay |
| AcceptManual, DispatchManual | manual independent properties; mixed coordinator Node properties | manual JSON/dedup; current WorkHandler handoff, capacity and override cases |
| DispatchBackfill | bounded range properties; mixed and delayed-capacity coordinator Node properties | coordinator JSON and historical handoff/rebase/lease tests |
| ConfigureGeneration, scan, lease claim/release, close/recreate | mixed coordinator generator uses stable definitions; activity generator has idle disabled | generation/coordinator JSON; definition replacement tests; lifecycle JSON, idle obligations/retention and History recreation |
| ConfigureIdle, observation-to-idle rearm, Terminate | activity generation covers observations but not idle | two lifecycle JSON traces, exact/early/late idle delivery, failed rearm/close; actual History delete persistence rejection |
| Full local source+pause+policy+budget update | Not implemented at audit base | WI-011 must add independent expected projection and deterministic saved trace after contract resolution. |

Delivery, external acceptance, execution-clock movement, crash/reload and rejected
publication are explicit stutters at the appropriate durable projection, not omitted
events. A successful scan can change continuation/task metadata while stuttering
policy admission. Lease acquisition is not occurrence disposal. Current mappings
must expose these distinctions instead of reusing old per-class timer mappings.

Generated exploration has limits: occurrence cases are bounded intervals/structured
UTC calendars with exclusions/jitter and generated partitions; DST gap/fold uses a
pinned TZif fixture and fixed examples. Coordinator generated scenarios use stable
specifications and bounded recovery/completion assumptions. They do not cover arbitrary
updates, every overlap policy under every contention history, or arbitrary timezones.
Saved traces contain inputs and expected projections rather than only random seeds.

## Obligation reconciliation

| Obligations | Existing support | Remaining ownership |
| --- | --- | --- |
| C1; SP1–4, SP6–7 | Independent bounded scan oracle; budget1/2/N resume/partitions, pending jitter, malformed/range boundaries, pinned DST; reviewed executable Lean draft | Current transition mapping M2; complete boundary-biased spec checks, Lean SP2/SP3 proofs/axiom audit and Go correspondence M3. No theorem proof claimed. |
| C2; SP5 | Jittered pending update, historical rebase after disposed nominal, lease invalidation, rollback and catchup tests | Atomic full local update compositions/current model mapping are WI-011 M2. |
| C3–4; S1,S3,S4 | Six policies, frozen overrides, pause-on-failure, one-time debit, queue/operation caps and recovery; WI-003–009 independent factors/properties | New atomic update must preserve admitted accounting and replace future budget/pause/policy together. |
| C5 | Stable request identity, manual/backfill request-ID dedup and 60s issuance horizon | Declare local update idempotency; public conflict token/dedup response correspondence M3. Remote dedup duration is an assumption, not extended by local retention. |
| C6; S2 | First-completion payload semantics, immutable whole requests, lost acknowledgments and replay after updates | Compose current carryover with atomic update/queued admission; retain frozen start/effect bytes. |
| C7 | Empty RunID plus frozen FirstExecutionRunID effects, keyed chain conflict rejection, early/duplicate completion, manual ALLOW_DUPLICATE requests | WI-011 must explicitly state continue-as-new/reset and workflow-ID reuse assumptions; no public authenticated callback/chain-following claim. Undefined local semantics cannot be deferred as API work. |
| C8; S7,S8 | WI-010 anchor retention, exact-run History allocation/recreation, failed delete, old results/tasks and in-flight RPC barriers | Mapping review M2; DB crash/physical queue or preauthorized-RPC cancellation are not established. |
| C9 | Explicit unsupported migration/sentinel boundary | M4 support decision; no production migration. |
| C10 | Local typed registration and controls only | Public API/errors/counts/ordering/visibility and shared functional backend matrix M3. |
| C11; S5,S6 | Queue/command/live caps, rejected commit/reload, token preflight, leased vacancy, sustained-ingress tests and fairness models | Current coordinator mapping M2; load/state-growth measurements M4. |
| L1,L2 | Backfill/coordinator finite conditional liveness plus bounded recovery traces | Map concrete external waits and fairness premises; no unconditional progress under permanently ambiguous operations. |
| L3 | Lifecycle finite weak-fair stable-idle model; temporal lost-rearm mutant, concrete rearm/close tests | Manual mapping plus composition, not automated Go refinement. |

## P3.3 measurement audit

`TestMetadataTransitionsAvoidPayloadDeserialization` poisons three DATA values,
executes metadata completion, checks unrelated paths absent from UpdatedNodes, and
logs `len(UpdatedNodes)` plus sum of `proto.Size(node)`. Poison explicitly fails when
loaded. E15's two modified nodes/268 bytes describe its original revision, not a
measurement of the current DATA-map/coordinator/idle schema. WI-006 keyed isolation
additionally poisons unrelated operation metadata; WI-010 open idle close poisons
four DATA payloads. These are meaningful negative decode witnesses.

They are not complete preparation counters: successful values decoded/prepared are
not counted. Serialized mutation bytes exclude storage envelopes/index overhead and
physical task bytes. The in-memory Node store performs no database reads. H5 source
inspection establishes lazy DATA decoding after a mutable-state fetch, not reduced
physical fetches. Actual History tests use a persistence test double but currently
record state/task output without read-count assertions.

Required small measurement increment before checking P3.3 fully:

1. Record prepared/decoded value counts (or precisely enumerated successful/poisoned
   accesses), updated/deleted node counts and serialized node/task bytes for current
   keyed completion, coordinator scan/handoff and full local update. Keep numeric
   evidence separate from no-unrelated-payload assertions.
2. Count GetWorkflowExecution requests at the existing History test-double boundary
   for cold cache and warm cache operations; report whole-state fetch scope. This
   measures persistence API calls, not physical database pages or bytes. No new DB
   dependency is necessary to measure that boundary.
3. Leave physical storage IO/latency and large-load/growth comparisons explicitly to
   M4. Do not mark P3.3 satisfied by poison alone or by a later-load deferral without
   recording the current measurable API boundary.

## Required discriminators identified at the start of WI-011

- A local update changes source/policy/pause/budget as one transaction; rejected
  publication restores the entire prior definition, continuation, lease, root and
  task snapshot. A stale/duplicate request cannot repeat the change or debit.
- Update while a concrete queue/backfill vacancy lease exists: immutable queued
  identity/policy remains, source pending is superseded/rebased at the correct
  boundary, then handoff uses current template/carryover exactly once. Reject both
  update and handoff publication and replay after reload.
- Early completion/failure carryover plus bound effect and admitted retry across
  update: only future admission changes; old start/effect whole request bytes do not.
- Name a final automatic exhausted-source/Pending idle trace for F7, rather than
  claiming the exact regression from separate tests. Instrument the boundaries
  above; resolve local chain semantics and current per-event model mapping.

These are distinguishing transitions/measurements, not a demand for every Cartesian
product. Existing accepted tests should be reused. Plan gate ownership is explicit
in [milestone/proof scope](../../../../CHASM_SCHEDULER_REDESIGN_PLAN.md): M2 requires
implemented-policy properties/mappings and reviewed executable Lean statements;
final proofs, full spec processing and differential/API/shared functional checks are
M3, and load/state growth plus migration boundary are M4. No blanket checkbox closure
or milestone acceptance follows from this audit.


## WI-011 current reconciliation

The initial tables above record the audit that drove implementation. Current
evidence is E67–E70, not a claim that every initially missing item remains absent.
UpdateConfiguration now prepares source/policy/pause/budget together. Directed
rejection, immutable request/effect, leased historical rebase, queued current
template/carryover, and saved update_v1 traces pass. Early failure before Ack with
both pause-on-failure toggles is covered by update_failure_test.go. The generated
UpdateComposedProperties relation adds independent accepted-input controls/source,
phases, counters, request bytes, queue identity, blocker, chain and logical pending
start obligations. Its19-transition two-operation fixture explicitly grants work;
it does not enumerate calendar outputs or prove CPU/resource arbitration. Final
strengthened exploration and sensitivity are separately recorded before acceptance.

F5 is now covered by local Update request-ID publication/replay/eviction tests. F7
has TestUpdateFinalAutomaticIntentHoldsIdle: exhausted automatic source retains
Pending then Ambiguous intent beyond the old idle deadline, and only confirmed
start evidence permits close. C7's normalized identity and intermediate-status
rules are explicit in contract.md; TestUpdateChainCompletionFence verifies them
across replacement. Real continue-as-new/reset translation remains M3.

P3.3 now observes attached materialized nodes before/after CloseTransaction through
a test_dep-only read-only observer, alongside node/task serialized bytes and avoided
payload witnesses. It counts loaded or newly prepared attached values, not repeated
decode invocations or detached nodes. Tree initialization already prepares the root
for visibility. Current measurements (replay-measure-race.log) are:

| Transition | Materialized before/after | Modified nodes | Node proto bytes | Published physical tasks / serialized task bytes |
|---|---|---|---|---|
| Keyed completion | 1 / 3 | 3 | 590 | 0 / 0 |
| Automatic scan | 1 / 3 | 3 | 502 | 1 / 53 |
| Automatic handoff | 1 / 6 | 6 | 1124 | 2 / 194 |
| Full update | 1 / 3 | 3 | 480 | 0 / 0 |

All four fixtures avoid two poisoned DATA leaves; these numbers are fixture/source
specific and are not cost bounds. Pure task payloads stored in node metadata are
included in node bytes, not double-counted as physical task records. History tests
measure one cold GetWorkflowExecution fetch and zero additional warm fetches. That
is a whole mutable-state persistence API boundary, not physical database pages,
latency or IO savings. Those remain M4 measurements.

The old per-slice models remain independent factors with recorded bounds. The
current transition matrix and Go input-based composition oracle connect them by
manual review and finite executable observations; no checked product/refinement
theorem is claimed. Lean propositions/definitions meet the stated M2 scope; their
proofs, axiom audit, executable correspondence and full spec checks remain M3.

## Numeric boundary matrix (WI-011 follow-up)

This section supplements the audit-base findings above. It maps plan section7's
limit−1/limit/limit+1 and recovery requirement to current named tests. Test source
existence and passing execution are distinguished; these are prototype bounds from
[profile.json](../profile.json), not production sizing recommendations. No tests
were executed by the author during this matrix update.

| Bound / unit | Neighbor and overload evidence | Recovery evidence / status |
| --- | --- | --- |
| Workflow template 65536 protobuf bytes | `configuration_limits_test.go: TestConfigurationResourceLimitNeighborsAndRecovery`, 65535/65536/65537; valid WorkflowId, Identity padding with explicit proto.Size including field overhead | Over-limit rejection deliberately committed with no semantic state/task changes, same revision valid retry succeeds. Parent reports pass; [configuration limits log](../evidence/wi011/configuration-limits.log). |
| Jitter seed 65536 bytes | Same test, 65535/65536/65537 ASCII byte strings | Same-revision recovery and persisted exact seed asserted; same configuration-limits log. |
| Catchup 365 days | Same test, 365d−1ns/365d/365d+1ns | Whole rejected configuration unchanged, valid retry publishes successor work; same configuration-limits log. |
| Idle duration 365 days | `TestConfigurationIdleLimitNeighborsAndRecovery`, 365d−1ns/365d/365d+1ns; existing `TestIdleConfigurationBoundariesRejectBeforeWrites` covers negative/overflow, zero-disable elsewhere | Existing idle task survives rejected commit; valid rearm preserves anchor, one task closes at exact deadline. Same passing log. |
| Compiled spec 65536 protobuf bytes; 64 terms; 1024 ranges | occurrence `TestLimitsDefinitionBoundariesAndRecovery`: exact size−1/size/size+1 for each. Valid unknown protobuf padding isolates byte size; duplicate valid intervals/ranges isolate structural counts | Input ownership, rejection reason and subsequent valid compile/enumeration checked; overlapping duplicate terms/ranges produce exactly three distinct expected occurrences. Parent execution is recorded in [occurrence limits log](../evidence/wi011/occurrence-limits.log). |
| Enumeration maximum 1024 probes | occurrence `TestLimitsProbeBoundaryAndResume`, 1023/1024/1025 requested probes against long excluded prefix | Over-limit call rejected without input mutation; valid retry yields at exact probe count, resume exhausts with independently listed remaining occurrences. |
| Enumeration maximum 128 outputs | occurrence `TestLimitsOutputBoundaryAndResume`, 127/128/129 requested outputs against130 occurrences | Over-limit call rejected; valid limited batch plus resume equals all130 independently constructed candidates. |
| Current coordinator raw scan quantum8; handoff quantum1 | `scan_budget_test.go: TestCoordinatorScanBudgetNeighbors` adds automatic/backfill7/8/9 excluded candidates; [focused race passes](../evidence/wi011/boundary-final-race.log) | Checks failed publication, exact8 disposal, reload/resume to terminal and no debit. Batch1 `TestGenerationAtomicScanAndHandoffAndSecondCarryover` separates scan/handoff with backlog; a second handoff requires a successor transaction, not a configurable batch2 call. |
| Buffered automatic occurrences cap4 | `TestBufferAutomaticCapacity`:3/4/5 queue attempts with exact queue/drop/budget results; BUFFER_ONE saturated separately | `TestBufferCompletionDrainsExhaustedGenerator`, coordinator full-queue vacancy contention and saved coordinator trace demonstrate drainage/recovery. Accepted WI-005/WI-009 evidence. |
| Pending manual commands cap4 | `TestManualPublicDedupAtomicityAndChangedBody`, `TestManualQueueWaitPolicyResolutionAndCapacityRecovery`, `TestManualFullUnfinishedWaitAndSameTimeEviction` | Reject/defer without dropping accepted intent; transfer frees slot, retry and same-time identity cases. Accepted WI-008 evidence; new update dedup tests are not capacity substitutes. |
| Backfill commands cap4 | `TestBackfillPublicDedupAndPublication`, `TestCoordinatorBackfillCapAndRequestRotation` | Four accepted ranges, fifth rejected without consuming request ID; completed range permits exact rejected-ID retry. Accepted WI-009 evidence. |
| Live and retained operation cap4 | `TestAllowAllKeyedIssuanceCapacityAndRecovery`, `TestAllowAllCapacityWaitingDrainWakesWithoutGenerator`; sequential/idle five-admission retention traces | Fifth unfinished automatic action drops without debit; terminal evidence permits admission/wakes retained waiter. Terminal-only eviction preserves activity and late evicted result is ignored. WI-006/WI-010. |
| Completion value65536 bytes | `TestCompletionResourceBoundariesAndRecovery`, result and failure exact wire65535/65536/65537 | Rejected callback retains pending obligation and carryover/accounting; valid exact-limit callback then succeeds. WI-003. Aggregate carryover bound is composed from separately bounded result/failure, not an independent unbounded ingress. |
| Frozen effect request131072 bytes | `effects_limits_test.go: TestEffectsRequestSizeNeighborsAndRecovery` independently constructs exact131071/131072/131073-byte cancel and terminate requests; [focused race passes](../evidence/wi011/effect-limits-race.log) | This limit is reachable: `FirstExecutionRunId` copies chain evidence, and the local event path does not enforce UUID length. No artificial UUID-only restriction is assumed. Rejected publication is deliberately committed and remains unchanged; matching completion permits a fresh blocker with a valid effect. This uses an explicit admission grant and does not substitute for coordinator arbitration coverage. |
| Effect records per blocker2 (cancel/terminate identities) | Effect supersession and duplicate demand tests show distinct kind identities and no third demand for repeat input | Retained settled/superseded records are diagnostic; this structural cardinality is not a user-configurable integer quota. Request-byte evidence is separate above. |
| Retry horizon60s / backoff1s | Start `decision/decision_test.go: TestEventSequences` samples−1/0/+1; `TestEffectsTaskDeadlineBoundary` now includes−1/0/+1ns ([focused race passes](../evidence/wi011/boundary-final-race.log)); source expiry/late-evidence and retry frozen-input checks | At expiry issuance stops and ambiguity remains recoverable through late evidence; external dedup beyond horizon is not promised. |
| Executor concurrency1 per library | `TestTaskCancellationAndConcurrencyBound`, `TestEffectsTaskPermitAndDetachedRequest` | First RPC holds permit, competing attempt cannot enter, cancellation and subsequent release/retry tested. Controlled synchronization and permit-removal sensitivity, not timing sleeps. |

All new numeric tests have passing focused race evidence linked above. Passing
neighbor tests alone does not prove all state-product combinations or M4 load bounds.

Additional profile constants are structural or test budgets, not freely configurable
resource quotas: one pending candidate per source/range is represented by one optional
field (second eligible candidate waits for disposition); range Peek returns no emitted
occurrence list and retains one candidate. Coordinator owns one concrete vacancy lease,
and one transaction scans OR transfers one owner. Unsupported second effect kind beyond
Cancel/Terminate cannot enter the typed kind contract; repeated demands retain the two
stable identities. Start and effect executors each have one permit, so the combined
library maximum is2; it is not one shared global permit.

The supported calendar domain `[2000-01-01,2101-01-01)` is covered by
`TestPrototypeBounds`, `TestJitterOutsidePrototypeRange`,
`TestBackfillRangeInclusiveFractionalAndMinimumBounds` and
`TestGenerationRangeEndResumesWithoutUnsupportedClock`; outside-domain requests reject,
while the final valid instant can resume finite bounded work. Manual time identity has
its distinct exact-int64-nanosecond check (`TestManualTimestampDefaultsAndInvalidInputs`).
Sequence/task uint64 exhaustion is a representability stop, not recoverable saturation:
manual/backfill acceptance and coordinator/generator/idle preflight tests assert no writes
at exhaustion; no wrapping or automatic reset is promised. The1ms continuation delay is
an exact scheduling rule tested by yield/range-end cases, not a maximum input limit.
The100/10000 scenario budgets,100/200 model action bounds,300s timeout and configured
per-property event bounds are verification limits whose manifests must report completion;
they are not runtime capacities requiring over-limit admission tests.
