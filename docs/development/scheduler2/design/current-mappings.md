# Current scheduler2 transition and evidence mappings

Reviewed source: WI-010 accepted implementation `5993fc195`, integrated as
`53ccfe669`, with main provenance `4e0b5d4d4`. This WI-011 inventory describes that
source, with the WI-011 local atomic update extension described below. It is a source-reviewed manual
mapping, not an executable cross-language trace adapter or refinement theorem.
No new checks were run for this document. Evidence identifiers, commands, bounds,
source hashes and outcomes remain in the [evidence ledger](../evidence/README.md).
An accepted slice establishes its recorded scope, not every product of the factors.

## Projection and observation boundary

The event-local projection in [component.go:41](../../../../chasm/lib/scheduler2/component.go#L41)
contains root accounting/control/activity metadata, accepted manuals, queue, effects,
and **only the addressed operation**. Admission and metadata Update use the latest
operation; keyed results use their explicit operation ID. This is a loading strategy,
not the full abstract state. A full verification projection additionally needs:

| Projection | Concrete source | Independently observed obligations |
| --- | --- | --- |
| Authority and identity | Root `Closed`, incarnation; exact CHASM execution key | A closed run authorizes no new snapshots; old exact refs never route to a replacement. Root tokens alone do not identify an incarnation. |
| Accounting and occupancy | Remaining/Limited/Started, Unfinished, BlockingOperationId, UnconfirmedStarts; all retained Operations | Admission debits automatic budget once; first chain evidence counts once. Unfinished excludes Completed/StartOnly; unconfirmed counts Pending/Ambiguous. ALLOW does not become the tracked blocker. |
| Immutable actions | Operation occurrence/action/request IDs; Requests and EffectRequests DATA | Full admitted request bytes, queued workflow ID/policy, admission-time template/carryover, frozen chain target for effects. Symbolic model atoms do not verify protobuf equality. |
| Enumeration | Generator continuation, revision/fingerprint, search/disposed cursors, pending candidate, terminal status; each backfill continuation/end | Discovery is distinct from disposition. Retain full nominal/actual/revision candidate until committed handoff or specified supersession. Automatic and historical update boundaries differ. |
| Accepted ingress | Manual/backfill sequences and pending records; queue with automatic/manual/backfill identity | Capacity rejection before acceptance, no debit on enqueue, no capacity drop of accepted work, legitimate overlap dispositions explicit. |
| Arbitration | Coordination token/armed/at, NextWork/NextQueue/NextOperation, OwnerClass/OwnerKey, BackfillFocus/LastBackfill | A protected physical vacancy and stable candidate entitlement survive operation-capacity waits. CPU, resource and request turns are distinct. |
| Lifecycle | ActivitySet/At, each operation StartObserved/At, IdleDuration/Token/Armed/At | First explicit start observation uses captured skipped offset plus post-RPC wall clock; anchor monotonic independently of retention; current eligibility and deadline required for close. |
| Obligations | Persisted logical Work/Start/Effect/Idle tasks and phase-specific external waits | Compare logical task identity/deadline separately from physical envelope identity. Pending execution authority does not imply a task while legitimately waiting for external capacity/evidence. |

Observe projections after successful CHASM transaction publication and fresh-object
reload. Failed publication must retain the old scheduler state and obligations.
RPC authorization/snapshot, remote acceptance and local result publication are
separate observations. History may emit replication bookkeeping tasks on a
scheduler no-op; these are not new scheduler work. Physical queue delivery, logical
validation, tree serialization and database atomicity are different evidence levels.

## All reducer events

[decision.Step:128](../../../../chasm/lib/scheduler2/internal/decision/decision.go#L128)
dispatches all fifteen event kinds below. [Apply:71](../../../../chasm/lib/scheduler2/component.go#L71)
rejects public admission events; only the coordinator invokes the internal admission
path. `applyTransition` supplies cached execution time, prepares only needed inputs,
checks work/idle publication requirements, then publishes metadata, DATA and tasks.
Every successful Step also reconciles activity/summary, desired overlap effects and
binding; those are not extra public event kinds.

Model file names below are relative to [model/](../model/). Named actions are actual
independent model actions. “Factor” means checked separately, not their Cartesian
product. Replay names identify current Go tests, not a universal event importer.

| Event | Concrete relation | Independent model / replay mapping and limits |
| --- | --- | --- |
| `Due` | Automatic coordinator handoff advances nominal cursor; admits, queues or disposes by pause/budget/catchup/overlap/capacity. | `buffer/buffer.fizz:AutomaticIngress`, `allowall/allowall.fizz:AutomaticAllowIngress`; arbitration `TransferAutomaticQueued`/`DirectAutomaticAllow`; `TestGenerationSavedReplay`, `TestBufferSavedReplay`, `TestAllowAllSavedReplay`. Calendar probe/disposition is a separate factor. |
| `Update` | Strictly newer revision changes root policy/pause/budget/pause-on-failure and activity. Does not replace Generator definition or admitted inputs. | `completion:TogglePolicy`, `buffer:UpdateDefinition/RecoverEligibility`, `manual/admission:Update`, `lifecycle/anchor:ControlUpdate`; model update atoms have different subsets. `TestIdleUpdateAdvancesAnchorAndInvalidatesOldDeadline` and policy tests. **No current single model action or API combines all source/control changes.** |
| `Acknowledged` | Matching nonempty chain confirms Pending/Ambiguous/Started; first evidence accounts once. Explicit first StartObserved may enrich retained Completed/StartOnly metadata without reopening phase/accounting/carryover. | `start:Acknowledge`, `allowall:KeyedEvidence`, `lifecycle/anchor:Acknowledge`; `TestActivityTerminalObservation`, `TestActivityFirstObservationAfterLegacyAcknowledgment`, lifecycle replay. Historical start/completion factors omit D52 observation metadata. |
| `RetryableFailure` | Pending retry uses max(current time, previous retry) plus bounded backoff, clamped at deadline; deadline reached retains Ambiguous. | `start:LoseAcknowledgment/DeliverStart/Expire` is a coarser relation, not exact backoff arithmetic. Task rollback regressions and literal tests check concrete timestamp successor. |
| `Complete` | First matching metadata completion settles phase/accounting; explicit failed/timed-out status applies current pause policy; no carryover enrichment. | `completion:PrepareCompletion/CommitCompletion` metadata case; `sequential:FirstCompletion`; `TestCompletionSavedReplay`. Completion carries no observed-start timestamp. |
| `Expire` | Pending at/after issuance deadline becomes Ambiguous; immutable intent remains, no budget refund or invented failure. | `start:Expire`, `sequential:ExpireAmbiguous`, `allowall:ExpireOne`; task/lifecycle ambiguity tests. No claim of eventual external resolution without assumptions. |
| `Callback` | Validate result/status presence, incarnation/op/chain; first terminal completion owns carryover and pause. ALLOW settles start-only and excludes carryover/pause participation. | `completion:PrepareCompletion/CommitCompletion/InvalidCompletion`, `allowall:KeyedEvidence`; completion/ALLOW replays. Local event authentication is assumed; no routed callback-token verification implied. |
| `Drain` | Select eligible queue entry, dispose ineligible prefix/set, implement last eligible cancel/terminate replacement while preserving intervening buffers; at most one admission. | `buffer:Drain`, `manual/admission:DrainManual`, `effects/selection:Drain`, arbitration `Drain`; buffer/effects replays. Buffer model's one-entry drain is not an identical Go transaction (bounded queue4 scan). |
| `EffectSucceeded` | Set matching Pending/Ambiguous effect Accepted; never releases blocker or accounts/completes a workflow. | `effects/effects.fizz:Result`, `effects/binding.fizz:Result` success branch; effects replay/task tests. |
| `EffectNotFound` | Set matching Pending/Ambiguous effect Absent; same non-completion restriction. | Same Result factors, absent branch; `TestIdleObligationsEffectCompletionAndDrain` checks queue still holds open. |
| `EffectFailed` | Pending retries with bounded monotonic retry timestamp; expiry becomes Ambiguous; Ambiguous remains retained. | Effects Result/AdvanceClock and binding Result/Advance; clock rollback tests check exact runtime arithmetic not represented by finite clock atoms. |
| `EffectExpired` | Bound effect past horizon becomes Ambiguous; no future issuance until external evidence. | Same effect factors; late success/NotFound can settle Ambiguous, superseded effects ignore results. |
| `AcceptManual` | Validate supported command/cap4/sequence, accept unique manual identity, refresh activity; no budget debit or automatic cursor advancement. | `manual/commands:Call`, `lifecycle/anchor:ControlUpdate`; `TestManualSavedReplay`. Framework request-ID retention is separately trusted/tested. |
| `DispatchManual` | Coordinator lease/operation entitlement transfers head to admission, queue or explicit overlap disposal; unspecified policy adopts current policy at transfer. | `manual/admission:Transfer`, arbitration `TransferManual`; manual replay and idle obligation composition. Old manual model lacks the current durable lease. |
| `DispatchBackfill` | Transfer one prepared historical occurrence with range identity, manual eligibility/budget bypass, queue policy or admission; then dispose nominal in same transaction. | `backfill/cursor:Publish` plus arbitration `TransferBackfill`; `TestCoordinatorSavedReplay`, backfill properties. Cursor/arbitration factors are not one checked product. |

The unconditional post-Step relations are [activity.go](../../../../chasm/lib/scheduler2/internal/decision/activity.go),
[reconcileEffects/BindEffects](../../../../chasm/lib/scheduler2/internal/decision/effects.go#L48),
and the adapter's [prepareEffects/publishEffects](../../../../chasm/lib/scheduler2/effects.go).
`reconcileActivity` derives unconfirmed-count deltas from the selected old/new phases;
a new operation ID adds an intent without removing the previously selected one.
Effect binding may read the actual tracked blocker even when another keyed operation
was addressed. Cancellation and termination have distinct immutable IDs; desired
termination supersedes future cancel issuance, not a cancel already authorized.

## Controls, scans and publication outside Step

| Entry / internal transition | Source and current behavior | Mapping / evidence boundary |
| --- | --- | --- |
| `NewExecution` / low-level `New` | [activity.go:13](../../../../chasm/lib/scheduler2/activity.go#L13) uses engine-allocated RunID and execution clock. `New` remains a low-level fixture constructor. | Lifecycle Init; actual History-engine recreation test verifies allocated refs and cached/persisted routing. Init alone does not prove framework allocation. |
| `Trigger` / `Backfill` wrappers | [manual.go:18](../../../../chasm/lib/scheduler2/manual.go#L18), [backfill.go:22](../../../../chasm/lib/scheduler2/backfill.go#L22) attach CHASM request ID; already-used ID returns success before another component mutation. | Manual commands Call/DuplicateChangedBody/ExpireFrameworkID; actual Node request-ID tests. Symbolic expiry does not reproduce real age/count retention. |
| `AcceptBackfill` | [backfill.go:34](../../../../chasm/lib/scheduler2/backfill.go#L34): cap4 durable requests, range/sequence checks, independent ID, activity and readiness published together. | Cursor fixture assumes an already accepted range; arbitration assumes two ready candidates. Public dedup/capacity tests supply acceptance boundary, not those models. |
| `ConfigureGeneration` | [generator.go:42](../../../../chasm/lib/scheduler2/generator.go#L42) replaces definition/seed/template/revision/catchup/policy; automatic update disposes through max(now, old disposed), supersedes unadmitted pending, refreshes activity and scan readiness. | Lean `update`, cursor `UpdateDefinition` only with the appropriate different boundary; generation replay and jitter update regression. Separate from metadata Update; atomic combined update is provided by UpdateConfiguration below. |
| `UpdateConfiguration` / local `Update` | [update.go](../../../../chasm/lib/scheduler2/update.go), common generation preparation: validate newer root/continuation revision, replace source and all controls in one publication; framework request-ID wrapper suppresses retained retries. | [D55/D56](atomic-update.md) compose source UpdateDefinition and control Update as one observed transition; Go whole-state rollback/composition tests plus update_v1 replay. Existing separate model factors do not mechanically prove this atomic composition. |
| Plan/grant/reconcile | [coordinator.go:122](../../../../chasm/lib/scheduler2/coordinator.go#L122): grant/revoke one stable vacancy lease, select operation winner, then runnable CPU class; publication stores lease even if operation capacity prevents a task. | `backfill/capacity:AcquireVacancy` and service actions; `backfill-composition.md` manual cyclic-rank argument. No automatic trace refinement of CPU selection. |
| Automatic scan | [coordinator.go:290](../../../../chasm/lib/scheduler2/coordinator.go#L290): at most8 probes; persist candidate/search/disposition/terminal state; Yield schedules bounded continuation, Waiting uses actual time. | Lean `scan` and occurrence oracle/properties; `sequential:YieldProbeWork/RunGenerator` coarsens this. Scans and handoffs are now separate commits. |
| Backfill scan/rebase | [backfill.go:113](../../../../chasm/lib/scheduler2/backfill.go#L113): newer definition rebases historical range after disposed nominal, preserving immutable range end; Active pins focus, Yield rotates, terminal removes request. | `backfill/cursor:Peek/UpdateDefinition`; backfill range oracle and coordinator replay. Lean update alone does not specify inclusive fractional backfill initialization. |
| Handoff/disposition | [coordinator.go:329](../../../../chasm/lib/scheduler2/coordinator.go#L329), [finishBackfill:168](../../../../chasm/lib/scheduler2/backfill.go#L168) combine admission/queue/drop with pending disposal. | Cursor Publish + policy factors. Failed handoff/reload checks, not a composed formal proof. Accepted-work disposition is observable progress, not a stutter. |
| Freeze / retention | [component.go:255](../../../../chasm/lib/scheduler2/component.go#L255): evict only terminal records at cap4, retain root activity independently; freeze full start request at admission. | Sequential retention, buffer/manual late-template factors, lifecycle anchor Evict; retention tests verify anchor and input bytes. |
| `ConfigureIdle` | [idle.go:16](../../../../chasm/lib/scheduler2/idle.go#L16): zero disables; duration0–365d; no activity refresh unless initializing a low-level root without anchor. | Idle factor assumes configured duration; concrete configuration/token/overflow tests cover API-like validation. |
| Idle reconciliation | [idle.go:42](../../../../chasm/lib/scheduler2/idle.go#L42): accepted work/source/phase holds are distinct from coordinator readiness; arm exact monotonic-anchor deadline with token. | `lifecycle/idle:HoldWork/UpdateAnchor`; eligibility is abstracted to booleans in model, concrete counts/phases are Go checks. |
| `Close` / `Terminate` | [lifecycle.go:20](../../../../chasm/lib/scheduler2/lifecycle.go#L20): durable terminal authority, invalidate task eligibility, retain history, no child-workflow termination. Framework Terminate additionally sets execution status. | Idle DeliverCurrent covers idle close; deletion/recreation is Go/History evidence, not an explicit create-delete product in lifecycle model. |
| Describe/read/snapshot | LifecycleState and request/effect snapshots do not advance activity; snapshots clone immutable RPC input, start snapshot captures skipped duration. | Effects Issue and start DeliverStart approximate authorization. Poison tests cover decoding avoidance; immutable snapshot tests cover ownership. No reduced DB-read claim. |

## Registered tasks and external waits

[Library.Tasks](../../../../chasm/lib/scheduler2/library.go) registers all seven task
identities, including three retained legacy identities. Validation is metadata-only
where documented; Execute rechecks authority/identity. Root closed guards are
required because framework root access itself does not universally reject closure.

| Task | Validation/execution | Mapping and recovery |
| --- | --- | --- |
| WorkTask | Exact token+scheduled time+open root; not-due is retryable. One scan, transfer or drain; advance CPU turn after successful quantum, queue turn after owner disposition, operation turn only after actual admission. | Capacity service actions and occurrence/cursor factor; fair single-task delivery plus CPU cyclic argument required. Blocked accepted candidates retain lease/readiness and reconcile on external results. |
| StartTask | PendingStarts ID and exact wake; locked snapshot checks Pending retry/horizon. One local permit, external RPC, then same-run keyed merge; successful RPC produces first explicit start observation. | start DeliverStart/ExternalAccept/Acknowledge/Expire; lifecycle anchor Acknowledge. Nil snapshot supplies Expire. Retryable failure preserves stable request. Overflow after acceptance returns error retaining the unconfirmed intent. |
| EffectTask | Effect ID/target blocker/exact wake; snapshot bound immutable cancel/terminate input; one local permit; classify success, NotFound, other failure or expired snapshot. | effects Issue/Result and binding factor; AwaitingTarget waits for chain evidence, Ambiguous waits for late evidence. Results never substitute for workflow completion. |
| IdleTask | Exact token/deadline, current eligibility/anchor, open root; early execution returns retryable not-due; due current task calls Close. | lifecycle idle DeliverCurrent/DeliverOld; L3 conditional on stability, time, delivery and publication assumptions. |
| GenerateTask | Validate false, Execute no-op. | LegacyTask stutter. Does not enumerate or admit. |
| ManualTask | Validate false, Execute no-op. | LegacyTask stutter. Does not transfer commands. |
| DrainTask | Validate false, Execute no-op. | LegacyTask stutter. Does not drain queue. |

Start/effect authorization ends at the locked snapshot boundary. A snapshot obtained
before close can still issue afterward; exact-run result merging cannot mutate a
replacement. Closed result handling can be a scheduler stutter while framework
transition stamps/replication bookkeeping change. Cancellation and bounded permits
are implementation tests, not modeled queue-network scheduling guarantees.

## Named stutters and fairness

These names describe the full projection above; a transition can stutter in a
coarser factor while changing real scheduler metadata. Never hide cursor disposal,
accepted ingress, queue drops, phase changes, lease grants or deadline replacements
behind a full-state stutter.

| Name | Permitted unchanged scheduler observation / distinction |
| --- | --- |
| FailedPublication | Durable tree/tasks stay unchanged; tentative work discarded. Remote acceptance already observed outside the transaction is not rolled back. |
| StalePhysicalDelivery / LegacyTask | Wrong token/time, removed operation, superseded effect or legacy admission handler cannot mutate current state. |
| DuplicateResult / UnknownResult | Already applied/evicted/wrong identity is ignored; conflicting live chain is rejected. First explicit start observation on a terminal record is **not** duplicate stutter. |
| ForeignIncarnation / ClosedResult | Early guard avoids payload preparation and successor scheduler work. Old exact refs do not resolve the new run. |
| RejectedControl / DeduplicatedControl | Invalid new command leaves state/tasks unchanged; framework-deduplicated successful ID does not reapply changed body or refresh anchor. |
| NotDue | Retryable error before scheduled time; task remains logically required. |
| StableReconcile | Same deadline/token obligation does not duplicate publication. A newly granted lease or idle obligation is not stable reconciliation. |
| CapacityWait / AwaitExternal | No executable quantum until usable capacity, chain or result evidence arrives. Not permission for a runnable class to starve. |
| Restart | Fresh objects reconstruct committed state; in-memory identities are discarded. No claim that real database crash atomicity was checked by the model. |
| AbstractScan | Arbitration model may ignore search-only quanta; concrete search/disposed state still advances and scan fairness needs separate justification. |

L1 belongs to the start model's bounded remote-acceptance/fair-delivery assumptions,
not an unconditional guarantee for permanently ambiguous intents. L2 uses weak
fairness for individual capacity-model actions, two fixed-policy ready backfill
candidates, queue4 and tracked+ALLOW cap4. The Go CPU coordinator is related by a
manual rank argument and sustained-competition tests, not mechanically derived
per-class fairness. Automatic tracked direct admission is more serialized by the
model's vacancy lease than in Go. L3's lifecycle factor assumes eventual stability
of eligibility/anchor, advancing time, fair service and eventual publication. It
abstracts accepted-work holds rather than proving every phase/count combination.
No universal elapsed-time or sixteen-grant bound follows from these factors.

## Replay coverage and explicit remaining gaps

Current versioned JSON adapters cover saved generation, buffer, completion, ALLOW,
effects, manual, coordinator and lifecycle scenarios in the corresponding
`*_replay_test.go` files. Generation/buffer/effects v2 reflect coordinator scan versus
handoff publication; v1 artifacts remain historical. The M1 shared trace schema and
replay oracle remain a smaller supported subset. FizzBee mappings are manual,
not importers of every Go trace; Lean consumes its own bounded enumeration examples.
The adapters reject unknown vocabulary; no common all-event trace adapter is claimed.

WI-011 reconciliation of the initial closure checklist (evidence present, not
milestone acceptance):

1. Atomic-update source received independent review. `update_v1.json`,
   `TestUpdateConfigurationPublishesWholeReplacement`, request dedup/rejection tests,
   and leased-backfill/queued-current-template composition cases exercise the local
   transaction boundary. `update_failure_test.go` adds early failure before Ack
   under both current pause-on-failure toggles. See
   [failure/chain race](../evidence/wi011/failure-chain-race.log),
   [five update mutants](../evidence/wi011/update-mutations/manifest.json), and
   [atomic update contract](atomic-update.md).
2. `TestUpdateComposedProperties` now independently checks accepted-input controls,
   source, phases, counters, requests, queue identities, blocker, chain and pending
   logical start obligations. The
   [strengthened exploration](../evidence/wi011/updates-exploration-final/manifest.json),
   [obligation mutant](../evidence/wi011/update-obligation-mutation/manifest.json)
   and [saved replay mutant](../evidence/wi011/update-obligation-replay-mutation/manifest.json)
   are separate from the initial narrower exploration. Current matrix rows retain
   explicit factor boundaries; neither these finite generated relations nor manual
   mappings claim an exhaustive product or Go refinement theorem.
3. The local normalized first-run chain contract and intermediate-result rejection
   are recorded in contract.md and `TestUpdateChainCompletionFence`. External dedup
   remains bounded by the recorded issuance horizon; framework request replay has
   its independent bounded retention. M3 still owns routed token authentication,
   actual continue-as-new/reset translation, API response/errors and visibility.
4. [Current measurement reconciliation](m2-coverage.md) records attached materialized
   values before/after CloseTransaction, modified/deleted nodes and serialized
   node/task bytes, with separate poisoned-DATA witnesses. History measurements
   count one cold and zero additional warm GetWorkflowExecution calls. These are
   whole-state persistence API calls, not database page/byte/latency measurements.
   Evidence: [measurement race](../evidence/wi011/replay-measure-race.log) and
   [History reads](../evidence/wi011/history-read-race.log).
5. Reviewed Lean statements remain limited to trusted ordered source, abstract
   natural-number time bounds and erased terminal cache. Nanosecond/fractional
   boundary mapping is manual. SP2/SP3 proof completion, axiom audit, executable
   Lean–Go correspondence and full spec-processing coverage remain M3.

WI-011/M2 acceptance is supported by the completed numeric matrix,
[boundary race](../evidence/wi011/boundary-final-race.log),
[effect-byte race](../evidence/wi011/effect-limits-race.log),
[final lint](../evidence/wi011/acceptance-lint.log) and
[independent review](../evidence/wi011/acceptance-review.md).
The full and incremental race logs cover their recorded snapshots; final source
fingerprints are in checks.json. M3/M4 obligations below remain open.

Historical per-slice READMEs should be read at their accepted revisions: one-slot
sequential occupancy, independent generator/drain/manual tasks, buffer one-entry
processing, and pre-D52 terminal-result stutters are not literal descriptions of
all current Go transitions. This document supersedes those descriptions only as a
manual current-source mapping; it does not retroactively broaden their checked
models. M3 differential/shared functional gates and M4 load/state-growth/migration
boundary remain unchanged.
