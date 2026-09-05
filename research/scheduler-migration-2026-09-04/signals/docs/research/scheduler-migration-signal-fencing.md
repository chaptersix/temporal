# Workflow to CHASM migration: signal acceptance and fencing

Scope: `/tmp/temporal-worktrees/signals`, branch `research/agent-signals`. This is an investigation and test prototype, not a production fix. No GitHub access, publishing, or commits were performed.

## Finding and evidence level

A successfully accepted V1 mutation can be absent from the CHASM scheduler after migration. There are two distinct mechanisms:

1. A signal delivered while the migration local activity is pending can remain in the SDK signal channel. The workflow returns immediately after activity success without consuming it.
2. If the first create committed but its response or workflow-task completion was lost, a later workflow attempt can process the signal and build a corrected snapshot. The target still contains the first snapshot: an existing non-sentinel causes `AlreadyExists`, which the activity treats as success without comparing or installing the corrected snapshot.

The server's `UNHANDLED_COMMAND` guard matters. A signal buffered on the server during a closing workflow task causes that task to fail. A synthetic replay demonstrates that the retry can process the signal into V1 state. It is therefore incorrect to claim that every signal during a migration activity is silently ignored by V1, or that the server lacks a completion guard. That guard cannot reconcile a target execution already committed outside the workflow-task transaction.

The evidence combines actual scheduler execution in the SDK test environment, synthetic replay based on the repository's recorded migration fixture, and the real frontend/history/CHASM call sites. No live-cluster race reproduction was run. The SDK harness models the remote create's commit/result boundary; its first and retry snapshots are directly inspected. The synthetic histories establish SDK behavior for specified event sequences, not measured production timings or server-generated histories.

## Acceptance and representation invariant

For V1 `PatchSchedule` and `UpdateSchedule`, API success means that History accepted the signal durably; it does not mean the workflow already processed it. The acceptance point is the successful mutable-state transaction in `signalworkflow.Invoke`, after `AddWorkflowExecutionSignaledEvent`. A duplicate request ID may return a successful no-op referring to the previous acceptance.

Let `A(e)` mean mutation `e` has been accepted. Let `N(e)` be its outcome under the native scheduler with the same valid request, processing order, conflicts, capacity limits, and later mutations. Until migration retires the source, every accepted operation that native execution would still preserve must have a durable representation with a live path to `N(e)`:

- Source history/state plus an owner that will process it;
- A durable forwarding/outbox item with stable operation identity;
- A staged snapshot or delta covering it, with a durable obligation to activate;
- Or the committed target state/result produced by applying it.

At source retirement, source-only history or an unread SDK channel is insufficient. The target snapshot plus durable deltas must cover the source's complete accepted prefix, and must preserve the ordering/outcomes that native execution requires. Independently, at most one scheduler may execute the transferred business work. Multiple durable copies during staging are permissible; two independently active owners are not.

This is a refinement invariant, not a new promise that every acknowledged V1 update changes state. Native V1 `processUpdate` can reject a stale conflict token after the signal was acknowledged (`workflow.go:1112`), and native patch handling has overlap/buffer policies. The tests use one valid mutation, no competing updates, an unlimited schedule, and available buffer capacity, so those exceptions cannot explain the difference. A native immediate Describe is also not a processing acknowledgement.

For successful Delete, the required native-equivalent outcome is termination of the schedule incarnation, with no later migration attempt able to recreate that incarnation. Delete needs the same ownership fence or a durable tombstone; it is not a signal-channel drain problem.

## Causal trace

| Boundary | Actual implementation | Consequence |
|---|---|---|
| Routing decision | `WorkflowHandler.UpdateSchedule`, `workflow_handler.go:4755`; `PatchSchedule`, `:4916`; `isSchedulerErrorLegacyRoutable`, `:3975` | CHASM is tried first; NotFound/sentinel/closed routes to V1. A prior miss can be followed by a later V1 signal after CHASM creation. |
| V1 acceptance | `updateScheduleWorkflow`, `workflow_handler.go:4832–4847`; `patchScheduleWorkflow`, `:4941–4956`; `signalworkflow.Invoke`, `service/history/api/signalworkflow/api.go:35–123` | Success follows durable signal acceptance; there is no migration epoch or target-existence check on the signal path. |
| Consumption | `scheduler.sleep`, `workflow.go:877–902,944–947`; `processSignals`, `:1209–1220` | The sleep selector receives into pending fields. `processSignals` processes those fields, not every signal channel. |
| Last ordinary processing | `scheduler.run`, `workflow.go:384–412` | Signals already consumed by sleep are processed before eligibility and snapshot creation. Refresh activities can also yield after this point. |
| Snapshot | `executeMigration`, `workflow.go:1187–1194`; `migration.LegacyToCreateFromMigrationStateRequest`, `migration/migration.go:54–68,111–123` | Schedule and relevant state are cloned. A later mutation cannot modify the already-submitted request. |
| Yield | `executeMigration`, `workflow.go:1195–1206` | A local activity with 5-second StartToClose and one attempt is awaited. Server/worker recovery can still cause another execution; this is not globally at-most-once. |
| Target creation | `handler.CreateFromMigrationState`, `handler.go:84–93`; `CreateSchedulerFromMigration`, `scheduler.go:301–348` | The target is immediately a running component. It creates a generator task or a callbacks task; there is no separate incoming-migration activation phase. |
| Target starts serving mutations | `Scheduler.Update`, `scheduler.go:995–1049`; `Patch`, `:1053–1082` | Sentinel/closed/outgoing-migration guards exist. No incoming stage guard prevents serving a newly imported snapshot while V1 still runs. |
| Duplicate create | `handler.CreateFromMigrationState`, `handler.go:95–125`; `activities.MigrateScheduleToChasm`, `activities.go:407–411` | Any non-sentinel existing target is converted to AlreadyExists, and that becomes success. Neither migration identity nor snapshot revision/content is checked. |
| V1 retirement | `scheduler.run`, `workflow.go:420–429` | Successful migration immediately returns nil. There is no drain, forwarding obligation, or final target acknowledgement of a signal watermark. |
| Buffered-event guard | `respondworkflowtaskcompleted/api.go:379–384`; `handleCommandCompleteWorkflow`, `workflow_task_completed_handler.go:800–806` | Server-buffered events reject completion. Events already delivered to an SDK channel are not covered by that check. |

The current request contains only `namespace_id` and `state` (`chasm/lib/scheduler/proto/v1/request_response.proto:76–83`). `WorkflowMigrationState` in the target is specifically the reverse, CHASM-to-workflow migration (`message.proto:63–68`), so it cannot be treated as an incoming ownership fence.

## Deterministic counterexamples and native controls

`service/worker/scheduler/migration_signal_window_research_test.go:26` executes the real workflow for four operations: pause, unconditional full update, immediate trigger, and a one-action backfill. A local-activity started listener injects the mutation after the request was cloned. No sleeps or random timing are used.

| Sequence | Observed outcome |
|---|---|
| Native scheduler; one mutation; force continue-as-new | Pause/update preserved in carried state; trigger/backfill starts exactly one action; no unread mutation. |
| Mutation processed, then migration snapshot | Pause/update included in snapshot; trigger in transferred buffer; backfill in transferred backfiller. |
| Snapshot; mutation delivered during successful migration activity | Workflow completes with mutation channel unread. Target snapshot has no mutation, buffered start, or backfiller; no native action was executed. |
| Snapshot; mutation during activity; activity fails before commit; migration disabled | Native execution resumes and preserves the mutation/action. |
| Snapshot commits; mutation during activity; create response lost; retry reports existing target | Source consumes the mutation and sends a corrected second snapshot. Source completes, while the target modeled by the first committed snapshot still lacks the operation. |

`service/worker/scheduler/migration_signal_replay_research_test.go:19` modifies the in-memory event sequence of `testdata/replay_migration_v1_to_v2.json.gz`:

- Local-activity heartbeat boundary: keep events 1–30, then inject a patch signal, a new workflow task, the migration local-activity marker, and completion. Replay succeeds with an unread patch and unpaused source state.
- `UNHANDLED_COMMAND` boundary: replace final WFT completion with WFT failure, flush a patch signal, and supply the next workflow task plus local-activity results. Replay succeeds with paused source state and no unread patch. This is a positive control for source recovery; it does not establish that CHASM's old snapshot was replaced.

Reduced seed/sequence: seed `0` (no PRNG), one schedule, one pause patch with note `accepted mutation`, one migration call. The minimal scheduler sequence is `snapshot(S0); deliver(pause); migration-success; return`. A frontend-realizable ordering is `CHASM lookup => NotFound; snapshot(S0); V1 accepts pause; target commits S0; activity-success; source-complete`. A workflow-task heartbeat can deliver the accepted signal while the migration future remains pending. If the signal instead forces `UNHANDLED_COMMAND`, the alternative sequence is `commit S0; task-failure; apply pause to S1; create S1 => AlreadyExists; source-complete`, leaving target S0.

A signal whose stale frontend route reaches V1 only after V1 completion receives an error, not successful acceptance. That is a distinct transient-routing defect and is excluded from the silent-loss claim. Simply retrying the frontend against CHASM after a V1 NotFound can improve that availability case, but cannot repair an already acknowledged V1 signal.

Delete-resurrection candidate, from code rather than a live reproduction: `DeleteSchedule` tries CHASM first (`workflow_handler.go:5092–5098`), then terminates V1 (`:5100–5101`), and returns success when either succeeds (`:5103–5114`). A migration create already in flight can commit after the CHASM delete missed and after V1 termination. Neither `CreateFromMigrationState` nor its request validates the continued existence/incarnation of the source. A single pre-create existence check would still have a check-then-commit race.

## Design A: staged snapshot revisions, then activation after source retirement

Introduce an incoming migration record containing `(namespace, schedule incarnation, source original-run/chain identity, migration epoch, revision, accepted-prefix watermark, snapshot digest, phase)`. Name proposals: `StageMigrationSnapshot`, `SealMigration`, and `ActivateMigration`. A business ID alone is insufficient identity.

`StageMigrationSnapshot(epoch, revision, digest, state)` durably installs an inactive target. Repeating the exact revision/digest is a no-op; lower revisions are rejected or reported stale; an equal revision with different digest is a conflict. Higher revisions may replace only a matching inactive migration. They cannot overwrite an active target or another incarnation. Staging creates no generator/invoker/backfiller/callback work. Mutation routing stays on V1 while the target is staged; native creation must not treat this record as an ordinary live schedule or reusable empty slot.

V1 freezes business execution while staging, drains mutation channels at every migration continuation, processes accepted operations into transfer state, and uploads newer revisions when that state changes. Do not run `processBackfills` or `processBuffer` on business work represented by a still-live stage unless that stage is durably aborted first. The current behavior of resuming V1 actions after any activity error is unsafe if the error can hide a committed active target.

After a successful revision acknowledgement, V1 drains again and completes with a terminal migration result naming the exact epoch/revision/digest/watermark. A failed workflow-task completion must take a versioned continuation that drains newly delivered signals and stages a new revision; it must not replay an old successful result and immediately complete. The History completion transaction then supplies a natural final acceptance barrier: no new signal can be accepted into that run after successful close. Continue-as-new is not this terminal barrier.

A durable target task activates only after confirming that the corresponding source chain ended with the matching successful migration result, not cancellation, failure, timeout, termination, or continue-as-new. Activation commits phase=active and the initial scheduling tasks together. The source must arrange this durable obligation before closing; a best-effort RPC after close is insufficient. The terminal source result needs retention/recovery support until activation is confirmed.

Feasibility: implementable with new scheduler protos, target tasks, source workflow versioning, and frontend stage handling. It avoids requiring a cross-shard transaction, but the activation proof must be unambiguous across failover and recreation. It introduces bounded downtime in the quiescent case. Under continuous accepted traffic there may be no quiet completion window, so progress is not bounded without an ingress fence or backpressure. Repeated whole-snapshot uploads cost `O(revisions × state size)`.

## Design B: explicit ingress fence plus staged finalize

Add a durable History-side source fence, committed under the same workflow lock/transaction used to accept signals and terminate/delete the schedule. A versioned workflow command or internal RPC changes the source to sealed for `(incarnation, epoch)` and records the last accepted signal watermark `H`. New signal attempts after the fence return a distinct retry/redirect error; they must not return success. The frontend knows how to retry the target and understands that the staged target is temporarily unavailable.

V1 drains exactly the accepted prefix through H and stages the final revision. `FinalizeMigration(epoch, revision, digest, H, fence-proof)` activates that matching revision atomically with its tasks. The fence prohibits any newer source acceptance, so no post-finalize drain race remains. V1 completes only after a durable finalize acknowledgement. Delete invalidates the incarnation or writes a tombstone in the same fence domain, which stale creates/finalizes must reject.

Feasibility: this is the clearest protocol for bounded cutover under sustained traffic, but it requires changes beyond scheduler workflow code: History signal admission and workflow completion/fencing state, frontend redirects, replication, and mixed-version behavior. A frontend-only boolean or routing cache cannot implement the fence because already-routed requests are in flight. Checking the source before target commit also is not atomic with source acceptance/deletion.

An older frontend can conservatively surface the new retryable error rather than silently acknowledge a dropped operation. An older History host that does not enforce the fence is not safe: capability gating must cover every possible active host and failover destination before enabling it.

## Design C: retained source forwarding

Keep the source workflow alive after target creation as a durable forwarder. Freeze V1 schedule execution and convert each subsequently consumed signal into a target operation with identity `(incarnation, migration epoch, monotonically increasing sequence)`. The target atomically applies the operation and advances its deduplication watermark; retries reuse the same identity. Acknowledged target operations can be removed from the source outbox. Never replace the active target with a stale whole snapshot after it has started work or accepted API mutations.

The source must forward the gap between the snapshot's watermark and the route fence before permitting direct CHASM mutations whose ordering could conflict with that gap. Either retain one serialized mutation ingress until fenced, or provide a sequencing protocol shared by direct and forwarded writes. Idempotency alone does not preserve mutation order.

Existing workflow signal values do not uniformly carry a request ID or source history event ID: updates are converted to `FullUpdateRequest`, patches are plain `SchedulePatch`. A deterministic consumer sequence carried in workflow state is sufficient to deduplicate forwarding retries of a consumed operation. Preserving original client deduplication across source runs/incarnations requires additional request metadata and explicit semantics; it cannot be inferred from the patch body.

Without an ingress fence, a finite grace period cannot safely retire the forwarder. A stale frontend or a client directly signaling the system workflow may still receive successful V1 acceptance. Keeping the forwarder forever preserves liveness but retains a live V1 execution, complicates reverse migration/recreation, and makes Delete/outbox termination part of the protocol. It is a substantial compatibility bridge, not a small drain fix.

## Continue-as-new and resource bounds

All three designs need state that survives continue-as-new: migration epoch, original source chain identity, latest acknowledged stage revision, next sequence, accepted/forwarded high-water marks, outstanding outbox, ownership phase, and whether any target activation is irrevocable. A new run ID must not reset idempotency identity. Finalize must follow the source chain and distinguish CAN from terminal migration completion.

Bound each workflow task by a fixed batch of operations and payload bytes; use existing history/CAN suggestions plus explicit outbox/state size limits. CAN with a pending outbox carries that outbox and the same epoch. CAN with a staged snapshot preserves the ability to advance/revoke that exact stage. Do not blindly carry raw unconsumed SDK channels across CAN; they must be drained into serializable state and protected by the final acceptance barrier. The current loop checks history/CAN near `workflow.go:350–367` and only persists `StartScheduleArgs` at `:472`.

A per-run history bound does not bound total forwarding backlog. At ingress rate `lambda` and target drain capacity `mu`, backlog grows without bound whenever `lambda >= mu`, including at 10× traffic. Use an admission fence/backpressure that returns retryable errors before acceptance, batch acknowledgements, and target/source rate limits. Dropping already accepted outbox entries or expiring them by TTL violates the invariant. Trigger/backfill request identity must remain stable so activity retries cannot duplicate business starts.

## Failure modes, mixed versions, failover, and load

| Condition | Required behavior |
|---|---|
| Stage committed, response lost | Retry exact epoch/revision/digest; recognize that stage. Do not activate it or call an unrelated existing scheduler success. |
| Source crashes after consuming mutations | Replay reconstructs the same revision or forwarding IDs; target deduplicates exact operations. |
| Signal arrives during stage/finalize activity | Before the ingress fence, retain and stage/forward it; after the fence, reject without acknowledgement. |
| Activation committed, acknowledgement lost | Retry discovers that matching epoch already active; source cannot resume business execution. |
| Source closes, activator unavailable | The durable activation task retries; retain required source proof. Staged data cannot be garbage-collected merely because a short TTL passed. |
| Delete/recreate overlaps migration | Tombstone/incarnation fencing rejects old epoch creates and forward messages; schedule-ID reuse alone is insufficient. |
| Namespace failover | Replicate fence/phase/watermarks and validate active-cluster epoch. A stale source or target read must cause retry, not activation/resumption. Cross-shard replication ordering needs explicit proof/reconciliation. |
| Migration flag disabled | Abort/revoke a still-staged epoch before resuming V1. Once ownership transfer is committed, recovery must finish or execute an explicit reverse migration; flag rollback cannot simply abandon activation. |
| Mixed worker versions | Gate all new workflow commands/activities under a new recorded scheduler version. Old histories remain on their old command paths; stage/fence records cannot be interpreted by old target/History hosts as an active ordinary schedule. |
| 10× namespace traffic | Staging increases snapshot write and local-activity load; forwarding adds target writes and source history. Batch/delta transfer can reduce amplification, but bounded completion requires stopping new source admissions. |

Do not mistake a 5-second local-activity timeout, an in-process flag, a final `ReceiveAsync` loop, or an arbitrary grace timer for an ownership barrier. A drain can reduce one window, but another acceptance can occur while the following RPC yields. Finalize without a fence or verified terminal-source watermark recreates the same race.

## Files and verification

Added only:

- `service/worker/scheduler/migration_signal_window_research_test.go`: 20 deterministic operation/boundary cases.
- `service/worker/scheduler/migration_signal_replay_research_test.go`: 2 synthetic replay/control cases.
- `docs/research/scheduler-migration-signal-fencing.md`: this report.

Commands used for inspection: `cat` on the mandated root `AGENTS.md`; `git status --short`; `git branch --show-current`; scoped `rg`, `rg --files`, `sed`, and `nl`; `gzip -dc ... | jq` on the existing migration fixture; `ps` to inspect the shared lint process. Files were added/edited with `apply_patch`; `gofmt -w` formatted the two Go files.

Executed checks:

- `go test -tags test_dep ./service/worker/scheduler -run '^TestResearchMigrationSignalWindow$' -count=1 -timeout=90s`: passed after correcting the test's native backfill range and removing an extra migration signal from its pre-snapshot control.
- `go test -tags test_dep ./service/worker/scheduler -run '^TestResearchMigrationSignalReplay$' -count=1 -timeout=90s`: first exposed that UNHANDLED_COMMAND repairs source state; the test now asserts that positive control explicitly.
- `go test -tags test_dep ./service/worker/scheduler -run '^TestResearchMigrationSignal' -count=1 -timeout=90s`: passed, all 22 cases.
- `go test -tags test_dep ./service/worker/scheduler -count=1 -timeout=180s`: passed in 9.317 seconds. An earlier invocation was interrupted by a session permission change and its result was unavailable, so it was rerun.
- `make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false`: passed with 0 issues, including the target's errortype vet step. Earlier attempts encountered the shared golangci-lint lock and then three missing default switch branches in the new harness; those branches were corrected. The focused 22-case tests passed again after that correction.

No production implementation was changed and no proto regeneration was needed.
