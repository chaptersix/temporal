# Scheduler migration ownership, rollback retries, and sentinels

Investigated commit `ad7b2298dcce00688cfe3b83ab02819abf9249e8` in `/tmp/temporal-worktrees/rollback`. No production changes, commits, or GitHub access. The three new test files are intentionally failing research prototypes.

## Findings and minimal sequences

### 1. Rollback can acknowledge an unrelated destination and close the source

Proposed safety invariant: a source may acknowledge successful migration only after the destination has accepted this source incarnation's state. Sharing namespace and schedule ID does not establish that relationship.

Production trace:

1. `AdminHandler.migrateScheduleToWorkflow`, `service/frontend/admin_handler.go:2301`, describes the existing V1 workflow. Only a dummy type blocks; a real or arbitrary workflow enters the warning-only branch at line 2322 and migration proceeds at line 2330. Thus a pre-existing real destination does not require a race to trigger this behavior. A new sentinel can also race the preflight.
2. `Scheduler.MigrateToWorkflow`, `chasm/lib/scheduler/scheduler.go:974`, creates migration state at line 979, pauses the source, and adds the task at line 989.
3. `SchedulerMigrateToWorkflowTaskHandler.Execute`, `chasm/lib/scheduler/scheduler_migrate_task.go:208`, starts with a new request ID, `ALLOW_DUPLICATE`, and `FAIL` conflict policy.
4. Any `WorkflowExecutionAlreadyStarted` is accepted at line 228. Neither its `StartRequestId`, run ID, first execution run ID, nor the destination's state is checked.
5. The source becomes closed and clears pending migration at lines 238–239, although the unrelated destination never imported the source snapshot.

Minimal sequence: create different schedules A (CHASM) and B (V1) at the same namespace/schedule ID; migrate A to V1. A is closed; B is untouched. The test injects the native collision response with `foreign-request`/`foreign-run`, executes the real task and real CHASM engine, and observes `closed=true` with a nil error. It does not start a real history service.

User impact: A's schedule specification, counters, pending work, and other state disappear from the active scheduler. Sentinel reservations reduce the chance of dual creation but are conditional (`workflow_handler.go:3622`) and expire; migration must still distinguish ownership. Ordinary create rejects real conflicts (`workflow_handler.go:3632`, `3778`) instead of reporting a successful handoff.

### 2. V1-to-CHASM has the symmetric collision behavior, already explicitly covered by visible tests

`handler.CreateFromMigrationState`, `chasm/lib/scheduler/handler.go:85`, creates without a request ID; the request proto (`proto/v1/request_response.proto:76`) has no migration/source ownership field. On conflict it checks only `Sentinel` (line 108), then returns `AlreadyExists` (line 125). `activities.MigrateScheduleToChasm`, `service/worker/scheduler/activities.go:410`, converts every `AlreadyExists` to success. `scheduler.run`, `workflow.go:421–429`, then completes the source workflow.

Minimal sequence: create CHASM destination B, create different V1 source A with the same logical ID, enable migration, trigger A's migration. A completes without B accepting A's state. The small activity test demonstrates the unchecked acknowledgement; the existing functional test is stronger evidence of the connected production path.

Overlap/caveat: `TestMigrateScheduleToChasm_AlreadyExists` (`activities_test.go:56`) and `TestScheduleMigrationV2AlreadyExists` (`tests/schedule_migration_test.go:54`, especially its comments near 177) explicitly expect this behavior, including retaining the pre-existing V2 state. This is a confirmed behavior and an ownership-invariant violation, but its classification as unintended must acknowledge that visible tests deliberately encode destination-wins semantics. Do not present it as a previously untested surprise. Namespace mismatch and live migration-disable checks already exist and are separate concerns.

### 3. Failed source close plus destination closure can restart a deleted schedule

Invariant: retrying one rollback operation must not create another destination workflow chain or restore old state after that destination has been deleted.

Minimal sequence:

1. Start rollback of CHASM A; V1 start commits successfully.
2. Fail the source close (`scheduler_migrate_task.go:234`) before commit once, or crash before attempting that update. A remains paused and migration-pending. Losing a response to an already committed close is different: the source is already closed and normal task validation stops the retry.
3. Close the V1 destination. A normal `DeleteSchedule` provides a concrete production trigger: CHASM delete returns `ErrMigrationPending` (`scheduler.go:952`), while the V1 path terminates its workflow (`workflow_handler.go:5151`). The frontend still returns success because one side deleted (`workflow_handler.go:5104`).
4. Retry the rollback task. It generates another random `RequestId` (`scheduler_migrate_task.go:208`) and requests `ALLOW_DUPLICATE` (`215`). History cannot deduplicate the old start request: `Starter.handleConflict`, `service/history/api/startworkflow/api.go:344`, checks the current run's request-ID map before policy resolution. A closed current run takes reuse policy (`service/history/api/workflow_id_dedup.go:78`), which permits another chain under `ALLOW_DUPLICATE` (`244`).
5. The second start imports A's old snapshot and closes A. The successfully deleted schedule has been resurrected.

The prototype uses the real CHASM engine, injects exactly one source-close failure, and feeds the second attempt through the real `historyapi.ResolveDuplicateWorkflowID` with either COMPLETED or TERMINATED current status. Both cases observe two chains where the invariant requires one. A separate native frontend control executes `WorkflowHandler.DeleteSchedule` and confirms it returns success after a pending-migration CHASM rejection and successful V1 termination. The native request-ID fast path is modeled at the mocked history boundary; a full history-service or frontend-delete integration run was not performed. This proves the task's identity rotation and its actual policy consequence; it does not prove that every action subsequently duplicates externally.

The adjacent test asserts request-ID stability directly: the two attempts get different UUIDs. A running owned destination is a control where the current code happens to complete the handoff safely by swallowing the collision.

### 4. Expired V1 dummy sentinels block rollback until their history is removed

Invariant: the sentinel reservation ends when the sentinel closes. Elapsed wall time alone is insufficient if a sentinel is still running, but a completed/terminated sentinel must not block ID reuse.

`dummy.DummyWorkflow`, `service/worker/dummy/workflow.go:26`, sleeps 15 minutes and returns; it does not delete its workflow history. `writeSchedulerWorkflowSentinel`, `workflow_handler.go:3790`, also documents that completion releases the reservation. `AdminHandler.migrateScheduleToWorkflow`, `admin_handler.go:2314`, checks only the described type. It computes remaining time for logging but returns `ErrSentinelBlocked` even when remaining time is zero and status is COMPLETED or TERMINATED.

Minimal sequence: create a normal CHASM schedule with sentinels enabled, allow the dummy to complete after 15 minutes, request rollback while completed history is retained. Rollback still returns Unavailable. Terminating the sentinel has the same failure. The frontend prototype supplies each native Describe status and proves Running remains blocked while Completed and Terminated incorrectly block as well.

Native comparison: `WorkflowHandler.isRealSchedulerInV1KeySpace` (`workflow_handler.go:3860`) correctly checks status. The new CHASM-sentinel control runs the actual idle task after advancing the fake clock 15 minutes and successfully migrates into the released ID. The defect is specifically the rollback admin preflight, not CHASM sentinel expiry.

Existing `TestMigrateScheduleToWorkflowBlockedByWorkflowSentinel` (`admin_handler_test.go:2223`) omits status entirely; a fix must give that fixture an explicit RUNNING status and add closed-status cases. `TestScheduleMigrationV2ToV1BlockedBySentinel` tests only a still-running dummy.

## Safe ownership design

1. Persist an immutable migration operation identity in the same source transition that begins migration. Include namespace ID, schedule ID, source execution incarnation/run ID, direction, and operation generation. Persist a stable destination start request ID before any external start. Do not use namespace/schedule alone or generate the ID in `Execute`. The existing `MigrateToWorkflowRequest.RequestId` is currently ignored and may be empty; a server-created durable generation is needed as fallback.
2. Store the immutable owner in destination state atomically with destination creation. For CHASM, add it to the internal migration state and pass a stable `chasm.WithRequestID` to creation; accept an existing execution only after authoritative owner comparison. For V1, carry it in internal `StartScheduleArgs` state and preserve it across continue-as-new. Do not infer identity from user memo, workflow type, schedule ID, or an unverified AlreadyStarted result.
3. An initial successful start or same-operation dedup response establishes a receipt; persist destination first-run identity and the receipt on the source before completing source closure. On collision, resolve the actual run/chain and verify the immutable owner. Mismatch or missing proof must retain the source and report a conflict; an ambiguous start outcome must not automatically resume source execution.
4. A stable request ID is necessary but insufficient across V1 continue-as-new. `MutableStateImpl.addWorkflowExecutionStartedEventForContinueAsNew` generates a new create request ID (`mutable_state_impl.go:2845`), whereas native start dedup consults the current run's request map. A legitimate descendant can therefore report a different `StartRequestId`. Use a verified first-run/chain owner or durable carried migration owner, and do not reject a verified descendant solely because its current create request differs.
5. An owned destination that has intentionally closed is evidence of completed handoff, not authorization to recreate it. A deleted/replaced destination with no surviving receipt is ambiguous; safely quarantine the source pending explicit recovery or retain a durable handoff tombstone beyond destination deletion/retention. A two-entity saga cannot guarantee both availability and safe replay after every trace of the first committed start is erased.
6. At final source update, recheck operation generation and source incarnation. Preserve normal CHASM stale-reference/failover fences. Read-then-start preflight alone is not ownership proof and has a TOCTOU gap.
7. Deploy owner-field preservation and validation to all task executors before enabling the protocol. Old task binaries ignore new owner fields and still accept unrelated collisions. Old pending operations lack proof: initialize a safe operation record before their first start if no start was attempted; otherwise reconcile actual destination ownership without guessing. Never treat a missing owner as a wildcard. V1 deterministic workflow changes also need the repository's versioning discipline.

## Crash, failover, and load assessment

Crash before destination start: source remains migration-pending and the durable operation can retry. Crash after destination start but before source receipt/close: this is the critical reproduced window. Reuse the operation identity, resolve owned chain descendants, and honor destination closure. Crash after source close: existing task validation (`scheduler_migrate_task.go:70`) rejects a closed source. An independently restarted stale executor still needs generation validation at commit.

Failover can move execution of the retry and can replicate the two records at different times. A process-local UUID or cache cannot prove ownership; owner/receipt must live in replicated durable state. Existing zombie/task-generation checks (`service/history/chasm_task_util.go:64`, `72`) fence some stale tasks but do not establish cross-entity ownership. No multi-cluster failover experiment was run, so this is a design assessment rather than an empirical guarantee.

At 10× load, longer RPC/queue delays increase the start-to-close ambiguity window and retry traffic. Completed sentinels do not clear sooner with additional retries. Current rollback retries clone the state and reserialize its snapshot each time (`scheduler_migrate_task.go:123–154`, `179`). Persisting a small owner/receipt costs O(1) metadata per operation; it avoids a full ownership scan. Verify by namespace/schedule/run key and use bounded exponential retries for transient failures, explicit conflict outcomes for mismatches, and no high-cardinality identity metric labels. Carrying an entire frozen snapshot improves repeatability but has storage costs proportional to buffered/backfill state; ownership alone does not solve snapshot-consistency bugs.

## Reproduction commands and outcomes

No randomized operation generator is needed. Tests use fixed namespace/schedule/request labels and fake time `2026-09-01T12:00:00Z`; only production-generated UUID values vary. The operation sequence and expected failure are independent of their values.

Expected failures:

```sh
go test -tags test_dep ./chasm/lib/scheduler -run '^TestRollbackResearch' -count=1
go test -tags test_dep ./service/worker/scheduler -run '^TestMigrationResearch|^TestMigrateScheduleToChasm_' -count=1
go test -tags test_dep ./service/frontend -run '^TestMigrationResearchSentinelBlocksOnlyWhileRunning$' -count=1
go test -tags test_dep ./chasm/lib/scheduler -run '^TestRollbackResearchCloseRetryMustNotRestartCompletedDestination$' -count=1
```

Passing native controls:

```sh
go test -tags test_dep ./chasm/lib/scheduler -run '^TestRollbackResearchNative|^TestMigrateToWorkflow_' -count=1
go test -tags test_dep ./service/worker/scheduler -run '^TestMigrateScheduleToChasm_' -count=1
go test -tags test_dep ./service/frontend -run '^TestMigrationResearchSentinelBlocksOnlyWhileRunning/Running$|^TestAdminHandlerSuite/TestMigrateScheduleToWorkflow' -count=1
go test -tags test_dep ./service/frontend -run '^TestMigrationResearchNativeDeleteAcknowledgesPendingSourceAndTerminatedDestination$' -count=1
```

Standards check: ran `make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false`. First attempt could not download the pinned tools under the original sandbox; approved retry downloaded them but hit a concurrent linter lock. Retried after interruption and hit the same lock. The equivalent pinned linter command with `--allow-serial-runners` finished with zero reported issues; the `--new-from-rev=HEAD` diff filter limits coverage of untracked prototypes. Parent subsequently requested reserving further lint for final stack verification; no further lint was run, including after the final native frontend-delete test was added.

Also ran read-only `rg`, `sed`, `nl`, `git status`, `git rev-parse`, CLI help/process inspection, and `gofmt -w` on the new test files. No new dependencies, proto edits, or generated code changes.

Files added:

- `chasm/lib/scheduler/migration_ownership_research_test.go`
- `service/worker/scheduler/migration_ownership_research_test.go`
- `service/frontend/migration_sentinel_research_test.go`
- `RESEARCH_ROLLBACK.md`
