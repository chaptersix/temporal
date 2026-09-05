# Ongoing backfill migration review

Reviewed commit: `ad7b2298d`, isolated worktree `/tmp/temporal-worktrees/backfill-review`.
No production changes or commits. No GitHub API/browser access.

## Invariants

1. For V1 ongoing-backfill cursor `C` and inclusive end `E`, only schedule occurrences in `(C,E]` remain to enqueue. Already enqueued actions retain their identities and overlap semantics. Migration must not enqueue `C` again, regardless of whether its prior action is pending, running, or completed.
2. An incomplete backfiller has a runnable successor task. For current stamped tasks, `task.Stamp == state.TaskStamp > state.Attempt`. Attempt counts successfully committed CHASM task executions, including capacity stalls; it does not prove cursor progress.
3. A capacity-only execution leaves the range cursor unchanged. Enqueueing a batch, persisting its cursor, and scheduling its successor must commit together.
4. Imported cached running state must be reconciled before it causes overlap-based action disposal. A pending callback attachment means the running entry can be stale.

## Findings and reduced sequences

### V1 exclusive cursor is migrated as a new inclusive request

V1 `processPatch` subtracts 1 ms from the user's inclusive start before storing the ongoing request (`service/worker/scheduler/workflow.go:557`). `processBackfills` passes that stored start directly to exclusive `processTimeRange`, then replaces it with the last enqueued actual time on partial batches (`:950`).

`convertBackfillsLegacyToCHASM` copies the request but clears LastProcessedTime (`chasm/lib/scheduler/migration/migration.go:429`). CHASM `processBackfill` interprets the missing cursor as a new inclusive range and subtracts another millisecond (`chasm/lib/scheduler/backfiller_tasks.go:173`).

Deterministic reduction: minute schedule; V1 has consumed `2026-08-01T12:00:00Z`, leaves `StartTime=12:00`, `EndTime=12:01`; conversion plus real SpecProcessor produces `[12:00,12:01]`, while the exclusive-cursor control produces `[12:01]`. No randomized seed is needed. An initially adjusted cursor at `11:59:59.999` does not exhibit duplication for this schedule; this control distinguishes initialization from continuation.

The new duplicate has a fresh backfiller-derived RequestId, distinct from imported pending/running starts. WorkflowId is based on nominal seconds and can match an earlier action, but manual starts explicitly use ALLOW_DUPLICATE reuse. Therefore workflow-ID reuse is not sufficient protection once the earlier run has completed. While running, overlap policies can skip, buffer, cancel, or terminate based on the duplicate; this is not harmless display duplication.

### Proposed Attempt=1 change strands every migrated backfiller

Hypothesis: clone StartTime into LastProcessedTime and set Attempt=1.

`newBackfillerWithState` schedules the initial task, advancing zero TaskStamp to 1 (`chasm/lib/scheduler/backfiller.go:52`). Validate rejects it because `1 > 1` is false (`backfiller_tasks.go:58`). CHASM engine test confirms: creation succeeds, no action is buffered, Backfiller remains at Attempt=1/TaskStamp=1; firing tasks one hour later changes nothing. The same cloned cursor with Attempt=0 finishes normally and buffers exactly the remaining occurrence. This is an introduced liveness failure, not merely an inaccurate retry count.

Minimal ordinary-timestamp fix: clone the stored exclusive cursor into LastProcessedTime and retain Attempt=0. Request, end time, overlap override, and ID behavior need not change. Faking TaskStamp alongside Attempt adds unnecessary state coupling and changes retry backoff.

### Unix epoch remains a valid-cursor edge case

`hasRecordedProgress` treats both nil and protobuf `{seconds:0,nanos:0}` as missing. An explicit Unix epoch cursor is a valid timestamp; cloning it alone still re-enqueues epoch for a calendar schedule. The real-spec test uses `* * * * *`, cursor `1970-01-01T00:00:00Z`, end `00:01`, and gets two actions versus the exclusive control's one. The interval implementation has separate negative-time integer-division behavior that masks this case, so the calendar control matters.

A complete cursor representation should distinguish absence from valid epoch, normally by presence alone. This would affect native CHASM epoch continuation too; it deserves an explicit regression test and any old-state compatibility audit. Missing legacy StartTime should not be fabricated as evidence of progress; public request validation is the normal guarantee, while malformed imported state is a separate policy decision.

### Backfill tasks bypass migration running-state reconciliation

`CreateSchedulerFromMigration` intentionally creates inactive Invoker and Generator components until SchedulerCallbacksTask reconciles imported running state (`scheduler.go:324`). It nevertheless creates each backfiller with an immediate task (`:331`). A backfiller enqueues starts, and `Invoker.EnqueueBufferedStarts -> addTasks` immediately schedules ProcessBuffer (`invoker.go:53`, `:338`). Neither this path nor ProcessBuffer validates the pending callback barrier.

Deterministic CHASM engine reduction, using the corrected cursor and Attempt=0:

- Paused minute schedule, SKIP policy, one imported pending manual action, one imported running workflow with HasCallback=false. The remote workflow may already have completed; no callback/status side effect has run yet.
- No backfiller control: creation retains both entries; OverlapSkipped=0.
- Add one ongoing backfill with cursor 12:00/end 12:01: creation drops the imported pending action and the new 12:01 backfill action; OverlapSkipped=2. Only the unreconciled running entry remains, still HasCallback=false.

The engine reproduction runs no status-check task: the loss happens during migration creation's immediate pure tasks. A genuine pending request and a future generated request both bypass the intended barrier. A fix must explicitly re-arm deferred backfill tasks after reconciliation; merely invalidating or returning without a successor can introduce another stranded component. This is separate from the cursor fix.

### Capacity and rollback context

Backfiller admission is `(maxBufferSize/2 - max(0,bufferedCount-10) - generatorReserve)/backfillerCount`, clamped before division. At defaults, empty buffer yields `450/N`: ten backfillers get 45, 450 get 1, 451 and 1000 each get zero. With more than 450 persistent ranges, no range can progress to reduce the divisor even when the buffer is empty. A tenfold increase from 100 to 1000 exposes this liveness limit. Each attempt also scans all backfillers, creating quadratic aggregate counting work. These are pre-existing limits, not consequences of the cursor fix.

V1 pending buffers, CHASM retained history, and running entries consume capacity differently, so migration can stall a range even when V1 had some free capacity. Such stalls increment Attempt and TaskStamp without moving LastProcessedTime. Cursor-only migration preserves the correct exclusive position through this path; using Attempt as the progress signal is wrong.

Native CHASM fresh/stalled `[T,T]` rollback exports exclusive V1 StartTime=T and loses the boundary action. Prototype confirms both Attempt=0 and Attempt=3 with nil cursor. Parent reports this is already covered by upstream #11878; do not propose a duplicate fix. Migrated cursor-only state keeps the original exclusive StartTime, so rollback before its first CHASM attempt is correct even though reverse conversion currently gates its cursor override on Attempt>0.

## Crash, retry, failover assessment

Backfiller is a pure task: successful batch buffering, cursor update, successor scheduling, and Attempt increment occur in the same CHASM mutation. A failed transaction cannot legitimately publish just the cursor or just the batch. Replayed old stamped tasks should invalidate after a committed successor. Existing lifecycle tests cover stale continuation suppression and forward-dated ranges. Cursor-only conversion does not add an external side effect and is independent of wall-clock migration time.

No actual process crash or cross-cluster failover was injected in this review. The stated durability behavior follows the existing CHASM transaction/task contract; an integration test should verify it rather than treating this review as failover evidence. Importantly, Attempt=1 task invalidation persists across retries/failover and cannot be repaired by waiting. Callback-barrier disposal is already committed before remote reconciliation and cannot be restored from the later completion response.

V1 migration retries use create-once schedule identity; the activity treats AlreadyExists as success. Backfiller IDs are newly generated during conversion but persist once creation commits. Changing IDs does not repair action-level cursor duplication, and a cursor fix should not alter those semantics.

## Test matrix

| Case | Assertion/control | Status |
| --- | --- | --- |
| Initial V1 inclusive adjustment | Original and cloned cursor each yield intended boundaries | Prototype passed |
| Continued V1 boundary | Baseline duplicates C; cloned cursor/Attempt=0 yields `(C,E]` | Prototype passed |
| Proposed Attempt=1 | Real CHASM creation plus later timer fire remains stalled; Attempt=0 completes | Prototype passed |
| Calendar epoch cursor | Baseline and cursor-only clone duplicate epoch | Prototype passed |
| Native fresh/stalled rollback | `[T,T]` loses one action versus native control | Prototype passed; known upstream scope |
| Stale imported running + pending + backfill | Two actions discarded before callback; no-backfiller control preserves them | Prototype passed |
| Concurrent backfill threshold | 10/450 progress; 451/1000 get zero despite empty buffer | Prototype passed |
| Pending/running/completed duplicate execution | Capture exact emitted RPC IDs and policy outcomes after migration | Code traced; RPC test still recommended |
| Capacity stall then drain | Watermark stable through stall, next batch exclusive, successor valid | Existing coverage + code trace; migrated-specific integration recommended |
| Crash before/after batch commit, failover | Same final action multiset and monotone exclusive cursor | Not injected |
| Multiple ranges/order, jitter, spec update | Preserve per-range request semantics; quantify allowed enqueue ordering differences | Further integration coverage recommended |

Prototype assertions characterize the observed bugs, so the reproduction suite is green. To turn each into a failing regression, assert the invariant instead (e.g. cursor continuation equals native action count; callback case retains pending while HasCallback=false).

## Files and commands

Added `chasm/lib/scheduler/backfill_migration_review_test.go` and this note only. No production files changed.

Commands: `gofmt -w chasm/lib/scheduler/backfill_migration_review_test.go`; `go test -tags test_dep ./chasm/lib/scheduler -run '^TestReviewBackfill' -count=1` (passed); `make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false`; `git diff --check`.

Both lint invocations passed (0 issues), including required errortype vet; the second included all final test additions. `git diff --check` passed. The normal Makefile installed local lint binaries through Go's tool installer; no GitHub issue/PR/repo API operations were performed.
