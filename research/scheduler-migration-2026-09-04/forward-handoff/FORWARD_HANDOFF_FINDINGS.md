# Forward migration investigation

All work was isolated to `/tmp/temporal-worktrees/forward-handoff`; no commits or GitHub access. The starting worktree was clean. This is an investigation and intentionally leaves failing regression prototypes, not a production fix.

## Confirmed finding: ambiguous create outcome breaks exclusive ownership

Once CHASM commits the transferred state, the workflow source must not execute the transferred actions or acknowledge changes that will disappear when it retires. A migration rollback must prevent new transfers without forgetting or abandoning an already committed transfer. An `AlreadyExists` result is not proof that the latest source snapshot was applied.

The current workflow treats every failed migration activity as permission to continue normal scheduling. The create may already have committed. This yields duplicate manual execution opportunities, lost changes, and two continuing schedulers after rollback. These are consequences of one ownership protocol defect, not three unrelated bugs.

### Production trace

1. `schedulerWorkflowWithSpecBuilder` selects migration in `service/worker/scheduler/workflow.go:411-420`. Its loop already processed signals and populated the pending action buffer before making the call.
2. `scheduler.executeMigration`, `workflow.go:1187-1206`, converts the current source state and calls one local activity attempt with a 5-second start-to-close timeout. `MaximumAttempts` is 1. It reconstructs the snapshot on each later workflow iteration.
3. `activities.MigrateScheduleToChasm`, `service/worker/scheduler/activities.go:398-407`, checks the live enable flag only before the RPC. Disabling the flag after this check does not fence an in-flight create.
4. `handler.CreateFromMigrationState`, `chasm/lib/scheduler/handler.go:84-93`, calls `chasm.StartExecution` using namespace and schedule ID, without `WithRequestID` or source-run/migration-attempt provenance. The request schema at `chasm/lib/scheduler/proto/v1/request_response.proto:76` contains only namespace ID and transferred state.
5. `ChasmEngine.startExecution`, `service/history/chasm_engine.go:233-242`, persists the execution and explicitly documents that returned errors can have ambiguous commit outcomes. `constructTransitionOptions`, the same file at 827-829, invents a new UUID when no request ID is passed.
6. `CreateSchedulerFromMigration`, `chasm/lib/scheduler/scheduler.go:328-345`, installs the transferred invoker/generator state and schedules callback reconciliation or generation; it does not await source retirement before CHASM can process actions.
7. A lost response or ambiguous persistence error propagates through `activities.go:413-417`. All `Unavailable` errors are even labeled "blocked by sentinel" although a general transport failure can be ambiguous.
8. `workflow.go:431-445` logs the error and immediately runs `processBackfills` and `processBuffer`. The source buffer was cloned during conversion, not drained or fenced.
9. `migration.convertBufferedStartsLegacyToCHASM`, `chasm/lib/scheduler/migration/migration.go:273-288`, assigns a deterministic target request ID to a clone. The original source still has an empty request ID. `scheduler.startWorkflow`, `workflow.go:1684-1689`, generates a fresh source request ID even at v13 because the source field is empty. Both source (`workflow.go:1679-1682`) and target (`chasm/lib/scheduler/invoker_tasks.go:662-665`) permit reusing a completed workflow ID for manual starts.
10. A subsequent create collides with the committed target. `handler.go:95-125` checks only whether it is a sentinel, then returns `AlreadyExists`. `activities.go:410-411` converts this to success, and `workflow.go:429` retires V1. Changes or new actions processed in V1 after the original snapshot need not exist in CHASM.
11. If rollback occurs instead, `activities.go:398-405` refuses all subsequent create calls before checking the existing target. At v12 the source continues with `PendingMigration=true`; v13 clears it at `workflow.go:405-407`. Both leave the committed target alive. The source need not ever reconcile ownership while the flag stays off.

### Minimal deterministic sequence

Fixed logical start: `2022-06-01T00:00:00Z`. One schedule, one pending manual `ALLOW_ALL` start, a one-minute periodic spec, no running actions. No random seed or sleeps are required.

1. Enable migration and run one source workflow activation.
2. At the `CreateFromMigrationState` RPC boundary, store a clone as the committed target, then return `Unavailable` to model response loss.
3. Observe V1 `StartWorkflow` for the transferred manual action, with the same workflow ID but a different request ID than the target copy.
4. At logical +1 second, either deliver refresh (retry gets `AlreadyExists`), disable migration and refresh (source continues), or deliver a pause patch (source applies pause before retry, but target keeps the original unpaused snapshot).
5. For the rollback/control cases, force continue-as-new at +2 seconds to inspect durable source input. The force-CAN signal is only a test termination mechanism.

Actual duplicate execution requires the first manual run to complete before the other scheduler starts it. The passing CHASM component-engine control advances to +1 minute, runs real Generator/ProcessBuffer/Execute task handlers, and observes the inherited manual start dispatched with the target request ID and `ALLOW_DUPLICATE`. Therefore a quick V1 run completed during this minute permits a second real run; this conclusion is from the tested request identities/reuse policies and production start semantics, not a live two-server integration test. Automatic starts with matching workflow IDs and `REJECT_DUPLICATE` have additional protection; this finding does not claim every occurrence duplicates.

### Prototypes and results

`service/worker/scheduler/forward_handoff_repro_test.go` uses the real workflow loop, converter, and `MigrateScheduleToChasm` activity. Only the scheduler RPC boundary and started action response are controlled. At v12 and v13:

| Case | Result |
| --- | --- |
| native_control | PASS: source executes exactly one manual start; no target |
| acknowledged_control | PASS: source emits no action and retires |
| lost_reply_retry | FAIL: source dispatches the transferred action before successful retry |
| lost_reply_rollback | FAIL: source dispatches it and continues; exactly one create RPC, pending true at v12 / false at v13 |
| lost_reply_pause | FAIL: source processes delivered pause, retries successfully, but target is still unpaused |

Six intended failures and four passing controls. Fixed target request ID for this seed is `sched-migrated-0-54f3a204-9695-53f8-affa-431cc49904f8`; source UUIDs vary but are always distinct.

`chasm/lib/scheduler/forward_handoff_repro_test.go` passes. It uses the real migration handler, component-engine persistence, exact replay, changed conflict-token/notes replay, and real generator, invoker buffer, and invoker execution handlers. Both replays yield the same `AlreadyExists`; original target state survives unchanged; the inherited manual start dispatches once on CHASM.

These prototypes deliberately do not mock the scheduling decisions that fail. The transport shim models a committed reply-loss boundary; component-engine persistence separately establishes target behavior. No live database/crash injection or frontend API acknowledgment was tested. The pause case tests a delivered and processed workflow signal, whose accepted routing to V1 is possible while migration is enabled but creation/routing configuration still sends mutations there.

## Recovery, versions, and load

- A source-worker crash alone, before any failed local activity result is recorded, can recover by replaying the create and observing `AlreadyExists`, provided migration remains enabled and the target still exists. This special case does not prove the general protocol safe.
- Once a failed activity outcome and source action are recorded, replay preserves the failed handoff/fallback history; restart cannot undo the action or lost update. Rollback/continue-as-new preserves the demonstrated split state.
- A single lookup returning `NotFound` after timeout is insufficient to authorize source resumption because the original RPC can still commit later. A safe resolution needs an attempt fence or equivalent durable terminal outcome.
- The current default recorded workflow version is 12 (`workflow.go:256-258`). Migration is deferred below 12 (`workflow.go:415-418`). Tests explicitly demonstrate the defect at both 12 and 13; v13's identity preservation does not help ordinary V1 buffers because conversion updates only clones. The v13 reset makes rollback discard the pending evidence. The activity's live guard is not version-gated.
- `determineVersionTransition` (`workflow.go:1930-1931`) retains the recorded version as a monotonic floor. Lowering a ceiling cannot roll v13 histories back to v12 behavior. Any corrected command sequence requires a new version/replay strategy and rollout that ensures old workers do not continue the unsafe handoff. No different historical binary was executed.
- At 10x load, additional queuing and RPC latency can increase exposure to the fixed 5-second timeout. More transferred actions enlarge the number of actions at risk. Retries occur per workflow wakeup, clone the then-current source state, and cannot repair divergence already created. No load benchmark was run.

## Fix assessment

A safe fix is feasible but is a handoff protocol change, not an `AlreadyExists` special case or a larger timeout. The bounded design needs a durable source migration attempt/snapshot identity, source quiescence before sending the first create, idempotent target acknowledgment of that same attempt, and ownership reconciliation that continues after the rollout flag turns off. Accepted signals while frozen must be forwarded, retained, or otherwise resolved before source retirement. A definitive aborted attempt must be fenced before source resumption. Existing ambiguous attempts need an explicit recovery strategy.

An always-frozen source retry loop improves exclusivity but trades availability for safety and must handle rejection/sentinel cases, signal accumulation, continue-as-new, and old-history replay; blindly retaining/clearing `PendingMigration` does not solve those issues. Merely reading the target while disabled does not cover delayed commits. Merely preserving action IDs does not protect updates or repeated future generation. Increasing retry count only reduces the probability of the window.

No safe one-file production patch is proposed or implemented here. Already duplicated customer actions cannot be automatically undone.

## Other observation, not promoted to a finding

The no-running-workflow migration branch schedules only Generator.Generate. Generator enqueues invoker processing only if it generates new starts. This appears capable of stranding a transferred manual buffer for a paused/manual-only schedule. It was communicated as a hypothesis for the task-chain investigation, not covered by a failing assertion here. The actual target control uses a one-minute periodic tick to provide an ordinary queued path to processing the inherited buffer.

## Files and commands

New files only: this note, `service/worker/scheduler/forward_handoff_repro_test.go`, and `chasm/lib/scheduler/forward_handoff_repro_test.go`. No production files changed. `make` created an empty ignored `.bin/` directory while trying to resolve lint tools.

Focused commands executed, always in the assigned worktree unless reading the mandated AGENTS.md:

```sh
cat /home/alex/Work/temporal/AGENTS.md
git status --short --branch
git status --short
gofmt -w service/worker/scheduler/forward_handoff_repro_test.go chasm/lib/scheduler/forward_handoff_repro_test.go
go test -tags test_dep ./service/worker/scheduler -run '^TestForwardHandoffResponseLoss$' -count=1
go test -tags test_dep ./service/worker/scheduler -run '^TestForwardHandoffResponseLoss$/v(12|13)/(native_control|acknowledged_control)$' -count=1
go test -tags test_dep ./chasm/lib/scheduler -run '^TestForwardHandoffTargetCommitAndReplayControl$' -count=1
set -o pipefail
go test -tags test_dep ./service/worker/scheduler -run '^TestForwardHandoffResponseLoss$' -count=1 -json | rg '"Action":"(fail|pass)"|same manual action|committed target,|Messages:'
git diff --check
make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false
make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false GOLANGCI_LINT=/home/alex/Work/temporal/.bin/golangci-lint-v2.13.0 ERRORTYPE=/home/alex/Work/temporal/.bin/errortype -o /home/alex/Work/temporal/.bin/golangci-lint-v2.13.0 -o /home/alex/Work/temporal/.bin/errortype
```

Read-only navigation used `rg`, `rg --files`, `sed -n`, and `nl -ba` for the referenced scheduler/worker/frontend/history files, `go.mod`, and `Makefile`; `command -v`, `ls -l`, and `ps -eo pid,args` identified existing lint tools and the lint lock. No git mutation commands were run.

The first Go test hit the then-active read-only Go cache sandbox; its approved rerun executed. The first make command failed on restricted proxy DNS/tool download (no GitHub access occurred). The existing-binary make run was interrupted once; the resumed invocation failed with `parallel golangci-lint is running`. Final lint is deferred per the parent instruction reserving one final pass per completed stack. The final focused tests completed under the updated unrestricted execution policy; no further lint attempt was made after that instruction.
