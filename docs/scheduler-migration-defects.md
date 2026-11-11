# Scheduler migration defect evidence

This investigation exercises workflow-backed scheduler to CHASM migration and CHASM to
workflow rollback. It adds deterministic generated-state tests, a Go fuzz target, scheduler
harness scenarios, and failure injection around both destination creation and source closure.
Production fixes are outside this change.

Known-defect tests are skipped during ordinary test runs. Set
`TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1` to turn them into failing regression tests. This
keeps normal CI useful while retaining exact counterexamples.

## Open PR status

Checked against open `temporalio/temporal` pull requests on 2026-09-04. “New” means no matching
open PR was found by scheduler-migration, symbol, error, and failure-mode searches; it does not
claim that no private work or unlinked issue exists.

| Finding | Status |
| --- | --- |
| Signal accepted during the migration snapshot is lost | **New: no matching open PR found** |
| Committed CHASM create with a lost response causes duplicate execution | **New: no matching open PR found** |
| Unrelated destination is accepted as a successful handoff | **New: no matching open PR found** |
| Completed workflow sentinel still blocks rollback | **New: no matching open PR found** |
| V1 to CHASM resets a progressed backfill cursor and replays it | **New: no matching open PR found**; [#11878](https://github.com/temporalio/temporal/pull/11878) fixes the opposite, CHASM to V1 fresh-backfill boundary |
| CHASM to V1 skips a fresh backfill boundary | Covered by open PR [#11878](https://github.com/temporalio/temporal/pull/11878) |
| Recent-action timestamps change | Covered by open PR [#11880](https://github.com/temporalio/temporal/pull/11880) |
| Multi-payload completion result is truncated | Already analyzed in [#11880](https://github.com/temporalio/temporal/pull/11880), which documents retaining only the first payload rather than preserving all values |

Open PR [#10729](https://github.com/temporalio/temporal/pull/10729) overlaps the round-trip test
method, but its stated assertions are conflict token and high-water-mark preservation; it does
not cover the four new counterexamples above. Open PR
[#11924](https://github.com/temporalio/temporal/pull/11924) changes sentinel error
classification and likewise does not cover genuine destination collisions.

## Findings

### Critical: an acknowledged signal can be lost during workflow to CHASM migration

PR status: **new; no matching open PR found**.

`TestMigrationCounterexample_TriggerDuringSnapshot` delivers a trigger while the migration
local activity is in progress. The activity creates the destination from the earlier snapshot,
then the V1 workflow returns successfully without draining the signal. Neither source nor
destination executes or retains the trigger. The SDK test harness also reports the unhandled
`patch` signal.

The snapshot is built before the local activity in
`service/worker/scheduler/workflow.go:1184-1206`, and successful migration immediately returns
from the workflow at `workflow.go:421-429`. Signals are only transferred from channels into
`pendingPatch` and `pendingUpdate` by `sleep`, so a signal accepted after the snapshot is outside
the handoff boundary.

Impact: a successful Trigger, Update, Pause, or Backfill API request can disappear. Fix the
handoff protocol so the source freezes or drains accepted operations before committing the
destination, and include a source sequence watermark that the destination can verify.

### Critical: a lost CHASM create response can execute an action on both schedulers

PR status: **new; no matching open PR found**.

`TestMigrationCounterexample_LostCreateResponse` injects a committed
`CreateFromMigrationState` write whose response is lost. V1 interprets the error as a failed
migration and executes its buffered manual action at `workflow.go:441-445`; CHASM still owns the
same action from the committed snapshot. This fails at workflow versions 12 and 13, with
migration left enabled and with it disabled before retry. The test records different request IDs
at source and destination, so StartWorkflow request deduplication cannot collapse the two starts.

`MigrateScheduleToChasm` returns an error for an ambiguous `Unavailable` response at
`service/worker/scheduler/activities.go:407-417`. A later `AlreadyExists` is treated as success,
but only after the source has resumed scheduling. Fix this by treating create errors as an
uncertain commit, reading back and validating a durable handoff token before the V1 scheduler
may process work again.

### Critical: destination collisions are accepted as successful transfers in both directions

PR status: **new; no matching open PR found**. PR #11924 handles sentinel classification, not an
unrelated genuine scheduler at the destination.

`TestMigrationCounterexample_ForwardDestinationCollision` supplies an unrelated existing CHASM
schedule. The activity converts every `AlreadyExists` into success at `activities.go:409-412`,
and V1 exits with its buffered action untransferred.

`TestMigrationCounterexample_RollbackDestinationCollision` supplies an unrelated existing V1
workflow. The rollback task converts every `WorkflowExecutionAlreadyStarted` into success at
`chasm/lib/scheduler/scheduler_migrate_task.go:222-230`, then closes CHASM. Its acknowledged
trigger remains only in the closed source.

Impact: a stale or independently-created destination can make the current schedule inaccessible,
lose pending work, or leave two divergent owners. Persist a stable handoff ID in source and
destination, reuse it on retries, and accept an existing destination only after checking that ID
and the snapshot epoch. The rollback task currently generates a fresh UUID at
`scheduler_migrate_task.go:207-216`, which cannot prove ownership on retry.

### High: workflow to CHASM migration replays a processed backfill boundary

PR status: **new; no matching open PR found**. PR #11878 changes only CHASM to V1 conversion of a
fresh or zero-watermark backfill.

`TestMigrationCounterexample_ForwardBackfillReplaysWatermark` starts from an incremental V1
backfill whose stored `StartTime` is an exclusive progress cursor. Native continuation emits only
the next action. Migration resets every backfiller's `LastProcessedTime` and `Attempt` at
`chasm/lib/scheduler/migration/migration.go:429-446`; CHASM therefore treats it as fresh and emits
the action at the cursor again.

Impact: a backfill action can execute twice. Preserve whether progress was recorded and the
exclusive watermark when converting an ongoing V1 backfill.

### Medium: a closed workflow sentinel still blocks rollback

PR status: **new; no matching open PR found**.

`TestMigrationSentinelEvidence_BlocksOnlyWhileRunning` proves that the rollback preflight rejects
completed and terminated dummy workflows even though their reservation has ended. The same
preflight correctly blocks a running sentinel. The rollback path must check workflow status as
well as workflow type before returning `ErrSentinelBlocked`.

### Safety boundary for the forward handoff

The forward response-loss, collision, and snapshot-signal failures share one ownership protocol
gap. A scheduler-only create token or final channel drain cannot safely close it. History can
deliver a signal into an SDK channel while the migration local activity is pending, after the
snapshot has been staged; successful workflow completion may then leave that accepted signal
unread. Repeating a drain merely moves the same race to the next yielding finalize call.

A safe bounded cutover needs a durable History ingress fence, replicated across failover, which
atomically stops V1 signal acceptance for a source incarnation and records the accepted-prefix
watermark. A matching inactive destination snapshot can then be activated only after that prefix
is represented. Without that fence, the alternatives are an indefinitely retained V1 forwarder
with an unbounded outbox under sustained traffic, or silent loss. Neither is a safe production
fix. The counterexamples remain as opt-in evidence and migration should not be activated until
the cross-service protocol is designed and deployed to every possible History host.

### High: CHASM rollback skips the inclusive first action of a fresh backfill

PR status: covered by open PR [#11878](https://github.com/temporalio/temporal/pull/11878).

`TestMigrationCounterexample_RollbackFreshBackfill` rolls back a fresh CHASM backfill whose start
and end equal a scheduled instant. CHASM starts fresh ranges one millisecond before the requested
start at `chasm/lib/scheduler/backfiller_tasks.go:181-188`. The rollback conversion leaves a fresh
request unchanged at `migration.go:551-557`, while V1 consumes `StartTime` as an exclusive cursor
at `service/worker/scheduler/workflow.go:950-966`. The action at the requested start is skipped.

Impact: the first requested backfill action can be lost. Convert a fresh CHASM request to V1's
inclusive representation by moving the V1 cursor one millisecond before the requested start;
continue using the exact watermark for progressed backfills.

### Medium: migration truncates multi-value last completion results

PR status: already analyzed by open PR
[#11880](https://github.com/temporalio/temporal/pull/11880), which explicitly standardizes the
current first-payload-only behavior instead of preserving the full V1 result.

`TestMigrationCounterexample_MultipleCompletionPayloads` proves that a two-payload V1 completion
result returns with only its first payload. `convertLastCompletionLegacyToCHASM` selects
`Payloads[0]` at `migration.go:452-465`, while CHASM's state has one `Payload` field.

Impact: a following scheduled workflow receives a changed last completion result. Preserve the
complete `Payloads` message, either with a schema field capable of representing all payloads or a
lossless envelope understood by both implementations.

### Medium: recent-action schedule times change across a round trip

PR status: covered by open PR [#11880](https://github.com/temporalio/temporal/pull/11880).

`TestMigrationCounterexample_RecentActionTimes` covers completed and running actions. Completed
actions return with `ScheduleTime` replaced by `ActualTime` because rollback reads
`start.ActualTime` at `migration.go:510-518`. Running actions lose both original timestamps and
return with migration time because `convertRunningWorkflowsToBufferedStarts` creates timestamps
from `migrationTime` and discards the matching recent-action record.

Impact: DescribeSchedule reports inaccurate action history and delay information. Map
`NominalTime` back to `ScheduleTime`; for a running workflow, merge the matching recent-action
timestamps instead of replacing them with migration time.

## Reproduction

All commands run from the repository root.

Ordinary generated checks and failure-boundary controls pass:

```sh
go test -tags test_dep ./chasm/lib/scheduler/migration ./chasm/lib/scheduler ./service/worker/scheduler -run 'TestMigration|FuzzMigration' -count=1
```

The deterministic counterexamples fail as evidence of current behavior:

```sh
TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 go test -tags test_dep ./chasm/lib/scheduler/migration ./chasm/lib/scheduler ./service/worker/scheduler -run 'TestMigrationCounterexample' -count=1
```

The generated conversion test records its seed in each subtest (`seed_0` through `seed_63`). The
fuzz target can explore additional states; a three-second run completed without finding a failure
beyond the named exclusions:

```sh
go test -tags test_dep ./chasm/lib/scheduler/migration -run '^$' -fuzz '^FuzzMigrationRoundTrip$' -fuzztime=3s
```

The passing controls cover normal migration, a trigger processed before the snapshot, failure
before destination commit, native inclusive backfill behavior, native exclusive watermark
behavior, repeated rollback tasks, and failures before and after destination creation and source
closure. Generated round trips cover every overlap policy, pending and running actions, pause and
limited-action state, counters, backfill progress, last result/failure, memo, custom search
attributes, and stable request/workflow identities.

## Rollout and rollback checklist

- Do not broadly enable migration until the critical handoff and collision tests pass.
- Activate workflow version 13 or newer before relying on migrated pending-action IDs; the current
  default remains version 12. This does not fix ambiguous CHASM create responses by itself.
- Before ramping up, verify both directions reject an existing destination with a different
  handoff ID and reconcile a matching destination after a lost response.
- Keep migration disabled for schedules with active backfills until both cursor tests pass.
- During rollout, alert on migration failures, duplicate action workflow IDs, both backends being
  live for one schedule ID, and accepted operations absent from both backends.
- Before rollback, disable new V1 to CHASM migration at the activity guard, allow pending attempts
  to quiesce, and audit ambiguous attempts. Do not assume disabling the flag repairs a destination
  that already committed.
- Account for the 15-minute CHASM sentinel lifetime before reversing direction or reusing a
  schedule ID. Verify the destination owner and handoff ID after the sentinel clears.
- Avoid rolling back schedules with pending triggers, fresh backfills, or multiple completion
  payloads until their conversion defects are fixed.

## Coverage limits

The unit harnesses deterministically model committed writes with lost responses and failures on
both sides of each handoff, but they do not reproduce persistence-process crashes, replication
lag, namespace failover, multi-cluster version skew, or real frontend routing races. They also do
not measure behavior at 10x load, backpressure, or recovery time after a worker fleet restart.
Those require integration or fault-injection environments after the unit-level defects are fixed.
