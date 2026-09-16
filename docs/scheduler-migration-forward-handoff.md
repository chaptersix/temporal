# Workflow-to-CHASM ownership handoff evidence

## Violated invariant

After destination creation may have committed, V1 must not execute work represented in that
snapshot unless it first proves that its exact destination stage was aborted. An existing CHASM
scheduler is a successful retry only when it records the same migration ID and snapshot revision.

## Deterministic response-loss sequence

`TestMigrationCounterexample_LostCreateResponse` uses one pending manual `ALLOW_ALL` action and a
fixed logical start of `2022-06-01T00:00:00Z`:

1. V1 submits a migration snapshot containing the manual action.
2. The test records the CHASM snapshot as committed and returns `Unavailable`.
3. V1 treats the error as permission to run `processBuffer` and starts the transferred action with
   a newly generated request ID.
4. With migration still enabled, a retry receives `AlreadyExists`, which the activity converts to
   success without checking ownership. With migration disabled, V1 continues independently while
   the committed CHASM scheduler remains present.

The test runs the sequence at workflow versions 12 and 13 with both flag outcomes. Its native
control returns an error before commit and correctly leaves work only at V1. The destination copy
uses the converter's deterministic request identity, while V1 generates a different request ID,
so request deduplication cannot collapse the two manual executions.

## Deterministic collision sequence

`TestMigrationCounterexample_ForwardDestinationCollision` supplies an unrelated existing CHASM
scheduler for the same namespace and schedule ID. `CreateFromMigrationState` reports
`AlreadyExists`; `MigrateScheduleToChasm` discards the distinction and returns success; V1 then
completes although the destination never accepted its buffered action or schedule state.

The current CHASM create request carries only namespace ID and the transferred state. It has no
source incarnation, migration ID, revision, phase, or receipt that could establish ownership.

## Causal production path

- `scheduler.executeMigration` snapshots state before awaiting a local activity with a five-second
  start-to-close timeout and one workflow-level attempt.
- `handler.CreateFromMigrationState` calls `chasm.StartExecution` by namespace and schedule ID.
  The engine documents that returned errors may have ambiguous commit outcomes.
- A migrated scheduler is active immediately and schedules generator or callback work.
- The activity accepts every non-sentinel `AlreadyExists` result.
- After an activity error, the workflow immediately processes backfills and buffered starts.
- After an accepted collision, the workflow immediately returns and retires V1.

## Safety disposition

The source must freeze after its first stage request and reconcile every outcome by immutable
migration identity. That change cannot safely activate by itself: a frozen source still accepts
workflow signals while staging, and finalization has no accepted-prefix fence. The required
cross-service protocol is specified in `scheduler-migration-handoff-protocol.md`; until it is
available, forward migration must remain disabled rather than shipping a partial ownership patch.

This path adds no proposed ordinary scheduler hot-path work. A complete protocol adds only
migration-state writes and identity reads while the migration is pending.
