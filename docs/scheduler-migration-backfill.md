# Workflow-to-CHASM backfill migration safety

## Invariant

An ongoing workflow-backed scheduler backfill stores `StartTime` as an exclusive progress
cursor. Migration must resume strictly after that timestamp. The action at the cursor has already
been accounted for and must not be generated again.

## Counterexample

`TestMigrationBackfill_MigratedResumesAfterWatermark` uses a one-minute schedule with an ongoing
backfill whose cursor is `11:59` and end time is `12:00`. Native CHASM continuation from the same
cursor emits only `12:00`. The migrated state currently has no recorded progress, so CHASM treats
the range as fresh, moves its start back one millisecond, and emits both `11:59` and `12:00`.

The ordinary test suite keeps the native control active and skips the known failing migrated case.
Run the counterexample explicitly with:

```sh
TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 go test -tags test_dep ./chasm/lib/scheduler \
  -run '^TestMigrationBackfill_MigratedResumesAfterWatermark$' -count=1
```

## Cause and fix boundary

`convertBackfillsLegacyToCHASM` clones each V1 request but clears the CHASM
`LastProcessedTime`. `BackfillerTaskHandler.processBackfill` interprets that state as a fresh
inclusive range. The conversion must preserve the V1 `StartTime` as the CHASM exclusive cursor,
which is CHASM's recorded-progress marker, while leaving the task attempt at zero and preserving
the request, overlap policy, and buffer-capacity behavior. Setting `Attempt` to one is unsafe:
creation schedules `TaskStamp` one and validation requires the stamp to exceed the attempt.

A timestamp value alone cannot distinguish an unset cursor from a valid Unix-epoch cursor. The
CHASM state therefore records progress explicitly while retaining the nonzero-timestamp inference
for older persisted state.

Migrated backfillers also wait behind the same callback reconciliation barrier as the generator
and invoker. Starting one immediately can process the imported buffer against stale running
workflow entries and incorrectly apply overlap policy before their completion status is refreshed.
The callback task starts each deferred backfiller idempotently after reconciliation.

This is independent of the opposite CHASM-to-workflow fresh-range boundary covered by upstream
PR #11878.

## Operational impact

The defect can start a scheduled action twice after migration. The fix adds no ordinary scheduler
hot-path work: it copies a timestamp and a progress bit while constructing migration state. Retry,
crash recovery, namespace failover, and a 10x increase in migrations do not add new reads or
writes; every retry reconstructs equivalent cursor state. Callback-bearing migrations defer
existing backfill tasks into the already-required reconciliation transition.
