# Workflow-to-CHASM backfill cursor migration

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
`LastProcessedTime` and attempt. `BackfillerTaskHandler.processBackfill` interprets that state as
a fresh inclusive range. The conversion must preserve the V1 `StartTime` as the CHASM exclusive
cursor and mark the backfiller as progressed, while preserving the request, overlap policy, and
buffer-capacity behavior.

This is independent of the opposite CHASM-to-workflow fresh-range boundary covered by upstream
PR #11878.

## Operational impact

The defect can start a scheduled action twice after migration. The fix adds no ordinary scheduler
hot-path work: it only copies a timestamp and initializes an attempt while constructing migration
state. Retry, crash recovery, namespace failover, and a 10x increase in migrations do not add new
reads or writes; every retry reconstructs equivalent cursor state.
