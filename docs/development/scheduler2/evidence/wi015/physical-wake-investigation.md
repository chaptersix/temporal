# WI-015 — Physical timer timestamps are not logical wake identity

## Failure and source-backed cause

Actual directed transport diagnostic2 retained Started0/Unfinished1 and observed no
frontend Start RPC (`/private/tmp/scheduler2-wi015-transport-diagnostic2.log`). The exact
physical ScheduledTime == PendingStarts nanosecond comparison can reject every valid
start before issuance. This is not merely SQLite microsecond loss:

1. Scheduler2 publishes a logical CHASM task with the exact nanosecond wake; tree.go's
   closeTransactionGeneratePhysicalSideEffectTask initially copies that timestamp.
2. Start/Effect have nonzero schedule time and no destination, so taskCategory selects
   CategoryTimer (tree.go:taskCategory).
3. `service/history/shard/task_key_generator.go:74–96` changes scheduled physical time
   to `(logical + common.ScheduledTaskMinPrecision).Truncate(precision)`. Precision is
   **1ms** (`common/constants.go`). It may then clamp further to the shard's minimum
   scheduled time plus1ms. Even an exactly millisecond-aligned wake changes by1ms.
4. SQLite InsertIntoTimerTasks additionally uses ToSQLiteDateTime (microsecond
   truncation). GetHistoryTasks returns the physical row key; execution_manager.go
   overwrites the deserialized task visibility timestamp with that row-key FireTime.
5. `chasm.Node.ValidateSideEffectTask` verifies component initial transition and logical
   task version/offset membership, deserializes logical task payload, then passes
   `chasmTask.GetVisibilityTime()` as TaskInvocation.ScheduledTime. This is physical
   queue metadata. Transaction-close validation instead passes logical ScheduledTime.

Thus truncating both sides to common precision is insufficient: intentional upward
rounding and shard-cursor clamping are legitimate. Physical delivery may be arbitrarily
later without changing which logical obligation it delivers. Pure task handlers follow
a separate framework path; no framework or pure-task behavior is changed here.

## D59 addendum and scoped correction

Integrator independently confirmed the source and authorized this correction:

- Add exact `int64 wake_time = 2` to scheduler2 StartTask and EffectTask payloads.
- Publish the field in the same transaction as PendingStarts/effect metadata and the
  logical CHASM task. Preserve the exact nanosecond value; do not round logical state.
- Start Validate requires live root, pending operation and payload.WakeTime equal to
  PendingStarts[operation]. Effect Validate requires live root, current blocking target,
  pending effect wake and payload.WakeTime equal to that exact wake.
- Do not compare the physical timestamp for identity. The framework still checks the
  exact component/logical-task version and offset. request/startSnapshot and effectRequest
  still enforce actual ctx.Now against RetryAt and deadline before any RPC.
- No legacy-zero fallback: these prototype task payloads have not been deployed. A zero
  value is merely exact zero and is never treated as a wildcard/default current wake.

Old tests treating a changed physical timestamp as a forged logical wake must instead
mutate the exact payload wake. A changed physical delivery time alone is now correctly
recognized as delayed/clamped delivery, not a new authorization. Same-operation stale
payload wake remains invalid even when its physical time equals the new current wake.
No polling, catch-all validation acceptance, current-run fallback or timestamp precision
change was introduced into the framework.

## Tests and commands

Updated direct Start/Effect fixtures, lifecycle tests and History scheduler2 binding
fixtures to carry the exact payload stamp. New physical_wake_test.go inspects the
published payload, routes a rounded-up and an hour-clamped physical timestamp through
Node.ValidateSideEffectTask, and separately rejects payload wake minus/plus1ns despite
a matching physical timestamp. Existing lifecycle/version/ref and actual pre-RPC clock
checks remain covered by the full scheduler2/History suites.

Generation: `CGO_ENABLED=0 make proto`, PASS; log
`/private/tmp/scheduler2-wi015-wake-proto.log`. Only the intended scheduler2 state.pb.go
was newly changed by this generation (earlier chain metadata generated diffs remain).

Full check command:

```sh
CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2/... ./service/history -run 'Test' -count=1
```

Log: `/private/tmp/scheduler2-wi015-wake-tests.log`; result appended after completion.
The parent owns rerunning the actual public start/callback transport test, which is
required to connect this source/validation correction to physical queue delivery.

Result: scheduler2 root package PASS7.357s, internal/decision PASS0.364s, and complete
service/history package PASS11.749s. The recursive command exits1 because the unchanged
internal/occurrence package fails TestSpecBoundaryProperties (family4, DST sequence;
first window produces5 rather than8 expected candidates). Full replay is in the log.
No occurrence source was changed by this correction; this failure is reported to the
integrator for separate investigation and prevents claiming a clean recursive suite.
The new physical-wake tests and existing History scheduler2 binding tests passed.
