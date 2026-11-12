# CHASM scheduler lifecycle driver coverage

## Commit provenance

Branch: `test/chasm-scheduler-lifecycle`.
Base: `03ecd1d228d2b8d9d433da55785afc9aeeead918`.
David Porter's [temporalio/temporal#13042](https://github.com/temporalio/temporal/pull/13042)
head `33ffb9e001299e3221e107ce781f832b4a616a93` is imported as one signed commit,
`aa47350a376ce3d993bebc069e862d2362bab8c5`. Its tree exactly matches that source
head. Its message records all twelve original commit IDs and preserves David's
authorship. The following signed commit contains the additions described here.

## Coverage and reference PRs

| Reference | Coverage on this branch |
| --- | --- |
| [#35](https://github.com/chaptersix/temporal/pull/35) | `task_validity_engine_test.go` ports generator, idle, callback-repair and migration validation cases. `schedulertest/task_validity_test.go` distinguishes stale physical deliveries from valid current work, checks retry equality/rearming, and detects an advertised idle deadline with no valid logical task. |
| [#41](https://github.com/chaptersix/temporal/pull/41) | `TestDeferredBufferOneProperty` adapts the occupancy/accounting property to actual handlers. Existing `TestCompletedHistoryDoesNotConsumeBackfillCapacity`, capacity arithmetic tests and duplicate-result tests remain part of the required package run; no experimental planner refactor is imported. |
| [#51](https://github.com/chaptersix/temporal/pull/51), [#52](https://github.com/chaptersix/temporal/pull/52) | Ports 450/451/1000-range finite-drain tests and the positive-capacity admission fix. The test supplies capacity by consuming each batch; it checks unique identities, bounds and termination, not cross-schedule fairness. |
| [#42](https://github.com/chaptersix/temporal/pull/42), [#43](https://github.com/chaptersix/temporal/pull/43) | Native exclusive-cursor control plus migrated cursor/epoch and callback-reconciliation regressions in `migration_backfill_test.go`, with the cursor and reconciliation-ordering fixes. Watermark presence replaces the separate progress bit, so no schema change is imported. |
| [#53](https://github.com/chaptersix/temporal/pull/53), [#55](https://github.com/chaptersix/temporal/pull/55) | Native phase controls, conversion evidence, rollback phase regressions and the rollback production gate. The driver reports execution/retry/cancel/terminate obligations made unexecutable by migration; it no longer exempts every migrating state from its ledger. |
| [#59](https://github.com/chaptersix/temporal/pull/59), [#62](https://github.com/chaptersix/temporal/pull/62) | Controlled acceptance/response events, buffer reordering during an outstanding RPC, repeated result reconciliation, and directed selected-start reservation regressions with a minimal reservation fix. Workflow scenarios are adapted without importing experimental activity APIs or action schemas. |
| [#70](https://github.com/chaptersix/temporal/pull/70) | Duplicate callbacks before/after local start recording for ALLOW_ALL, SKIP and BUFFER_ALL, plus the early ALLOW_ALL callback guard. This branch retains callback attachment; the separate rollout-dependent callback-omission change is not imported. |
| [#44](https://github.com/chaptersix/temporal/pull/44), [#45](https://github.com/chaptersix/temporal/pull/45) | `migration_handoff_test.go` and the V1 SDK workflow scenarios exercise lost responses, source-close boundaries, destination collisions and signal admission. |
| [#56](https://github.com/chaptersix/temporal/pull/56) | `service/frontend/migration_delete_race_test.go` includes the native control and a skipped held-create/delete counterexample. V1 termination is mocked. |
| [#57](https://github.com/chaptersix/temporal/pull/57) | `migration_chain_ownership_test.go` adapts owned-descendant, foreign-chain and expired-first-run ownership checks to this baseline, with the persisted request ID and first-run ownership check. Receipt and start-pending persistence are not imported because they make a pre-commit Unavailable permanent. |

## Driver changes

- Start calls can be accepted externally, followed by a callback, a lost response,
  or successful response delivery. The real handler then records its result.
  Tests run engine operations only while all controlled RPCs are blocked.
- External acceptance has stable run identity and survives response loss. Local
  observations retain recorded identities separately. Assertions catch accepted
  work disappearing, identity mismatches, missing counts and duplicate accounting.
- Side-effect dispatch and progress checks consult `ValidateSideEffectTask`.
  Idle-route checks inspect serialized logical tasks and run the production idle
  validator. They do not infer progress from stale physical envelopes alone.
- Unexpected patch errors fail the property test. Only the documented closed
  schedule rejection is accepted.
- The broad known-bug exemption is removed. Paused completion and catchup expiry
  both exposed a discarded selection leaving the remaining buffer deferred.
  A narrowly scoped rearming fix now reconsiders deferred work when a discard
  leaves no running or in-flight start. Each such immediate pass removes work;
  it cannot repeat forever without progress.
- `spec_processor_properties_test.go` supplies independent arithmetic expectations
  for interval occurrences, capacity accounting, pagination and jitter boundaries.
  It does not establish calendar enumeration or timezone correctness.

## Reproducible evidence

All commands run from the repository root. Results recorded 2026-11-11 with
Go `go1.27.3 darwin/arm64` and Rapid `v1.3.0`.

```sh
CGO_ENABLED=0 go test -tags test_dep ./chasm/chasmtest/... ./chasm/lib/scheduler/... ./service/frontend ./service/worker/scheduler -count=1
CGO_ENABLED=0 make property-test TEST_RACE_FLAG=off
make mutation-test
CGO_ENABLED=0 make lint-code-fast GOLANGCI_LINT_BASE_REV=03ecd1d228d2b8d9d433da55785afc9aeeead918 GOLANGCI_LINT_FIX=false
```

The package tests, 5,000 Rapid cases, all four mutation checks and fast lint pass. Mutations
remove the early callback guard, deferred rearming, positive-capacity admission
and the selected-start reservation independently; each produces assertion
failures in its targeted tests.

`make property-test` is also a dedicated CI job. The unit-test job retries
failures, and each retry draws a new Rapid seed, so a rare violation could be
retried away there. The dedicated job runs more cases with no retries and
uploads the minimized failfiles from `testdata/rapid`.
The original early callback run failed on `accepted-start-lost`, with ActionCount
still zero after external acceptance. The fix preserves exactly one counted start.

The catchup-expiry failure is retained both as
`TestExpiredSelectionReleasesRemainingBuffer`. The Rapid failfile is not
committed: it replays raw draws, which any generator change invalidates, and
`testdata/rapid` is ignored. The explicit trace is: fixed initial clock
2020-01-01 UTC; 10-second interval; end after 31 seconds; BUFFER_ALL; 60-second
catchup; 10-minute idle; 5-minute workflow-duration bound; `step, step, step, pure`.
The seeded replay is:

```sh
CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler/schedulertest -run TestSchedulerProperties -rapid.seed=7487548259739501542 -count=1
```

## Production fixes made for reproduced bugs

Each fix has a default-run regression; none is gated.

- **Backfill cursor replay.** A migrated backfiller carries the V1 exclusive
  cursor as its watermark. Watermark presence, not value, marks recorded
  progress, so an epoch cursor is honoured.
  `TestMigrationBackfill_MigratedResumesAfterWatermark`.
- **Backfill before callback reconciliation.** Migrated backfillers get their
  first task only after `SchedulerCallbacksTask` reconciles imported running
  state. `TestMigrationBackfill_WaitsForCallbackReconciliation`.
- **Fresh backfill rollback boundary.** Rollback shifts an unstarted range back
  1ms to keep its inclusive first point on V1's exclusive cursor.
  `TestMigrationScenario_RollbackFreshBackfill`.
- **Rollback execution phases.** `MigrateToWorkflow` and the migrate task return
  the retryable `ErrMigrationActionsPending` while a selected start is
  unrecorded or cancel/terminate requests are queued, because V1 state cannot
  represent them. `TestRollbackPhaseGate`, `TestRollbackPhaseTransferable`.
- **Rollback destination ownership.** `WorkflowMigrationState.request_id` is
  persisted and sent on every start attempt. An already-started destination is
  accepted only if its start request, or the create request of its chain's first
  run, matches. A foreign chain or a missing first run leaves the source open.
  A migration persisted without an ID is assigned one before its first attempt;
  if an earlier binary already created its destination under a random ID, the
  task fails closed and needs an operator.
  `TestMigrationRollbackDestinationCollision`, `TestMigrationRollbackChainOwnership`.
- **Selected-start reservation.** A selected but unacknowledged start holds the
  overlap slot. Deferred starts are reconsidered when a start records its run or
  is dropped, so a failed start cannot strand the buffer and TERMINATE_OTHER and
  CANCEL_OTHER still act. `TestSelectedStartReservesOverlapSlot`,
  `TestFailedSelectedStartReleasesOverlapSlot`,
  `TestSelectedStartReservationStillTerminatesOther`.
- **Action budget shared across phases.** `InvokerExecuteTask` passed its
  per-task context by value, so the terminate, cancel and start phases each
  spent `MaxActionsPerExecution` separately. It is now passed by pointer. Found
  by the widened property. `TestActionBudgetSharedAcrossPhases`.

## Harness structure

The component-agnostic stepping loop is `chasmtest.TaskRunner`: it finds the
next pure or side-effect task, validates and dispatches it, advances the clock,
and exposes hooks for component-specific behaviour. The engine, the runner and
the driver take `chasmtest.TB`, which `*rapid.T` satisfies, so a harness failure
inside a property shrinks like any other.

`schedulertest` holds the one copy of the scheduler's test scaffolding, used by
both the unit tests in `chasm/lib/scheduler` and the lifecycle driver:

- fixtures and library wiring (`DefaultSchedule`, `DefaultConfig`,
  `NewTestLibrary`);
- `SchedulerEngine`, a `chasmtest.Engine` with typed `Read` and `Update`;
- `StateInvariant`, the stuck and idle-deadline checks, installed on every
  engine through `chasmtest.WithInvariantCheck`. The idle-deadline check runs
  armed idle tasks through the handler's `Validate`.

The `Driver` embeds `SchedulerEngine` and adds the fake frontend and history
services, the runner hooks, the schedule operations, and `Snapshot` for the
invariants that need history or the start ledger. It keeps its own `no-stuck`
rule next to the shared one: the test engine never dispatches the framework
visibility task, which stays in the tree and hides a taskless schedule from the
shared count, while the shared rule has no exemption for paused or backfilling
schedules.

`TestSchedulerProperties` runs as a Rapid state machine (`rt.Repeat`) over
stepping, completion, pause, unpause, trigger, backfill, update, failed starts
and lost start responses, across all six overlap policies. Invariants run in the
after-step hook, so they also see the settled states inside a compound action.
Idle time is drawn; the other tweakables stay at their defaults. Rollback
migration is not a property operation.

## Unresolved counterexamples and limits

Four migration cases remain evidence, not fixes. Each is an unconditional
`t.Skip` naming the missing handoff protocol; remove the skip to see the
assertion fail.

- `TestMigrationCounterexample_TriggerDuringSnapshot`: a signal accepted while
  the migrate activity is pending is dropped with the old snapshot.
- `TestMigrationCounterexample_ForwardDestinationCollision`: the forward
  activity accepts an unrelated existing CHASM schedule as its destination.
- `TestMigrationCounterexample_LostCreateResponse`: after an ambiguous create,
  the V1 source keeps executing transferred work.
- `TestMigrationCounterexample_DeleteDuringCreate`: a create held across
  `DeleteSchedule` commits an active scheduler afterwards.

All four need a handoff protocol that fences source admission and identifies the
migration at the destination; no local production change closes them. The
lost-response case also asserts behaviour at existing workflow versions, so its
fix must be version-gated and its test adjusted. None of these tests establishes
namespace failover safety.

The default driver remains a bounded, single-schedule environment. Request IDs
are unique per generated occurrence. The external ledger retains deduplication
identities for the whole scenario, assuming no retention expiry. Recorded-result
observation uses buffered starts and recent history: keep each ALLOW_ALL batch
within the retained-history window and observe after every task. Large-batch
history eviction needs a separate observer. Long-running capture/memory work
from [#68](https://github.com/chaptersix/temporal/pull/68) and
[#69](https://github.com/chaptersix/temporal/pull/69) remains a scaling follow-up.

Serialized restart, physical timer redelivery, durable commits, and History
admission fencing remain separate integration coverage. No new in-memory helper
is presented as persistence or crash evidence. The source-close fault wrapper
only tests transaction outcome/response ordering in the component engine.

The package tests above and `make property-test` at 5,000 cases also run under
the race detector with no data race reported.
On a macOS host whose default SDK is newer than its linker, the race build fails
with `unknown architecture arm64e.x1`; set `SDKROOT` to the SDK matching the
installed Command Line Tools.
