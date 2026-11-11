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
| [#42](https://github.com/chaptersix/temporal/pull/42), [#43](https://github.com/chaptersix/temporal/pull/43) | Native exclusive-cursor control plus opt-in migrated cursor/epoch and callback-reconciliation counterexamples in `migration_backfill_test.go`. Existing stamped/legacy task tests remain required. The migration schema/progress-bit fix is not imported. |
| [#53](https://github.com/chaptersix/temporal/pull/53), [#55](https://github.com/chaptersix/temporal/pull/55) | Native phase controls, conversion evidence and opt-in rollback phase counterexamples. The driver reports execution/retry/cancel/terminate obligations made unexecutable by migration; it no longer exempts every migrating state from its ledger. The rollback production gate is not imported. |
| [#59](https://github.com/chaptersix/temporal/pull/59), [#62](https://github.com/chaptersix/temporal/pull/62) | Controlled acceptance/response events, buffer reordering during an outstanding RPC, repeated result reconciliation, and a directed selected-start reservation detector. Workflow scenarios are adapted without importing experimental activity APIs or action schemas. |
| [#70](https://github.com/chaptersix/temporal/pull/70) | Duplicate callbacks before/after local start recording for ALLOW_ALL, SKIP and BUFFER_ALL, plus the early ALLOW_ALL callback guard. This branch retains callback attachment; the separate rollout-dependent callback-omission change is not imported. |
| [#44](https://github.com/chaptersix/temporal/pull/44), [#45](https://github.com/chaptersix/temporal/pull/45) | `migration_handoff_test.go` and the V1 SDK workflow scenarios exercise lost responses, source-close boundaries, destination collisions and signal admission. |
| [#56](https://github.com/chaptersix/temporal/pull/56) | `service/frontend/migration_delete_race_test.go` includes the native control and an opt-in held-create/delete counterexample. V1 termination is mocked. |
| [#57](https://github.com/chaptersix/temporal/pull/57) | `migration_chain_counterexample_test.go` adapts owned-descendant, foreign-chain and expired-first-run ownership checks to this baseline. Receipt persistence and the production ownership protocol are not imported. |

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
CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler/schedulertest -run TestSchedulerProperties -rapid.checks=1000 -count=1
python3 chasm/lib/scheduler/schedulertest/check_mutations.py
CGO_ENABLED=0 make lint-code-fast GOLANGCI_LINT_BASE_REV=03ecd1d228d2b8d9d433da55785afc9aeeead918 GOLANGCI_LINT_FIX=false
```

The package tests, 1,000 Rapid cases, all three mutation checks and fast lint pass. Mutations
remove the early callback guard, deferred rearming, and positive-capacity
admission independently; each produces assertion failures in its targeted tests.
The original early callback run failed on `accepted-start-lost`, with ActionCount
still zero after external acceptance. The fix preserves exactly one counted start.

The catchup-expiry failure is retained both as
`TestExpiredSelectionReleasesRemainingBuffer` and a Rapid failfile under
`schedulertest/testdata/rapid`. The explicit trace is: fixed initial clock
2020-01-01 UTC; 10-second interval; end after 31 seconds; BUFFER_ALL; 60-second
catchup; 10-minute idle; 5-minute workflow-duration bound; `step, step, step, pure`.
The seeded replay is:

```sh
CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler/schedulertest -run TestSchedulerProperties -rapid.seed=7487548259739501542 -count=1
```

## Unresolved counterexamples and limits

The migration cases are evidence, not fixes. Enable their intended correctness
assertions explicitly; this command is expected to fail on this branch:

```sh
TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler ./service/frontend ./service/worker/scheduler -run 'TestMigrationCounterexample|TestMigrationBackfill_(Migrated|Waits)|TestMigrationDeleteCounterexample|TestRollbackPhaseCounterexample' -count=1
```

Observed failures include non-epoch migrated cursor replay, callback-barrier
ordering, rollback execution phases, foreign ownership, source signals during
handoff, ambiguous committed create responses, fresh rollback range boundaries,
delete/create ordering, and descendant ownership verification. The epoch case is
included but does not fail in this fixture; that is not a proof of epoch-state
representation. None of these tests establishes namespace failover safety.

`TestSelectedStartReservationCounterexample` is an always-run detector asserting
the observed two selected BUFFER_ALL starts before the first acknowledgement.
It records the remaining reservation gap; replace that characterization with a
one-slot correctness assertion when the production fix is ported.

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

A race-enabled test build was attempted but blocked by the host SDK/linker:
`Security.tbd: unknown architecture arm64e.x1`. This is not a successful race run.
Normal tests use `CGO_ENABLED=0`; rerun the race check on a working C toolchain.
