# Bounded immediate coordinator progress (WI015 investigation)

Status: implemented bounded runtime correction, focused unit/mutation checks pass; all18 exact functional contract ports now pass; independent acceptance pending.

## Causal evidence

The exact newly parameterized contracts retain original timing assertions. Three failed: weekly SKIP had only3 starts within10s; weekly SKIP with child execution timeout reached4 but fast-forward had not completed within10s; BUFFER_ALL backfill heading toward idle had fewer than10 historical starts within30s.

The archived `evidence/wi015/contract-ports/scheduler2-wi015-contract01.log` records:

- Weekly SKIP first workflow-side execution markers at wall19:42:03.236,19:42:05.245,19:42:08.259, for three distinct consecutive weekly nominals. The fourth nominal never starts before the test's deadline.
- Weekly execution-timeout variant starts at19:42:05.229,07.241,09.252,11.262. The fourth arrives near the assertion deadline, while the callback/next-ready transition needed to reach fast-forward completion is still pending. This does not show an incorrect fast-forward target.
- Idle backfill first historical workflow starts at19:42:19.266, then successive nominals at23.275,27.280,31.285,35.287,39.289,43.294: essentially four wall seconds per successful historical action. The automatic predecessor is a separate eighth execution marker. The expected ten historical ticks cannot finish within the original30s assertion with this pipeline latency.
- Root pure-task logs include repeated `New timer generated is less than min scheduled time` and subsequent task timestamp assignment about one second beyond the current wall timestamp. Weekly root examples are19:42:02.231 -> physical01:42:03.230Z and19:42:07.251 -> physical01:42:08.251Z.

`chasm/tree.go:AddTask` routes only zero/Immediate scheduled pure tasks to `immediatePureTasks`; other tasks enter persisted scheduling. `executeImmediatePureTasks` drains these tasks before transaction close. Legacy scheduler components use this mechanism for immediate generator/invoker/backfiller work. Scheduler2's `reconcileCoordinator` always publishes its concrete `wake`, even when equal to execution now; each bounded scan and each candidate handoff therefore pays another physical timer turn. Framework time skipping also refuses to skip past any pending logical timer at/before now (`defaultFindNextTargetTime`). The correct task classification matters to both real-time backfill throughput and virtual-time completion.

## Bounded runtime pump

Keep every current scan/disposition/handoff quantum and resource arbitration decision. Enable a maximum of two valid coordinator quanta per public runtime transaction: enough for a scan/candidate-handoff pair, while remaining strictly bounded. Do not change global timer precision or queue polling. Do not issue external workflow RPCs under the execution lock.

Use existing immediate pure-task support. An immediate WorkTask carries an explicit remaining-quanta budget (1 or2). A physical task's default zero payload begins a chain with budget2. Each valid handler consumes one quantum and passes the remaining value through a scoped ContextWithValue to reconciliation. At zero remaining, publish the ordinary physical continuation. The budget travels in immediate task payloads because framework close-transaction execution creates fresh contexts; it must not rely on an unreset transient field on a cached Scheduler object.

Existing token fencing remains authoritative. Physical delivery retains exact persisted deadline matching. An immediate delivery additionally requires a valid bounded budget, current token and logical deadline <=execution now; zero ScheduledTime is the framework's execution mechanism, not a replacement logical deadline. Any duplicated old immediate or physical envelope becomes stale after a successful quantum. If repeated business calls in one transaction enqueue identical immediate work, only the current token may execute. Each successful quantum still updates resource-turn counters and runs existing idle/token preflight.

The proposed activation is explicit runtime library context configuration in NewPublicExecutionTestFrontend. Offline/internal finite-model fixtures preserve their single-quantum physical profile, with no claim those models prove atomic composition of the new public pump. A dedicated real-CHASM tree test must enable the runtime capability and establish the budget, stale-envelope rejection, rollback and successor publication rules. The same exact failing functional contracts must then pass without timing/expectation changes; existing shared and time-skipping checks must remain green.


## Runtime binding and transaction refinement

`NewLibrary` binds the runtime maximum2-quanta capability; `NewNilLibrary` (offline/no-starter registration) binds0. All actual NewPublicExecutionTestFrontend executions use NewLibrary, so there is no per-test public API fallback. Internal `PublicRevision=0` finite-model/harness executions retain the one-quantum profile, and these models are not claimed to prove the new atomic grouping. The new real-CHASM tree test deliberately registers NewLibrary to exercise the actual close-transaction immediate loop. Immediate WorkTask budgets are1 or2; physical envelopes always retain protobuf-default0.

The pure-task pump preserves a sequential trace of at most two old quanta, with each quantum updating arbitration/lease/token metadata as before. It changes visibility/rollback granularity: either the whole two-quantum transaction commits or neither does. The test rejects an overlay that removes decrementing, since the entire finite range then disappears in one transaction. Existing enumeration/refinement proofs still concern candidate/disposition transitions, not this atomic grouping.

## Due side-effect task classification

The first pump-only three-case rerun passed idle backfill and ordinary weekly SKIP, but timeout-weekly still had3/4 starts by10s. Source inspection found newly admitted StartTask at component.go was also scheduled at concrete execution-now, forcing it through the timer queue. Legacy invoker schedules due starts as immediate side effects. The corrected runtime path uses immediate attributes for a start whose logical wake is <=execution now; this routes to the transfer queue, not the in-lock pure loop. The StartTask payload still carries the exact logical WakeTime, StartHandler snapshots still enforce due/deadline, and future retries remain timers. A physical task regression verifies transfer then timer categories and invalidates the old initial logical wake after retry. The pure-pump test observes zero external starter calls under lock.

## Validation checkpoint

All commands run in execution worktree with `CGO_ENABLED=0 GOMAXPROCS=4`:

- `make proto`: PASS.
- `go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestCoordinatorImmediate' -count=1`: PASS0.676s.
- `go test -tags test_dep -p 2 ./chasm/lib/scheduler2/... -count=1`: PASS root16.015s, decision0.317s, occurrence1.514s (pump before due-start classification).
- `go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestCoordinator(Immediate|DueStart)' -count=1`: PASS0.713s after due-start correction and strengthening physical fallback assertions. Serialized WorkTask fallback has current token, exact logical deadline, remaining0, and created physical status; stale/zero/overbudget immediate envelopes do not mutate root state. Failed publication retains prior durable nodes/tasks.
- Read-only Go overlays: budget decrement removal fails bounded-range assertion; transfer classification removal fails actual physical task category assertion. Both named expected failures, no tracked-source writes, no shrink claim.
- Unchanged three previously failing cases: pump-only run contract01 FAIL17.424s with2 passing, timeout-weekly remaining failure.
- Unchanged all18 cases after due-transfer correction: contract02 FAIL35.123s, with all original three failures fixed. Only ALLOW_ALL later-patch now failed0/4.

The newly exposed ALLOW_ALL condition was source-resolved independently: immediate paused Create preloads a future pending candidate; later patch arms generator WakeAt=now, while automaticDemand correctly selects future pending.Actual. The old runtime gate looked only at stale WakeAt and refused skipping forever. oracle_review owns the targeted gate correction allowing only a future nonexcluded pending candidate in this situation, preserving absent/excluded/due blocking. Its independent unit check passed; unchanged full18 contract03 rerun PASSed34.673s. Command: `go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2Contract' -count=1 -timeout=5m`. All previous failures and the newly exposed later-patch case pass.

Raw logs, overlay sources/manifests and frozen pump source fingerprints are in `evidence/wi015/coordinator-progress/`. This document records the implementation boundary and test results; it does not accept WI015/M3 or authorize production routing.
