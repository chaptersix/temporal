# WI-010 lifecycle contract and implementation sequence

Status: terminal fence, activity anchor and idle scheduling implemented and
independently source-reviewed. Verification and independent acceptance are recorded in E59–E66, including
expanded History recreation and definite delete-rejection coverage.
Local-only; no migration, production routing or rollout.

## D51 terminal fence

A persisted root Closed flag ends scheduling authority. Lifecycle reports Completed;
History DeleteExecution separately records TERMINATED and queues physical deletion.
Close is idempotent and retains history. New/non-deduplicated controls return
NotFound before body validation; previous successful request IDs can still return
success. Results, request readers and active task validators stop at the root fence,
without loading operation/payload children. Exact execution RunID is preserved.
A snapshot authorized before close may still issue outside the lock; its result
cannot mutate a replacement. Deletion does not terminate child workflows.

## D52 activity evidence

Creation and successful definition/control updates advance a persisted monotonic
activity timestamp. The creation factory must use the engine-allocated RunID and
execution clock. The low-level constructor alone is not an execution allocator.
Store the anchor on root metadata, independent of retained action records.

A successful start handler captures virtual skipped duration during its locked
issuance snapshot, then samples an injectable wall clock immediately after RPC
success and adds that offset, matching the reference formula. Do not substitute
nominal, jittered eligibility, admission, callback delivery or result-merge time.
The starter function can retain its existing chain/error signature; the handler
owns the observation timestamp. Explicit presence and nanosecond-range validation
prevent zero/default values becoming untrusted activity evidence.

A retained operation records the first matching successful-start observation once.
A first acknowledgment may add this metadata after an early completion only when
incarnation/operation/nonempty chain match. It never reopens phase, budget, occupancy,
carryover, pause, accounting or immutable requests. Duplicates cannot extend idle
retention repeatedly. Eviction cannot regress the root anchor. An acknowledgment
for an evicted operation remains ignored under bounded retention.

Completion alone does not supply an observed start timestamp. Preserve the last
known create/update/start anchor until matching start evidence arrives; do not invent
callback-time activity. If the completed record is evicted or the execution closes
first, late start metadata is ignored. This preserves the reference's limited
availability of start-time evidence, while making the terminal metadata exception
explicit. An untracked reference entry can disappear earlier than scheduler2's
bounded retained record; exact retention differences remain documented.

## D53 idle eligibility

Idle duration zero disables automatic close. A positive duration uses the persistent
activity anchor; duplicate reconciliation at the same token/deadline emits no extra
task. Reads/validation never mutate that anchor.

| State | Idle eligibility |
| --- | --- |
| Closed | No new work or idle task. |
| Paused | Held open for explicit resume. |
| Accepted manual/backfill command or queued occurrence | Held open until handoff/disposition. |
| Pending or ambiguous start intent | Held open for its durable retry/result/recovery path. |
| Unsettled bound overlap effect | Held open; an RPC result is not workflow completion. |
| Future automatic source with positive/unlimited budget | Held open while it can generate eligible work. |
| Empty/exhausted source or exhausted budget, without the holds above | Eligible. |
| Started tracked workflow, without other scheduler obligations | Does not alone hold open, matching reference retention behavior. |
| Settled ALLOW intent, completed retained records or diagnostics | Does not hold open. |

The accepted-work holds are an intentional protection of plan S5. The old scheduler
predicate does not explicitly retain queue/unacknowledged/ambiguous/effect work;
copying it alone could revoke a committed obligation. This divergence must receive
independent review and named tests before acceptance. No unconditional idle progress
is claimed for permanently unresolved starts. Started workflows are different: no
unissued start obligation remains, and the reference permits idle close while they
run. A future eligible source or accepted waiter still holds open independently.

Maintain an explicit bounded root summary of unconfirmed starts (Pending/Ambiguous)
so ordinary keyed results need not hydrate unrelated operation records to reconcile
idle. Derive changes from before/after phase and admission; validate summary against
full state in tests. Root effect metadata suffices for effect readiness.

## D54 idle publication and limits

Persist an idle generation token and deadline. A task validates token, exact deadline,
current eligibility and current anchor. Before deadline, return retryable not-due;
at deadline, close only if conditions remain valid. Anchor/configuration changes
atomically publish a successor idle obligation. Failed publication exposes neither
new deadline nor closed state. Guard token/time arithmetic before writes.

Conditional L3 assumes positive duration, eventual stability of eligible state and
anchor, advancing execution time, fair task delivery and eventual successful commit.
Model these assumptions explicitly. Permanent external waits, perpetual updates,
exhausted representability and endless publication failure are outside L3 premises.
Task existence or finite recovery alone is not the liveness proof.

## Sequence and gates

1. Verify/record terminal-fence increment, including actual History allocation and
   exact-run routing with captured persistence (not a physical database proof).
2. Review D52–D54 and resolve source-backed contract details. Implement observation,
   anchor and summary before idle tasks so tests can distinguish each clock/phase.
3. Independently model monotonic anchor/retention, rejected publication, stale timers
   and conditional L3; run real Node timer/restart, clock and update compositions.
4. Extend callback/effect/recreate coverage where exported adapter boundaries exist.
   Keep public frontend wiring and full differential coverage in M3.

WI-010 is accepted locally; full M2 completion remains open under WI-011.


## Review clarifications before anchor/idle implementation

Independent review found D52–D54 coherent with reference evidence and S5, with
these predicate/range clarifications:

- Active, Yield, or an unfinished scan holds open when automatic budget permits.
  An unarmed coordinator/generator alone never proves source exhaustion. Only an
  established Empty/Impossible/Exhausted continuation (or exhausted action budget)
  can remove that source hold. Missing HasGenerator remains uninitialized, not a
  proven empty source; idle is disabled until explicit lifecycle/source setup.
- Unsettled effects are AwaitingTarget, EffectPending and EffectAmbiguous. Accepted,
  Absent and Superseded do not independently hold open.
- Activity control events are initial/successful ConfigureGeneration, revision-
  advancing decision.Update, and newly accepted Trigger/Backfill ingress (matching
  reference Patch UpdateTime). Stale revisions, framework-deduplicated request IDs,
  rejected input and failed publication do not advance the durable anchor.
- Prototype idle duration accepts zero (disabled) or positive values up to365 days.
  Negative/larger durations reject. Anchor/deadline must be exactly representable
  int64 Unix nanoseconds; overflow rejects before state or task publication. Timer
  arithmetic is separate from the occurrence enumerator's2000–2101 source range.

These are routine bounded-prototype decisions, not a production default or a waiver
of M3 public compatibility classification. The terminal metadata exception and
accepted-work holds need independent behavior tests/model mapping before WI-010
can be accepted.

## Current implementation correspondence

This table is a manually reviewed correspondence, not a mechanically checked
refinement relation. The two finite model factors retain the limits described in
model/lifecycle/README.md; composition is checked by real Node tests and saved
traces, with History routing checked separately.

| Implementation transition | Independent specification / observation | Discriminating evidence |
| --- | --- | --- |
| NewExecution initializes allocated incarnation and activity | Creation evidence; anchor factor starts at creation | ActivityExecutionFactory; actual History create/recreate factory |
| ConfigureGeneration, accepted Trigger/Backfill, revision-advancing Update | ControlUpdate advances the ghost maximum; rejected/stale/deduplicated inputs stutter | ActivityNodeProperties; idle update/pause/backfill tests; request dedup tests |
| StartHandler snapshot and post-RPC sample | Trusted clock adapter supplies explicit first observation | ActivityStartObservationUsesSnapshotOffset; timestamp overflow preserves retry |
| reconcileActivity first matching acknowledgment, including terminal retained operation | Acknowledge records first evidence without reopening terminal state | ActivityTerminalObservation; lifecycle_late_observation_v1 replay |
| Duplicate, foreign-chain/incarnation, evicted result | Named evidence stutter | ActivityNodeProperties; idle cap4 eviction test; closed old-run History results |
| Completion before acknowledgment | Completion changes phase/accounting but supplies no start clock | Saved late-observation replay and core metadata tests |
| Terminal eviction at admission | Evict retains independent root maximum | ActivityNodeProperties; IdleAnchorSurvivesRetentionAndClockRollback |
| UnconfirmedStarts phase delta | Concrete adapter implements pending/ambiguous eligibility hold | Keyed summary tests and independent full-state count in activity property |
| Idle reconciliation / hold resolution | Idle factor replaces or removes current authority; hold labels abstract concrete predicate | Real command/queue/backfill/effect/pause transitions; metadata predicate fixtures |
| Idle Validate / Execute / Close | DeliverCurrent requires current token, deadline, eligibility and due time; closure absorbing | Saved stale task replay; deadline minus1/exact/plus1; failed publication tests |
| Close or History Terminate | Terminal authority; distinct from task consumption or external child termination | Closed DATA/result/task fences; History failed delete/retry and recreation checks |

ConfigureIdle does not refresh an existing activity anchor. Its duration change
replaces idle authority when needed; the model represents eligibility/stability
rather than enumerating every duration. Timestamp arithmetic, finite token overflow,
external snapshot issuance and exact RunID routing are adapter obligations covered
by directed tests, not derived from the finite lifecycle factors.

The implementation has no public callback authentication adapter yet. The History
result fixture is test_dep-only and applies the existing internal event interface
through the real engine's exact-run update path. Public authentication, API errors,
visibility and workflow-chain correspondence remain M3 obligations. Likewise,
History handler dispatch with recorded tokens is not physical queue processor
delivery, and its captured persistence manager is not a database durability test.
