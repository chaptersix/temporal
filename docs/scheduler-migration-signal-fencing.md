# Scheduler migration signal-fencing evidence

## Violated invariant

Every schedule operation acknowledged by V1 before ownership transfer completes must execute or
remain actionable exactly once at the active scheduler. Workflow task completion is not an
admission fence when a signal has already been delivered into an SDK channel.

## Reduced reproduction

`TestMigrationCounterexample_TriggerDuringSnapshot` uses a fixed logical clock and no random
timing:

1. V1 clones snapshot `S0` and starts the migration local activity.
2. The test delivers an immediate-trigger patch while that activity is pending.
3. CHASM commits `S0` and the activity returns success.
4. V1 completes without consuming the patch channel.

The target contains no trigger and V1 executed none. The test harness reports the unread `patch`
signal. The matching control delivers the trigger before migration eligibility; V1 processes it
and the snapshot retains exactly one pending action.

Pause, full update, and backfill use the same acceptance and consumption path. A server-buffered
event can reject a workflow completion as an unhandled command and give V1 another task, but that
positive recovery case does not cover a signal already delivered to the SDK while a local
activity heartbeat keeps the workflow task alive.

## Causal production path

- Frontend tries CHASM first and can route a prior `NotFound` result to V1 while CHASM creation is
  concurrently committing.
- History acknowledges V1 update and patch requests after durable workflow signal acceptance.
- The workflow selector receives signals into pending fields; `processSignals` processes those
  fields only during a loop iteration.
- `executeMigration` snapshots before yielding to its local activity.
- A successful activity causes the workflow to return immediately, without a final accepted-event
  watermark or ingress seal.
- CHASM serves mutations and begins scheduler work immediately after imported execution creation.

Delete has the same incarnation-fencing requirement: a CHASM lookup can miss, V1 deletion can be
acknowledged, and an already in-flight create can commit afterward. A pre-create existence read
does not close this check-then-commit race.

## Why bounded drains are not a fix

Draining before staging leaves a window during the stage RPC. Draining after staging requires a
replacement snapshot RPC, which creates another window. Finalize adds another yielding boundary.
Continue-as-new persists already consumed state but is not a terminal admission fence. Repeating
dirty rounds can reduce probability but cannot establish the invariant or terminate under
sustained accepted traffic.

An indefinitely retained V1 forwarder also lacks a finite safe retirement point. At source input
rate `lambda` and target drain capacity `mu`, its durable outbox grows without bound whenever
`lambda >= mu`, including a 10x migration-load case.

## Required fence

History must seal the exact source incarnation under the same durable workflow lock used to
accept signals and return its final accepted-event watermark. After sealing, later callers receive
a retryable redirect and are not acknowledged by V1. V1 drains through that watermark, stages the
matching revision, and only then permits CHASM activation. The fence, watermark, migration ID,
and destination phase must replicate across namespace failover.

Older History hosts cannot safely ignore this state, so rollout requires fleet capability gating
and a new recorded scheduler workflow version. Until that cross-service mechanism exists, this
defect has no safe scheduler-only production fix.
