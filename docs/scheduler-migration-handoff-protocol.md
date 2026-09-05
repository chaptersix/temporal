# Scheduler migration handoff protocol requirements

## Decision

Workflow-to-CHASM migration must remain disabled until History can durably fence mutation
admission for the source workflow incarnation. Scheduler-only staging, retries, or channel drains
cannot establish a lossless ownership boundary.

This is a safety decision, not an implementation deferral. The deterministic evidence in
`scheduler-migration-defects.md` shows that History can accept a signal and deliver it to an SDK
channel while the migration local activity is pending. The workflow can then complete from the
older snapshot without consuming the signal. Any additional stage or finalize activity yields
again and recreates the same window.

## Required invariants

For one namespace, schedule ID, and source incarnation:

1. At most one implementation may execute schedule actions.
2. Every API operation acknowledged before cutover is represented exactly once at the active
   destination or remains durably pending there.
3. Destination ownership is proven by an immutable migration ID, never inferred from a shared
   schedule ID or an `AlreadyExists` response.
4. A snapshot revision can activate only after History has stopped accepting mutations at the
   source and the snapshot covers History's final accepted-operation watermark.
5. Delete and reverse migration fence older migration IDs so an ambiguous retry cannot resurrect
   an earlier schedule incarnation.

## Required durable state

History must replicate a source fence containing the workflow first-execution identity, migration
ID, admission phase, and final accepted-event watermark. CHASM must persist the same migration ID,
snapshot revision, covered watermark, and staged/active phase. Workflow state must preserve the
migration ID and last acknowledged revision across continue-as-new.

The stage operation is idempotent for an equal ID and revision, replaces only an older revision
for the same inactive ID, and rejects a different ID. Staging starts no generator, invoker,
backfiller, or callback task. Finalize atomically activates only the matching staged revision.
Abort removes only a matching inactive stage and can never deactivate an active scheduler.

## Cutover sequence

1. V1 records a stable migration ID and freezes action execution.
2. V1 stages snapshot revision 1. Ambiguous responses are reconciled by migration ID and revision.
3. History seals mutation admission for that source incarnation and returns the last accepted
   event watermark. Later callers receive a retryable redirect and are never acknowledged by V1.
4. V1 drains through that watermark. If state changed, it stages a higher revision.
5. CHASM finalizes the matching revision and starts scheduler tasks in the same durable transition.
6. V1 records the finalize receipt and completes. A lost response reconciles the active migration
   ID; it never resumes local action execution.

If rollout is disabled before activation, V1 confirms abort of its exact inactive stage before
resuming. If activation has committed, recovery must finish the handoff or perform an explicit
reverse migration.

## Compatibility and deployment

Protocol support must reach every possible active and failover History host before a new scheduler
workflow version can emit fence commands. Older create requests without migration fields retain
legacy behavior for existing histories, but the rollout flag must not select them for safe
cutover. An older host that cannot enforce the ingress fence is a capability-gating failure.

The workflow change requires a new recorded scheduler version and replay fixture. CHASM protocol
support deploys first; History fence support and frontend redirect handling deploy next; only then
may the workflow version and migration rollout advance.

## Failure and load assessment

- A crash after staging replays the same ID and revision.
- A crash after sealing cannot reopen source admission; failover must replicate the fence.
- A lost finalize response reconciles the active ID and keeps V1 frozen.
- A namespace failover with an unconfirmed fence retries instead of activating or resuming.
- At 10x migration load, staging adds migration-only writes proportional to snapshot revisions;
  ordinary scheduler execution gains no reads or writes.
- Dirty-snapshot rounds are bounded because sealing stops new V1 acceptance. Without sealing,
  an input rate at or above forwarding capacity creates an unbounded outbox.

## Rejected partial fixes

- Treating every `AlreadyExists` as success does not prove ownership or snapshot freshness.
- Reusing a request ID prevents some duplicate creates but does not preserve accepted operations.
- Draining SDK channels before or after an activity moves the race to the next yield.
- A timeout or fixed grace period cannot prove that no stale frontend request remains in flight.
- Activating CHASM before source retirement permits duplicate action execution after response loss.
- Retaining a forwarder without an ingress fence has no finite safe retirement point and is
  unbounded under sustained traffic.
