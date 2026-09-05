# CHASM-to-workflow rollback ownership

## Fix

Rollback now stores the initiating request ID in `WorkflowMigrationState` in the same CHASM
transition that pauses the source. Every destination `StartWorkflowExecution` attempt reuses that
ID. A `WorkflowExecutionAlreadyStarted` response is accepted only when its recorded start request
ID matches; otherwise the task reports a conflict and leaves CHASM paused and migration-pending.

The admin handler generates an ID when an older caller omits one before beginning the CHASM
transition. A task created by an older binary without durable identity fails closed and retains
the source instead of guessing ownership.

This prevents two reproduced failures:

- An unrelated V1 workflow at the destination no longer causes CHASM to close.
- If the destination start commits but the CHASM close fails, retry uses the same start identity.
  A destination that subsequently closes cannot be recreated with a rotated request ID and
  `ALLOW_DUPLICATE`.

The strict check may retain CHASM if the owned V1 workflow has already continued as new and the
current run no longer reports the original start request ID. That is a safe availability failure:
the task does not close the source without ownership proof. A future chain-owner field can permit
that case without weakening the invariant.

## Sentinel release

The rollback preflight now blocks a dummy workflow only while its status is running. Completed and
terminated sentinels have released their reservation and no longer delay rollback until history
retention deletes them. Existing running-sentinel behavior is unchanged.

## Failure and load assessment

- Crash before destination start: the durable request ID is retried.
- Destination commit with lost response: the matching `AlreadyStarted` response reconciles it.
- Source-close failure: retry cannot create another workflow chain with a different ID.
- Foreign collision or missing identity: CHASM remains paused and visible for operator recovery.
- Namespace failover: the request ID is replicated as CHASM state; no process-local cache is used.
- At 10x rollback load, the fix adds no RPC and no ordinary scheduler hot-path work. It stores one
  string per pending rollback and replaces random identity generation with a state read already
  required to export the snapshot.

The forward workflow-to-CHASM defects require the separate History ingress-fence protocol and are
not claimed fixed by this rollback layer.
