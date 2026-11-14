# Start dispatcher concrete integration

Owner: root. Implementation map following dispatch-cleanup-contract.md and the accepted earliest-ready query. This document does not claim implementation or activation. Existing finite tasks remain available for existing records; new public activation requires the complete operation/payload/projection profile.

## Readiness ownership and fixed control

OperationStoreControl gains maintained PendingCount, NextReadySequence and one StartDispatch control. PendingCount counts decision.Wake presence, including an in-flight claimed operation. NextReadySequence is monotone and checked before increment; sequence0 is absent. OperationState gains ReadySequence, which is zero exactly when no issuance wake is owned. It is storage metadata, not part of the finite decision oracle.

A dedicated SequenceIndex under OperationPages orders (effective ready time, sequence). ReadyRecords is a DATA map keyed by the fixed-width sequence, containing operation ID, original logical Wake and effective ReadyAt. The ready record remains owned while claimed, while its index entry is removed. Thus records are bounded by pending operations and each index identity maps to one addressed operation without scanning IDs. Never reuse a sequence after removal.

Normally ReadyAt is decision.Wake. Recovery reinsertion uses current execution time and a new sequence, while retaining the original logical wake for validation. These times must not be conflated: resetting the operation's RetryAt/Deadline would change the accepted retry contract. Positive evidence can settle the operation after recovery; an old negative result cannot rewrite its new readiness.

StartDispatch holds a monotone Token, Armed flag, At and optional Claim. Claim contains OperationId, ReadySequence, LogicalWake and Expiry. The token fences both the side-effect dispatch and pure recovery task; they are mutually exclusive for the slot. Any rearm, claim release or replacement increments the token before publication. New arrivals while a claim is active do not invalidate its token. No per-operation StartTask or root PendingStarts map is produced in the complete keyed profile.

The exact ready index count equals PendingCount minus one when a claim is active. Reject an active claim with PendingCount below1 before subtraction. Compare the scalar to the bounded index aggregate before selecting or mutating. Missing root with a positive expected count is corruption, not an empty queue. Check the selected locator, operation ReadySequence, decision.Wake and index time against each other. The root does not cache an unvalidated minimum that can drift from the addressed index.

## Operation record publication

Extend the existing fixed operationRecordPlan with a detached readiness/index plan. Metadata changes compare old/new decision.Wake. When wake presence/value is unchanged, retain the sequence and existing ready/claimed authority. Entering pending allocates one sequence and locator/index entry. Leaving pending removes that exact locator/index or matching claim, decrements PendingCount once and clears ReadySequence. Changing a pending wake removes the old identity and allocates a new one. An already settled operation has no repeated decrement.

All readiness, retirement and record changes must share one preflight overlay or explicitly composable disjoint-index overlays with no stale root overwrite. Root control is the same detached prospective object, so retirement/count/pending changes cannot overwrite one another. Stage DATA/index changes only after every payload, token, identity and coordinator check; publish records, counts and fixed tasks in the same CHASM transaction. No legacy scan fallback is allowed on the completed keyed profile.

## Claim before exposing RPC bytes

Register one StartDispatch side-effect task and one StartRecovery pure task. The new side-effect handler shares the existing StartHandler permit and RPC configuration, preserving global local issuance serialization across finite and keyed executions. Nil-library registration exposes both task types without enabling execution.

Acquire the existing permit before the claim update. Require the actual physical task context to have a live deadline. Pass that captured deadline into the internal update; do not add a new timeout or margin. Inside the claim transaction, obtain the immutable start snapshot and its existing skipped-duration conversion. Convert the physical deadline to execution time with that same skipped duration and reject if it is no longer later than ctx.Now(s). Computing remaining duration before waiting for the workflow lock and then adding it to a later execution time would incorrectly extend the claim; avoid that pattern. No wall-clock read is needed inside the mutation callback.

Validate the current dispatcher token and due index minimum. If the pending operation has reached its issuance Deadline, apply the existing decision.Expire transition without issuing RPC; Ambiguous retains its existing positive-reconciliation authority and owns no new start wake. Otherwise remove only the selected index membership, persist the claim and its recovery task, and return the detached request snapshot only after UpdateComponent successfully commits. The ready locator remains until release/settlement. A failed claim publication exposes no request for issuance.

Check ctx.Err again before calling the RPC. Cancellation after claim publication leaves the recovery wake; it does not undo the committed claim. Use the same immutable request and canonical first-execution-run acknowledgment as StartHandler. Convert successful wall observation using the issuance snapshot's skipped duration, exactly as today.

## Result and recovery fencing

Merge against the original execution incarnation. Positive acknowledgment and actual completion callback remain valid against operation/chain authority after claim expiry; do not reject them solely because the token changed. Negative results affect retry scheduling only when token, operation ID, ReadySequence and logical wake still match the active claim. A delayed negative result cannot clear another operation's newer claim or reschedule an old operation after positive settlement. Settling an old operation from positive evidence removes only its own readiness; if another operation holds the active claim, its token and expiry remain unchanged.

Recovery validates token and exact persisted Expiry. It clears only the matching claim and either requeues its still-pending operation at current execution time with a fresh sequence, or leaves a settled/expired operation without readiness. If the issuance Deadline has elapsed, apply decision.Expire before dropping readiness; otherwise Pending phase would still own a Wake and violate the count/index invariant. Requeue preserves original RetryAt/Deadline. Reconcile the next bounded index minimum and arm one successor dispatch task; stale physical envelopes cannot authorize issuance. Close revokes the fixed task set and retains DATA for ordinary whole-execution deletion, without scanning pending operations.

Validators check stored token/state and logical scheduled time, following the existing WorkTask physical envelope convention. Clock skips may make recovery due before the RPC returns; late positive evidence still reconciles, late failure is fenced. A rollback keeps future readiness pending and does not fabricate due work. Avoid immediate side-effect task chains: each claim handles one addressed operation and yields through the task queue.

## Effects scope clarification after independent review

Source inspection shows reconcileEffects keeps at most one cancel and one terminate intent for one blocking target, resets on target change, and EffectHandler already serializes through one permit. These per-effect tasks are already a fixed-size set. The earlier two-slot proposal is therefore simplified to one new start dispatcher plus the existing at-most-two effect tasks, with EffectRequests converted to addressed/fixed storage and an explicit two-intent validation boundary. This retains the same four-task upper envelope (start dispatch/recovery plus two effects), existing effect RPC/response-loss behavior and first-chain fencing. Independent index_finish source review confirms the reducer and handler bounds. Root has added metadata/request-membership validation before hot loops and bounded old-intent cleanup. Addressed request storage and fixed framework task-count/churn tests remain gates; this is not permission to retain an unbounded raw map or skip effect recovery tests.

## Required witnesses and order

First implement addressed readiness plan/summary and actual record hooks, then claim/recovery transitions, then side-effect claim/issue/merge and registration. Do not expose the profile before public projections, owner accounting and fixed-layout gates are complete. Compare logical operation results against the finite reducer; claims and recovery without external evidence are named internal stutters, not admissions.

Test failed claim publication/no RPC; due/future ordering and ties; count/index/locator mismatch; context expiration while waiting for permit or lock; crash after claim before RPC; accepted RPC response loss; callback before merge; expiry with late success/failure and another claimed operation; schedule recreation; clock skip/rollback; deadline-to-Ambiguous; sequence/token exhaustion; sustained arrivals with an older ready waiter. Assert unchanged request bytes, exact scalar/index correspondence and fixed logical task counts after each publication/reload. Actual SQLite/physical queue evidence remains required for final M3 acceptance; a tree harness alone does not establish it.
