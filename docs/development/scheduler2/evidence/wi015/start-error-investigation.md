# WI-015 start rejection versus unknown prior acceptance

Status: source-backed design investigation; no source edits. D59's start-error table
needs an explicit qualification: a permanent error is terminal only when nonacceptance
of the operation is established, not merely because the latest attempt was rejected.

## Concrete source evidence

- `scheduler2/tasks.go:StartHandler.Execute` reads a snapshot then calls the start
  function outside the transaction; errors all become RetryableFailure. There is no
  durable pre-RPC issuance claim or attempt identifier in OperationState/decision.Operation.
  Crash after remote acceptance but before the result transaction leaves Pending.
- `common/resource/fx.go:FrontendClientProvider` (292–299) wraps the actual frontend
  client with `frontend.NewRetryableClient`. Generated frontend StartWorkflowExecution
  (retryable_client_gen.go:1529) uses ThrottleRetryContext. One invocation observed by
  scheduler2 can therefore contain multiple remote attempts. The final nonretryable
  error does not expose earlier transient outcomes.
- The frontend calls `wh.historyClient.StartWorkflowExecution` after its validation
  (`workflow_handler.go:569–605`). That History client also has a retry wrapper
  (`resource/fx.go:HistoryClientProvider`), and its generated Start method retries.
  Merely removing the scheduler-side retry wrapper does not remove hidden downstream
  retries. Do not infer absence of acceptance from a single scheduler-level invocation.
- Frontend prepareStartWorkflowRequest and History Starter.prepare perform validation
  before their respective start operations. Later config/authorization/namespace state
  can reject a retry before reaching persistence/dedup. Their error is evidence about
  that invocation, not every earlier invocation for the same operation.
- History `startworkflow/api.go:handleConflict` first looks up the request ID in the
  current execution's persisted RequestIDs. A match returns StartDeduped with the actual
  FirstExecutionRunId. This is positive ownership evidence, not a negative certificate.
- `EnableCrossRunRequestIDDedup` defaults **false** (`dynamicconfig/constants.go:3412`).
  When enabled and allowed, Starter.newRunID derives a run ID from namespace, workflow,
  archetype, request and cluster IDs. runIDDedupResponse can recover that earlier run
  even when no longer current. Neither mode guarantees that pre-persistence validation
  can never reject a retry. Do not assume the optional setting is enabled or alter a
  global solely to make tests pass.
- `workflow_id_dedup.go:generateWorkflowAlreadyStartedError` returns the conflicting
  execution's create request ID, run ID and canonical first run ID (possibly empty for
  legacy records). A different request ID proves that this conflict is not an ownership
  acknowledgment. It does **not** prove that our earlier run never existed. With
  cross-run dedup disabled, an accepted run can finish, another run become current,
  and a retry observe that unrelated conflict.
- Old scheduler invoker drops nontransient errors and AlreadyStarted from its buffer
  (`invoker_tasks.go:400–444`, `isRetryableError`). That behavior is not a sufficient
  implementation of scheduler2's accepted no-lost-accepted-work invariant.

## Counterexample the model must retain

1. Admit A with request ID R; keep its overlap authority.
2. Send R. History durably accepts workflow W; reply is lost (or scheduler crashes).
3. Retry R after authorization/config changes, or after a different workflow becomes
   current. Observe PermissionDenied, InvalidArgument or unrelated AlreadyStarted.
4. Treat this error as a terminal rejection and release authority.
5. Admit overlapping B while W remains live, or lose W's completion/carryover tracking.

Steps 2–3 can occur inside one wrapped client invocation. A boolean `attempted` set
before that invocation is necessary crash evidence but is not enough to certify that
its final error means no acceptance.

## Safe transition contract

Maintain three independent facts: an operation's immutable identity; whether any
issuance may have been accepted; and positively known chain identity. Never clear
possible acceptance because a later call failed. A callback or successful deduplicated
start response establishes actual first-run identity and can resolve ambiguity.

Recommended dependency order:

1. **Pre-issuance validation/rejection.** Construct the complete frozen request and
   callback and run available deterministic/configured validation before any issuance
   claim. A proven local invalid request can enter a distinct Rejected terminal phase
   only while no issuance has ever been claimed. Do not invoke the RPC then relabel its
   ordinary error as local validation. Public request validation remains authoritative;
   a preflight success is not a promise that later config cannot reject execution.
2. **Durable issuance claim.** In one CHASM transaction, verify exact pending operation,
   deadline, retry eligibility and logical wake; allocate a monotonically increasing
   attempt token and record that the operation may be accepted. Commit before RPC.
   Return the frozen request and token only to that successful claimant. No RPC after
   a failed publication. The in-process permit is not a durable claim.
3. **Token-fenced observation.** Acknowledgment/callback may establish chain and terminal
   outcome using existing operation/incarnation guards. A rejection observation must
   carry its claim token and an explicit nonacceptance provenance class; stale results
   cannot retire newer work. Unknown/timeouts/cancellation or ordinary final service
   errors retain possible acceptance. Stop futile issuing on permanent errors if
   desired, but keep Ambiguous authority and permit later callback resolution.
4. **Provably rejected remote attempts require a real certificate.** A certificate must
   cover *all* wire attempts hidden inside the invocation, state that none accepted,
   and be tied to R and the durable claim. This needs either source-boundary provenance
   collected through every retry layer, or a dedicated internal server result emitted
   before forwarding/mutation plus retry aggregation that preserves earlier unknowns.
   An arbitrary gRPC status, error message, different AlreadyStarted.RequestId, or a
   successful Describe of the current run is not such a certificate.
5. **Terminal rejection eligibility.** Retire/unblock only when no prior unknown claim
   exists and every claimed invocation has a certified nonacceptance outcome. The
   implementation may keep a conservative monotonic `acceptance_unknown` bit instead
   of storing all attempts: once any invocation is unknown it cannot be cleared by
   rejection. Crash after claim is unknown even if the RPC might never have been sent.
   Retain that conservative case rather than inventing a crash-safe send atomicity.

The integrator resolves D59 as follows: common validation rejects before publishing or
issuing; after an actual possibly retried RPC, an ordinary permanent status without
proof remains ambiguous. Existing bounded retries may continue, then authority remains
retained under the accepted M2 contract. No terminal-drop or attempt-marker shortcut is
authorized or required. The durable-claim discussion above is a possible future protocol
for certified remote rejection, **not** a prerequisite to preserve the current safe
all-errors-unknown behavior or an approved source change.

This is an explicit safety refinement of D59's original error table. The source does not
currently provide a no-acceptance certificate through ordinary Start response/error.
No user decision is needed to weaken an already accepted invariant.

For local-only test transport, observing raw attempts can validate the proposed model,
but an injected test assertion is not production evidence of nonacceptance. Do not
silently turn off client retries globally or bypass the frontend API validation path.
A History lookup returning NotFound is also insufficient while a prior in-flight request
can still commit; timeout does not revoke server execution.

## Required invariants and witnesses

- Rejected implies no accepted start can exist for R under the declared transport
  assumptions; it has no fabricated ChainID and does not increment successful starts.
- Unknown acceptance never releases blocking authority merely due to error class,
  deadline expiry, newer definition, or stale claim result. Existing policy-specific
  ALLOW_ALL admission behavior remains distinct from retirement accounting.
- Every RPC has a committed matching claim. No failed claim publication can issue.
- A callback arriving before claim acknowledgment remains accepted; late failure cannot
  overwrite its completion or count success twice.
- Model/test: first unsent validation rejection; first certified remote rejection;
  accepted/lost response then permission rejection; hidden retry accepted then permanent
  error; unrelated AlreadyStarted after unknown; claim commit then crash before send;
  RPC accepted then crash before result; stale result from an older token; callback
  before rejection observation; expired unknown; failed claim persistence; positive
  dedup with actual first-run identity across continue-as-new.

No test command was run for this source investigation. The current ordinary error
interface is the concrete design gap; this is resolvable by an internal implementation
contract, not a request for user permission or a reason to imitate unsafe drop behavior.


## Named old/new discrepancy and conditional progress

**D59-E1: permanent status after uncertain issuance.** Old invoker marks a start failed
and drops it after a nontransient error. Scheduler2 retains ambiguous ownership when no
no-acceptance proof exists, including PermissionDenied/InvalidArgument/AlreadyStarted.
Reproduce with a committed workflow start whose response is faulted transiently, followed
by a retry rejected before dedup (permission/config change or injected boundary error).
The new state must not become terminal-rejected or release overlap authority; an actual
later callback must still resolve it. Run the same sequence with a never-issued invalid
request and verify common validation rejects atomically without any action publication.
Do not assert that the old implementation is safe just because its error classification
is source-compatible.

**Conditional progress:** an unknown start resolves only if a later trustworthy positive
observation arrives (same-request dedup acknowledgment with canonical chain, or a matching
completion callback), or a future protocol proves nonacceptance. If the workflow never
existed and all replies are forever ambiguous, safe automatic retirement is impossible
under this transport contract. Bounded retries bound issuance work, not the duration of
ambiguous overlap authority. Operators/configuration recovery may restore delivery, but
this review does not introduce an unsafe administrative release or promise unconditional
liveness. A crashed-before-send claim illustrates the indistinguishable case even with
perfect persisted markers.

## Existing APIs considered as negative evidence

Start success/dedup establishes ownership positively. AlreadyStarted reports an existing
conflict and possibly its creating RequestId; it has no guarantee about every earlier
attempt of ours. DescribeWorkflowExecution/GetMutableState concern an observed workflow
execution; neither cancels or fences an older in-flight Start nor certifies that R can
never commit. History/event reads can establish an actual started request positively;
absence from a current run/history page is not an atomic exclusion of pending issuance.
Cross-run dedup improves positive recovery only and is optional/default-off. Cancel or
terminate NotFound similarly does not invalidate a concurrent pending start. There is
no decisive no-acceptance certificate in the inspected Start/dedup/Describe/History
interfaces. Adding a durable no-start fence would be a new protocol, not an inference
from existing status codes.
