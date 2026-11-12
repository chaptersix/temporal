# D59 — Public scheduler2 execution contract

Status: specified by integrator from source-backed investigation. Local implementation
is authorized under the active M2–M4 goal. Companion work item: WI-015. D58/WI-014 remains
the accepted paused persistence baseline. This draft orders the next implementation;
it does not narrow M3's required full public execution/shared-suite scope.

## Sources and retained decisions

Read with `evidence/wi014/next-execution-investigation.md`, `atomic-update.md`,
`current-mappings.md`, D58 and the current plan. Existing M2 contracts retain immutable
admitted requests, execution-incarnation and operation identity, chain-fenced overlap
effects, bounded transaction work, coordinator arbitration, exact logical wake stamps,
and failed-publication rollback. C7's ChainID is first execution run ID, not latest run.
The local callback assumption must now be discharged through actual History transport.

Source anchors: old scheduler `invoker_tasks.go:startWorkflow`, `isRetryableError`,
`cancelWorkflow`, `terminateWorkflow`; `scheduler.go:Update`, `Patch`, `jitterSeed`,
`identity`, `useScheduledAction`, `validateConflictToken`; `completion.go`;
`chasm/nexus_completion.go`; `service/history/handler.go:CompleteNexusOperationChasm`;
new scheduler2 `generator.go`, `component.go:freezeAdmission`, `tasks.go`,
`effects_tasks.go`, `completion.go`, and decision reducers.

## request template and real runtime binding

Use actual workflow frontend StartWorkflowExecution, matching the old scheduler's
validation/service path, and actual History RequestCancelWorkflowExecution and
TerminateWorkflowExecution with namespace-ID wrappers. Bind clients through dedicated
cluster construction before service start. Register the live scheduler2 library in
History and offline scheduler2 identities in frontend. Keep normal/default routing,
auth interceptors, namespace validation and selected no-fallback behavior unchanged.

Build the template from the validated, detached public StartWorkflow action: header,
input, memo, user metadata, priority, task queue/type, retry policy and all timeouts;
versioning override follows the existing configured compatibility gate. Preserve user
search attributes plus the old scheduler's generated schedule identity/nominal-time
attributes (`startWorkflowSearchAttributes`). Use namespace name for public requests,
namespace ID for History wrappers, seed `namespaceID + "-" + scheduleID`, and identity
`temporal-scheduler-<namespace>-<scheduleID>`, matching the existing source.

Automatic requests explicitly use REJECT_DUPLICATE; manual and backfill requests use
ALLOW_DUPLICATE. Nominal time suffix and stable operation RequestId remain those of
the admitted operation. Materialize carryover only for policies that track completion;
ALLOW_ALL omits both last result and continued failure. Time-skipping configuration and
propagation must use the existing actual execution clock/configuration interfaces,
including post-RPC observation using captured skipped duration. Request/template and
callback bytes freeze at admission; retries and later definition updates cannot change
an already admitted request.

Creation initializes generator/controls/public DATA atomically, including empty specs
that must still support later trigger/backfill. Configure idle retention from the
namespace setting. Initial patches are part of the same creation transaction. Public
Update atomically replaces future generation/public metadata and optional memo/SA while
preserving admitted requests, accepted operation identity and coordinator authority.

## callback envelope and outcome contract

Generate the callback with Context.Ref to the scheduler2 root and the admitted operation
ID before freezing issuance bytes. For this isolated local profile choose the existing
request-ID envelope format (`GenerateNexusCallback(..., true)`), since the same local
binary owns encode/decode and continue-as-new must retain the operation ID. This is not
a production rollout policy; any future rollout must separately honor fleet support.
Attach callbacks for every policy, including ALLOW_ALL; late ALLOW_ALL completions do
not modify start-only history or carryover.

Scheduler implements chasm.NexusCompletionHandler. The real History callback executor
uses temporal://internal and CompleteNexusOperationChasm; do not bypass that path in
public acceptance evidence. Map RequestId to operation ID, retain the root incarnation
and exact component ref, then apply decision.Callback atomically. Do not add scheduler
current-run fallback: History's existing current-run fallback is limited to workflow
refs, and old scheduler refs must remain fenced after delete/recreate.

Translate the persisted union exactly: success→COMPLETED; failure details distinguish
FAILED, TIMED_OUT, CANCELED and TERMINATED. Invalid/absent failure or unknown outcome
rejects before mutation for an active matched operation. Stale/unmatched/already-terminal
callbacks remain harmless without loading unrelated payload partitions. Any success
clears prior failure; an empty success payload clears prior result for the CHASM contract.
Preserve explicit null payloads. Failure retains prior successful carryover and stores
the new failure. Only FAILED/TIMED_OUT may cause pause-on-failure, never cancel/terminate.
Duplicate delivery, completion-before-ack and completion-after-update use existing
reducer fencing; real retry/continue-as-new verifies first-run identity is preserved.

## start and overlap error behavior

The current StartHandler's all-errors→RetryableFailure mapping is insufficient. Match
the old source classification while retaining the new bounded issuance horizon:

| Observation | Proposed scheduler2 behavior |
| --- | --- |
| Successful Start response | Acknowledge with actual first-run/chain identity and execution-time start observation; count one successful action. |
| Transient service error, context deadline or explicit rate-limit delay | Retry the same frozen request/ID with existing bounded backoff/deadline; no invented success and no second admission. |
| WorkflowExecutionAlreadyStarted without an ownership certificate | Retain ambiguity: do not infer ownership or nonacceptance from an arbitrary error RunID. |
| Permanent status after an actual possibly retried RPC | Retain unresolved authority, bounded retries then Ambiguous; final status cannot certify no prior acceptance. Common pre-issuance validation rejects before mutation/issuance. |
| Issuance/retry horizon exhausted | Terminal expiry according to the accepted decision contract; do not release overlapping authority early when remote acceptance is ambiguous. |
| Cancel/terminate NotFound | Existing EffectNotFound resolution, preserving chain fence and reconciliation semantics. |
| Cancel/terminate other failure | Existing bounded effect retry/ambiguity contract; do not change target to a newer chain. |

A new explicit reducer event/outcome for nonretryable start rejection may be required;
it must not be encoded as a successful acknowledgment or workflow completion. Add a
refinement/correspondence row and regenerate schema if persistence changes. Retry after
a lost successful start response must establish how the actual frontend response
replays the same request ID; an AlreadyStarted response alone is not sufficient evidence
that scheduler2 owns the workflow.

## bounds reconciliation

Public API validators and persistence limits remain authoritative. The prototype's
64KiB serialized template/completion limits, 128KiB effect-request limit and 365d catchup
maximum are not new accepted public restrictions. Preserve bounded *work* per transaction
and payload partitioning; do not equate bounded work with arbitrary payload rejection.

Before enabling general creation, inventory the frontend blob/SA/memo limits and the
actual callback payload limit, then remove or replace the prototype byte ceilings with
limits compatible with accepted public messages plus serialized envelope overhead.
Keep payloads in DATA partitions and out of frequently materialized root/queue state.
Never truncate success/failure or silently drop callbacks exceeding64KiB. Derive catchup
from existing default/minimum semantics without imposing the prototype upper bound;
preflight time arithmetic to avoid overflow and preserve existing supported ranges.

### Bound accounting resolved from current code

| Boundary | Actual enforcement and defaults |
| --- | --- |
| Schedule Create/Update payload | `WorkflowHandler.validateSchedulePayloadSize` checks schedule memo.Size + action input.Size against namespace BlobSizeLimitWarn/Error (512KiB/2MiB). It does not bound the entire serialized schedule/start envelope. |
| Action/namespace search attributes | Existing validators use namespace limits:100 keys,2KiB per value,40KiB total by default (`common/dynamicconfig/constants.go:SearchAttributes*Limit`). Preserve alias/type checks and these configured limits. |
| Real workflow start | Actual frontend validators remain authoritative for action fields, memo, headers, callbacks, metadata and transport. Memo error default2MiB; do not substitute the schedule input+memo check for workflow start validation. |
| Frozen callback | Callback header limit defaults8KiB, URL length1000; callback library total size defaults2MiB and max count2000. The real callback validator measures the constructed callback. Serialized ref and request-ID envelope are base64 encoded; actual generated bytes, not a guessed fixed allowance, determine size. |
| Workflow result/failure before callback | Workflow task command handling checks completion result and failure sizes with workflowSizeChecker and namespace BlobSizeLimit limits. Canceled details and other terminal events follow their existing command/event validation. |
| Internal CHASM callback transport | `callbacks/chasm_invocation.go` decodes the token, constructs CompleteNexusOperationChasmRequest, and calls History directly. Neither getHistoryRequest nor History's handler adds a callback64KiB cap. Failure conversion is performed on the actual outcome. It bypasses Nexus HTTP body/result checks. |
| External Nexus HTTP | Separate path: MaxNexusAPIRequestBodyBytes is2MiB and the success payload is additionally checked against namespace BlobSizeLimitError. This is not an internal scheduler callback limit. |
| CHASM DATA node | `execution_manager.go:makeInternalChasmNodeMap` serializes each node (Cassandra whole blob; SQL metadata/data separately). No per-node64KiB/128KiB ceiling is applied there. Backend/transport constraints still apply. |
| Mutable state | Active update calls ContextImpl.enforceMutableStateSizeCheck; default warn1MiB/error8MiB. Accounting includes CHASM nodes. Exceeding the error limit can force-terminate the execution on a later active update; this is not a harmless request rejection. |
| Transaction setting | system.transactionSizeLimit defaults4MiB; inspected checks in history_manager enforce serialized appended history batches. executionManager stores the setting but the CHASM node serialization path does not apply it as an aggregate CHASM mutation cap. Do not claim a universal4MiB CHASM node/transaction guarantee from this setting's description. |
| Transport | HTTP public request body limit4MiB; internode RPC client max receive128MiB. These are transport-specific, not a new scheduler payload acceptance promise; retain configured/default server RPC enforcement. |

Compatible strategy: remove the prototype whole-template64KiB, completion64KiB and
effect128KiB rejections as public policy. Keep existing configured validators at the
public ingress/actual workflow start boundary, and validate the fully constructed
callback using the real callback validator before a request can be issued. Do not
revalidate already accepted callback payloads against a newly lowered namespace blob
limit: an accepted workflow outcome must not become undeliverable solely because of
configuration drift. Preserve the actual callback payload/failure in DATA and let the
existing authenticated transport/engine validation govern delivery. The reducer's
payload semantics must remain bounded in record count and transaction work, without
using a new fixed byte ceiling. Large-payload tests must exceed64KiB and approach
configured public limits, including a failure retaining a previous successful result.

The public Schedule, generator template, admitted request and carryover currently can
duplicate large payloads. Prevent avoidable multiplication by storing immutable payload
content once where feasible and projecting/assembling the envelope from stable DATA
references; copying mutable wrappers does not require duplicate persisted large input.
Definition replacement must retain old DATA referenced by admitted operations until
those operations retire. Delete historical payload partitions when their retention
obligation ends, preserving required metadata history. Do not move bytes into the root
or bypass8MiB accounting. Test aggregate state under large input + prior result + new
failure + admitted request, and prove failed publication leaves no half-applied state.

**Remaining inherent resource constraint, not an unresolved API policy:** arbitrary
numbers of maximum-size concurrent/retained actions cannot fit finite mutable-state
capacity. Existing scheduler behavior also operates under this capacity. WI-015 must
measure and preserve shared public cases within configured limits and record actual
capacity behavior, avoiding extra rejection caused by avoidable copies. If exact
compatibility needs a different persisted payload representation, implement that
representation and regenerate schema; do not increase globals for tests or quietly
lower public limits. No evidence currently forces a new scheduler-specific byte cap.

### First execution identity and token verification

The public StartWorkflowExecutionResponse carries both RunId and
FirstExecutionRunId; frontend workflow_handler.go forwards both from History. History
startworkflow/api.go preserves canonical first-run identity on request-ID dedup and
repairs old missing metadata from mutable state when possible. generateResponse's
explicit comment says an empty first ID must not be replaced by RunId. Therefore the
new runtime wrapper returns **response.FirstExecutionRunId** as decision ChainID,
not the old scheduler's response.RunId shortcut. If the canonical ID remains empty,
resolve it through source-backed workflow description or retain ambiguous/retry state;
never bind an overlap effect to a guessed latest run. Test both new start (IDs equal)
and dedup after retry/continue-as-new (IDs may differ).

GenerateNexusCallback's envelope serializes NexusOperationCompletion{ComponentRef,
RequestId}; it does not encode workflow chain ID. chasmInvocation.Invoke takes the
operation ID from this envelope and uses callback state request ID only for legacy
bare-ref tokens. Thus operation matching comes from the frozen envelope, while the
chain fence comes from actual start response/verified workflow metadata. The completion
outcome cannot manufacture or substitute first-run identity. Callback-before-start-ack
must therefore retain existing reducer identity ordering without deriving a chain from
the final workflow run or callback link.

## public conflict-token and dedup semantics

Use a dedicated public mutation counter, separate from definition revision. Tokens remain
opaque and include allocated RunID so an old incarnation's token cannot update a recreated
schedule. Initialize the public counter to1. Preserve D58 Create/Describe token agreement.
Wire bytes need not equal old scheduler bytes; matching lifecycle semantics is required.

The old source bumps the token on every successful Update and Patch, on limited automatic
budget consumption (`useScheduledAction`), and when a completion newly pauses the schedule
and updates pause notes. Ordinary completion alone does not bump it. Proposed scheduler2
triggers mirror these logical mutations; when several occur in one atomic request,
clients only require a changed token, not a particular numeric delta. Definition revision
continues to govern generator replacement independently. Counter exhaustion rejects
before writes; never wrap.

Nil conflict token means unconditional update; supplied nonnil bytes must equal the
current token or return FailedPrecondition("mismatched conflict token"). Check inside
the same transaction as update. Existing framework WithRequestID dedup is outside the
mutation body: a retained successful request replay returns success without rechecking
a now-stale token or repeating writes. Failed validation/transaction does not consume the
ID. Maintain request-ID retention limits explicitly; do not promise unlimited replay.

A whole Patch (pause/unpause, trigger, all backfills, time-skipping changes, notes and
UpdateTime) uses one transaction and one dedup key. Do not sequentially call independent
Trigger and Backfill public wrappers using the same request ID. Update's memo/SA behavior
matches old source: nil means preserve, supplied empty map means clear, supplied values
replace. Public definition and compact visibility projection publish together.

## Sequencing and acceptance

Implement in dependency order: apply the source-resolved bound accounting; runtime clients
and frozen callbacks; callback translation and start classification; atomic full public
creation and controls; matching-time reads and live projections; then full shared-suite
execution plus CHASM-specific recovery/time-skipping obligations. Directed vertical tests
may land earlier but cannot redefine M3 acceptance or mark unimplemented shared cases
unsupported. Actual workflow execution, callback transport, chain-fenced effects, public
conflicts/dedup, list/count isolation and same-backend cleanup are required.

Retain source/command/log provenance, negative/no-partial-state tests, independent typed
and database identity checks, actual physical task delivery/reload evidence, applicable
race/lint, and independent review. No pushes, production routing, rollout or migration.

## Integrator specification and first increment

D59 accepts source-backed runtime/client/envelope and first-run fencing decisions,
retaining the full WI-015 endpoint. The first implementation increment wires real
callback generation/receipt and transport with existing paused public creation plus
a directed admission through actual typed state. That directed admission is only
transport evidence; it is not public Trigger or full Create parity. Permanent-start
classification, payload compatibility, public controls and projections remain required.

Callback generation applies only to configured public schedules; internal deterministic
fixtures retain their explicitly supplied transport inputs. The callback is frozen
in the same admission transaction. Completion projects actual outcomes and first-run
identity through existing matched-operation guards before payload validation. Retain
real-transport tests across continue-as-new, lost acknowledgment, and close/recreate.

Removing prototype ceilings requires updating rejection tests/decision mappings and
recording the actual configured limits. Aggregate state capacity remains a measured
M3/M4 concern, including duplicated definition/templates and retained snapshots; do
not claim that raising byte limits alone preserves the public input domain under load.

## D59-E1 — Start errors after hidden retries

The source investigation in evidence/wi015/start-error-investigation.md supersedes
the draft's unsafe permanent-error terminal disposition. Frontend and History client
wrappers may retry within one apparent scheduler call. No examined existing API
provides a no-acceptance certificate covering prior/in-flight attempts. Accepted M2
no-loss authority therefore governs: ordinary post-issuance errors preserve the
immutable intent, bounded retry deadline and eventual Ambiguous state until positive
acknowledgment/callback reconciles it. This is an explained safety difference from
old scheduler drop behavior, not an unbounded retry guarantee. The named actual
StartHandler regression simulates hidden acceptance then final PermissionDenied and
checks retained authority through expiry and eventual callback reconciliation.

## Definition bounds and terminal payload retention checkpoint

Public StartTime/EndTime and historical range acceptance use protobuf validity,
with optional timestamp fields for manual/queue/operation/candidate/continuation
values outside the int64 nanosecond range. These values must round-trip without
wrapping, including workflow identity and search/recent metadata. The public path
preserves the reference enumerator's nominal/calendar horizon through2100; accepted
later ranges can settle without occurrences. Contemporary execution clocks and
retry deadlines remain int64 nanoseconds. The internal finite enumeration profile
retains its original domain; its proofs do not automatically cover the larger public
profile. See timestamp-domain evidence and independent-timestamp-review.md.

The prototype365d catchup maximum is removed. A nonnegative duration remains intact;
adding it to an occurrence saturates only the stored int64 deadline when the true
value exceeds representable time. This preserves eligibility for every representable
execution instant without wrapping a far-future deadline into the past.

Completed and StartOnly operation metadata continues to fence late callbacks, start
results and physical tasks. Their issuance payload is now reclaimable immediately:
Issue never returns a terminal request, and effect publication freezes its own
request while its target is active. Pending, Started and Ambiguous records retain
their immutable issuance bytes. This implements the existing D59 reclamation
requirement. Immutable typed payload owners now share action/result/failure bytes
among public definitions, active admissions and carryover. Aggregate transport/state
capacity retains its measured limits; no arbitrary-concurrency capacity guarantee follows.

### Measured aggregate transport boundary

Actual WI015 physical capacity tests retained near2MiB input/result/failure in
about6.25MB of scheduler state, but the next assembled6243479-byte Start RPC was
rejected by the existing4194304-byte transport limit. Preserve this inherited
boundary without changing globals, truncating accepted carryover or falsely marking
the pending intent successful. Separate positive within-transport and negative
aggregate-overflow tests are required; individual blob limits do not promise
unbounded combined RPC size. See evidence/wi015/physical-capacity/investigation.md.

## Closure-audit bounded-work requirement

The public capacity/timestamp compatibility changes preserve a bounded number of
logical coordinator quanta, but metadata copying/scanning still grows with public
queue/operation cardinality. This does not establish the stronger bounded-work
contract retained above. The independent M3 closure audit records it as unresolved;
it cannot be closed by relabeling it as a performance caveat or by the existing
finite-profile proofs. A source-backed representation/work-budget correction and
verification are required before accepting the applicable gate. M4 measurements
must then report actual costs; they do not substitute for that correctness claim.
