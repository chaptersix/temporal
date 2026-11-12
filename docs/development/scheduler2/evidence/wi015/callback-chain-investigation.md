# WI-015 — Actual first-run identity for early completion

Source investigation only; no schema or implementation changes in this investigation.
Parent owns scheduler2 generation/receipt. This proposes a narrow internal transport
addition needed to discharge the accepted callback-before-start-ack contract.

## Confirmed source path

- `MutableStateImpl.GetNexusCompletion` in service/history/workflow/mutable_state_impl.go
  reads the workflow terminal event and returns nexusrpc.CompleteOperationOptions.
  Links include the **current terminal run**, not canonical first-run identity.
- `GetFirstRunID` in the same file uses ExecutionState.FirstExecutionRunId, then the
  deprecated ExecutionInfo copy, then the WorkflowExecutionStarted event for old
  records. NewMutableStateFromDB already copies legacy ExecutionInfo into ExecutionState;
  workflow-start event application populates both fields. Retry/continue-as-new carry
  the canonical chain identity in this metadata. Current RunId is not a valid fallback.
- The HSM callback executor chooses chasmInvocation for temporal://internal. Its
  getHistoryRequest copies outcome, close time and completion token/request ID only.
  It neither forwards existing Links nor supplies canonical first-run identity.
- `chasm/lib/callback/invocable_internal.go:getHistoryRequest` has the same omission.
  CHASM workflow callback source (`workflow_callbacks.go:GetNexusCompletion`) already
  delegates to the same MutableStateImpl getter, so one workflow source change reaches
  both transports. Workflow-update and activity completion sources are distinct.
- `CompleteNexusOperationChasmRequest` and persistence.ChasmNexusCompletion both currently
  use fields1–7. History handler copies the known fields before invoking the component.
- `nexusrpc.CompleteOperationOptions.applyToHTTPRequest` explicitly assembles HTTP fields;
  an added internal-only Go field will not be serialized externally unless code
  explicitly starts using it. No external API proto change is needed.

The existing token authenticates/routes the target component and correlates RequestId;
it does not prove the source workflow's first run. Neither a final-run Link nor
OperationToken should be reinterpreted as the chain ID. Waiting for Start response
would remove the early-completion guarantee and prevent recovery from a lost response.

## Proposed schema and propagation

Use a narrowly named optional string, not a new generic operation-token meaning:

```go
// Internal workflow completion metadata. Empty for non-workflow sources.
// Never emitted by external Nexus HTTP serialization.
WorkflowFirstExecutionRunID string
```

Add that field to nexusrpc.CompleteOperationOptions. Add the following internal proto
field with explicit comments to BOTH request_response.proto's
CompleteNexusOperationChasmRequest and persistence/chasm.proto's ChasmNexusCompletion:

```proto
// Canonical first execution run ID of the source workflow, when known.
// Internal Temporal workflow callbacks only; absent for generic Nexus/activity
// sources. This is not the source workflow's current/latest run ID.
string workflow_first_execution_run_id = 8;
```

Regenerate affected protos/helpers and any interface-derived code required by normal
repository commands. No public API schema changes, callback-token format changes,
new transport route or external HTTP header is needed.

MutableState.GetNexusCompletion should enrich its successful outcome object with the
canonical first-run metadata from ExecutionState, falling back to ExecutionInfo. Use
one post-conversion assignment or a small source-local helper covering all five terminal
statuses; avoid changing the shared success/failure builders because those builders
also serve workflow-update outcomes. Strings are copied by value and do not alias
mutable payloads. Do not populate this field for workflow-update callbacks merely
because their host happens to be a workflow: their completion contract is different.

Both internal transports assign the field after constructing either success or failure
request, avoiding branch drift. History CompleteNexusOperationChasm copies it into the
component completion object. Parent scheduler2 receiver uses this exact value as
Callback.ChainID after matching operation/root identity, with existing nonempty-chain
validation. This allows actual completion to establish the canonical chain before
start acknowledgment. Later start responses must agree with the same first-run ID;
never overwrite the observed chain with a successor's current RunId.

## Missing old metadata and unchanged external semantics

The new scheduler2 profile starts fresh workflows, whose mutable state has canonical
first-run metadata. Reading the two persisted canonical fields adds no I/O and cannot
introduce new errors for external callbacks. Generic sources simply leave the optional
field empty and existing consumers continue ignoring it.

For workflows dating before canonical metadata existed, GetFirstRunID can consult the
start event. Calling it unconditionally from GetNexusCompletion would add history reads
and possible errors to otherwise valid external HTTP callbacks. Avoid that unrelated
behavior change in this narrow addition. Leave metadata absent when both canonical
fields are absent; do not guess RunId. If support for those old sources is required,
add a targeted internal-only metadata resolver through an optional workflow-source
interface and resolve before the internal RPC, preserving retryable lookup errors.
That extension must not reinterpret arbitrary Nexus links or change external completion
success/failure. No migration of old sources is authorized or required by WI-015's new
local workflow starts. Empty metadata is not sufficient evidence to accept a scheduler2
early completion; the receiver must remain explicit about this unsupported source.

## Ownership proposal and acceptance tests

A dedicated transport/schema owner can own common/nexus/nexusrpc completion options,
the two internal protos and generated output, MutableState.GetNexusCompletion,
HSM/chasm-lib internal request conversion and History's copy. Parent retains scheduler2
callback generation/consumption and public execution tests. Agree this ownership before
parallel edits, especially regenerated historyservice and persistence files.

Required focused tests:

1. Workflow source emits canonical first ID distinct from current run on completion;
   cover ExecutionState authority, legacy ExecutionInfo fallback and absent metadata.
2. Both internal transports preserve it on success and failure; generic activity/Nexus
   options remain empty. Test conversion directly without mocking away the new field.
3. History handler passes the field to the component without deriving it from token
   or links. Existing target-reference/request-ID checks remain intact.
4. External HTTP serialization produces the same headers/body with metadata set or
   empty; there must be no new externally visible workflow identity field.
5. Actual scheduler2 workflow completion before held/lost Start acknowledgment uses
   the field, including a continue-as-new chain whose final RunId differs. Verify
   carryover, pause behavior and successor dispatch; exact old refs remain fenced
   after scheduler close/recreate. A manual reducer callback is insufficient evidence.

This additive internal metadata is independent of whether HSM or CHASM callback storage
is selected. Test both selection paths; implementing only the currently enabled default
would leave a valid runtime configuration unable to satisfy early-completion fencing.

## D59 addendum — implementation authorized and verified

Integrator accepted the additive proposal and assigned transport/schema ownership in
this worktree. Implemented WorkflowFirstExecutionRunID in internal options and
workflow_first_execution_run_id=8 in both internal protobuf messages. MutableState
completion enriches all successful outcome conversions using canonical ExecutionState,
then ExecutionInfo; it adds no history fetch/error to external delivery. Both HSM and
CHASM-library internal converters forward it for success/failure, and History passes
it into ChasmNexusCompletion. Parent owns scheduler2 consumption and real early callback
integration. Optional empty metadata for old/generic sources is preserved; no RunId
fallback or external HTTP wire change was introduced.

Generation command: `CGO_ENABLED=0 make proto` (PASS), log
`/private/tmp/scheduler2-wi015-chain-proto.log`. Only generated diffs are the two intended
api/historyservice and api/persistence protobuf outputs; other generated files remain
unchanged.

Focused validation (PASS, all five packages):

```sh
CGO_ENABLED=0 go test -tags test_dep ./common/nexus/nexusrpc ./service/history/hsm/callbacks ./chasm/lib/callback ./service/history/workflow/workflow_test ./service/history -run 'Test(InternalCompletionWorkflowChain|WorkflowChainMetadataIsNotSerializedToHTTP|GetNexusCompletion|CompleteNexusOperationChasm_PreservesWorkflowChain)$' -count=1
```

Log: `/private/tmp/scheduler2-wi015-chain-focused.log`. Tests cover actual success/failure
conversion in both internal transports, canonical first-run precedence over legacy copy,
legacy fallback and absent metadata across workflow terminal outcomes, History component
forwarding, and equal external HTTP headers/body with metadata present versus absent.
This is focused source/transport-conversion evidence, not yet real callback-before-ack
or continue-as-new integration evidence. Parent owns combined integration/race/lint.
