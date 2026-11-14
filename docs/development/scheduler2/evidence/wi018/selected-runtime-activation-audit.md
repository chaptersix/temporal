# Fresh addressed runtime activation audit

Source audit after projection freeze, 2026-11-13. This is an implementation map,
not completion evidence. No shared source changed by this audit.

## Immediate private integration path

Add a private constructor selector beneath newExecutingPublicSchedule (leave the
public wrapper unchanged until physical integration tests pass). Configure all
selectors together, before storePublicDefinition and before ConfigureGeneration
or InitialPatch can admit work:

- IndexedQueue = newQueueState(), QueueRecords/QueuePages DATA maps (already done).
- PayloadStore version1, BlobRecords/BlobReferences DATA maps, before initial
  definition ownership is installed (existing keyedPayloads constructor seam).
- OperationStore version1, zero counts, NextReadySequence=1,
  StartDispatch=&StartDispatchControl{Token:1}; initialize OperationRecords,
  RequestRecords, OperationPages and ReadyRecords DATA maps.
- ProjectionStore version1, NextActionSequence=1, RunningCount=0; initialize
  ActionRecords, RunningRecords, ProjectionPages and empty PublicRuntime.
- Legacy Operations, Requests, PayloadBlobs, EffectRequests and PendingStarts
  remain empty; CancelRequest/TerminateRequest fixed slots initially unset.

Do not initialize operation/projection controls after ConfigureGeneration or an
initial patch: an admission would already have used the old owner layout.
Remove the unconditional OperationStore rejection in component.applyTransition
only for this complete selector combination. Incomplete operation-store fixtures
may retain explicit rejection. A small validateSelectedRuntime helper should
reject missing selectors, invalid readiness fixed scalars, nonempty PendingStarts
and EffectRequests, in addition to existing map/format validation. It should not
become a scan or duplicate root-index traversal. Full readiness consistency is
already checked by newOperationReadinessEditor before any plan publication.

## Call graph coverage and remaining fixes

- component.stateFor uses addressed operation lookup and checked queue/projection
  summaries. prepareTransitionRecords -> prepareOperationRecords captures request,
  retirement, readiness and dispatch mutations. finalizePublicTransition captures
  projection changes, then blob delta preflight runs before queue and owner writes.
  Root replacement preserves all selected controls. This is the correct common
  admission/ack/callback/update seam; do not add a separate selected reducer.
- Public generation/manual/backfill/coordinator grants converge on this seam.
  Public capacity uses Unfinished plus the fixed CompletedOperations window;
  MaxBuffer<=0 semantics are unchanged. Indexed retention uses exact nominal/ID
  ordering and checks RetirementCount. No retained-operation scan is needed.
- Selected publishTransition omits PendingStarts cloning and legacy StartTask
  publication. Fixed start dispatcher/recovery owns issuance. Existing old
  StartHandler.Validate sees empty PendingStarts and rejects stale old envelopes;
  test this using actual physical invocation, not direct Execute alone.
- Selected effect request slots are chosen by OperationStore.StartDispatch;
  validateEffects bounds metadata to two. Definition/generator/request/carryover
  owner deltas use PayloadStore; collectPayloadBlobs returns before its old scan.
- Projection selected paths have no backlog Actions scan. Describe is explicitly
  output proportional for running workflows. List/visibility remain fixed size.
- IsExecutionSkippable currently checks only queue summary plus old PendingStarts.
  Add selected summary validation and explicitly reject PendingCount>0, a claim,
  or due dispatcher wake. Valid reachable pending work is also covered by
  UnconfirmedStarts, but the selected gate should fail closed on mixed/corrupt
  controls rather than relying on accidental agreement.
- Close's Closed flag already makes both new handlers stale. It leaves selected
  dispatch Armed/Claim behind while clearing legacy PendingStarts. Prefer clone
  and clear Armed/Claim at Close to revoke persisted authority explicitly, without
  enumerating records or releasing historical owners. Test in-flight RPC after
  Close/reload; response remains fenced and owner bytes remain retained.
- Idle uses UnconfirmedStarts and bounded stateFor, not operation cardinality;
  retain this distinction. Add selected claim/pending validation at the common
  state entry so malformed counts cannot accidentally allow idle closure.

## Framework bounded-layout blocker and concrete next slice

Current Scheduler cannot register WithBoundedDataLayout: compileBoundedLayout
first requires MemoVersion for Memo providers, and compileFields rejects each
legacy chasm.Map field even when empty. Declaring root/Generator/Visibility slots
alone is insufficient. The existing library has no bounded descriptor.

Least invasive compatibility design: an explicit descriptor declaration of
schema fields required to remain empty, initially just the four legacy maps.
Compile-time validation must accept only named chasm.Map fields in that list;
root attachment, decode and mutation publication must reject any nonempty map or
persisted child beneath those fields, with no traversal fallback. Testing must
cover an attempted map insert, a persisted legacy child, malformed declaration,
rollback, and finite/default-library behavior. A separate private bounded library
registry can select this descriptor for fresh-profile tests while default finite
fixtures use the old registry. Live and nil/offline registration for a selected
profile must use the same descriptor. A separate root type would avoid this
framework extension but duplicate many typed handlers and parent pointers.

Add a dedicated Scheduler memo revision and MemoVersion hook. Root review found
PublicRevision is insufficient: ack/callback refresh PublicList without advancing
the public conflict revision. Index agent owns the dedicated counter/hook.
Declare fixed Scheduler root, Generator child, Visibility child; current Calendar-
Preparation is not attached to the Scheduler tree. Root task limits must derive
from actual work/idle/recovery/effect/dispatch maxima and coordinator quanta=2;
measure the composed AddTask/close transaction before freezing exact limits.
Neither arbitrary high limits nor a fixture-only bounded root establishes the
actual runtime bound.

## Separate remaining WI-018 definition work

CalendarPreparation explicitly remains unselected and unattached. Generator
newEnumerator still calls occurrence.NewPublic and publicFuture calls legacy
NewCompiledSpec on the definition. Thus activating addressed operations removes
operation/request/payload backlog scans but does not finish the prepared-calendar
integration or establish definition-size-independent transaction work. Preserve
this limitation while executing the real selected operation lifecycle tests now.

## Required next physical tests

Use the private fresh selected constructor in the actual engine harness: admission
through coordinator, physical StartDispatchTask claim commit, RPC success/ambiguous
failure, callback-before-ack, crash/reload after claim, recovery/reclaim, stale task
replay, earlier-ready arrival, retry fairness, permit contention, Close during RPC,
capacity zero/negative and live reduction, and retirement/projection/blob cleanup.
For each rejection compare the persisted semantic image and task publication;
separate CHASM transaction evidence from actual storage commit failures. Once
framework opt-in is implemented, repeat full-transaction work observations on this
same Scheduler profile with increasing operation cardinality and returned output.
