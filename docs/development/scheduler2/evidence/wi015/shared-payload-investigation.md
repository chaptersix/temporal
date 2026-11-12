# WI-015 shared payload representation investigation

Status: proposal only; implementation requires integrator agreement. No representation
source edits. Current terminal request reclamation is a useful first reduction but
cannot remove multiplication while admissions and prior carryover are still live.

## Framework facts

`chasm/field.go:67` DataPointerTo takes a whole proto.Message. `tree.go:2613`
resolveDeferredPointers resolves that exact message through dataNodePath at transaction
close, then persists its node path. Unlike component pointers, data pointers do not have
the ancestor restriction. The target must be an actual registered DATA node: a nested
Input inside PublicSchedule, or bytes embedded in GenerationDefinition.Request, is not
independently addressable merely by calling DataPointerTo on it. This mechanism does not
establish ownership, reference counting, copy-on-write, or retention. Deleting/replacing
a referenced target path can invalidate/retarget a logical pointer. Stable owner nodes
and explicit lifetime accounting remain necessary.

Current large copies occur in PublicSchedule.Action, Generator.Definition.Request,
each Requests[id].Data, and carryover serialized again into each admitted request.
Effect requests only need namespace/workflow/chain/reason/identity, so they should never
require a full large start payload just to freeze an effect. Admission/request tests
currently decode Requests.Data directly; these assertions must intentionally move to
an assembled immutable-request accessor, retaining exact semantic/byte equivalence.

## Recommended representation: one explicit action bundle and separate completion blobs

Use an explicit ActionPayloadBundle proto with Input (*Payloads), Header (*Header),
Memo (*Memo), UserSearchAttributes (*SearchAttributes), and UserMetadata (*UserMetadata).
Store each immutable canonical bundle once in scheduler-owned DATA keyed by a
type-qualified SHA256 digest. Public action, generator template and request envelope
store a small skeleton plus action_bundle_id. This shares the differently typed public
and workflow-start envelopes without a generic reflection or field registry framework.

Use explicit typed extraction/assembly helpers for NewWorkflowExecutionInfo and
StartWorkflowExecutionRequest. Preserve nil versus allocated-empty wrapper presence in
the bundle and explicit null Payload metadata. Split only those known fields from a
detached clone; retain unknown fields in the skeleton. Small timeout, priority, policy,
versioning, workflow/namespace/identity fields remain in their respective skeletons.

Success Payload and Failure are independently serialized typed blobs with separate
result_blob_id/failure_blob_id references. Do not combine prior success with each new
failure or action bundle, since that recreates avoidable large-value multiplication.

The original public action, generator template and operation request have different
wire envelopes. Each stores its own small skeleton and references the same immutable
field blobs. Generated scheduled-by/time attributes stay in the operation envelope;
user search attributes must be a separate referenced map merged with those reserved
keys on assembly, otherwise each operation's changing nominal timestamp defeats sharing.
Callbacks and stable RequestId remain per-operation frozen envelope bytes. Namespace,
workflow identity, reuse policy, versioning gate result and timeout scalars remain frozen
in their appropriate template/request skeleton; deriving an admitted request from the
current mutable definition is prohibited.

Carryover result and failure need separate blob references, not one combined blob:
failure preserves prior success, so a combined success+failure object would duplicate
large success bytes. A pending operation snapshots result/failure references at admission.
Replacing root carryover changes only current references; old references stay retained
while a live issuance request needs them. ALLOW_ALL stores neither reference.

Use explicit IDs rather than CHASM pointer fields for these references. IDs live inside
small proto records and allow bounded reachability without materializing payload DATA.
The owning map gives every blob a stable path. DataPointerTo would still require the
same ownership/GC protocol and adds node identity concerns without reducing bytes here.
Hash collisions must not cause silent substitution: compare type/serialized content when
reusing an already present digest, or return an internal error on mismatch.

## Lifetime and transaction rules

Reachability roots are current public definition, current generator template, all live
issuance requests, and current carryover result/failure. Retained terminal metadata has
no large payload obligation. Queue/manual/backfill entries refer to the current template
only when admitted, matching the existing contract; they do not pin obsolete templates.
Definition replacement retains blobs referenced by admitted requests from older revisions.

At transaction publication, compute the union of referenced IDs from bounded metadata,
then remove unreachable PayloadBlobs. Current operation capacity is4, with one current
public definition/template and one current result/failure pair; each record has a fixed
three-reference set. Thus work is bounded independently of payload byte length. Avoid unbounded
historical tombstones/refcounts. The resulting maximum number of blob entries must be
an explicit derived invariant, not a newly arbitrary payload byte limit.

Prepare canonical bytes/digests/decoded validation before publishing any fields, then
publish skeletons/references/blobs and GC atomically. Failed publication retains old
references and owner nodes. A callback-before-ack may retire the issuance request while
its detached RPC snapshot is in flight; this is safe because the in-memory request is
already independently cloned. Keep existing terminal/closed metadata checks ahead of
loading any blob. Missing referenced blobs are corruption, not a fallback to current
values or an empty payload.

An assembled read returns a new StartWorkflowExecutionRequest whose complete proto
matches the previously frozen request. Effects read small request skeleton identity
without fetching payload fields. Public Describe assembles a detached public schedule.
Visibility continues to read compact list DATA only. No stored payload enters RootState.

## Minimal implementation sequence and ownership proposal

1. Integrator specifies schema: explicit action-bundle and result/failure references, blob map ownership,
   skeleton representation for public definition/generator/request/carryover, GC invariant.
   Parent owns schema/generation, component fields and admitted request assembly/publication.
2. api_seam can own public definition split/assembly in public.go/public_mutation.go and
   public tests, plus exact Create/Update old-generation retention witnesses. Coordinate
   oracle's active public projection edits before touching shared files.
3. Parent owns generator template and operation split/assembly, request/effect accessors,
   terminal cleanup, callback carryover references and bounded GC. Alternatively assign
   a separate agent only after non-overlapping helper interfaces are fixed.
4. Independent reviewer validates rollback/recovery/immutability and aggregate accounting;
   do not let the author approve their own schema/GC implementation.

## Required witnesses

Near-configured-limit input stored once across public+generator+multiple admissions;
prior large result followed by large failure stored once each; Update changes template
while earlier pending/ambiguous admissions assemble exactly their old bytes; terminal
reclamation removes only unreferenced blobs; callback-before-ack; rejected mutation and
failed persistence rollback; reload resolution; visibility payload-avoidance; malformed
or missing refs; nil/empty/explicit-null preservation; and actual aggregate node-size
measurement under8MiB. Include enough metadata overhead headroom and exercise real
History mutable-state accounting, not merely summing requested field lengths.

No claim that all arbitrary concurrent maximum-size actions fit finite mutable state.
The goal is removal of avoidable multiplication, preserving the configured public input
domain for the required shared cases and documenting actual remaining resource limits.
