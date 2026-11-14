# Fresh-profile packed runtime integration

No migration or default constructor change. Existing capacity, admission, deadline,
Remaining/Unfinished, request freezing and stale task authority remain unchanged.

## Exact root/schema seam

Propose RootState field49 `PackedStorageControl packed_storage`, message fields
`uint32 format_version=1; uint64 next_locator=2`. Fresh version1 starts next_locator1.
Root owns generation, constructor selection and RootState replacement preservation.
Scheduler gains `PackedPages DataMap[*commonpb.Payload]` and
`PackedRequests DataMap[*scheduler2pb.Payload]`. Q stores only exact immutable request
wire deltas, addressed by never-reused admission locator. It is never a packed B+tree
value. Deletes do not read Q bytes. Existing action/result/failure ownership stays in
BlobDeltaPlan; request reference envelope E supplies those references without Q reads.

Fixed template addresses t/O,t/A,t/R,t/E,t/U,t/B,t/Q are in PackedPages. Template bytes
are immutable while their owning namespace has records. Every encoded record declares
that baseline ownership, even raw fallback; remove baseline only when owner count0.
Q uses E's request-envelope count. No variable template-owner scan or global blob scan.

One exact identity index I maps external operation ID to `{locator:uint64, owners:byte}`.
Owner bits O and A preserve lookup while either operation or retained public action lives.
No uncharged reverse lookup index: operation metadata reconstructs canonical IDs only
when exact current grammar matches, otherwise retains literal IDs. Ready and operation-
owned action metadata resolve ID through O. When action.OperationOwned becomes false,
its encoded action retains literal ID before O is deleted. This keeps recent-only history
addressable without retaining an entire operation/request or a second identity table.

Locators are distinct from retry/readiness sequence and public action sequence. They
never change during recovery or callbacks, and never reuse after retention deletion.

## One detached publication batch

A single packed editor spans all record, readiness and public projection changes for
one semantic publication. All reads/decodes/identity checks, template selection and
B+tree mutations finish before any DATA/control/task writes. Finish captures detached
physical changes; publication only performs trusted Put/Delete, then installs the
prospective PackedStorageControl. Error or backend fault discards the entire editor.

Do not independently prepare two physical batches against the same packed namespace:
logical readiness/retirement keys can be disjoint while their B+tree pages overlap.
Logical index stores use explicit separate packed namespaces for queue, readiness,
retirement and running. Retirement's `retirement[-root]/` keys never share canonical
count roots with readiness, matching current formats. Queue's six classes remain one
CountBatch, so shared count-node reference ownership stays atomic.

Root applyTransition hook is after finalizePublicTransition and BlobDelta preflight,
before stageQueueAppend/stageQueueDrain (the first existing publication). It prepares
one packed mutation from operationRecordPlan + projectionMutation + recovered readiness
metadata. Existing publishing helpers skip physical record/index writes under this
selector and still install their controls/tasks. Claim/recovery paths in start_dispatch
need the same packed preflight immediately before publishOperationReadiness. They must
not use a post-publication rejecting packed Put.

Queue maintenance/append/drain each has its own bounded packed queue overlay; it does
not share physical namespace pages with operation/projection metadata. Queue payload
records contain no issuance request bytes, so their point packing is independent.

## Implementation/ownership slices

1. Framework agent: isolated production codec and detached PackedPages backend/editor
   files, no Scheduler field references until root schema release; actual CHASM backend
   tests show no writes before Finish/publication and atomic failure/reload behavior.
2. Framework agent: operation/request/ready/action record adapters plus logical index
   adapters; typed error returns. Convert metadata request cleanup to E-only reads.
   Root: shared constructor/library/RootState preservation, component publication hook
   and start_dispatch hook after exact adapter signatures are available.
3. Coordinate direct callers in operation_store.go, operation_readiness.go,
   operation_indexes.go, public_action_store.go/public_projection.go and queue_state.go.
   Existing addressed and finite profiles remain selected by their old state. Fresh
   packed selector must not silently fall back to old DATA collections on absence.
4. Activate only private fresh constructor after all paths work. Run actual dispatcher/
   recovery/callback/Close/retention/visibility and whole-handler+Close measurements,
   then actual SQL public suite. Measure all PackedPages/PackedRequests, identity flags,
   templates, controls and production execution framing at1000/10000; existing prototype
   headroom is not an acceptance result.

Read-side corruption errors propagate through error-returning boundaries. Existing
DataMap decode panics remain framework aborts; new packed semantic errors must not turn
into absent records or fabricated counts. Public output enumeration remains output-
proportional; no hot scans, old collection fallback, capacity limit or freeze delay.
