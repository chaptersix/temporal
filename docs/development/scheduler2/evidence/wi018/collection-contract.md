# Proposed lazy DATA collection and bounded-layout transaction protocol

Owner: api_seam. Design only; no production implementation. Coordinates with proof_review's instrumentation, root's dispatcher/cleanup protocol and oracle_review's indexed queue reference. This resolves the intended API and task-worklist direction; source acceptance still requires the listed tests and measured counts.

## Source constraints

`Field.TryGet` exposes an attached proto and `prepareDataValue` marks it dirty whenever the supplied context is mutable. `fieldsOf` recognizes Field/Map by generic type prefix; `deserializeComponentNode` currently creates every raw-map Field handle. `syncSubComponents` enumerates raw keys and removes non-kept children. `markSubtreeDirty` walks descendants; serialization, cleanup and EachPureTask also traverse the tree. `immutableCtx` already stores the execution root. `CloseTransaction` cleans transaction bookkeeping even when close fails; History engine releases the execution lease with the returned error, so cache invalidation/fresh durable reload is part of failure handling, not an automatic in-memory rollback implemented by cleanupTransaction. None of these defaults should change for raw Map users.

## Additive API

Use an explicitly DATA-only string-keyed handle; avoid generic key conversions and mutable Field aliases in the first API:

```go
type DataMap[D proto.Message] struct { Internal dataMapInternal }
func NewDataMap[D proto.Message](ctx MutableContext) DataMap[D]
func (m DataMap[D]) Get(ctx Context, key string) (D, bool)
func (m DataMap[D]) Put(ctx MutableContext, key string, value D)
func (m DataMap[D]) Delete(ctx MutableContext, key string)
```

`Internal` follows Field's reflection convention, with an unexported concrete type. Registration validates that D is a concrete supported protobuf message and not a Component. No Range/Keys/Len, exported backing map, component/pointer values, collection replacement or bulk-clear API. Scheduler indexes own traversal/counts. Missing Get returns zero,false; a present wrong-kind/unreadable entry is a framework Internal error via the existing Field panic/recovery convention, never false. Put rejects nil; absent Delete is a no-op. Unknown protobuf fields and absent-versus-present-empty values remain exact.

Get always returns a detached clone and never implies mutation, even with MutableContext. It can deserialize the addressed DATA node with an immutable view of the context, then clone; it must not call mutable prepareDataValue. Put clones its input into the staged entry, so later caller mutation cannot alter the transaction. Only Put/Delete mark structure/bytes dirty. This deliberately trades an addressed-payload clone for safe ownership; payload bytes are an explicit input/output cost in the bound, never multiplied by unrelated records. A later borrowed-read optimization is outside this contract.

Point operations consult the transaction overlay then one `children[key]` lookup. Same-key operations coalesce to their final value/tombstone while preserving a distinction between absent at transaction start and existing: put/delete on a new key yields no persistent entry; delete/put on an existing key yields replacement; delete/delete is idempotent. The emitted NodesMutation must contain no contradictory update/delete for a path. Each coalesced touched key is charged, not every untouched child.

### Initialization and attachment

NewDataMap creates a transaction-owned staging handle usable by constructors before the owning component is attached. Attachment binds it to exactly one component field path during structure synchronization. Sharing one staging collection across two field paths is an Internal error. Root constructors must not silently allocate an unbounded initial staging map: definition/index building follows separately bounded preparation; explicit request processing cost is accounted separately.

Hydration creates one handle from collection path/root, without entry enumeration. An absent collection is a valid empty logical handle bound to its owning field, and first Put creates the collection node. Empty collection metadata may remain persisted; no hot automatic whole-collection deletion is needed. Raw Map's nil/empty deletion semantics remain unchanged. `fieldsOf` gains a distinct kind/prefix; pointer-to-DataMap is rejected like pointer-to-Field. DataMap is excluded from unmanaged-field copying/serialization. Synchronization keeps its collection field and drains only its overlay; it must not pass the collection through raw-map `deleteChildren`.

No path-format migration is required to represent a collection and DATA children, but persisted kind/type checks remain strict. The new scheduler layout uses new fields/paths; this API does not authorize loading old scheduler2 raw-map state as the new format implicitly.

## Context and lifetime rules

Add an internal root-access/session token to Context implementations (including wrapped ContextWithValue), not a caller-settable public token. New contexts capture a tree transaction epoch; framework-created contexts within CloseTransaction share that epoch. Closing/aborting an execution transaction expires the epoch. Point mutation checks mutable context, same root instance and live epoch. It also resolves the collection's current owner/path identity rather than trusting a detached old Node pointer.

A hydrated handle can remain in a cached component across successful transactions; its durable owner identity is stable and a fresh context supplies the new epoch. A context from a finished transaction cannot mutate it. A handle from a replaced tree or removed/replaced owning component cannot be transplanted into a new tree/context, even if the execution key/path is identical. Bind owner initial-version identity/tree-instance identity and validate it. Newly attached owners use an instance identity until their initial version is assigned. No public operation is permitted outside the engine-held execution lease.

Replication child replacement already calls clearAncestorNodeValues; preserve that invalidation and invalidate/rebind collection handles by owner/path on subsequent hydration. A stale retained handle must either reject or resolve a still-identical live owner and current collection node; it must never read an orphaned Node. Choose rejection for removed/replaced owners. DATA values returned from Get and arguments passed to Put are detached, so old values cannot mutate current state accidentally.

Failure semantics follow existing CHASM: a failed application/publication transaction exposes neither NodesMutation nor tasks; the failed mutable tree is discarded or restored by the harness/backend and next access uses committed state. Overlay cleanup is not a substitute for restoring already mutated component state. Test injected application errors, serializer errors and persistence rejection with fresh durable reload, plus misuse of old context/handle after failure. Do not promise in-place rollback that the engine does not provide.

## Bounded-layout opt-in and task protocol

Introduce a separate registration option for the new scheduler root, tentatively `WithBoundedDataLayout(layout)`. The descriptor names the finite allowed component paths and concrete component types (root, generator, visibility and any explicitly added fixed maintenance component), plus finite logical-task budgets per component. It is part of library schema configuration and must be installed identically by the live and nil libraries. It is not a namespace dynamic switch and is not enabled for existing libraries.

At genuine cold reconstruction, validate the entire persisted topology once against the descriptor: unbounded descendants may be DATA entries only; components/pointers cannot hide beneath DataMap; component path/type and task-count violations fail explicitly. Build a fixed component-slot array and task-bearing membership. At attachment/removal maintain those slots pointwise. Do not fall back silently to an unbounded traversal for a root claiming the bounded profile. Default libraries retain the old traversal behavior and require independent regression tests. A mixed raw/new collection library may use DataMap but is not automatically eligible for the bounded-layout guarantee.

Keep separate insertion-ordered deduplicated worksets for structure-dirty nodes, byte-dirty nodes and touched/flagged nodes needing cleanup. Collection changes enqueue only owner/collection and addressed DATA paths; ancestor path traversal is bounded by declared layout/index depth. Newly discovered work is appended and drained until fixed point; every insertion/visit is instrumented. No hot workset is rebuilt by walking descendants. Existing NodesMutation/systemMutation/NewTasks maps remain transaction-owned and are cleared proportional to touched entries.

For this finite-component profile, prefer conservative validation of **all fixed component slots whenever any state or pure task changes**, rather than a complicated dependency graph. Reuse existing task validation/cleanup/generation logic for each slot. This preserves ancestor/parent-pointer invalidation without visiting DATA descendants, and bounds work by the descriptor's component/task constants. Pure task enumeration and time-skipping eligibility iterate the same fixed slots; newly attached tasks and removals update slots immediately, including during immediate pure tasks. Preserve original lifecycle/detached checks and physical task identity; the optimization never treats a task as valid merely because its DATA is untouched.

A task-bearing index is derived cache state, not replicated authority. Rebuild it during cold topology reconstruction; restore/invalidate it on rollback and replication component replacement. Cached per-component earliest wake must be updated on every logical task insertion/removal/validation outcome, or simply inspect the bounded task lists directly. A fixed component count alone is insufficient if any component retains an unbounded task list; root's dispatcher design must establish the finite task budgets before enabling this profile. Erroring on a task-count violation is an invariant check, not a new public queue capacity.

All close paths participate: immediate pure task sync, regular structure sync, deferred pointer handling, lifecycle/visibility/principals/user metadata, serialization, task cleanup/generation, time skipping and final flag cleanup. Existing raw deferred pointers can retain old traversal behavior; the bounded profile must reject unsupported unbounded pointer layouts at registration/reload instead of discovering them during a hot pointer-resolution walk.

## Required tests and source gates

- Hydrate/read one key at N=0/4/5/1000/large disabled-capacity: no child-key enumeration; read under mutable context creates no update/stamp change. Poison an unrelated DATA blob and prove Get/Put/Delete another key never decodes it.
- Input/output alias mutation, empty/nil/unknown fields, wrong-kind/corrupt entry, invalid context/root/epoch and removed/recreated owner. Include a retained pre-replication handle and child-only replacement.
- Same-key operation permutations, new constructor attachment, double attachment rejection, failed close/persistence rollback and fresh nil-library detached reload. Existing Map tests unchanged.
- Root and child mutation invalidate every relevant fixed component task; clean unrelated DATA contributes zero visits. Pure tasks create/delete/replace tasks in the same transaction; lifecycle close, paused/detached behavior, visibility and time skipping match the default implementation's projected outcomes.
- Compare default traversal and opt-in profile on equivalent finite test components, including duplicate/stale physical envelopes and replication updates. Exercise task-budget and unexpected topology corruption explicitly.
- Full handler/CloseTransaction counters include workset insertion, metadata/clean-node visits, bytes, task validations, pointer resolution and cleanup. Cold reconstruction remains separately reported O(N); the first following ordinary transaction must use the same fixed bounds as later transactions.

Implementation coordination: proof_review owns instrumentation only at present. No API or tree edits begin until parent assigns the framework implementation and agrees the registration/session mechanics. This proposal does not implement scheduler ready-claim protocol, cleanup ownership, queue equivalence or incremental spec evaluation; those are independent required WI-018 contracts.
