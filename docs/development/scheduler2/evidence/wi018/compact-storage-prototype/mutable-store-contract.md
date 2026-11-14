# Mutable packed store proposal

Status: proposed isolated capability; no runtime schema or activation selected.

Use an exact-key B+tree, not static rank-based packing or a hash-collision assumption.
Each namespace has control `{version=1, root_page:uint64, next_page:uint64,
record_count:uint64}`. Page IDs monotonically increase, are never reassigned while any
record/task can refer to them, and reject exhaustion before mutation publication.
Empty store has root0/count0/next1. A root is a leaf or internal page; pages have
format/kind plus up to16 entries. Leaves hold exact byte key/value records; internal
pages hold up to16 child IDs and exact child minimum keys. Keys use page-local common
prefix plus length-delimited suffix; values are opaque. Full external identities
remain exact keys in the identity namespace. Metadata and request-envelope namespaces
remain disjoint. No metadata Get invokes a request decoder.

Proposed package-local API (standalone prototype package first):

- `StoreID [16]byte`, `Batch(Backend)` where backend reads opaque keys and atomically
  applies one list of changes, with the same enclosing transaction semantics as
  queueindex.IndexBackend.
- `Get(store,key) ([]byte,bool,error)`; `Put(store,key,value) error`;
  `Delete(store,key) error`; `Count(store) (uint64,error)`;
  `Commit() error`; `Abort()`.
- One overlay owns all namespaces in a transaction. Control/page modifications remain
  detached until Commit; read/decode/validation failure permanently poisons the batch.
  Underlying Apply must be atomic; the CHASM adapter publishes only after all preflights.

Lookup follows exact separator keys to one leaf. Updates replace only a search path
and a bounded adjacent sibling at each level. Leaf overflow combines16 decoded entries
with one incoming value in transient workspace, then serializes two pages of at most16.
Internal splits obey the same bound. Deletion borrows or merges bounded siblings,
updates exact minimum separators and collapses a single-child root. Never move a record
merely because its ordinal rank changed. Retained task authority uses logical identities,
not unversioned physical page/slot addresses. Numeric direct-map metadata can adopt a
simpler fixed locator page later only when occupancy/lifetime bounds justify it; that
optimization is not assumed in this prototype's accounting.

Measure every intermediate committed size across ascending/descending/random inserts,
point replacements, deletes, and reinsertions at N4/5/1000/10000. Include roots, separators,
page headers, exact identity index, template ownership/refcounts, CHASM map framing,
and peak detached transaction bytes separately. Assert <=16 decoded/serialized entries
per individual page; total page work is O(height), with depth/reads recorded. Test exact
reference-map equality, cold reload, commit rejection at every read fault and atomic
Apply rejection, malformed page/count/root relationships and page-ID exhaustion.

The static7,947,424-byte result leaves only441,184bytes. A mutable design must report
actual overhead, not subtract speculative savings. Six-class/random-time queue and
index update workloads remain mandatory adversarial checks before live profile choice.
Full production execution framing is only measured by the actual runtime/SQLite suite;
this capability must label estimates separately and cannot declare universal8MiB fit.

## Implemented isolated primitive and source bounds

`internal/packedstore` is not registered by Scheduler. Point operations validate
control/root counts, touched separator minima/counts, exclusive inherited upper bounds,
and touched page IDs against the allocation frontier. Allocation reads its prospective
key and rejects an existing page. Local serialization rejects unsorted keys. Corruption
of untouched descendants is detected when traversed; Count is not a complete tree audit.
An absent control means a new store, so a runtime owner with nonzero expected count must
check its own maintained scalar before treating absence as empty.

Bounds are per point operation, not arbitrary multi-operation Batch/Commit:

- At most16 entries decoded or serialized per individual page; a split uses17 transient
  entries and emits two pages, each at most16. Byte work depends on encountered key and
  value sizes; this is not a constant-byte guarantee for arbitrary opaque values.
- Get permits64 loop iterations, so malformed-depth rejection can read control+root+
  64 child pages=66 reads. Valid trees have stronger occupancy/cardinality bounds.
- Insert conservatively reads130 records: control1, path64, allocation collision checks
  for at most64 splits plus a new root65. Its staged distinct changes are at most130.
- Delete conservatively reads255 records: control1+path64+three sibling/child reads at
  each of63 levels+root reload1. At most190 page serialization calls and128 distinct
  staged keys (path64+one sibling63+control1). Overlay reads may avoid backend reads.
- Commit sorts the staged changes and is O(changes logchanges); caller must cap logical
  point operations to derive a full handler bound. One backend Apply publishes all
  namespaces atomically. This primitive does not make an unbounded caller bounded.

Independent review retained three failing corruption witnesses: upper separator
crossing, allocator collision, and child beyond allocation frontier. Fixes reject all
three before Apply; independent full suite then passed1.311s. Additional witnesses cover
nonreuse across empty/reinsert cycles, detached overlay reads and a poisoned cross-store
transaction. No per-record external identity is replaced by a collision-prone hash.
