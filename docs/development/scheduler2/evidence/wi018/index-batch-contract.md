# Shared index batch and backend contract

Status: proposed implementation boundary before outer index work. The independently reviewed count primitive is frozen pending parent checkpoint. No adapter or runtime adoption is claimed here.

## One transaction for all layers

The outer sequence root, its compressed pages, every secondary count root/node/reference, and the published owning root record must share one staged batch. CountStore.Update's current immediate map commit is only a standalone convenience wrapper; outer code must never call that wrapper for each ancestor. An outer failure after ancestor k must discard updates to ancestors0..k together.

Proposed public primitive: `Begin() -> Batch`; batch methods `UpdateCount`, `ForkCount`, `Count`, `CountBefore` and outer `Insert/Delete` share an overlay. Every backend or validation error poisons the batch permanently. Later operations return the original error without more backend work. `Commit` rejects poisoned or already-closed batches. `Abort` discards the overlay. A convenience CountStore.Update creates/uses/commits one batch; it is not used recursively by the outer index.

The batch contains keyed page overrides and deletion markers, plus bounded root-record updates. Read-your-writes looks in the overlay then the backend. No store enumeration or cloning is needed. All root ownership transfers happen inside the batch: retain replacement roots before releasing old ownership; old references are not exposed as independently committed intermediate states. Exact-byte canonical reuse compares the node payload, independently of mutable reference counters.

## Keyed backend

Use a minimal backend with `Read(key) -> immutable bytes, found, error` and `Apply(changes) -> error`, where changes contains only keys touched by the batch. Keys distinguish count pages, outer pages and owning root records. Values have explicit schema/kind tags and fixed maximum byte sizes. Read returns detached bytes (or a documented immutable value); callers never modify backing storage. A test backend may store a map, but the interface provides no Keys/Range/export operation. Tests may separately enumerate their backend to audit invariants.

Apply's contract is atomic relative to subsequent reads in its enclosing transaction: either all staged index and root changes become visible or none. It is **not inherently a durable database commit**. The standalone memory backend validates injected publication failure before applying any entries. A CHASM adapter would translate all changes to DataMap point staging inside one existing mutable-context transaction; the History engine's publication determines durable atomicity. It must not separately commit count/outer/root updates, and if DataMap staging itself can fail after partial writes the adapter must use the framework's rollback mechanism or preflight all potentially failing stages. No claim of durable rollback follows merely from successful in-memory Apply tests.

An alternative adapter can export the complete bounded staged mutation set to the enclosing CHASM transaction instead of calling Apply internally. Either form must keep an explicit single publication boundary. Root names and namespace/incarnation ownership are supplied by that enclosing authority, not invented by the count trie.

## Failures, ownership and limits

- Load failures, malformed/absent referenced pages, invalid node kinds, canonical identity mismatch, count/reference overflow and exhausted sequence fail before publication; no missing-page fallback manufactures an empty index.
- Publication failure leaves backend state and original roots unchanged; a caller cannot retry a poisoned/closed batch as if its effects were unknown. Retry creates a new batch from committed roots.
- Read-only queries can use the same overlay while preparing a point mutation. Their lookup/decoded-byte work is charged to the batch, including canonical reuse probes.
- Backend storage size includes key bytes, encoded payload/ref counters and adapter envelopes separately. Hash-input EncodedBytes is not serialized mutation size. Commit records actual staged key/value byte totals and deletion counts.
- Node/lookup/write budgets derive from fixed key depths and the fixed number of affected ancestor summaries. Enforce/report them at the shared batch boundary; a bound on each count update alone does not bound an outer mutation if the caller invokes an unbounded number.
- There is no arbitrary bounded DropRoot. Canonical point replacement can release changed paths because off-path subtrees remain shared. Whole-subtree detachment needs explicit bounded garbage ownership, or a separate proof that the surviving canonical root holds every unvisited subtree.
- Callers serialize batches over the same backend state. Overlapping writers require a version/CAS or the existing execution lock; the prototype does not invent cross-execution transaction isolation.

## Required tests for the next increment

Inject read failure after earlier ancestor changes were staged; reference overflow after previous secondary updates; and final Apply failure. Compare the entire test backend and externally owned root records to their pre-operation values. Verify that no subsequent method on a poisoned batch performs I/O, and that success invokes exactly one Apply containing all layers. Include canonical root reuse/collapse, duplicate timestamps, reverse timestamp versus sequence order and clock-rollback non-prefix expiry. Independent reference audits recompute both outer summaries and all root/edge references. Test count/storage bounds on adversarial fixed-depth shapes as well as random traces; no full-state snapshots occur in implementation code.
