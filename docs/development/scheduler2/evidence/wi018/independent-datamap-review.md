# Independent DataMap implementation review

Reviewer: api_seam. First compile-ready source reviewed: data_map.go, context.go, fields_iterator.go, registry.go, tree.go integration and data_map_test.go. Author owns implementation; reviewer has made no framework edits. This artifact will be updated after fixes/final gates. No bounded-layout or whole-transaction performance acceptance follows from this slice.

## Confirmed finding, resolved before checkpoint

**Same-key changes across structure-sync boundaries can emit contradictory mutations.** The initial test coalesces operations in one delta overlay, but executeImmediatePureTasks can call syncSubComponents multiple times before one CloseTransaction. Delete a persisted key and sync: child.delete records its path in DeletedNodes. Put the same key and sync/close: final serialization adds UpdatedNodes for the same path without cancelling that deletion. The API contract requires coherent final per-key mutations; a one-flush test does not cover the actual framework boundary.

Author independently confirmed and implemented preservation of the removed DATA node plus tombstone cancellation on restoration. Merely clearing the tombstone and creating a brand-new node is insufficient for delete/reinsert/delete identity and final deletion accounting. Required regression exercises multiple sync permutations with initially present/absent records, coherent mutation sets, original persistence identity where applicable, and final durable reload.

## Reviewed ownership and compatibility

Get uses immutable prepareDataValue even when supplied MutableContext, then clones the addressed proto. Put clones its input. This avoids implicit mutation on read and caller aliases; proto unknown fields are preserved. Point lookup neither enumerates unrelated keys nor decodes unrelated DATA. Corrupt addressed kind/bytes fail through framework panic recovery rather than becoming absent. Nil/empty DATA storage follows existing unmarshalProto behavior: a persisted empty blob represents the zero message, not absence.

Constructor staging binds to one owner field; hydration creates one handle without raw-map enumeration. A zero/empty field acquires a handle on structure synchronization. Existing collection replacement and multi-attachment fail explicitly. Owner value identity/valueToNode plus root identity reject pre-replication/removed-owner handles, while fresh contexts can use a still-live cached handle after successful publication. ContextWithValue preserves epoch. Old completed contexts fail epoch checks.

An application error before CloseTransaction does not run cleanupTransaction and therefore does not expire the old context epoch in place. The guarantee is failed-tree discard/fresh durable reload and rejection against the new root, not a new universal in-place rollback/abort hook. No escaped clone can mutate committed records. Keep this scope explicit in docs/tests.

Default raw-map traversal remains unchanged. DataMap dirty() still marks the owner's full subtree dirty; sync/serialize/task/cleanup traversal remains baseline behavior. This first slice demonstrates lazy record ownership, not bounded mutation visits. Later worklist gates must count all visits and avoid reusing a once-large delta-map capacity in a way that makes clear/reset cost depend on historical maximum size.

## Additional gates

- Pending DataMap mutation followed by owner replacement must have an explicit outcome. Author chose rejecting the transaction, consistent with unsupported collection replacement; test failure plus committed-state reload, not merely panic text.
- Actual ApplyMutation child-only replacement/deletion should accompany the direct clearAncestorNodeValues unit witness. Fresh read must see new bytes and a retained old handle must reject; invalidation itself is not proof that replication called it correctly.
- No-op missing delete/read must not rewrite addressed/collection stamps. Serializer/publication failure must preserve committed records and tasks on fresh reload.
- Mixed raw Map/DataMap fixtures and detached nil-library decoding must retain baseline behavior. Invalid generic DATA type and pointer field shape should fail through established framework validation, without reflect panic escaping normal recovery.
- Final complete chasm suite, relevant History adapter tests and scoped lint on final source are needed; author-reported initial focused PASS0.474s is not a substitute for those gates.

## Multi-flush repair review

Author repaired the confirmed finding by retaining removed DATA nodes for the current transaction and cancelling the exact deletion tombstone when restored. Subsequent deletion still sees the original persisted node, so final deletion accounting survives delete/reinsert/delete. Touched collections clear their removal journals at cleanup. Reviewed interleaved-sync test applies actual UpdatedNodes/DeletedNodes to a separate durable map and reloads; closeDataMap also rejects contradictory mutation sets. Independent targeted ownership/interleaving/replaced-owner run PASS0.448s; exact command: `CGO_ENABLED=0 go test -tags test_dep ./chasm -run '^TestDataMap(InterleavedSynchronization|DetachedOwnership|RejectsCorruptKindAndReplacedOwner)$' -count=1`. Initial finding is resolved on this reviewed source. Final added replication/serializer tests are reviewed separately below when frozen.

## Final checkpoint review

No remaining blocking finding for the additive DataMap capability checkpoint. Reviewed actual child-only ApplyMutation (new payload and version, stale owner rejection, rehydrated new value), invalid-UTF8 serialization rejection with fresh committed reload, detached publication-rejection fixture and cold visitation assertions. Independent second directed run PASS0.415s: `CGO_ENABLED=0 go test -tags test_dep ./chasm -run '^TestDataMap(AttachmentAndReplicationIdentity|SerializerFailureReload|LazyHydrationAndCorruption|NoOpDoesNotPublish|DetachedAndFailedPublication)$' -count=1`. The final author manifest was checked against current source; separate reviewer snapshot preserves these hashes. Author complete CHASM full05 PASS0.435s includes baseline default tests. Parent consolidated lint remains the acceptance authority. No SQL fault-injection, universal abort revocation or bounded whole-transaction claim is inferred.
