# Independent count batch review

No blocking finding for this standalone prototype increment. Reviewed count.go, backend.go, tests and index-batch contract; exact hashes are in independent-count-batch-snapshot.json. Historical independent-count-review.md describes the preceding snapshot and is not silently refreshed.

The shared overlay stages secondary refcounts and outer/root entries together, publishes exactly once, poisons on first failure, and prevents subsequent I/O after poisoning. Canonical deletion followed by recreation deliberately ignores stale committed refs. Child retains and old-root releases are part of the same discarded overlay on any later failure. Tests compare the entire backend after read, absent deletion, reference exhaustion and publication failures following earlier ancestor staging. Abort never publishes.

Touched branch validation checks increasing bit positions, split/minimum key and exact child count sum; query reuses validated children. Traversal has at most 96 branch positions. Point-update conservative bounds now include these child reads (32*97 metadata reads; query <=193 node reads), replacing the earlier snapshot's smaller read formula. MutationBytes and KeyBytes measure emitted values and keys separately; EncodedBytes is hashing input. These are not CHASM envelope, database or heap bounds. Unlimited operations in one batch are not a bounded outer operation.

Independent command: CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2/internal/queueindex -count=1 -rapid.checks=100 -rapid.seed=1. PASS 3.443s; archived count-batch-independent01.log.gz.

Remaining integration obligations: IndexBackend.Apply must be atomic on error; serialization is caller-owned, and a CHASM adapter must include count, outer and owning-root updates in the same engine transaction. The memory adapter and injected pre-publication failures do not prove durable engine rollback. Whole-subtree reclamation remains a separate bounded-cleanup problem; point replacement relies on retained sibling sharing. No scheduler selection, outer-tree equivalence or WI018 acceptance follows from this review.

Final style followup reviewed: named branch result roles, unchanged changeLeaf extraction and Count read closure moved before condition. Final hash scope is recorded separately; author rerun TestCount 100 seed1 PASS 3.606s. Prior independent execution remains attached to its original tested hashes.
