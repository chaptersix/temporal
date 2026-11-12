# Shared index batch evidence

Standalone CountStore backend/batch increment, following reviewed primitive checkpointc8d06cbb7. No outer tree, scheduler runtime, DataMap adapter or durable-engine integration yet.

IndexBackend has only Read(key) and Apply(changes). MemoryIndexBackend copies bytes; Apply stages all keyed values atomically in memory. A future CHASM adapter must stage count nodes, outer nodes and owning root records in one existing engine transaction. This prototype does not claim durable publication from memory Apply. Backend atomicity on error is an explicit interface requirement; adapters that partially mutate before returning an error are invalid.

CountBatch shares a keyed overlay across Update/Fork and queries; first error permanently poisons it. It commits once or aborts without writes. Canonical recreation after an earlier staged deletion does not reuse stale backing reference counts. Commit includes generic outer/root staged mutations in the same Apply as count pages. No implementation path enumerates or clones the backend. Test snapshots independently audit rollback and are not implementation work.

Count pages encode94 bytes including mutable refs, with70-byte hex `count/` keys. Commit measures exact emitted value bytes and key bytes separately; deletions have no value. EncodedBytes remains canonical hash-input work. Decoder checks local schema/length/hash/count/reference/child-presence fields. Touched branch relationships additionally check increasing child bit positions, correct split/minimum keys and aggregate sum. Point descent and reference release enforce increasing parent bits. This is touched-path validation, not an eager proof of every untouched stored subtree. Backend malformed branches cannot invalidate the96-depth traversal bound silently.

Final routine command:

`CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2/internal/queueindex -count=1 -rapid.checks=100 -rapid.seed=1 -v`

PASS3.969s. Existing100 Rapid×150 reference events and storage/depth witnesses remain. New tests verify one Apply contains secondary pages plus outer/root records; later backend read/absent-key/final-publication errors preserve all prior committed data; poisoned methods do no subsequent I/O; Abort; staged canonical delete/recreate; shared-child ref overflow after another ancestor/root update; and hash-consistent malformed nonincreasing branch bits, wrong sums and missing children after prior staging. Parent requested touched-edge hardening after initial routine03; final04 includes it.

Conservative point reads now<=32*97 to include touched-child validation (full-depth measured1927), writes<=8*97, release visits<=2*97, hash-input bytes<=97*86. Query CountBefore still<=193 node reads because validated child entries are reused while descending. Full-depth increment emitted18142 value bytes and20300 key bytes in the memory adapter; there is no CHASM envelope/DB/heap size claim. A composed outer operation must separately bound its fixed number of ancestor summaries; unlimited calls inside a batch are not magically bounded by this primitive.

Source hashes are frozen in manifest.json. Independent review and parent consolidated lint remain distinct acceptance evidence.

Final lint-only extraction checkpoint: routine05 PASS3.606s with `-run '^TestCount'` (outer source compiled but not exercised); earlier routine04 retained as routine-before-lint.log. Named branch return roles, extracted leaf adjustment and reader closure formatting do not change semantics or counters. Manifest hashes updated to this count/batch source.

Final required make lint-code-fast passes with zero issues on the detached6e80c11cf verification worktree plus these exact four frozen count/backend files and the separate DataMap baseline test. This isolates the batch checkpoint from in-progress outer-index/framework edits. First lint findings were corrected by naming results and extracting helpers; no behavior change. Source hashes verified by root before commit.
