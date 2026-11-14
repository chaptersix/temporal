# Inline sequence leaves and compact count pages

This is a local standalone index representation checkpoint plus the CHASM adapter fixture. The runtime Scheduler remains unchanged. Earlier failed amplification evidence and intermediate compact-count/compact-outer snapshots remain preserved.

## Representation and ownership

Count envelope6 stores one canonical unsigned-varint reference count, then the canonical hashed node. Branch schema4 remains94bytes; bucket schema7 holds at most16 fixed12-byte chronological keys and canonical unsigned-varint multiplicities. Each integer consumes at most10bytes; truncated, nonminimal, overflow and zero ownership/multiplicity encodings fail. Canonical node maximum354bytes, complete count envelope maximum365bytes. Hash strength remains SHA256; backend keys use unpadded base64url rather than hex. Bucket ordering/aggregate/edge checks remain enforced.

Outer branch schema3 stores exact minimum8bytes, summary32bytes and two9-byte child IDs plus tag:59bytes. The branch bit comes from the canonical keyed prefix; exact minimum/child split checks remain unchanged. Outer leaf schema5 stores its chronological time12bytes plus tag:13bytes. Its sequence is in its key. It has no secondary summary owner.

Splitting a leaf builds a two-record count summary through the same batch; splitting an existing branch forks its existing summary owner before update. Deleting a leaf removes only its outer record. When a branch collapses to a leaf, the residual summary must equal the canonical singleton for that leaf's exact time and multiplicity1; removing that one count retains any independent external owners. When a branch collapses to another branch, the existing equality plus at-least-two-owner check releases only the disappearing parent's duplicate owner. There is no arbitrary subtree-drop operation. All roots, refcounts and outer pages publish in one backend Apply.

## Validation

Final source hashes and copies are in final-manifest.json/final-source. Final routine PASS1.449s with100 Rapid traces for count and sequence reference models, full-depth paths, exact canonical refcount audits, every-read-failure rollback and publication rollback, clock-rollback nonprefix eligibility, and four shared singleton-owner split/collapse cases (equal/distinct times, both deletion orders). Malformed/nonminimal integer encodings are tested independently of hash mismatch. The initial inline-leaf build failure was a test helper call retaining its old signature; initial-build-failure.log preserves it. Lint01 found deleteSequence cognitive complexity29; extracting releaseCollapsedSummary fixed it. Scoped required lint02 passes normal/minimal/vet with zero issues.

Commands:

- CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2/internal/queueindex -count=1 -rapid.checks=100 -rapid.seed=1 -v
- CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2 -run TestIndexBackend -count=1 -v
- CGO_ENABLED=0 GOMAXPROCS=4 make lint-code LINT_CODE_TARGETS=./chasm/lib/scheduler2/internal/queueindex GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false

Final CHASM adapter suite PASS2.207s. N10000 index uses43891 persisted nodes,4,296,403 node-protobuf bytes and2,313,892 encoded-path bytes =6,610,295bytes. N1000 total530,612bytes. The following point update at N10000 updates73nodes/deletes30 and writes12,987 updated-node/path bytes. Unrelated-record hot-work fixtures at N4/5/1000 have identical complete counters: treevisits0,mapkeys0,2DATAdecodes,5serializednodes,233protobufbytes; poisoned unrelated DATA survives unchanged.

Conservative single-count point limits remain reads32*97,writes8*97,releases2*97,canonical bytes97*354. Sequence tests assert reads65*4000,writes65*800; schema depth remains64bits, secondary depth96bits and bucket16. Observed full-sequence-depth deletion reads2093/writes243/releases163; these observations do not replace the conservative formulas or prove a complete scheduler quantum bound.

## Limits and next gate

The6.61MB witness includes index CHASM nodes and paths, but excludes queue records, other scheduler metadata, operation/request data, complete WorkflowMutableState framing, database rows and Go heap. QueuedOccurrence protobuf alone measures44/88/153bytes for20/64/128-byte workflow IDs with narrow timestamps, or74/118/183bytes with exact timestamp fields, plus per-record framework/path overhead. Public user-byte costs remain an explicit input term; no arbitrary capacity reduction is introduced. Multiple policy classes, two generations and cleanup debt still require end-to-end wrapper accounting. Root's adapter publication test discards candidate trees and reloads prior serialized images; it is not an actual SQLite fault experiment.

Independent review is pending. Runtime queue integration, full M3 acceptance and M4 measurements remain open.
