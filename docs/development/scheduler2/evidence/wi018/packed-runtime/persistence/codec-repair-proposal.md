# Exact-wire compression candidate after actual storage failure

Actual persistence10000 failed at8218/6857 pending operations for1KiB/1280KiB shared
input. The actual1000 diagnostic snapshots in breakdown preserve real SQLite images
and an offline wire parser; this diagnostic source was not separately frozen. The
failure binary remains separately frozen and authoritative. Snapshots and parser have
content hashes. The parser validates protobuf wire boundaries and packed page lengths;
all candidate expanded requests round-trip byte-for-byte, without protobuf remarshal.

For the1KiB actual1000 snapshot, Q's existing deltas occupy367,159bytes. Completion
callback field21 accounts for306,097bytes of that, versus320,756 raw callback bytes.
The single prefix/suffix splice cannot retain a shared middle region between changed
callback authority fields. Metadata has the same issue: O and ready records wrap
binary data into one protobuf field; action fields also differ at multiple positions.
Current O values97,746bytes, A72,414, R21,673. Identity/index/node overhead is separate:
readiness X alone contains254,191 encoded page bytes and3291 logical index records.

A raw preset-dictionary deflate level1 candidate, with the existing immutable template
as dictionary, was applied offline to each exact expanded wire and retained only when
smaller than the current encoding. Exact decompression equality was checked for every
record. Q falls to63,913bytes, O30,826, A22,119, R17,664; E stays4,000. Aggregate value
saving424,470bytes per1000 for this snapshot. This is a concrete encoding experiment,
not a10k extrapolated fit claim or an implementation/CPU acceptance result. No key,
page shape, template ownership, locator or request freeze timing changes are assumed.

## Proposed bounded mode2

Keep modes0(raw) and1(splice) readable. Add mode2 with an exact raw-output length and
raw deflate bytes using the immutable template dictionary; choose it only if smaller
and original length <= len(template)+len(encoded mode2). Otherwise retain existing
encoding. Decoder rejects a declared size exceeding that same bound before allocation,
reads at most declared size+1, requires exact size and clean EOF, and rejects trailing
compressed bytes. A bytes.Reader provides ReadByte so the inflater cannot hide trailing
wire by over-reading. This prevents compression amplification beyond existing baseline
plus encoded input, even for corrupt records. It imposes no new accepted-input cap:
otherwise-valid records simply use their prior raw/splice format.

Use the standard Go compress/flate implementation, explicit deterministic level and
no shared global mutable writer. Metadata reads still never touch Q. Dictionary window
is bounded by the format; template parsing/materialization remains counted. Measure
actual encode/decode calls, bytes, allocation and latency, since smaller storage does
not prove lower CPU. Round-trip fuzz/differential tests must include duplicate/unknown
protobuf fields, noncanonical varints, arbitrary bytes in wrappers, template changes,
trailing/truncated streams, forged size and expansion attempts, poison/rollback and
actual callback cleanup. Only then integrate, remeasure actual1000/10000 under the same
accepted workloads and default state limit, and obtain independent review. All old
failure evidence remains immutable. No capacity reduction or limit increase is a repair.
