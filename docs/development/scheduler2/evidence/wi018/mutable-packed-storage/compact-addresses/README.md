# Lossless address encoding follow-up

Final frozen large matrix PASS52.47s. Prior mutable binary/results remain unchanged.
Binary eb27052a30aea775ac8c8a50c669970490a2fc035cfe8dd7291a06b672616311;
599 all-chasm-Go plus module manifest entries verified before/after compilation.

Packed page addresses change from `packed/<32hex StoreID>/<hex ordinal>` to
`p/<raw URL-base64 canonical StoreID>/<hex ordinal>`. The ID is exactly 128 bits:
only trailing zero padding of its fixed16-byte representation is omitted, and decoding
restores those zeros. Zero ID is the empty namespace token. Canonical decoding re-encodes
and compares the whole address, rejecting padded base64, alternate pad bits, redundant
zeros, uppercase or signed hex, oversized IDs and overflowing ordinals. No logical
ordering depends on physical address string order. Maximum address length is41bytes;
fixture logical store-kind IDs use one nonzero byte, while full-width IDs need22base64
characters. Both namespace shapes are included in the measured variant matrix.

Individually addressed immutable request deltas use `q/<raw URL-base64 uint64>` with
exactly8big-endian ordinal bytes, preserving all64bits in13address bytes. These are
never-reused admission locators in the proposed runtime, not retry/readiness sequence
identities. Prototype initial fixtures happen to assign the same monotone initial order;
future live integration must maintain that distinction through recovery.

Canonical roundtrip/collision tests cover zero/all-ones/high-bit/trailing-zero IDs,
1,000deterministic full128-bit IDs, boundary uint64 ordinals, malformed encodings and
request ordinal extremes. Exact-key B+tree ordering and all ownership/lifecycle behavior
are unchanged. No capacity reduction or request-freezing change is introduced.

Initial fastlint found only concurrent unrelated test unused variables; no packed/address
finding. Final large results and lint evidence will be recorded after the coordinated
proto generation and frozen build. Production execution/calendar/list/templateownership
framing caveats remain unchanged; address savings are not credited until measured.


## Frozen results

| N | Isolated issuance, kind namespace | Isolated issuance, full128 namespace |
|---:|---:|---:|
|1,000|770,891|793,658|
|10,000|7,860,057|8,089,853|

Values are peak committed framed bytes across insertion,25% mixed deletes and
reinsertion. The10k full-width case leaves298,755bytes below8MiB, **not a runtime
acceptance claim**. Fixed exemplar CHASM transition metadata is used; current production
execution metadata and final prepared calendar/list/template ownership are not established
by this representation experiment. Unchanged/template/collection state contributes4,091bytes.

Index-only six-class/random-time10k peak4,417,695bytes; two-generation state2,454,837.
Largest composite index mutation296,508updated framed bytes,276changed pages and2,634
backend reads. Intermediate packed B+tree shape depends on batched update order; these
are observed frozen-run maxima, not universal worst-case byte constants. Complete logical
values and exact request bytes roundtrip; immutable-request deletes perform no Q read.

Independent codec source review and tests passed (packedstore0.191s, requestaddress0.645s).
Root's final required fastlint06 passed normal/minimal/vet on the final packed source;
root retains that combined log. See compact-storage-prototype/independent/compact-address-review.md.
