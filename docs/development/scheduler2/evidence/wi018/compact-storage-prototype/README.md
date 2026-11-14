# Exact-wire packed storage prototype

Test-only source, no production registration/schema, no changed admission or capacity.
The source manifest (319 files) was checked before/after frozen binary compilation;
binary SHA256 is b3a008d0a8bac6bee476ba7a18eacdbbce95da03f7f176fba6e8f72a5ac7efa4.
The original whole-runtime evidence and binary are unchanged.

| Pending operations | Original framed bytes | Prototype framed bytes | Prototype nodes |
|---|---:|---:|---:|
| 1,000 | 1,898,183 | 780,904 | 601 |
| 10,000 | 18,975,955 | 7,947,424 | 5,660 |

This is exact WorkflowMutableState.ChasmNodes protobuf framing of the constructed
representation, **not a registered/reloadable runtime tree or a storage-gate pass**.
The original fixture here has 255 additional fixed bytes compared with the earlier
frozen whole-runtime fixture. Changing source snapshots are not conflated.

The 10k prototype is only441,184bytes below8MiB. Current execution metadata, physical
persistence overhead, complete prepared-calendar/list controls and mutable index
split/locator costs have not been established by this encoding. No universal fit claim.

For10k, encoded page data bytes by collection:

- Q exact request skeleton deltas:2,524,349.
- E payload reference envelopes:80,625.
- O compact operation metadata:1,120,269.
- A public action metadata:298,589.
- R ready metadata:150,213.
- I exact external-ID→locator directory:529,221.
- X exact source index key/value pairs:3,005,147.

Templates,65-byte16-slot directories, fixed count controls, collection nodes and CHASM
node/path/map framing are included in the total. The index key/value pairs retain the
original source key bytes; there is no free uncharged dictionary. Static index packing
uses sorted keys while metadata uses admission locator order. That static order is not
a mutable address scheme: insertion shifts/splits and stable routing remain next work.

Each page decoder visits exactly16 directory slots, rejecting truncation/trailingbytes.
Metadata and issuance occupy disjoint pages; a poisoned issuance page is not required
for metadata decoding. Template delta encoding preserves exact top-level wire fields,
including unknown/duplicate tags and noncanonical field encodings; it retains callback
bytes verbatim and does not regenerate from current settings. Raw fallback handles
changed field count or nonbeneficial compression. Each actual request skeleton and
reference envelope roundtrips byte-exactly. Operation identity compression is allowed
only after exact equality with current decision.startOperation grammar; otherwise the
original complete metadata is retained. Automatic/manual/backfill/wide-time and future
grammar fallback tests roundtrip. All operation fields including deadline/retry survive.

Validation: frozen complete prototype tests PASS3.47s including both large fixtures;
required fastlint02 PASS normal/minimal/vet. First compile failure was an unused test
import; lint01 was sort.Strings→slices.Sort, fixed before final binary freeze. Retained
logs contain both failures and terminal reruns. No failures were hidden by altering
capacity, request freezing, or production behavior.
