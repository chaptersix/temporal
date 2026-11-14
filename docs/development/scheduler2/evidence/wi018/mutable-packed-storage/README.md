# Mutable packed store: measured representation, not runtime activation

Frozen binary6916bf76067d5817377230ab4bd2ad8de56a6b4ab9bf99d991fa5fd78ae08ed7,
335source manifest entries verified before/after compile. Large run PASS49.11s. Prior
static prototype and original whole-runtime measurements remain unchanged.

The isolated capability implements exact-key B+tree16 with stable monotone page IDs,
path-local split/borrow/merge, exact subtree minima/counts, shared atomic overlay and
poisoned failure semantics. Independent review found three malformed-state defects
(upper separator crossing, allocator collision, allocation-frontier violation); retained
counterexamples failed before fixes and pass afterward. Full reference/fault suite passes.
See compact-storage-prototype/mutable-store-contract.md for API and per-point bounds.

## Actual fixture records in mutable representation

| N | Issuance storage | Initial/peak bytes | After25% deletes | After reinsertion |
|---:|---|---:|---:|---:|
|1,000|packed experimental|767,769|550,424|715,217|
|1,000|separate immutable nodes|821,638|593,240|774,186|
|10,000|packed experimental|7,835,573|5,566,238|7,276,600|
|10,000|separate immutable nodes|8,371,789|6,000,690|7,865,556|

**The required isolated-issuance variant has only16,819bytes below8MiB. This is not a
credible whole-runtime fit acceptance.** Production execution metadata, completed
calendar/list integration and final template ownership/routing overhead remain unmeasured.
The original current-source10k fixture is18,975,955bytes. Fixed unchanged/template/
collection state in this encoding is4,091bytes. This is a size experiment, not a valid
selected Scheduler tree: root controls still represent the original fixture while
individual store rows undergo structural stress deletes/reinserts. No runtime contract
or admission timing is changed by these tests.

Every atomic backend Apply updates exact CHASM map-entry protobuf-framed byte accounting;
peak includes every intermediate committed mutation. Paths include `PackedPages#` and
opaque packed namespace/page IDs. Exact identity→locator directory is charged. Index
keys and values remain exact. Templates and fixed collection bytes are charged. Verification
reads occur after mutation measurements and are counted separately. Deleting/reinserting
every fourth row in deterministic mixed order stresses splits/merges without changing
logical payload bytes; every final value is read and compared exactly.

Packed issuance pages are only a comparison: deleting one request through that B+tree
would load unrelated variable request bytes. The required variant places each exact
request delta in a separate immutable DATA node; deletion addresses its key without
reading bytes. Operation/action/readiness/reference metadata remains separately packed.
This choice is why the safety-relevant variant is larger. The measured10k metadata
mutation maximum is5,788updated framed bytes,10changed pages and10backend reads;
these are observed workload maxima, not universal upper bounds.

## Actual queueindex batches through mutable store

| N, six classes/random times | Initial bytes | Two-generation bytes | Peak committed bytes |
|---:|---:|---:|---:|
|1,000|344,237|195,251|344,335|
|10,000|4,583,844|2,555,365|4,583,844|

This uses real queueindex CountBatch insert/delete through the packed atomic adapter,
then deletes half generation1 and inserts N/8generation2 records. It verifies remaining
counts across all six classes. **Index-only**: queue payload records, complete Scheduler
and execution framing are excluded. At10k the worst composite batch updates304,691
framed bytes across269changed pages and makes2,625backend reads. This includes the
composition of multiple packed point operations in one index batch; it must not be
misrepresented as a single packed point bound or a full handler result.

All point/page bounds are byte-parametric:16entries does not limit opaque key/value
length. Batch/Commit callers must bound their number of point operations. No capacity,
payload limit, freeze timing, migration, rollout or routing change is introduced.

A follow-up can shrink repeated opaque namespace/path encoding losslessly; the current
hex namespace repeats32characters although fixture namespaces use one nonzero byte.
That is a concrete remaining storage cost, not unmeasured savings credited above.
