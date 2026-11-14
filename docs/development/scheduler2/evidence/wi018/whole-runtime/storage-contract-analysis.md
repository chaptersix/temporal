# Accepted workload and compact storage follow-up

## Capacity is not permission to reduce accepted work

Source: `chasm/lib/scheduler/config.go` defaults MaxBufferSize=1000 and
MaxActionsPerExecution=5. The latter limits one ExecuteTask iteration, not retained
or simultaneous actions (`invoker_tasks.go:newInvokerTaskHandlerContext`). A source
search of production Go/proto under chasm/service/common found no MaxConcurrentActions
setting. Generator/backfiller capacity checks apply only when MaxBufferSize>0;
selected capacity preserves <=0 as unlimited. Namespace-filtered scheduler.tweakables
can therefore accept 10,000 pending/buffered actions when configured unlimited or
sufficiently large. This fixture explicitly uses zero; it does not claim default
1000 capacity admits 10,000 pending intents. ALLOW_ALL running workflow concurrency
is a distinct population; successful start-only records retire rather than remaining
pending. No cap reduction is an acceptable repair.

Legacy CHASM `InvokerState.buffered_starts` holds BufferedStart timestamps, policy,
request/workflow identity, attempts/backoff and execution outcomes. It does not store
one immutable serialized StartWorkflow request per admitted start. `startWorkflow`
constructs that request using current Schedule action, current carryover and a generated
callback. A structural 10,000-entry InvokerState with matching timestamp and ID lengths
is 1,509,746 protobuf bytes (`breakdown01.log`), excluding shared schedule, CHASM framing
and execution state. This is neither a whole legacy database measurement nor evidence
that dropping immutable selected snapshots preserves the approved contract.

## Concrete next representation experiment

Do not implement skeleton sharing in isolation or promise it will fit. First make a
reviewable storage-only encoding prototype for the same 10,000 logical records, with
round-trip equality and explicit per-collection byte accounting:

1. Store one content-addressed immutable request template per exact admitted skeleton.
   Remove per-admission workflow ID, request ID, generated scheduled-time attribute and
   completion callback from that template only when a versioned reconstruction envelope
   carries their exact values/encoding authority. Keep arbitrary unknown protobuf fields,
   optional presence, policy overrides and retained carryover references unchanged.
   Keep callback bytes verbatim initially: reconstructing signed/internal tokens from
   current configuration is not equivalent to immutable admission. Header/input/action
   blobs are already shared and must not be charged as new savings.
2. Replace repeated long string identifiers in OperationRecords, ActionRecords,
   ReadyRecords, and collection keys with an internal never-reused numeric locator.
   Preserve exact external operation/request/workflow identities once in the admitted
   immutable record; external callbacks resolve through a bounded exact lookup or a
   validated reversible existing identity grammar. Do not hash away collision checks.
   Preserve stale fencing across incarnation, admission and readiness sequence.
3. Combine per-operation public metadata and readiness locator ownership where their
   lifetimes permit, or use compact fixed-capacity pages with bounded page reads/writes.
   The index's 30,001 nodes and 1.82MB of paths merit an encoded-page prototype, not
   speculative subtraction. Retain lexicographic readiness and retirement ordering,
   full public running output ordering, and independent bounded recent/completed windows.
4. Reuse existing BlobDeltaPlan owner accounting and atomic preflight/publication.
   A template owner is retained for every admitted request even after definition update;
   deletion occurs only at zero owner count. No hot request assembly may scan owners.

Measure the prototype before selecting a schema: removing all request bytes still
leaves >11MB, so both metadata/key repetition and index/node overhead must shrink.
Then integrate only the private fresh profile with explicit format selection and
round-trip differential tests. Existing profiles remain readable; no migration,
production routing or capacity/payload limit change is authorized. Rerun frozen whole
transaction/storage witnesses and actual public contracts on the final encoding.

An arbitrary accepted payload or 10,000 distinct immutable definitions may exceed 8MiB
regardless of sharing. Separate unavoidable user bytes from avoidable metadata overhead;
a universal execution-size promise cannot be inferred from this simple workload.

## Concrete candidate and estimate (design, not measured fit)

Choose 16-slot DATA pages, addressed by `floor(locator/16)` with fixed slot
`locator%16`. A never-reused admission locator is distinct from readiness sequence:
recovery changes the latter without changing request ownership. Read-modify-write
loads one page; up to 16 records may be decoded/encoded, independent of backlog.
Deletion clears a slot, never compacts or moves other logical identities. Empty pages
are deleted; source/incarnation and format are validated in the page header. Use binary
encoded page numbers through a collision-free path-safe encoding, not external IDs in
CHASM paths. A page must not hold variable issuance payload bytes: request envelopes
stay separately addressed so ordinary metadata mutation never decodes large input.

Candidate collections:

* OperationPage: 16 compact metadata slots. Store nominal/wide nominal, manual/backfill
  discriminator, phase, deadline/retry, readiness sequence, chain/observation state,
  and immutable request-envelope locator. Reconstruct occurrence/action/start IDs only
  for the exact grammar in decision.startOperation (incarnation plus occurrence suffix).
  Validate grammar when encoding; fresh private constructor makes this possible without
  reinterpreting old records. Retain exact strings in a fallback immutable identity
  record if future grammar is not reconstructible; no lossy hash mapping.
* PublicActionPage: 16 action slots keyed by admission sequence. Store timestamps,
  public run ID and operation locator; resolve workflow ID from immutable identity
  record. Preserve operation-owned/recent-only lifetime explicitly. Running records
  refer to action locators; Describe necessarily materializes output-proportional items.
* ReadyPage: 16 locator slots holding operation locator/logical wake/ready time. The
  readiness index continues to order `(ready_time, readiness_sequence)` exactly.
* IndexPage: bucket existing fixed-width trie records into 16-slot physical pages
  under a separate stable index-node locator. This requires a bounded node-address
  translation (or direct compact trie path encoding), not a dictionary scan. Prototype
  stable allocator/free lifecycle and atomic multi-page overlay before adoption. A
  node pointer must not silently change canonical ownership/refcount semantics.
* RequestEnvelopePage: 16 small envelopes of template locator, identity locator,
  exact frozen callback wire bytes, generated-time override wire bytes, and existing
  action/result/failure blob references. Large/unknown override bytes are immutable
  blobs, not in mutable page bodies. Shared template uses content identity over exact
  deterministic frozen wire segments; assembly concatenates exact segments or checks
  byte equality against the original frozen wire during encoder tests. Never regenerate
  callbacks from current config. Identity/namespace/workflow type/static generated SA
  belong to the immutable template when byte-identical, not current definition state.

Planning estimate for this particular homogeneous 10k fixture, deliberately rounded:
request envelopes/templates 2.5–3.5MB (1.98MB callbacks cannot be assumed deduplicated),
compact operation metadata0.7–1.2MB, public metadata0.4–0.7MB, ready locators0.25–0.4MB,
index payloads2.1–2.5MB, page/path/framing0.2–0.5MB: **6.15–8.8MB** before production
execution metadata. These are design budgets derived from current per-field/node
sizes, not encoded measurements or proof of fit. They show why page packing plus
identity/template sharing deserves a prototype, and why a representation decision
must wait for its exact size. Shared action payload is already outside these repeated
records; no new savings credited. Unique callbacks, unique definitions and wide IDs
must remain in the prototype matrix. External ID-to-locator lookup overhead must be
included; a reversible admitted identity parser can use nominal/manual/backfill fields,
otherwise add a bounded index and charge its bytes explicitly.

## Admission pipeline boundary

`decision.startOperation` sets immutable IDs, Pending, RetryAt, Deadline=Now+RetryHorizon,
Unfinished and Remaining consumption together. `component.applyTransition` immediately
calls freezeAdmission for the newly admitted operation, then splits/stages the frozen
request before publishing any of this authority. Queueing before admission is already
separate: queued identity uses the current template when later drained (see buffer
contract tests). Moving accepted ALLOW_ALL operations back into that pre-admission
queue, or delaying freezing until dispatch, changes definition-update/carryover snapshot,
retry deadline, capacity counters, and what callback/request identity is durable.
Therefore a bounded admission pipeline cannot be used as a storage shortcut under the
current decision contract. It would need a separately approved contract change, not a
routine representation choice. Keep freezing at the existing admission transition;
compact only the exact frozen representation afterward within the same atomic preflight.
