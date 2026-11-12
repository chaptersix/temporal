# Indexed queue equivalence protocol

Status: exact reducer contract and implementation obligations, not an implemented index or accepted complexity proof. Reference fingerprints accompany independent-index-design-review.md.

## Authoritative state and eligibility

Assign each queued record an immutable monotonically increasing sequence, preserving slice order independently of nominal/actual timestamp. Store concrete policy, manual/backfill identity, exact nominal/actual time, workflow identity. An automatic entry has both command IDs zero. An explicit entry has either nonzero. Preserve accepted policy: root policy updates do not rewrite queued records. Unspecified manual/backfill policy resolves against the current root at dispatch, then defaults to SKIP; once queued it is concrete. Pending commands retain their existing dispatch-time resolution behavior.

Drain is disabled while BlockingOperationID is nonempty or catchup is negative. Otherwise eligible(q) is explicit(q) OR (not paused AND (not limited OR remaining>0) AND actual(q)>=now-catchup). The final inequality is mathematical time arithmetic, including future timestamps; it must implement the current `now<=Actual || now-Actual<=catchup` relation without overflow. Explicit wide timestamps are never accidentally compared using their zero nanosecond placeholder. A root update changes automatic eligibility but must not eagerly erase queued entries: erasure occurs only on the corresponding drain transition, and a later update before drain can restore eligibility.

When no eligible waiter exists, drain removes every queued entry even if operation capacity is unavailable. When an eligible waiter exists but operation capacity is unavailable, drain changes nothing, including ineligible entries. Missing prepared request fails before any publication. These ordering cases are part of equivalence.

## Exact selection and removal

Let E be eligible records in sequence order, f its first member, and C its members with CANCEL_OTHER or TERMINATE_OTHER. Select max-sequence(C) if C is nonempty; otherwise select f. After successful admission:

- Remove all ineligible records, including those after the selected record.
- Remove selected and issue exactly its identity using the current prepared request.
- Remove f if f precedes selected.
- Remove each eligible effect-policy record preceding selected.
- Retain all other eligible records, in original sequence order.

OverlapSuperseded increases by the size of the union of the third and fourth removal sets, never double-counting f. Public MissedCatchupWindow increases only for expired automatic records removed while automatic scheduling is enabled; paused/limited invalidation is not reported as missed catchup. QueueCount decreases by all removed members. Queue-to-operation transfer leaves occupancy unchanged; policy supersession/expiry decreases it. Action budget/operation admission changes come from the existing startOperation transition exactly once.

Concrete trace: [ordinary A, ordinary B, cancel C, ordinary D, terminate E, ordinary F] all eligible selects E, supersedes A and C, retains [B,D,F]. If A is ineligible, first eligible B is superseded instead. If no effect-policy member is eligible, select first eligible only. These are mandatory full-state reference cases, not only selected-ID tests.

## Timestamp and key encoding

Sequence ordering is uint64 big-endian. Time ordering uses signed int64 seconds with sign bit flipped, uint32 normalized nanoseconds, then uint64 sequence: 160 bits, 40 nibble levels. Normalize nanosecond-model fields with floor division (negative remainder corrected), not truncation toward zero. Explicit timestamps use their exact seconds/nanos. Lexicographic byte order equals chronological order, with sequence tie breaker. RFC3339 strings and UnixNano conversion are not suitable ordering keys for this domain. Include pre-epoch fractions, year1/year9999, identical times and maximal sequence tests. Exhaustion fails before changing any owner/index.

## Atomic index protocol and unresolved bound

At one logical transition, read root eligibility inputs and capacity; compute exact selected key and removal counts; prepare issuance/payload changes; publish queue roots/counts, operation admission, counters and detached cleanup descriptors atomically. Root revisions and concrete leases fence stale preparation. Failed preparation/publication preserves all roots, records, refcounts and dedup state.

Indexes must answer: minimum sequence among explicit and currently eligible automatic entries; maximum eligible effect-policy sequence; count/removal of all automatic entries below the actual-time threshold; count/removal of eligible effect entries before selected; and removal of the first eligible point. Explicit and automatic classes are fixed finite partitions, as are policy classes. However, automatic eligibility is a time suffix while replacement is a sequence prefix. They are independent dimensions: time need not increase in admission order under jitter/backfills/updates.

**Open implementation blocker:** separate time and sequence radix trees with cross-links do not establish constant work for these combined operations. Detaching a time prefix can invalidate arbitrarily many sequence leaves; detaching a sequence range can invalidate arbitrarily many time leaves. Lazy tombstones with “skip dead frontier entries” merely defer an unbounded scan unless there is an independently bounded repair protocol with preserved selection. A time tree augmented with sequence extrema answers selection but does not by itself support bounded sequence-prefix removal and aggregate updates.

Before implementing that representation, provide the concrete multidimensional range-summary/detachment algorithm and derive worst-case page touches for both operations, including stale secondary memberships, or choose a different representation with the same exact atomic result. Fixed 160-bit key width alone does not prove a practical bound for arbitrary rectangle queries. Do not claim the draft's I*C_paths*F*(H+1) formula until this cross-index update obligation is discharged. A bounded preparation protocol may replace atomic physical index maintenance only if its durable snapshot, concurrent arrivals/updates, admission point and fairness refinement are explicit; partial erasure cannot become visible as partial logical replacement.

## Cleanup ownership and reference checks

A detached subtree has exactly one durable cleanup owner (generation, root key, cursor). Logical membership and capacity counters exclude it immediately. Cleanup visits a fixed page/record batch, releases each distinct payload owner once, and deletes only unreachable pages/blobs; a callback/duplicate task cannot resurrect detached intent. Queue records currently contain identity metadata rather than frozen requests, so queue detachment normally releases no issuance blob; operation/request ownership remains separate. Reused blob content must remain alive while any current definition, generator, unresolved issuance or carryover owner references it. Cleanup debt may consume storage and is not a silent configured queue cap. Close fences issuance first; cleanup never grants action authority.

Reference tests should compare complete state/counters after every transition under updates, catchup changes, paused/limited states, explicit wide commands, all policies, capacity reduction/nonpositive limits, callback-before-ack and reload. Then test fixed visit counts at N=4,5,1000 and larger with adversarial inverse time/sequence ordering. Include interrupted cleanup and failed publication. No benchmark or finite proof can substitute for these representation obligations.
