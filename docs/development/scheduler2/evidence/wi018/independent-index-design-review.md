# Independent scheduler index design review

Reviewed draft WI-018 and bounded-public-work.md against current reducer, public projections, capacity retention and payload owners. This is a design review, not implementation acceptance, benchmark evidence or a proof of fixed work. No runtime source changed.

<details><summary><strong>med</strong> — Transition-driven List preparation still compiles and clones the full definition.</summary>
`publishPublicTransition` calls `publishPublicList` on ordinary acknowledgments/callbacks. That helper clones the entire spec before truncation, and `publicListProjection` calls `publicFuture`, which invokes `NewCompiledSpec` and full union searches. The proposed incremental `currentEnumerator` replacement does not itself remove this independent hot path. A callback can therefore remain proportional to unrelated definition size even with indexed queue and operations.
</details>

**Suggestion:** Include stored bounded List spec preparation and future-preview maintenance in the revisioned incremental definition protocol. Specify when the exact preview is refreshed as clock/cursor/config changes; preserve live compute-limit semantics and required public output. Instrument both helpers on an unrelated callback with a large accepted definition. Do not classify their mutation-side work as full Describe output cost.

<details><summary><strong>med</strong> — Counting terms alone does not bound work within an accepted calendar term.</summary>
The public profile accepts arbitrary range counts. `makeYearMatcher` expands ranges into a slice, retaining duplicates, and each match scans that slice. `iterateRanges` also expands every input range during compilation. A single term containing many repeated year ranges therefore defeats a fixed terms-per-quantum budget. A term page must not contain an unbounded raw calendar representation that is decoded or compiled on each access.
</details>

**Suggestion:** Normalize into fixed-domain bitsets during request preparation, or page range preparation with explicit range/expanded-value counters and a bounded compiled representation. Include timezone data and compiled year matching in the byte/work formula. Keep validation proportional to accepted input explicitly outside repeated mutation preparation; do not add an undocumented public range ceiling.

The queue selection paragraph correctly retains ordinary intervening waiters. See queue-contract.md for exact semantics and a remaining multidimensional index proof obligation. Separate time and sequence frontiers alone cannot establish bounded simultaneous expiry and replacement updates.

Capacity's correct equality is occupancy = queue count + unfinished + retained completed-operation membership (at most ten), not occupancy = configured limit. Admission compares occupancy with a live positive limit; reduction may leave occupancy above it, while nonpositive disables that comparison. Acknowledged ALLOW_ALL becomes StartOnly and leaves occupancy; tracked completion retains occupancy until canonical-close-time eviction. Recent-start ordering and completed membership are separate, with stable tie order and self-eviction preserved.

Reference counting must count distinct typed blob IDs per reachable owner, including both current public definition and generator definition, each immutable unresolved issuance, and carryover. Same blob shared across generations increments owner count; callback-before-ack, ambiguous starts and failed publication must not prematurely release it. Range-detached records may retain physical references until cleanup, but must be excluded from logical occupancy immediately. Document physical-retention debt separately.

The fixed dispatcher proposal is appropriate only with explicit concurrency and fencing: delivery snapshots one operation/wake/request; readiness selection must reserve or fence it before releasing lock, preserve independent ALLOW_ALL starts, and avoid reissuing one minimum deadline forever. Duplicate physical envelopes must not duplicate logical authority. Tests must include callback-before-ack and out-of-order stale replies across dispatcher reuse.

No claim that existing Lean finite model proves new indexed storage, cleanup, collection behavior or public-wide timestamps. Required gates remain reference equivalence, exact node/page/byte counts including CloseTransaction, rollback/reload, poisoned unrelated DATA plus clean-node visits, nil-library reads, and physical recovery. Cold topology reconstruction and full response/export remain separately reported costs. Draft is suitable for staged investigation after the two findings are incorporated; final schema/index count proof remains open.

Source fingerprints are in independent-index-design-review-hashes.json. No tests run for this read-only review.
