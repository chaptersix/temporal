# Independent prepared timezone review

Reviewed steps 1–3 against the frozen seven-file manifest, prepared-timezone-contract.md, prepared-calendar-contract.md, surrounding calendar evaluator, and local Go 1.27.3 time source. All seven source hashes matched. The independent full occurrence package command recorded in independent-source.json passed in 1.068s; output is independent01.log.gz. Go reference hashes are preserved alongside the reviewed source hashes.

No blocking defect found within this unselected, immutable in-memory adapter scope. This is not acceptance of persisted timezone storage, named resolution, or scheduler runtime routing.

## Semantic checks

The parser retains positional unsorted/duplicate transitions, signed offsets, byte-index validation, v2/v3 second counts, ignored leap/standard/UTC metadata, empty-transition synthesis, and all four before-first-zone choices. Its uint64 byte preflight bounds allocations by present input without inventing a public input cap. Names and flags omitted from hot state affect only preparation; the adapter promises calendar offsets and civil conversion, not timezone-name or IsDST APIs.

Cache construction reproduces Go loader first-adjacent-interval selection and extension substitution; lookup consults cache before the first-transition check and exact midpoint/end-tracking search. The cache tuple is owned, immutable and fingerprinted. The explicit resolvedAt input must still be supplied by the future resolver outside mutation; independently loading Go does not provide exact control over its hidden runtime clock. The unsorted fixture separately demonstrates the cache-sensitive output and fingerprint change.

POSIX parsing preserves invalid-extension fallback, comma-terminated standard-only acceptance, default DST rules, semicolon delimiter, signed 168-hour rule times, quoted names, Julian/day-of-year/month-week-weekday forms and southern swaps. Evaluation retains signed pre-epoch remainder and Go's 365-day terminal interval. Date preserves the second-lookup condition and final offset subtraction; wall conversion uses UTC-only field extraction. The one-hour first-copy correction compares all civil fields. Calendar-supported input plus signed32 offsets remains far from int64-second overflow; arbitrary int64-extreme public conversion semantics are not claimed.

## Work and failure checks

Hot lookup has two endpoint reads plus logarithmic midpoint reads, bounded by 66 fixed16-entry pages. No parser or transition/name scan remains in hot conversion. Rule week iteration is at most four. Worst calendar normalization performs two Date operations and four wall conversions: at most eight lookups and 528 addressed reads per phase. Counts include failing reads. These are work bounds, not serialized-byte or latency measurements.

Addressed reads validate identity, ordinal and expected page length. Private pages and copied parser output avoid caller-owned mutable backing. Full forged semantic-content validation is outside the trusted immutable representation contract. Advance mutates a local value copy and returns the original incoming continuation on any phase error, even after earlier phases succeeded. Existing fault tests fail each addressed read in a multi-phase call from a noninitial continuation and require exact original-state equality; the gap witness executes Date's second lookup.

## Remaining evidence limits

Source review plus targeted differential tests are not exhaustive arbitrary-TZif parity. The existing matrix does not exhaust every proposed unusual extension/name and extreme transition boundary combination. Persisted header/page encoding, immutable ownership across reload, bounded installation/cleanup, named source order/Local/cache refresh, physical failure recovery, and complete transaction byte/work measurements remain required subsequent gates. The current in-memory fault reader does not establish CHASM persistence fault recovery. No source was changed by this review.
