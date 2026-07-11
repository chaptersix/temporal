# Plan 3 Repeated-Abuse, Cache, Retry, And Memory Results

OBSERVATION: Campaign 20260711-plan3-operational-red-team-b960841d9-repeated-abuse ran sequential identical expensive requests, ten concurrent callers on one spec, callers distributed across expensive specs, alternating cheap/expensive requests, and repeated expensive invalid validation in explicit cached and uncached modes.

OBSERVATION: Cold and warm execution are separate. Cache hits retain logical validation/matching work while executed work is reported separately; cached and uncached semantic results remained equal.

OBSERVATION: Benchmem and the retained heap profile cover maximum result slices, validation proof buffers, many calendars/ranges, large bounded timezone data, rapid cancellation, and repeated validation with and without cache.

NEGATIVE EVIDENCE: The finite repeated workloads stayed within the documented memory plateau bounds, cancelled buffers were released, and partial errors retained no excess result-slice capacity.

INFERENCE: An immediate identical retry after deterministic validation or matching budget exhaustion is non-progressing. A changed query/result limit or larger phase-appropriate analysis budget can be actionable.

RECOMMENDATION: Keep cache behavior explicit and test-only. Select no production cache, retry policy, work guard, or memory guard in Plan 3.
