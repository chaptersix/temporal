# Public timestamp domain reconciliation

## Source and baseline

Frontend validates public timestamp syntax with protobuf Timestamp.CheckValid. The TriggerImmediately.ScheduledTime API comment explicitly uses the timestamp for target workflow identity. V1 processPatch honors it under the existing TriggerImmediatelyTimestamp version; old CHASM NewImmediateBackfiller/processTrigger instead uses creation time. That is a named existing CHASM defect, not the new contract to copy.

The initial three-backend executable baseline showed V1 and old CHASM accept1999 and explicit-year0100 interval backfills. V1 preserves manual ScheduledTime2500; old CHASM executes it with current2026 metadata/identity instead. Scheduler2 rejected all three due prototype bounds. baseline-references.log retains these observed failures. baseline-order-assumption.log also retains the first draft's incorrect assumption that RecentActions under parallel ALLOW_ALL must be ordered by nominal time; the corrected test compares the exact set of times, because RecentActions is ordered by actual start observation. This did not change which occurrences were required.

The final regression uses V1 as authoritative reference for timestamp identity and scheduler2 as implementation under test. It adds year0001-Jan2 interval backfill and manual9999 maximum timestamp, requires exact scheduled times including nanoseconds, and checks timestamp-derived workflow IDs. Both backends pass all five scenarios (ten leaves) in8.074s. Old CHASM is not silently claimed to pass; its baseline discrepancy remains explicit.

## Representation and semantics

Nanosecond int64 fields remain unchanged for representable instants. Optional protobuf Timestamp fields carry seconds/nanoseconds only where UnixNano would wrap: manual command, queued occurrence, operation nominal time, candidate nominal/actual time, continuation origin/search/disposition boundaries, and backfill bounds. The reducer uses a value Timestamp with an explicit presence bit, avoiding mutable protobuf aliases and correctly representing Go's zero time (year0001). Decode selects the explicit representation when present. Retry deadlines, physical wakes and the actual execution clock remain in their existing nanosecond domain; this change is about accepted public definition/range/identity timestamps.

Manual/queued metadata survives admission, durable reload, request search attributes and recent-action projections. Wide backfill identities and lease keys use RFC3339Nano; representable/model identities retain their original decimal-nanosecond form. An incidental normal-key formatting change was caught by the unchanged saved coordinator replay and repaired; internal-replay.log shows the unchanged fixture passes.

Public occurrence validation accepts protobuf years1..9999 and the single immediately preceding internal cursor needed for an inclusive minimum bound. Public nil StartTime uses epoch, matching the reference GetNextTime clamp; explicit pre-epoch StartTime can produce earlier interval matches. Nominal enumeration still stops above the existing reference2100 horizon. Structured-calendar year validation remains2000..2100; accepting a future window does not invent calendar support beyond that reference. A public range2500..9999 is accepted and exhausts without actions. The internal New constructor keeps the original2000..2100 model domain and original definition/scan limits; only NewPublic chooses the expanded input domain. No wrapped integer stands in for a public timestamp.

The minimum-minus-one-nanosecond continuation cursor is an internal sentinel, not an externally accepted protobuf timestamp. Serialization preserves it exactly; public ingress still rejects invalid protobuf timestamp values. The existing raw compiled-spec zero-time sentinel is retained, so no claim is made that the year0001 midnight instant itself becomes a new calendar occurrence where the reference treats it as empty. Manual identity timestamps are preserved independently of calendar enumeration.

## Checks and limits

- Actual V1/new functional differential:1999,0100,0001 interval backfills;2500 and9999 manual identities and exact subsecond metadata.
- Continuation serialize/restore after each candidate:years1,100,1677,1999,2100; empty2500/9999 ranges. No duplicate/lost candidate across restores.
- Queued manual2500 then9999:component publication/reload, retained operation timestamp, generated request search attribute and recent actions all exact.
- Empty future backfill2500..9999:accepted durable command settles without operations or action count.
- Representation extremes:year1 minimum, internal sentinel just before it, year9999 maximum; no ns wrap.

Boundary tests pass0.650s; original internal coordinator replay passes0.653s. Existing bounded Lean/WI013 correspondence is not evidence for these new wide public serialized fields or arbitrary full-domain workloads. Finite model semantics remain selected by internal New; source fingerprints from earlier accepted proof runs remain historical. Parent owns consolidated regression/lint, independent review and acceptance. Exact commands and final source hashes are in manifest.json; no central ledger or plan changes were made here.
