# Independent wide timestamp review

Reviewer: proof_review; implementation author: oracle_review. Reviewed timestamp changes only, not my own capacity/projection counter changes in shared files. No blocking behavioral finding on the frozen timestamp source. Parent owns acceptance.

All22 source fingerprints in `timestamp-domain/manifest.json` matched current files; independently recorded in `independent-timestamp-review-hashes.json`. Read the reference source, final regression source, baseline discrepancy account and final ten-leaf functional log (PASS8.074s). Independently executed boundary/reload and unchanged saved coordinator replay checks: PASS0.634s (`timestamp-domain/independent-review.log`). Exact command:

```
CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep -p 2 ./chasm/lib/scheduler2 -run '^Test(Public(Timestamp|WideManual|EmptyFuture)|CoordinatorSavedReplay)' -count=1 -v
```

## Reviewed obligations

- `encodeTime` tests exact roundtrip before retaining int64 nanoseconds; overflowed instants use explicit seconds/nanoseconds with a presence bit. Reducer timestamp values do not alias protobuf pointers. Manual commands, queued occurrences, operations, range bounds and continuation candidate/search/disposition values preserve that representation through publication and reload.
- Manual admission identity uses the command sequence, while workflow ID derives from the exact supplied date. Wide backfill occurrence and coordinator lease keys use RFC3339Nano. Representable instants retain original decimal-nanosecond keys; the unchanged replay fixture verifies this compatibility. Request search attributes and public schedule-time projections decode the explicit representation, including fractional nanoseconds.
- Wide historical commands carry ManualID/BackfillID, so `queueEligible` deliberately bypasses automatic catchup comparisons; their nanosecond fallback zero is not mistaken for epoch in those comparisons. Retry deadlines derive from the actual execution clock, not requested historical/future nominal time.
- Automatic generation still uses nanosecond wakes/catchup. This does not wrap the newly accepted wide manual/range identity domain: automatic nominal generation retains the existing2100 horizon, and reference `CompiledSpec.addJitter` caps offset at uint32 milliseconds (under50days). Historical wide candidates run through the separate backfill dispatch. This review does not expand supported physical execution-clock years.
- `NewPublic` uses epoch for nil StartTime, matching reference `GetNextTime`; explicit pre-epoch StartTime can yield earlier interval occurrences. Accepting a protobuf-valid future range settles empty past the existing nominal2100 horizon. Structured-calendar validation remains2000..2100. The implementation does not reinterpret acceptance of year9999 as calendar generation there.
- Inclusive minimum year0001 needs an internal cursor one nanosecond before the minimum. This cursor is intentionally outside external Timestamp.CheckValid and is documented as an internal sentinel. Existing raw compiled-spec Go-zero-time exhaustion remains; the code does not claim a new occurrence exactly at year0001 midnight when the reference treats zero as exhaustion.
- Internal `New` leaves `public=false`; supported-window and continuation validation retain the finite original domain and budgets. Existing Lean/WI013 proof artifacts are historical source evidence and do not prove the new public wide fields. The author explicitly states that boundary rather than silently extending theorem claims.

## Evidence boundaries and minor test note

The functional differential uses V1 for ScheduledTime authority because the recorded old CHASM baseline ignores manual requested timestamps; the reference discrepancy is retained, not averaged away. It checks exact occurrence sets and workflow IDs; it correctly does not require parallel ALLOW_ALL acknowledgments to appear in nominal order. Unit tests separately inspect generated request search attributes and queued reload.

No new general malformed-persisted-state proof, migration guarantee, arbitrary-century execution-clock support or wide-domain Lean correspondence is established. Public input validation remains the boundary; optional stored timestamp fields are trusted persisted application records. Inner functional subtests currently serialize without an explicit parallelism rationale; this is a test-guideline documentation issue, not a timestamp semantic blocker.
