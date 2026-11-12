# WI-015 independent definition and catchup boundary review

Reviewer: api_seam. Scope: parent changes in occurrence.New/validateBounds,
generator.configureGeneration/admitGenerated/catchupDeadline, and boundary tests.
This is not approval of this reviewer's public mutation implementation.

No blocking defect found **within the retained execution domain**. Source checks:

- StartTime and EndTime still require valid protobuf timestamps; accepting epoch or
  far-future bounds does not accept malformed nanos/seconds.
- The original canonical spec is preserved for compilation, identity and returned
  public definition. The lower bound is not replaced by the execution-domain minimum.
  Therefore a2024 cursor against an epoch1970 lower bound correctly has FullRange=false,
  rather than falsely claiming a full definition scan. The epoch regression explicitly
  checks this and verifies unchanged caller proto plus an emitted occurrence.
- Effective enumeration end remains the minimum of the supplied end and
  2100-12-31T23:59:59.999999999Z. Begin/Run/update/continuation validation still constrain
  execution cursor and time to [2000,2101), apart from the explicit predecessor cursor.
  Jitter beyond this horizon remains rejected. The change does not serialize year9999
  into a nanosecond cursor or silently widen the finite verification domain.
- Catchup configuration still rejects negative durations, but no longer rejects valid
  windows merely because they exceed365d. Stored eligibility deadline uses saturating
  addition: when actual > MaxInt64-window it stores MaxInt64, otherwise actual+window.
  With the validated nonnegative duration this avoids overflow and preserves eligibility
  for all representable observation times. The helper's nonnegative-window precondition
  comes from configuration validation. Exact boundary, overflowing duration, negative
  epoch and maximum-duration configuration tests exercise the arithmetic.
- Coordinator eligibility uses supported-domain timestamp differences; this patch does
  not broaden those clocks to arbitrary signed nanosecond endpoints. Deadline saturation
  is not a promise to execute outside the supported occurrence domain.

## Explicit remaining boundary

Valid public timestamps can now describe bounds outside the execution horizon, but
runtime continuation/backfill support remains bounded. A start after2101 may have valid
real occurrences yet no occurrences inside this domain; bounded Impossible/Exhausted
results must not be described as proof of global impossibility. Current idle logic can
close a schedule after bounded exhaustion, so full future-date compatibility is not
established by accepting its definition. Do not claim wider-domain public execution or
full shared-suite/M3 acceptance from this fix. Resolving the larger domain belongs to
its own specified representation/verification work, not an accidental UnixNano cast.

## Executed verification

`CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2SharedSuite$/TestListScheduleMatchingTimes$' -count=1 -timeout 3m`
passed1.213s. `/private/tmp/scheduler2-wi015-shared-matching-fixed.log` now establishes
that the exact shared epoch-StartTime case which previously failed Create reaches and
passes its public MatchingTimes assertions.

`CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2 -run
'^Test(PublicPatchGenerationTokenExhaustion|CatchupDeadlineSaturatesWithoutWrapping)$'
-count=1` passed0.645s, `/private/tmp/scheduler2-wi015-patch-token-catchup.log`.
The token test is this reviewer's helper regression, not independent review evidence.
Parent separately reports full occurrence package pass1.051s; parent owns that command.
