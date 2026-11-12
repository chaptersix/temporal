# WI-016 independent calendar fold fix review

Reviewer: proof_review, author of the new SP7 oracle/property tests, **not** author
of the production helper or legacy regression tests. Read-only review of those
production changes against D23/D25, existing calendar.next callers and literal
pinned physical instants. This does not self-certify the SP7 oracle implementation
or WI-016 acceptance.

Frozen reviewed sources:
- `service/worker/scheduler/calendar.go`: `479689bac6ee9d23ddf4e721d174a6578781fd1008cb56b4040981dc04dbd97d`
- `service/worker/scheduler/calendar_fold_regression_test.go`: `bcacf90d4e9903eb00e85310baf59b23628d4a59a8cd0ba57a5cf3402274a643`

## Behavioral finding and correction

Before the fix, the pinned London one-hour fold disproved next-occurrence
completeness: after2024-10-27T00:00:00Z, local01:00:30 occurs first at00:00:30Z(BST)
and again at01:00:30Z(GMT). Production time.Date selected the second copy and skipped
the first. A second-fold cursor also risked adding the existing dstoffset to an
already-late base. Both are correctness defects under D25's both-fold-instant
contract. Literal UTC/local mapping assertions, a simple public compiled-calendar
reproduction by oracle_review, and standalone minimized replay establish this
without treating a majority of implementations as the oracle.

The helper chooses result−1h only when year/day-of-year/hour/minute/second all equal
result's local fields. Thus its scope is exactly an ambiguous one-hour wall time;
it never subtracts an hour merely because a timezone has daylight saving. It makes
the previously implicit earliest-copy premise of dstoffset explicit.

Reviewed all three changed call sites:

1. Hour-carry `prev`: normalizing the repeated hour to its earliest physical copy
   lets leaving the first copy measure the two-hour physical interval to the next
   wall hour, correctly triggering the second-copy traversal.
2. Hour-carry `next`: normalizing the destination prevents entry into a repeated
   hour from spuriously measuring two hours and replaying the preceding wall hour.
3. Final candidate: earliest base plus existing dstoffset selects first/second copy
   consistently for London (time.Date selected late) and New York (selected early),
   including cursors already in the second copy. The second-copy offset is no
   longer applied twice.

Strict advancement at integer-second resolution retains the existing seconds+1
step; −1ns/exact/+1ns literal cursor neighbors in the new legacy test cover both
fold copies. Pending actual/nominal identity and exclusion handling are outside
this helper and unchanged. Both ambiguous copies have the same civil date, so the
unchanged weekday check's time.Date choice does not alter their date eligibility.

For the selected one-hour spring gaps, time.Date normalization and the existing
`nextTS.Hour()!=h` skip check remain in place; the helper does not invent a missing
local instant. Legacy gap regressions and pinned NY gap-neighbor literals passed.
The inherited one-hour traversal is not a general proof of historical non-hour
transitions; D25 retains that explicit library boundary.

## Evidence inspected

- Initial minimized failure: `/private/tmp/scheduler2-wi016-routine02.log`.
- Literal failure without generator: `/private/tmp/scheduler2-wi016-london-golden-initial.log`.
- Standalone explicit JSON failure: `/private/tmp/scheduler2-wi016-fold-standalone-initial.log`.
- Post-fix new SP7 routine100 seed1:1.915s; exploration1000 seed17:9.090s;
  exploration10000 seed29:57.090s. Full occurrence package final post-style routine100
  seed1:1.049s. Parent archives logs and final command/source provenance.
- Legacy regression tests assert8 literal physical candidates per London/NY fold,
  strict advancement/matching at cursor neighbors, and both one-hour gap cases.
  They use host zones and are supplementary; pinned SP7 fixtures supply byte-level
  reproducibility. No claim that host-based legacy tests alone satisfy SP7.

No blocking defect found in this targeted one-hour fix. Parent retains independent
review of my oracle/tests, mutation sensitivity and final work-item acceptance.
