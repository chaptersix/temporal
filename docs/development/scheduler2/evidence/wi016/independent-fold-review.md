# WI-016 London fold review and authorized repair

## Independent diagnosis

Reviewed the failed routine02 Rapid trace, the direct bounded UTC-second reference,
its pinned London TZif, and the actual `compiledCalendar.next` implementation before
changing production. The oracle expectation is correct; this is a concrete shared
calendar defect. There is no unresolved contract choice.

For calendar year 2024, October 27, Sunday, local hour 1, minute 0, second 30,
with zero jitter and cursor `2024-10-27T00:00:00Z`, the earliest match must be
`2024-10-27T00:00:30Z`, followed by `2024-10-27T01:00:30Z`. Literal conversion gives:

| UTC | London local | Offset |
| --- | --- | --- |
| 2024-10-27 00:00:30Z | 2024-10-27 01:00:30 BST | +01:00 |
| 2024-10-27 01:00:30Z | 2024-10-27 01:00:30 GMT | +00:00 |
| 2024-10-27 02:00:30Z | 2024-10-27 02:00:30 GMT | +00:00 |

Both Go `time.LoadLocationFromTZData` and Python `zoneinfo.ZoneInfo.from_file`
independently produce this mapping from pinned TZif SHA256
`c85495070dca42687df6a1c3ee780a27cbcb82f1844750ea6f642833a44d29b4`.
No production next-occurrence search was used to obtain these expected instants.
Python supplies a separate timezone implementation for this diagnostic; the general
SP7 reference still names Go timezone conversion as a shared primitive.

A standalone program invokes the real public `NewBoundedCompiledSpec` and
`NextTimeCandidate` directly, bypassing scheduler2 and its property harness.
Before repair it returns UTC 01:00:30 from cursor 00:00, skipping the first copy.
From second-copy cursor UTC 01:00 it returns UTC 02:00:30, whose local hour is 2
and does not even match the specified hour 1. This independently confirms both
loss and an invalid extra candidate, not an error in window collection, jitter,
source bounds or the expected-stream generator.

The local `time.Date(2024,10,27,1,0,30,...London)` chooses the **later** GMT copy.
`cc.next` treated its result as the first copy and then added `dstoffset=1h` when
traversing the second copy. Its hour-carry comparison made the same first-copy
assumption. US tests passed because time.Date chooses the earlier copy in those
fixtures. The violated contract is `next`'s own earliest matching instant after
cursor guarantee, not a new policy for ambiguous timestamps.

## Exact minimal replay and retained failure

`calendar-fold-fix/minimal-replay.json` contains one explicit structured match,
zero jitter, one window, budgets 1/1, pinned timezone bytes, and the two literal
expected UTC candidates above. It independently fails the standalone saved replay
against the archived original calendar source using Go's build overlay, and passes
against the fixed source. The overlay substitutes a read-only archived source file;
it does not mutate the checkout or change the test oracle. Exact commands and exits
are in `minimal-replay-commands.json`; both logs are retained. The original larger
Rapid-shrunk fixture remains unchanged at
`chasm/lib/scheduler2/internal/occurrence/testdata/spec_boundaries_fold_regression_v1.json`.

`minimal-public-candidate.go.txt` is the standalone public primitive reproducer;
copy it to a temporary `.go` file and run it from the repository root with
`CGO_ENABLED=0 GOMAXPROCS=4 go run <file>`. `minimal-before.log` and
`minimal-after.log` record the direct primitive outputs. `calendar-before.go.txt`
and its SHA256 retain the exact original implementation for the overlay reproduction.

## Authorized repair, distinct from independent review

After the independent diagnosis, the parent authorized this reviewer to implement
`service/worker/scheduler/calendar.go` and a dedicated legacy regression file.
Consequently **this report does not independently approve the resulting fix**;
proof_review and the parent own that non-author review.

The targeted `calendarDate` helper resolves a one-hour ambiguous wall time to its
first physical copy: it accepts the instant one hour earlier only when year,
year-day, hour, minute and second are identical. This is independent of which copy
time.Date initially chose. Both hour-carry comparison endpoints and the final
candidate construction use that normalized base; the existing dstoffset traversal
then selects the second copy exactly once. Unambiguous times and time.Date's
nonexistent-hour normalization are retained, so the existing gap-skip check remains.

D23 requires strict nominal advancement and exact inclusive window semantics.
D25 explicitly requires gap skip and **both** repeated-hour instants; general
historical non-one-hour transitions remain its stated unproved boundary. This
repair enforces those existing one-hour fold semantics. It does not broaden the
calendar/timezone contract or claim arbitrary historical-offset correctness. The
shared calendar primitive is used by legacy and scheduler2; correcting its lost/
nonmatching candidates intentionally affects both. No fixture expectations were
weakened to preserve the defect. No routing, deployment or migration occurred.

## Validation and remaining integration gate

- New `TestCalendarFoldOrdering` failed on original London source and passed after
  repair. It checks eight literal physical candidates in each London and New York
  fold and cursor neighbors -1ns/exact/+1ns, asserting earliest result, strict
  advancement and actual local-calendar membership. These legacy tests use named
  host zones following that package's existing conventions; reproducible London
  evidence comes separately from the pinned-TZif golden and saved replay.
- New gap regression checks London and New York spring gaps. The final command
  `CGO_ENABLED=0 GOMAXPROCS=4 go test -p 2 -parallel 4 -tags test_dep -count=1
  -timeout=5m ./service/worker/scheduler -run '^Test(Calendar|Spec)' -v` passed,
  covering existing legacy calendar/spec tests and the new regressions.
- Frozen `TestSpecBoundaryLondonFoldGolden`, the unchanged shrunk saved replay,
  and this review's minimal saved replay pass on fixed production. Initial
  golden/Rapid failure logs remain parent/proof_review evidence; this directory
  retains the independently reproduced original-source failures.
- `make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false`
  initially failed with 20 findings across concurrently edited files. This author's
  two findings (nextTs casing and test import grouping) were corrected without
  semantic changes before the final legacy test run. The other owners were notified
  of their findings. The initial failed lint log is retained; final shared lint and
  expanded SP7 exploration remain integrator/proof_review evidence, not claimed by
  this initial repair report.

The accompanying manifest records source/test/fixture hashes, tool versions and
all archived evidence hashes. This is a defect diagnosis plus implementation and
scoped validation report, not WI-016 or M3 acceptance.
