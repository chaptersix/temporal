# Initial listing and ScheduleId query compatibility

New tests/schedule2_listing_test.go adds two actual selected-scheduler2 frontend tests.
No existing helper or old suite was modified. Each created schedule is read through its
typed scheduler2 reference and Admin DescribeMutableState verifies actual persisted
archetype identity; there is no fallback or per-test skip.

TestSchedule2ListBeforeActionRun extracts the public assertions of V1 TestListBeforeRun:
non-nil Info, exact spec, workflow type, unpaused state, more than one future action time,
and first future action after request start. The original fixture disables V1's per-NS
scheduler worker so the backing workflow cannot initialize its own metadata. Scheduler2
has no such worker/workflow. The port instead withholds downstream action Start RPCs and
additionally asserts ActionCount0/no recent actions, proving public metadata exists
before action execution. This is the applicable public guarantee, not a claim scheduler2
has passed a V1 backing-workflow initialization experiment. The schedule remains unpaused
with the same3-second interval. Actual Create/List/Describe and persistence are exercised.

TestSchedule2ListingWithSearchAttributes preserves the AdvancedVisibility scenario:
ScheduleId equality and IN select the schedule's synthetic alias; register a custom
keyword ScheduleId through Operator API and wait for registration; create a second
schedule with a distinct custom value; equality and IN now select that custom value.
Original page size1, expected IDs, and query assertions remain. Context/polling use the
standalone testing.T backend pattern. The worker queue comes from the fixture instead
of the original unused literal default queue.

## Memo rejection disposition

ScheduleV1Suite.TestUpdateScheduleMemoRejected specifically asserts FailedPrecondition
"memo updates are not supported on workflow-backed schedules". Frontend UpdateSchedule
performs common validation, then selected scheduler2 dispatch or old CHASM routing,
and only afterwards rejects memo updates on the V1 workflow-backed branch. Old CHASM
Scheduler.Update calls ReplaceCustomMemo on non-nil memo. Scheduler2 UpdatePublic does
the same; D59 selects that CHASM behavior. Therefore copying the V1 rejection assertion
would assert a different backend's deliberate limitation. No rejected-memo test is marked
passed or silently skipped here. Existing TestSchedule2UpdateScheduleMemo is the positive
public capability witness (replace, nil/nochange, empty/clear); it is rerun separately.

## Evidence

`CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2(ListBeforeActionRun|ListingWithSearchAttributes)$' -count=1 -timeout 3m`
PASS2.190s. No production fix was needed. Exact source/log hashes in manifest.json.
Parent owns combined lint and milestone bookkeeping; no central ledger was edited.

Memo capability rerun: `CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run
'^TestSchedule2UpdateScheduleMemo$' -count=1 -timeout 2m` PASS1.130s.
