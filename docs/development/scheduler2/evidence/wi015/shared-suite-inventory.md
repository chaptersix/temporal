# WI-015 scheduler2 shared-suite inventory

The scheduler2 backend is now registered in `tests/schedule2_shared_test.go` and runs
all methods of the existing ScheduleSuite. The descriptor uses an explicitly dedicated
cluster with actual runtime frontend/History bindings and offline frontend archetypes.
No test-name routing, fallback, skips, reduced assertions, or unsupported-case list is
introduced. Cleanup uses the selected backend's List/Delete boundary.

Source inventory: `tests/schedule_test.go` SHA256 `20c8a9feb80eb516f8e08187f6725d7703f4f39b287978f0534883003001fc65`.
The suite currently has 29 top-level methods; nested backfill/trigger cases
remain included by the shared suite itself.

| Shared method | Initial execution status |
| --- | --- |
| TestAllowAllDescribeContract | Pending full suite |
| TestBufferSizeReportedWhenBuffered | Pending full suite |
| TestPatchTriggerRetryDedupsRequestID | FAIL: buffered count expected1 actual0 |
| TestBufferOneDeferredFiresAfterCompletion | Pending full suite |
| TestDeletedScheduleOperations | Completed without reported failure in bounded run |
| TestBasics | FAIL: immediate FutureActionTimes empty |
| TestInput | Pending full suite |
| TestLastCompletionResultAfterNoResultWorkflow | Pending full suite |
| TestLastCompletionAndError | Pending full suite |
| TestScheduleContinuesAfterWorkflowRetryFailure | Pending full suite |
| TestListSchedulesReturnsWorkflowStatus | Pending full suite |
| TestListSchedulesRecentActionsCapped | Pending full suite |
| TestUpdateIntervalTakesEffect | Pending full suite |
| TestListScheduleMatchingTimes | Initial FAIL retained; after bound fix isolated PASS1.213s |
| TestLimitMemoSpecSize | Pending full suite |
| TestCountSchedules | Pending full suite |
| TestListSchedulesPagination | Pending full suite |
| TestListSchedulesFilterAndEntryFields | Pending full suite |
| TestListSchedulesFilterByScheduleId | Pending full suite |
| TestSchedule_InternalTaskQueue | Pending full suite |
| TestCreateScheduleDuplicateSdkError | Pending full suite |
| TestUnpauseResumesProcessing | Pending full suite |
| TestPausedScheduleNeverIdles | Pending full suite |
| TestPausedEmptySpecStaysOpen | Pending full suite |
| TestUpdateScheduleRequestIDTooLong | Pending full suite |
| TestUpdateScheduleBlobSizeLimit | Completed without reported failure in bounded run |
| TestJitteredWakeupSurvivesCompletion | Pending full suite |
| TestTriggerImmediately | Pending full suite |
| TestBackfill | Pending full suite |

Bounded discovery command:

```sh
CGO_ENABLED=0 go test -tags test_dep,integration ./tests -run '^TestSchedule2SharedSuite$/(TestBasics|TestPatchTriggerRetryDedupsRequestID|TestListScheduleMatchingTimes|TestDeletedScheduleOperations|TestUpdateScheduleBlobSizeLimit)$' -count=1 -timeout 4m
```

Log: `/private/tmp/scheduler2-wi015-shared-initial.log`. This bounded run is for gap
discovery and is not full shared-suite acceptance. Existing standalone CHASM-specific
recovery, visibility, time-skipping and workflow-pause tests remain separate required
mapping/evidence obligations; adding this entry does not assert those are automatically
covered. Parent/proof reviewer own actual time-skipping framework/child propagation;
oracle reviewer owns live projections.


## Initial bounded discovery result

Command exited1 in33.241s. Full failed log archived as `shared-initial.log.gz`;
uncompressed SHA256 `c1856450c14f9d2771ec4c7f8649ec684370d79cb3925cb9712c5f673477ae72`. The failures are retained,
not converted into skips:

- MatchingTimes: Create contains a public-valid explicit epoch1970 StartTime. The
  prototype occurrence [2000,2101) validation rejects it. Parent owns range reconciliation.
- Basics: immediately after creation Describe has no FutureActionTimes. Oracle reviewer
  owns live projection implementation.
- Patch dedup: after one real running workflow and a second trigger, Describe reports
  BufferSize0 instead of1. Oracle reviewer owns live projection; the run does not yet
  establish whether further control discrepancies lie behind the missing projection.

The initial TimeSkippingConfig double-application was removed by applying it once before
root initialization, cloning the patch and removing only that field before remaining
controls. A focused regression asserts exactly one setter call, unchanged caller patch,
retained pause and initial captured transaction clock. CHASM Context.Now is captured when
constructing the context (context.go175,205); a first test incorrectly assumed changing
backend clock inside SetTimeSkippingConfig would change that captured value. The test
expectation was corrected from an artificial1h jump to the original transaction time.
Final focused command passed0.659s (`/private/tmp/scheduler2-wi015-initial-clock-fix.log`).
This unit boundary does not replace proof reviewer's actual time-skipping transport gate.

Follow-up: isolated shared MatchingTimes now passes1.213s after parent definition-bound
fix; see `definition-catchup-review.md`. Initial failed compressed log is retained.
Generator token overflow preflight was added before public Patch mutates any controls;
its regression verifies no paused/manual/data change after rejection.
