# Schedule functional coverage inventory

Inventory baseline: `352dafa83` (WI-012 setup worktree). This document records
source scenarios, not test outcomes. No functional test was run by this inventory
author, and no scheduler2 public-backend pass is claimed. Source line numbers are
baseline locators; symbols and the companion file hashes identify drifting files.
The machine-readable [inventory](functional-coverage.json) contains one explicit
row per test method/standalone entry, literal subcase, policy matrix expansion and
scenario helper, with invocation selectors, dependencies and source fingerprints.

Plan numbering: the present plan's P5.4 is Lean proof/correspondence. This inventory
implements P5.1/P5.5/P5.7 classification and supports P5.6 setup, without satisfying
P5.4 or the scheduler2 shared-suite execution gate.

## Classifications and dependencies

- **required-shared-pending**: current shared V1/CHASM scenario; scheduler2 execution
  is required before M3. Pending dependencies are blockers, not skips or passes.
- **promotion-or-related-pending**: currently backend-specific or outside ScheduleSuite,
  but public behavior needs scheduler2 adaptation. Existing-only placement does not
  make the contract implementation-specific.
- **implementation-specific**: implementation mechanism, old migration/sentinel,
  routing rollout, cache or metric regression. Preserve existing coverage; decide
  scheduler2 applicability explicitly before claiming full correspondence.
- **differential-pending**: mixed implementations with normalized observations and
  explicit exceptions. Do not silently import V1's carryover defects.
- **existing-skip**: a source skip already exists. It is not passing evidence.
- **suite-entrypoint/scenario-helper**: discovery or reusable execution body, not an
  independently completed test. Helper rows retain caller selectors.

Dependency keys: `frontend-identity` means a real scheduler2 frontend adapter and
positive component identity/fallback rejection; `typed-visibility` includes typed
List/Count, memo/search fields, eventual observation and same-backend cleanup;
`spec-projection` means validation/matching/future times; `update-clock` means public
Update/Patch and time-skipping conversion; `execution-policies`/`controls-accounting`
mean scheduled dispatch plus overlap/pause/budget observations; `request-conflict`
means public dedup/conflict errors; `carryover` means payload absence/null/failure;
`callback-chain`/`execution-callbacks` include real workflow execution, token transport,
retry/reset/continue-as-new and callback authentication; `lifecycle-history` means
public close/delete/history visibility; `capacity` means public overload disposition;
`migration-boundary` remains explicitly unsupported for scheduler2 pending M4 policy.
Contract IDs refer to C1–C11 in the plan. They identify relevant obligations, not
proof that every assertion for the contract is covered by that row.

## Invocation and evidence rules

Selectors in JSON are intended `go test -tags test_dep,integration ./tests -run`
selectors (the XDC row uses `./tests/xdc`). Use repository functional runner/setup
and its local SQLite configuration; selectors alone do not establish a running
cluster. Parent execution manifests own exact flags, environment and outcomes.
Parallelsuite parameterized discovery may add path segments: use the named entry
and method selector, and verify the actually executed test names in logs. Existing
suite root names must remain explicit after descriptor refactoring. Scheduler2
requires a separately named entrypoint and component identity evidence; no alias to
current CHASM is acceptable. No invocation here was executed for this inventory.

## Explicit scenario rows

Each row below is individually classified. All outcomes are **not run in this
inventory / scheduler2 pending** unless marked implementation-specific, entrypoint,
helper or an existing skip. See JSON for exact selectors and literal subcases.

| Source symbol | Contracts | Classification | Dependencies |
| --- | --- | --- | --- |
| [ScheduleCHASMSuite.TestPauseOnFailureCancelDoesNotPause](../../../../tests/schedule_test.go#L443) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestPauseOnFailureTerminateDoesNotPause](../../../../tests/schedule_test.go#L447) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestIdleClose](../../../../tests/schedule_test.go#L451) | C8, C10 | promotion-or-related-pending | frontend-identity, lifecycle-history |
| [ScheduleCHASMSuite.TestIdleClose/ManualOnly](../../../../tests/schedule_test.go#L451) | C8, C10 | promotion-or-related-pending | frontend-identity, lifecycle-history |
| [ScheduleCHASMSuite.TestIdleClose/PauseDuringWindow](../../../../tests/schedule_test.go#L451) | C8, C10 | promotion-or-related-pending | frontend-identity, lifecycle-history |
| [ScheduleCHASMSuite.TestIdleClose/BackfillBlocks](../../../../tests/schedule_test.go#L451) | C8, C10 | promotion-or-related-pending | frontend-identity, lifecycle-history |
| [ScheduleCHASMSuite.TestPausedBehavior](../../../../tests/schedule_test.go#L466) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestPausedBehavior/DropsCatchup](../../../../tests/schedule_test.go#L466) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestPausedBehavior/RecentActionsAdvance](../../../../tests/schedule_test.go#L466) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestPausedBehavior/FutureActionTimesAdvance](../../../../tests/schedule_test.go#L466) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestPausedBehavior/BackfillDrains](../../../../tests/schedule_test.go#L466) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestTimeSkippingWeeklySkipCreateWithPatch](../../../../tests/schedule_test.go#L482) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestTimeSkippingWeeklyAllowAllPatch](../../../../tests/schedule_test.go#L486) | C2, C3, C10 | promotion-or-related-pending | execution-policies, frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestTimeSkippingWeeklyWithWorkflowExecutionTimeout](../../../../tests/schedule_test.go#L490) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestMaxSkipCapsScheduleTimeSkipping](../../../../tests/schedule_test.go#L494) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestTimeSkippingDynamicConfigGate](../../../../tests/schedule_test.go#L684) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestTimeSkippingFeatureDisabledWhileWorkflowRuns](../../../../tests/schedule_test.go#L729) | C2, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate](../../../../tests/schedule_test.go#L850) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate/unset](../../../../tests/schedule_test.go#L850) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate/zero](../../../../tests/schedule_test.go#L850) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate/negative](../../../../tests/schedule_test.go#L850) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate/below minimum](../../../../tests/schedule_test.go#L850) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate/above minimum](../../../../tests/schedule_test.go#L850) | C2, C10 | promotion-or-related-pending | frontend-identity, update-clock |
| [TestScheduleV1Suite](../../../../tests/schedule_test.go#L906) | C10 | suite-entrypoint | frontend-identity |
| [TestScheduleV1ActionDelayMetrics](../../../../tests/schedule_test.go#L910) | C10 | implementation-specific | execution-callbacks, frontend-identity |
| [ScheduleSuite.TestAllowAllDescribeContract](../../../../tests/schedule_test.go#L1058) | C3, C10 | required-shared-pending | execution-callbacks, execution-policies, frontend-identity |
| [ScheduleSuite.TestBufferSizeReportedWhenBuffered](../../../../tests/schedule_test.go#L1180) | C3, C10, C11 | required-shared-pending | capacity, execution-callbacks, execution-policies, frontend-identity |
| [ScheduleSuite.TestPatchTriggerRetryDedupsRequestID](../../../../tests/schedule_test.go#L1243) | C3, C5, C10 | required-shared-pending | execution-callbacks, execution-policies, frontend-identity, request-conflict |
| [ScheduleCHASMSuite.TestBufferOverrunDropsActions](../../../../tests/schedule_test.go#L1307) | C3, C10, C11 | promotion-or-related-pending | capacity, execution-callbacks, execution-policies, frontend-identity, typed-visibility |
| [ScheduleSuite.TestBufferOneDeferredFiresAfterCompletion](../../../../tests/schedule_test.go#L1444) | C3, C10, C11 | required-shared-pending | capacity, execution-callbacks, execution-policies, frontend-identity |
| [ScheduleSuite.TestDeletedScheduleOperations](../../../../tests/schedule_test.go#L1521) | C8, C10 | required-shared-pending | execution-callbacks, frontend-identity, lifecycle-history |
| [ScheduleSuite.TestBasics](../../../../tests/schedule_test.go#L1579) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestInput](../../../../tests/schedule_test.go#L2008) | C6, C10 | required-shared-pending | carryover, execution-callbacks, frontend-identity |
| [ScheduleSuite.TestLastCompletionResultAfterNoResultWorkflow](../../../../tests/schedule_test.go#L2078) | C6, C10 | required-shared-pending | carryover, execution-callbacks, frontend-identity |
| [TestScheduleLastCompletionResultMatchesV1](../../../../tests/schedule_test.go#L2161) | C6, C10 | differential-pending | carryover, execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestLastCompletionAndError](../../../../tests/schedule_test.go#L2314) | C6, C10 | required-shared-pending | carryover, execution-callbacks, frontend-identity |
| [ScheduleSuite.TestScheduleContinuesAfterWorkflowRetryFailure](../../../../tests/schedule_test.go#L2393) | C4, C7, C10 | required-shared-pending | callback-chain, controls-accounting, execution-callbacks, frontend-identity |
| [ScheduleCHASMSuite.TestScheduledWorkflowContinueAsNewCompletion](../../../../tests/schedule_test.go#L2484) | C7, C10 | promotion-or-related-pending | callback-chain, execution-callbacks, frontend-identity |
| [ScheduleSuite.TestListSchedulesReturnsWorkflowStatus](../../../../tests/schedule_test.go#L2600) | C7, C10 | required-shared-pending | callback-chain, execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesRecentActionsCapped](../../../../tests/schedule_test.go#L2702) | C8, C10 | required-shared-pending | execution-callbacks, frontend-identity, lifecycle-history, typed-visibility |
| [ScheduleSuite.TestUpdateIntervalTakesEffect](../../../../tests/schedule_test.go#L2747) | C1, C2, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection, update-clock |
| [ScheduleSuite.TestListScheduleMatchingTimes](../../../../tests/schedule_test.go#L2813) | C1, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestListScheduleMatchingTimes/action](../../../../tests/schedule_test.go#L2813) | C1, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestListScheduleMatchingTimes/valid range](../../../../tests/schedule_test.go#L2813) | C1, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestListScheduleMatchingTimes/epoch range](../../../../tests/schedule_test.go#L2813) | C1, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestListScheduleMatchingTimes/invalid start time](../../../../tests/schedule_test.go#L2813) | C1, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestLimitMemoSpecSize](../../../../tests/schedule_test.go#L2899) | C1, C10, C11 | required-shared-pending | capacity, execution-callbacks, frontend-identity, spec-projection, typed-visibility |
| [ScheduleSuite.TestCountSchedules](../../../../tests/schedule_test.go#L2972) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesPagination](../../../../tests/schedule_test.go#L3047) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesPagination/action](../../../../tests/schedule_test.go#L3047) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterAndEntryFields](../../../../tests/schedule_test.go#L3121) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/action](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/Equal](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/NotEqual](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/StartsWith](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/StartsWithSpecific](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/NotStartsWith](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/In](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/InSingle](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/NotIn](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestListSchedulesFilterByScheduleId/IsNotNull](../../../../tests/schedule_test.go#L3216) | C10 | required-shared-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleSuite.TestSchedule_InternalTaskQueue](../../../../tests/schedule_test.go#L3349) | C10 | required-shared-pending | execution-callbacks, frontend-identity |
| [ScheduleSuite.TestSchedule_InternalTaskQueue/CreateSchedule_PerNSWorkerTaskQueue](../../../../tests/schedule_test.go#L3349) | C10 | required-shared-pending | execution-callbacks, frontend-identity |
| [ScheduleSuite.TestSchedule_InternalTaskQueue/UpdateSchedule_PerNSWorkerTaskQueue](../../../../tests/schedule_test.go#L3349) | C10 | required-shared-pending | execution-callbacks, frontend-identity |
| [ScheduleSuite.TestSchedule_InternalTaskQueue/action](../../../../tests/schedule_test.go#L3349) | C10 | required-shared-pending | execution-callbacks, frontend-identity |
| [ScheduleCHASMSuite.TestDoubleReset_HSMCallbacks](../../../../tests/schedule_test.go#L3611) | C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity |
| [ScheduleCHASMSuite.TestDoubleReset_ChasmCallbacks](../../../../tests/schedule_test.go#L3615) | C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity |
| [ScheduleCHASMSuite.TestDoubleReset_ResetRequestIDDeduplicationDisabled](../../../../tests/schedule_test.go#L3619) | C5, C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity, request-conflict |
| [ScheduleCHASMSuite.TestResetThenContinueAsNew](../../../../tests/schedule_test.go#L3742) | C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity |
| [ScheduleCHASMSuite.TestContinueAsNewThenReset](../../../../tests/schedule_test.go#L3746) | C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity |
| [ScheduleCHASMSuite.TestResetWithAdditionalCallback_HSMCallbacks](../../../../tests/schedule_test.go#L3920) | C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity |
| [ScheduleCHASMSuite.TestResetWithAdditionalCallback_ChasmCallbacks](../../../../tests/schedule_test.go#L3924) | C7, C10 | promotion-or-related-pending | callback-chain, frontend-identity |
| [ScheduleCHASMSuite.TestCreatesWorkflowSentinel](../../../../tests/schedule_test.go#L3930) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleCHASMSuite.TestStateSizeBytesReported](../../../../tests/schedule_test.go#L3995) | C8, C10 | implementation-specific | execution-callbacks, frontend-identity, lifecycle-history |
| [ScheduleV1Suite.TestCreatesCHASMSentinel](../../../../tests/schedule_test.go#L4042) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleCHASMSuite.TestSkipsWorkflowSentinelWhenDisabled](../../../../tests/schedule_test.go#L4127) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleV1Suite.TestSkipsCHASMSentinelWhenDisabled](../../../../tests/schedule_test.go#L4178) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleCHASMSuite.TestCreateScheduleAlreadyExists](../../../../tests/schedule_test.go#L4234) | C5, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, request-conflict |
| [ScheduleCHASMSuite.TestCreateScheduleAlreadyExists/action](../../../../tests/schedule_test.go#L4234) | C5, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, request-conflict |
| [ScheduleSuite.TestCreateScheduleDuplicateSdkError](../../../../tests/schedule_test.go#L4283) | C5, C6, C10 | required-shared-pending | carryover, frontend-identity, request-conflict |
| [ScheduleCHASMSuite.TestPatchRejectsExcessBackfillers](../../../../tests/schedule_test.go#L4312) | C3, C10, C11 | promotion-or-related-pending | capacity, execution-callbacks, execution-policies, frontend-identity |
| [ScheduleCHASMSuite.TestMigrationCallbackAttach](../../../../tests/schedule_test.go#L4485) | C7, C9, C10 | implementation-specific | migration-boundary |
| [ScheduleCHASMSuite.TestCallbackCompletionMetrics](../../../../tests/schedule_test.go#L4626) | C7, C10 | implementation-specific | callback-chain, execution-callbacks, frontend-identity |
| [ScheduleCHASMSuite.TestMigrationCallbackReattachSynthesized](../../../../tests/schedule_test.go#L4678) | C7, C9, C10 | implementation-specific | migration-boundary |
| [ScheduleCHASMSuite.TestMigrationCallbackReattachSynthesized/AlreadyClosed](../../../../tests/schedule_test.go#L4678) | C7, C9, C10 | implementation-specific | migration-boundary |
| [ScheduleCHASMSuite.TestMigrationCallbackReattachSynthesized/TargetGone](../../../../tests/schedule_test.go#L4678) | C7, C9, C10 | implementation-specific | migration-boundary |
| [ScheduleV1Suite.TestCHASMCanListV1Schedules](../../../../tests/schedule_test.go#L4767) | C10 | implementation-specific | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleV1Suite.TestCHASMCanListV1Schedules/action](../../../../tests/schedule_test.go#L4767) | C10 | implementation-specific | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleV1Suite.TestRefresh](../../../../tests/schedule_test.go#L4840) | C10 | implementation-specific | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleV1Suite.TestListBeforeRun](../../../../tests/schedule_test.go#L4957) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleV1Suite.TestRateLimit](../../../../tests/schedule_test.go#L5007) | C10, C11 | implementation-specific | capacity, execution-callbacks, frontend-identity |
| [ScheduleV1Suite.TestNextTimeCache](../../../../tests/schedule_test.go#L5064) | C10 | implementation-specific | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleCHASMSuite.TestUpdateScheduleMemo](../../../../tests/schedule_test.go#L5221) | C2, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility, update-clock |
| [ScheduleV1Suite.TestUpdateScheduleMemoRejected](../../../../tests/schedule_test.go#L5347) | C2, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility, update-clock |
| [ScheduleSuite.TestUnpauseResumesProcessing](../../../../tests/schedule_test.go#L5408) | C2, C10 | required-shared-pending | execution-callbacks, frontend-identity, update-clock |
| [ScheduleSuite.TestPausedScheduleNeverIdles](../../../../tests/schedule_test.go#L5543) | C4, C8, C10 | required-shared-pending | controls-accounting, execution-callbacks, frontend-identity, lifecycle-history |
| [ScheduleSuite.TestPausedEmptySpecStaysOpen](../../../../tests/schedule_test.go#L5597) | C1, C4, C10 | required-shared-pending | controls-accounting, execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestUpdateScheduleRequestIDTooLong](../../../../tests/schedule_test.go#L5967) | C2, C5, C10 | required-shared-pending | execution-callbacks, frontend-identity, request-conflict, update-clock |
| [ScheduleCHASMSuite.TestLargeScheduleID](../../../../tests/schedule_test.go#L6001) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleSuite.TestUpdateScheduleBlobSizeLimit](../../../../tests/schedule_test.go#L6027) | C2, C10, C11 | required-shared-pending | capacity, execution-callbacks, frontend-identity, typed-visibility, update-clock |
| [ScheduleCHASMSuite.TestScheduleCreationRolloutPercent](../../../../tests/schedule_test.go#L6096) | C9, C10 | implementation-specific | migration-boundary |
| [TestScheduleV1SharedSuite](../../../../tests/schedule_test.go#L6499) | C10 | suite-entrypoint | frontend-identity |
| [TestScheduleCHASMSharedSuite](../../../../tests/schedule_test.go#L6503) | C10 | suite-entrypoint | frontend-identity |
| [ScheduleSuite.TestJitteredWakeupSurvivesCompletion](../../../../tests/schedule_test.go#L6510) | C1, C10 | required-shared-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleSuite.TestTriggerImmediately](../../../../tests/schedule_test.go#L6605) | C3, C5, C10 | required-shared-pending | execution-policies, frontend-identity, request-conflict |
| [ScheduleSuite.TestTriggerImmediately/OnActiveSchedule](../../../../tests/schedule_test.go#L6605) | C3, C5, C10 | required-shared-pending | execution-policies, frontend-identity, request-conflict |
| [ScheduleSuite.TestTriggerImmediately/OnPausedSchedule](../../../../tests/schedule_test.go#L6605) | C3, C5, C10 | required-shared-pending | execution-policies, frontend-identity, request-conflict |
| [ScheduleSuite.TestTriggerImmediately/AfterActionsExhausted](../../../../tests/schedule_test.go#L6605) | C3, C5, C10 | required-shared-pending | execution-policies, frontend-identity, request-conflict |
| [ScheduleSuite.TestBackfill](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/ReprocessCompletedActionExactTimePaused](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/ReprocessCompletedActionExactTimeActive](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/ReprocessCompletedActionInRange](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/SkipOverlap](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/BufferOneOverlap](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/MultiRangeCountedExactlyOnce](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [ScheduleSuite.TestBackfill/RangeSmallerThanInterval](../../../../tests/schedule_test.go#L6618) | C3, C10 | required-shared-pending | execution-policies, frontend-identity |
| [TestScheduleCHASMSuite](../../../../tests/schedule_test.go#L6647) | C10 | suite-entrypoint | frontend-identity, typed-visibility |
| [ScheduleCHASMSuite.TestScheduleNextActionTimeVisibility](../../../../tests/schedule_test.go#L6654) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [ScheduleCHASMSuite.TestListSchedulesPreservesV2ScheduleIDWithV1Prefix](../../../../tests/schedule_test.go#L6732) | C10 | promotion-or-related-pending | frontend-identity, typed-visibility |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/CreateSchedule schedule policy](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/CreateSchedule initial patch](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/CreateSchedule initial patch timestamp](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/CreateSchedule timestamp](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/CreateSchedule remaining actions](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/UpdateSchedule schedule policy](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/UpdateSchedule timestamp](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/UpdateSchedule remaining actions](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/PatchSchedule trigger](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/PatchSchedule backfill](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/PatchSchedule trigger timestamp](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests/PatchSchedule backfill timestamp](../../../../tests/schedule_test.go#L6766) | C10 | promotion-or-related-pending | frontend-identity |
| [ScheduleCHASMSuite.TestMirroredIncludeExcludeSpec](../../../../tests/schedule_test.go#L7014) | C1, C10 | promotion-or-related-pending | frontend-identity, spec-projection |
| [ScheduleCHASMSuite.TestMirroredIncludeExcludeSpecOnUpdate](../../../../tests/schedule_test.go#L7049) | C1, C2, C10 | promotion-or-related-pending | frontend-identity, spec-projection, update-clock |
| [ScheduleCHASMSuite.TestScheduleFarFutureActionTimes](../../../../tests/schedule_test.go#L7098) | C1, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, spec-projection |
| [ScheduleCHASMSuite.TestScheduleManyCalendars](../../../../tests/schedule_test.go#L7155) | C1, C10, C11 | promotion-or-related-pending | capacity, execution-callbacks, frontend-identity, spec-projection, typed-visibility |
| [ScheduleCHASMSuite.TestScheduleCountsVisibility](../../../../tests/schedule_test.go#L7240) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [TestScheduleV1AuditVisibility](../../../../tests/schedule_audit_test.go#L17) | C10 | promotion-or-related-pending | frontend-identity, typed-visibility |
| [TestScheduleCHASMAuditVisibility](../../../../tests/schedule_audit_test.go#L18) | C10 | promotion-or-related-pending | frontend-identity, typed-visibility |
| [TestScheduleMigrationTestSuite](../../../../tests/schedule_migration_test.go#L50) | C9, C10 | suite-entrypoint | frontend-identity, migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV2AlreadyExists](../../../../tests/schedule_migration_test.go#L55) | C5, C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV2ToV1BlockedOnlyByRunningSentinel](../../../../tests/schedule_migration_test.go#L215) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestCreateFromMigrationStateBlockedBySentinel](../../../../tests/schedule_migration_test.go#L341) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV1ToV2RetriesAfterSentinelDeletion](../../../../tests/schedule_migration_test.go#L388) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestMigrateToWorkflowBlockedByCHASMSentinel](../../../../tests/schedule_migration_test.go#L514) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationDynamicConfig](../../../../tests/schedule_migration_test.go#L547) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV1ToV2](../../../../tests/schedule_migration_test.go#L652) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV1ToV2ToV1RoundTrip](../../../../tests/schedule_migration_test.go#L785) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV2ToV1](../../../../tests/schedule_migration_test.go#L985) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV2ToV1Idempotent](../../../../tests/schedule_migration_test.go#L1192) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestCHASMScheduleDescribeAfterDisablingCreationAndMigration](../../../../tests/schedule_migration_test.go#L1262) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV2ToV1RoutingFallback](../../../../tests/schedule_migration_test.go#L1355) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleUpdateAfterDelete](../../../../tests/schedule_migration_test.go#L1491) | C2, C8, C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, lifecycle-history, update-clock |
| [ScheduleMigrationTestSuite.TestScheduleMigrationV1ToV2WithClosedV2](../../../../tests/schedule_migration_test.go#L1601) | C9, C10 | implementation-specific | migration-boundary |
| [TestScheduleMigrationV1ToV2NoDuplicateRecentActions](../../../../tests/schedule_migration_test.go#L1751) | C5, C8, C9, C10 | implementation-specific | migration-boundary |
| [TestScheduleMigrationDeferredWithRunningWorkflow](../../../../tests/schedule_migration_test.go#L1893) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata/BothStacks](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata/CHASMOnly_V1Sentinel](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata/CHASMSentinel_V1Real](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata/V1Only_NoCHASM](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata/CHASMSentinel_V1Gone](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata/NeitherStack](../../../../tests/schedule_migration_test.go#L2027) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestPatchScheduleContextMetadata](../../../../tests/schedule_migration_test.go#L2230) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestPatchScheduleContextMetadata/CHASMSchedule](../../../../tests/schedule_migration_test.go#L2230) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestPatchScheduleContextMetadata/V1Schedule](../../../../tests/schedule_migration_test.go#L2230) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [ScheduleMigrationTestSuite.TestPatchScheduleContextMetadata/CHASMSentinel_V1Gone](../../../../tests/schedule_migration_test.go#L2230) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity |
| [TestScheduleMigration_StaleRunningDoesNotSkipPending](../../../../tests/schedule_migration_test.go#L2392) | C9, C10 | implementation-specific | migration-boundary |
| [TestScheduleMigration_ClosedFailedWorkflowRecordsCompletion](../../../../tests/schedule_migration_test.go#L2538) | C9, C10 | implementation-specific | migration-boundary |
| [ScheduleMigrationTestSuite.TestScheduleMigrationRolloutPercent](../../../../tests/schedule_migration_test.go#L2648) | C9, C10 | implementation-specific | migration-boundary |
| [TestScheduleMigration_NoRunningWorkflows_GeneratorStarts](../../../../tests/schedule_migration_test.go#L2768) | C9, C10 | implementation-specific | migration-boundary |
| [TestScheduleMigrationV1ToV2_AdminMigratePreservesRunningWorkflowHistory](../../../../tests/schedule_migration_test.go#L2847) | C9, C10 | existing-skip | execution-callbacks, frontend-identity, migration-boundary |
| [ScheduleV1Suite.TestWorkflowPauseInteraction](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/UnpauseRecovery](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/ContinueAsNew](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Reset](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/UnpauseRecovery](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/ContinueAsNew](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Reset](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C4, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [AdvancedVisibilitySuite.TestScheduleListingWithSearchAttributes](../../../../tests/advanced_visibility_test.go#L3204) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [AdvancedVisibilitySuite.TestScheduleListingWithSearchAttributes/default](../../../../tests/advanced_visibility_test.go#L3204) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [CallbacksSuite.TestScheduledCallbackTokenMigration_LegacyWriteEnvelopeRead](../../../../tests/callbacks_test.go#L132) | C7, C9, C10 | promotion-or-related-pending | callback-chain, execution-callbacks, frontend-identity, migration-boundary |
| [chasmTimeSkippingReplicationSuite.TestStandbyRestampsCHASMTimerAfterSkip](../../../../tests/xdc/chasm_timeskipping_replication_test.go#L56) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [chasmTimeSkippingReplicationSuite.TestStandbyRestampsCHASMTimerAfterSkip/unused](../../../../tests/xdc/chasm_timeskipping_replication_test.go#L56) | C10 | promotion-or-related-pending | execution-callbacks, frontend-identity, typed-visibility |
| [testPauseOnFailureIgnoresCancelTerminate](../../../../tests/schedule_test.go#L352) | C4, C10 | scenario-helper | controls-accounting, execution-callbacks, frontend-identity |
| [testScheduleTimeSkippingWeekly](../../../../tests/schedule_test.go#L545) | C2, C10 | scenario-helper | execution-callbacks, frontend-identity, typed-visibility, update-clock |
| [testRecentActionsAdvanceWhilePaused](../../../../tests/schedule_test.go#L1356) | C4, C8, C10 | scenario-helper | controls-accounting, execution-callbacks, frontend-identity, lifecycle-history, typed-visibility |
| [testFutureActionTimesAdvanceWhilePaused](../../../../tests/schedule_test.go#L1407) | C1, C4, C10 | scenario-helper | controls-accounting, execution-callbacks, frontend-identity, spec-projection, typed-visibility |
| [testScheduledWorkflowDoubleReset](../../../../tests/schedule_test.go#L3443) | C7, C10 | scenario-helper | callback-chain, execution-callbacks, frontend-identity, typed-visibility |
| [testScheduledWorkflowResetAndContinueAsNew](../../../../tests/schedule_test.go#L3625) | C7, C10 | scenario-helper | callback-chain, execution-callbacks, frontend-identity, typed-visibility |
| [testResetWithAdditionalCallback](../../../../tests/schedule_test.go#L3750) | C7, C10 | scenario-helper | callback-chain, execution-callbacks, frontend-identity, typed-visibility |
| [testPausedDropsCatchup](../../../../tests/schedule_test.go#L5501) | C2, C4, C10 | scenario-helper | controls-accounting, execution-callbacks, frontend-identity, update-clock |
| [testTriggerImmediatelyOnActiveSchedule](../../../../tests/schedule_test.go#L5648) | C3, C5, C10 | scenario-helper | execution-callbacks, execution-policies, frontend-identity, request-conflict |
| [testTriggerImmediatelyOnPausedSchedule](../../../../tests/schedule_test.go#L5699) | C3, C4, C5, C10 | scenario-helper | controls-accounting, execution-callbacks, execution-policies, frontend-identity, request-conflict |
| [testTriggerImmediatelyAfterActionsExhausted](../../../../tests/schedule_test.go#L5738) | C3, C4, C5, C10 | scenario-helper | controls-accounting, execution-callbacks, execution-policies, frontend-identity, request-conflict |
| [testBackfillReprocessesCompletedAction](../../../../tests/schedule_test.go#L5775) | C3, C10 | scenario-helper | execution-callbacks, execution-policies, frontend-identity |
| [testBackfillWithBufferOneOverlap](../../../../tests/schedule_test.go#L5851) | C3, C10, C11 | scenario-helper | capacity, execution-callbacks, execution-policies, frontend-identity |
| [testBackfillRangeSmallerThanInterval](../../../../tests/schedule_test.go#L5897) | C1, C3, C10 | scenario-helper | execution-callbacks, execution-policies, frontend-identity, spec-projection |
| [testBackfillWithSkipOverlap](../../../../tests/schedule_test.go#L5938) | C3, C10 | scenario-helper | execution-callbacks, execution-policies, frontend-identity |
| [testScheduleClosesFromIdle](../../../../tests/schedule_test.go#L6192) | C8, C10 | scenario-helper | frontend-identity, lifecycle-history |
| [testScheduleClosesFromIdle/SingleDate](../../../../tests/schedule_test.go#L6192) | C8, C10 | scenario-helper | frontend-identity, lifecycle-history |
| [testScheduleClosesFromIdle/MultiDate](../../../../tests/schedule_test.go#L6192) | C8, C10 | scenario-helper | frontend-identity, lifecycle-history |
| [testScheduleClosesFromIdle/LimitedActions](../../../../tests/schedule_test.go#L6192) | C8, C10 | scenario-helper | frontend-identity, lifecycle-history |
| [testScheduleClosesFromIdle/FinalAllowAllAction](../../../../tests/schedule_test.go#L6192) | C8, C10 | scenario-helper | frontend-identity, lifecycle-history |
| [testScheduleClosesFromIdle/IntervalEndTime](../../../../tests/schedule_test.go#L6192) | C8, C10 | scenario-helper | frontend-identity, lifecycle-history |
| [runScheduleClosesFromIdleCase](../../../../tests/schedule_test.go#L6257) | C8, C10 | scenario-helper | execution-callbacks, frontend-identity, lifecycle-history |
| [testManualOnlyUnpausedClosesFromIdle](../../../../tests/schedule_test.go#L6304) | C2, C8, C10 | scenario-helper | execution-callbacks, frontend-identity, lifecycle-history, update-clock |
| [testPauseDuringIdleWindow](../../../../tests/schedule_test.go#L6329) | C4, C8, C10 | scenario-helper | controls-accounting, execution-callbacks, frontend-identity, lifecycle-history |
| [testBackfillBlocksIdleClose](../../../../tests/schedule_test.go#L6369) | C3, C8, C10 | scenario-helper | execution-callbacks, execution-policies, frontend-identity, lifecycle-history |
| [testMultiRangeBackfillCountedExactlyOnce](../../../../tests/schedule_test.go#L6411) | C3, C10 | scenario-helper | execution-callbacks, execution-policies, frontend-identity |
| [testBackfillOnPausedSchedule](../../../../tests/schedule_test.go#L6465) | C3, C4, C10 | scenario-helper | controls-accounting, execution-callbacks, execution-policies, frontend-identity |
| [testScheduleAuditVisibility](../../../../tests/schedule_audit_test.go#L22) | C10 | scenario-helper | execution-callbacks, frontend-identity, typed-visibility |
| [runSchedulePauseOverlapMatrix](../../../../tests/schedule_workflow_pause_interaction_test.go#L92) | C3, C4, C10 | scenario-helper | controls-accounting, execution-policies, frontend-identity |
| [runSchedulePauseRecoveryMatrix](../../../../tests/schedule_workflow_pause_interaction_test.go#L100) | C4, C10 | scenario-helper | controls-accounting, frontend-identity |
| [testSchedulePauseOverlap](../../../../tests/schedule_workflow_pause_interaction_test.go#L262) | C3, C4, C10 | scenario-helper | controls-accounting, execution-callbacks, execution-policies, frontend-identity |
| [testSchedulePauseUnpauseRecovery](../../../../tests/schedule_workflow_pause_interaction_test.go#L293) | C2, C4, C10 | scenario-helper | controls-accounting, execution-callbacks, frontend-identity, update-clock |
| [testSchedulePauseContinueAsNew](../../../../tests/schedule_workflow_pause_interaction_test.go#L432) | C4, C7, C10 | scenario-helper | callback-chain, controls-accounting, execution-callbacks, frontend-identity |
| [testSchedulePauseReset](../../../../tests/schedule_workflow_pause_interaction_test.go#L510) | C4, C7, C10 | scenario-helper | callback-chain, controls-accounting, execution-callbacks, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_SKIP](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_BUFFER_ONE](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_BUFFER_ALL](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_ALLOW_ALL](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_SKIP](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_BUFFER_ONE](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_BUFFER_ALL](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleV1Suite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER](../../../../tests/schedule_workflow_pause_interaction_test.go#L76) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_SKIP](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_BUFFER_ONE](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_BUFFER_ALL](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/Overlap/SCHEDULE_OVERLAP_POLICY_ALLOW_ALL](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_SKIP](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_BUFFER_ONE](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_BUFFER_ALL](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |
| [ScheduleCHASMSuite.TestWorkflowPauseInteraction/UnpauseRecovery/SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER](../../../../tests/schedule_workflow_pause_interaction_test.go#L84) | C3, C4, C7, C10 | promotion-or-related-pending | controls-accounting, frontend-identity |

## Named differences, exclusions and audit limits

`TestLastCompletionResultAfterNoResultWorkflow` distinguishes V1's retained previous
result from current CHASM's clearing on a no-result completion; explicit binary/null
remains a result. `TestScheduleLastCompletionResultMatchesV1` intentionally excludes
cancel/terminate/timeout failure comparison. Scheduler2 C6 semantics are independently
specified; those exclusions require separate expected assertions, not silent equality.
V1 rejects memo update while current CHASM accepts it (`TestUpdateScheduleMemoRejected`
versus `TestUpdateScheduleMemo`); choose and record the scheduler2 public contract.

Pause interaction expands all six overlap policies, but recovery intentionally covers
only SKIP/BUFFER_ONE/BUFFER_ALL/CANCEL_OTHER: ALLOW_ALL and TERMINATE_OTHER keep
progressing while the workflow is paused. This is a semantic inapplicability for that
recovery subcase, not a skipped required scheduler2 overlap test. Continue-as-new and
reset remain separate cases on each existing backend.

The admin migration history-preservation scenario already calls t.Skip because the
admin migration path inserts WORKFLOW_EXECUTION_OPTIONS_UPDATED. Preserve that blocker
and its source text in JSON; neither WI-012 nor a backend descriptor fixes it.
Migration-only rows retain C9 unsupported status; public delete/patch context metadata
and update-after-delete are separately classified even when located in migration files.

Related scope explicitly includes audit visibility, advanced schedule search attributes,
legacy callback-token migration and XDC standby timer restamping. Generic activity
scheduling, cron and workflow timer tests are not Schedule API scenarios merely because
their names contain “schedule”. Lower-level scheduler2/M2 unit/model evidence remains
in its own ledger and cannot substitute for these functional rows.

The inventory is a baseline snapshot. Descriptor refactoring must preserve every listed
method and named scenario, update selectors as necessary, and attach exact local run
results without converting pending scheduler2 dependencies into passes. No full M3
acceptance follows from source enumeration or successful existing-backend samples.

## Classification refinements: observable obligations are not exempted

This addendum refines the effective meaning of the six baseline implementation-specific
rows below without changing their source fingerprints or historical classification.
The baseline label identifies the existing mechanism; the pending scheduler2 obligation
in this table is mandatory unless a reviewed contract decision establishes a specific
alternative. None of these rows grants a new unsupported divergence. Machine-readable
consumers must apply `classification_refinements` in addition to baseline `rows`.

| Baseline symbol | Observable assertion | Required scheduler2 equivalent / dependency |
| --- | --- | --- |
| `ScheduleV1Suite.TestRateLimit` (C11,C10) | V1 config SchedulerNamespaceStartWorkflowRPS=1 caps ten1s schedules to fewer than10 starts in5s; source explicitly says current CHASM lacks this scheduler-owned limiter. | The V1 limiter mechanism is implementation-specific, but scheduler2 needs an explicit namespace admission/rate-limit contract and a reproducible overload/recovery expectation. Executor concurrency1 is not rate limiting. Do not infer an approved no-limit divergence from current CHASM. **Gate:** M3 C11/API behavior decision and test; M4 sustained load report. |
| `ScheduleCHASMSuite.TestStateSizeBytesReported` (C8,C10) | Public DescribeSchedule Info.StateSizeBytes is positive for a created paused schedule. | Adapt this public field assertion to scheduler2 Describe and define its byte-accounting scope. Internal P3.3 mutation-byte logs do not satisfy the public response contract. **Gate:** M3 Describe response/projection; M4 growth measurements separately. |
| `ScheduleCHASMSuite.TestCallbackCompletionMetrics` (C7,C10) | One triggered workflow is reported COMPLETED, internal callback delivery metric fires, and native-start callback reattach metric remains absent. | Scheduler2 requires the completed-action public observation and real callback path. Exact metric names/backend tags and native versus recovery metric semantics need explicit mapping; internal result injection alone is insufficient. **Gate:** M3 execution/callback/public observations and metric contract; no migration implementation implied. |
| `TestScheduleV1ActionDelayMetrics` (C2,C3,C10) | Three executions yield two action-delay samples tagged legacy; sample equals actual action time minus max(own scheduled time, prior blocking completion). | Legacy tag and refresh implementation stay baseline-specific. Scheduler2 must define equivalent desired/actual timing and correct backend metric attribution, or document a reviewed semantic divergence with a named reproduction; no absence exemption. **Gate:** M3 timing/metric correspondence; M4 latency distributions separately. |
| `ScheduleV1Suite.TestCHASMCanListV1Schedules` (C10) | V1-created paused schedule has equal list Info and equal Count through V1 and CHASM contexts. | Scheduler2 typed List/Count must have a defined mixed-backend visibility policy and tests for identity/filtering/counts. Listing an existing schedule is not a silent execution fallback; neither cross-backend union nor isolation is presumed without the public routing contract. **Gate:** M3 typed visibility, mixed-backend routing and no-fallback identity checks. |
| `ScheduleV1Suite.TestRefresh` (C7,C8,C10) | Exact V1 scheduler workflow history remains unchanged after child timeout; Describe forces refresh, clears RunningWorkflows, then history advances. | Exact workflow-history events and refresh signal remain V1-only. Scheduler2 must expose correct running/terminal state after child timeout at a defined observation point, using its callback/recovery mechanism; no claim of eager or Describe-triggered refresh without a contract. **Gate:** M3 Describe/callback recovery correspondence. |

## Post-refactor helper and worker scenarios

These additions are separate from the unchanged baseline inventory. Current source:
`tests/schedule_backend_test.go`, SHA256 `7379ea05ab53b33bb1c2d30627bae8237f47882d90eb8efecc88e624a0b48fa6`.
No execution result is asserted here; parent WI-012 logs own each command/result.

| New source symbol | Assertions / applicability | Invocation selector and evidence |
| --- | --- | --- |
| `TestScheduleBackendDescriptorAndCleanup` | arbitrary backend names; configured worker flag and experiment headers; two-page collection before delete; NotFound accepted; unit helper, mock frontend client. | `./tests -run ^TestScheduleBackendDescriptorAndCleanup$` with `test_dep,integration`; not run by inventory author. |
| `TestScheduleBackendCleanupFailureAndCancellation` | canceled list stops deletion; delete errors aggregate while later IDs still attempted; unit helper, mock frontend client. | `./tests -run ^TestScheduleBackendCleanupFailureAndCancellation$` with `test_dep,integration`; not run by inventory author. |
| `TestScheduleDescriptorStartsLegacyWorker` | non-V1-prefixed test uses descriptor to start worker and execute actual shared public active-trigger workflow/count assertions; local SQLite functional scenario required. | `./tests -run ^TestScheduleDescriptorStartsLegacyWorker$` with `test_dep,integration`; not run by inventory author. |

The associated setup helpers (`scheduleCommonOpts`, `newScheduleEnv`) centralize
options, declare worker/context/cleanup ownership, and copy the cleanup descriptor
slice. `cleanupScheduleBackend` retains page-before-delete and error behavior.
Mock header tests cannot establish real backend execution. The non-prefix worker
scenario supplies that additional existing-V1 behavioral check when its local run
passes; it still supplies no scheduler2 frontend identity evidence.
