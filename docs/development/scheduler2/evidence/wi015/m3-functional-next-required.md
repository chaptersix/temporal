# M3 functional closure audit

The accepted WI-012 baseline contains242 inventory rows, including methods, literal/policy subcases, helpers and entrypoints. It is not242 independent tests. The new scheduler2 entry wires all29 shared methods (57 baseline method/subcase rows), but it does not run the CHASM-only or related public scenarios.

The full shared run inspected here (`/private/tmp/scheduler2-wi015-shared-full01.log`) finished65.939s with two failing methods: TestLimitMemoSpecSize and TestBackfill/SkipOverlap. Their fixes are in progress/targeted verification; no clean full-batch acceptance is inferred. The machine-readable [next-required list](m3-functional-next-required.json) preserves all242 baseline rows, classifications, contracts, dependencies and invocation provenance, then adds audit status/evidence mappings. It is an execution overlay, not a rewrite of the accepted baseline.

There are109 required-port/differential rows outside the shared set, plus three baseline classifications that need promotion or explicit public-policy reconciliation. They represent48 root rows, with duplicated V1/CHASM semantics that can share a scheduler2 port. This is not48 unrelated implementations. There are39 discovery/helper rows and one existing skipped row; none counts as passing evidence.

## Required public scenarios beyond the shared29

The following root rows still require scheduler2 adaptation/evidence or the explicitly stated policy decision. Their nested cases/policy expansions remain separately enumerated in JSON. Use of old internal protobufs in a test is an adaptation requirement, not a reason to drop its public assertions.

| Family | Source root scenarios |
| --- | --- |
| callback-chain-reset | `ScheduleCHASMSuite.TestScheduledWorkflowContinueAsNewCompletion`; `ScheduleCHASMSuite.TestDoubleReset_HSMCallbacks`; `ScheduleCHASMSuite.TestDoubleReset_ChasmCallbacks`; `ScheduleCHASMSuite.TestDoubleReset_ResetRequestIDDeduplicationDisabled`; `ScheduleCHASMSuite.TestResetThenContinueAsNew`; `ScheduleCHASMSuite.TestContinueAsNewThenReset`; `ScheduleCHASMSuite.TestResetWithAdditionalCallback_HSMCallbacks`; `ScheduleCHASMSuite.TestResetWithAdditionalCallback_ChasmCallbacks`; `CallbacksSuite.TestScheduledCallbackTokenMigration_LegacyWriteEnvelopeRead` |
| cancel-terminate-pause-policy | `ScheduleCHASMSuite.TestPauseOnFailureCancelDoesNotPause`; `ScheduleCHASMSuite.TestPauseOnFailureTerminateDoesNotPause` |
| capacity-and-buffer-accounting | `ScheduleCHASMSuite.TestBufferOverrunDropsActions` |
| idle-close-lifecycle | `ScheduleCHASMSuite.TestIdleClose` |
| other-public-contract | `TestScheduleLastCompletionResultMatchesV1`; `ScheduleCHASMSuite.TestStateSizeBytesReported`; `ScheduleV1Suite.TestRateLimit`; `ScheduleMigrationTestSuite.TestCHASMScheduleDescribeAfterDisablingCreationAndMigration` |
| paused-public-behavior | `ScheduleCHASMSuite.TestPausedBehavior` |
| physical-recovery-replication | `chasmTimeSkippingReplicationSuite.TestStandbyRestampsCHASMTimerAfterSkip` |
| spec-projection-and-defaults | `ScheduleCHASMSuite.TestDescribeCatchupWindowAfterCreateAndUpdate`; `ScheduleCHASMSuite.TestMirroredIncludeExcludeSpec`; `ScheduleCHASMSuite.TestMirroredIncludeExcludeSpecOnUpdate`; `ScheduleCHASMSuite.TestScheduleFarFutureActionTimes`; `ScheduleCHASMSuite.TestScheduleManyCalendars` |
| time-skipping-control-and-recovery | `ScheduleCHASMSuite.TestTimeSkippingWeeklySkipCreateWithPatch`; `ScheduleCHASMSuite.TestTimeSkippingWeeklyAllowAllPatch`; `ScheduleCHASMSuite.TestTimeSkippingWeeklyWithWorkflowExecutionTimeout`; `ScheduleCHASMSuite.TestMaxSkipCapsScheduleTimeSkipping`; `ScheduleCHASMSuite.TestTimeSkippingDynamicConfigGate`; `ScheduleCHASMSuite.TestTimeSkippingFeatureDisabledWhileWorkflowRuns` |
| typed-visibility-and-metadata | `ScheduleV1Suite.TestListBeforeRun`; `ScheduleCHASMSuite.TestUpdateScheduleMemo`; `ScheduleV1Suite.TestUpdateScheduleMemoRejected`; `ScheduleCHASMSuite.TestLargeScheduleID`; `ScheduleCHASMSuite.TestScheduleNextActionTimeVisibility`; `ScheduleCHASMSuite.TestListSchedulesPreservesV2ScheduleIDWithV1Prefix`; `ScheduleCHASMSuite.TestScheduleCountsVisibility`; `TestScheduleV1AuditVisibility`; `TestScheduleCHASMAuditVisibility`; `AdvancedVisibilitySuite.TestScheduleListingWithSearchAttributes` |
| validation-and-lifecycle | `ScheduleCHASMSuite.TestCreateScheduleAlreadyExists`; `ScheduleCHASMSuite.TestPatchRejectsExcessBackfillers`; `ScheduleCHASMSuite.TestScheduleRejectsInvalidRequests`; `ScheduleMigrationTestSuite.TestScheduleUpdateAfterDelete`; `ScheduleMigrationTestSuite.TestDeleteScheduleContextMetadata`; `ScheduleMigrationTestSuite.TestPatchScheduleContextMetadata` |
| workflow-pause-overlap-recovery | `ScheduleV1Suite.TestWorkflowPauseInteraction`; `ScheduleCHASMSuite.TestWorkflowPauseInteraction` |

## Priority and evidence boundary

1. **Physical recovery and identity:** new actual History cache/shard-reload witnesses for pending/ambiguous start, lost response, callback redelivery and stale physical start/effect/timer envelopes; actual cancel/terminate first-run fencing across continue-as-new/reset; the existing standby timer-restamp/XDC scenario needs the scheduler2 identity/registration on both clusters. Core serialization/replay/model tests do not establish physical queue/database recovery.
2. **Lifecycle/pause/time skipping:** idle ManualOnly/PauseDuringWindow/BackfillBlocks; paused automatic catchup/recent/future/backfill behavior; six overlap/four unpause-recovery policies while child workflows are paused; reset/CAN interaction; weekly3+ticks, execution timeout, max-session caps, namespace gate and feature disable while a child runs. The new one-tick physical propagation test covers initial/later patch plus child timer/config/offset, not these whole matrices.
3. **Public metadata/validation/capacity:** next-action/running/buffer search-attribute queries, audit visibility, memo replacement, reserved-looking and large schedule IDs, create/update/patch invalid request matrices, empty/mirrored/sparse/far-future/many-calendar specs, excess backfill rejection and buffer-overrun accounting. Current typed List/Count and compact projected metadata are related evidence, not proof of missing query attributes.
4. **Payload/callback completeness:** the real new near2MiB three-start trace is under diagnostics at this snapshot; accepted capacity is only that concrete trace once it passes. Completion-before-ack/CAN transport already gives physical chain evidence, but does not close double reset, reset/CAN order, extra callbacks and dedup-disabled reset scenarios.

## Classification corrections and decisions

- **StateSizeBytesReported:** baseline implementation-specific classification is too narrow for its actual public assertion (positive Describe.Info.StateSizeBytes). Promote that observation to required public evidence. The new capacity test could cover it once its actual run passes.
- **RateLimit:** namespace SchedulerNamespaceStartWorkflowRPS is an externally configured throughput behavior, even though the existing test is V1-specific. Require an explicit source-backed scheduler2 policy decision or equivalent runtime-governor witness; do not silently dismiss it as old code.
- **Describe after disabling creation/migration:** extract the public guarantee that existing schedules remain describable when creation flags change. Original routing/migration mechanics remain M4 and local-only restrictions still apply.
- Sentinel migration, rollout percentages, V1 workflow cache/history/refresh mechanics and implementation-specific metric assertions retain baseline scope. Their underlying public timeout/result/visibility behavior is covered by the required public families, not erased with the mechanism. Existing skipped migration test stays a skip, never a pass.

## Recommended disjoint ports

- Add only `tests/schedule2_pause_interaction_test.go`, invoking existing backend-parameterized overlap/recovery/CAN/reset helpers with schedule2Backend. The helper file has no old typed scheduler-state reads outside the original suite wrappers; retain actual child pause/reset APIs and all six/four policy rows. This directly stresses real cancel/terminate and callback chains.
- Add only `tests/schedule2_audit_test.go`, invoking `testScheduleAuditVisibility(t,schedule2Backend)`. It checks real workflow visibility for scheduled-by and nominal time, including a blocker before the query window and a buffered action starting after the window. Existing helper filters use public workflow observations; no scheduler-state cast is needed.

These can proceed independently of the current payload/backfill production owners. Separate physical reload/timer-restamp port requires dedicated recorder/cache-eviction work and should remain an explicit next required witness rather than be claimed by these two low-overlap ports.

No test execution or production edits were performed by this audit. Source fingerprints and evidence paths are in JSON; rerun manifests should update its execution statuses without deleting unresolved rows.

## Completed standalone ports after initial audit

The two proposed ports now exist in new files only. Combined actual command in `pause-audit-ports/manifest.json` passed86.504s; raw log archived there. Original helper assertions were unchanged: six overlap policies, four blocked-policy unpause recovery cases, continue-as-new, reset, and actual audit workflow visibility. This maps30 duplicated V1/CHASM method/subcase rows plus two audit root rows to `ported-functional-pass` in the JSON overlay. It does not close unrelated reset matrices, idle, weekly time skipping, XDC/restamp or physical reload gaps. The initial counts above describe the pre-port audit snapshot; JSON now carries the updated row statuses. Parent owns final consolidated lint and production-source acceptance.
