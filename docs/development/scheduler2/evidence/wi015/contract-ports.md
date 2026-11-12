# WI015 remaining readily reusable contract ports

The new `tests/schedule2_contract_test.go` registers the existing test bodies against the explicit scheduler2 backend, with no skips or alternate assertions:

| Family | Registered cases / original assertions |
| --- | --- |
| PauseOnFailure | cancel does not pause; terminate does not pause |
| Idle | original spec-exhaustion matrix (SingleDate, MultiDate, LimitedActions, FinalAllowAllAction, IntervalEndTime); ManualOnly; PauseDuringWindow; BackfillBlocks |
| PausedBehavior | DropsCatchup; RecentActionsAdvance; FutureActionTimesAdvance; BackfillDrains |
| Weekly time skipping | SKIP initial patch; ALLOW_ALL later patch; workflow execution timeout |
| Continued workflow completion | original scheduled CAN completion test, including at least three completed buffered actions and identical callback-token headers across original/continued histories |

Only two helper seams were extracted in `tests/schedule_test.go`: weekly time-skipping and standalone scheduled CAN completion now accept `testing.T` and `scheduleBackend`. Existing original-CHASM methods retain their wrappers and pass the old backend. All assertion bodies, timing windows, and dynamic-config inputs remain unchanged. The new top-level wrappers and independent subcases call `t.Parallel`; invocation limits physical test concurrency to four.

Commands from `/private/tmp/sch-redesign-execution`, both with `CGO_ENABLED=0 GOMAXPROCS=4`:

```
go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2Contract' -count=1 -timeout=5m
go test -tags test_dep,integration -p 2 -parallel 4 ./tests -run '^TestSchedule2ContractContinueAsNewCompletion$' -count=1 -timeout=2m
```

The first binary began before the CAN wrapper was added; its domain is the other four families (17 leaf cases). The second command separately executes CAN. First run completed FAIL57.567s: only three leaf failures, with the other14 leaves passing:

- Weekly SKIP initial-patch: ActionCount remained3 rather than4 within the existing10s bound.
- Weekly workflow-execution-timeout: four starts became visible but parent fast-forward HasCompleted was not true within the existing10s bound.
- Idle BackfillBlocks: fewer than10 historical actions fired within the existing30s bound.

The separate scheduled CAN test PASSed11.305s, including the original three-completed-actions and callback-header propagation assertions. No production source changes, skipped cases, relaxed timing windows or altered expectations were made by this port. These failures remain actionable integration gaps. Logs show physical pure-task scheduled times adjusted to the queue's minimum, but this review has not established whether latency alone explains every failure.

Remaining adaptation inventory, not implied covered by these wrappers:

- DoubleReset helpers take `ScheduleCHASMSuite`, hardcode the original backend, and use suite.Await/assertions; three flag combinations need preserving (HSM callbacks, CHASM callbacks, reset request-ID dedup disabled).
- ResetThenContinueAsNew and ContinueAsNewThenReset share a suite-bound helper; two variants need preserving.
- ResetWithAdditionalCallback has two callback-engine variants and suite-bound await/assertions.
- TimeSkippingDynamicConfigGate, MaxSkipCapsScheduleTimeSkipping, and TimeSkippingFeatureDisabledWhileWorkflowRuns remain standalone original-backend method bodies rather than backend-parametric helpers.

The parent assigned the reset helper extraction to api_seam. Workflow-pause interaction and audit wrappers belong to oracle_review and are intentionally not duplicated here. This is an execution-coverage record, not acceptance of WI015/M3.

Test source frozen for parent investigation. Existing helper bodies are preserved; registered wrappers remain active even where failing. Reset adaptation is being handled independently by api_seam.

`make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false` completed with no port/helper findings; its only two findings were deprecated env.Context calls in other-owner schedule2_callback_test.go, reported to that owner. Raw lint log archived.

## Resolved runtime-progress failures

The exact same18 registered leaf cases now PASS34.673s after the bounded immediate coordinator pump, due-start transfer classification, and cached-future time-skipping gate correction. Full command/result is archived in `coordinator-progress/scheduler2-wi015-pump-contract03.log`; source decisions, proof-domain limits and earlier failing evidence are in `design/coordinator-immediate-progress.md`. No expectations, test timeout windows, or conditional skips were added. Parent owns consolidated all-public/core/lint checks and acceptance.
