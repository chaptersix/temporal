package tests

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

func TestSchedule2ContractPauseOnFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		stop terminalStop
	}{{"CancelDoesNotPause", stopByCancel}, {"TerminateDoesNotPause", stopByTerminate}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testPauseOnFailureIgnoresCancelTerminate(t, schedule2Backend, tc.stop)
		})
	}
}

func TestSchedule2ContractIdleClose(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		run  func(*testing.T, scheduleBackend)
	}{
		{"SpecExhaustion", testScheduleClosesFromIdle},
		{"ManualOnly", testManualOnlyUnpausedClosesFromIdle},
		{"PauseDuringWindow", testPauseDuringIdleWindow},
		{"BackfillBlocks", testBackfillBlocksIdleClose},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); tc.run(t, schedule2Backend) })
	}
}

func TestSchedule2ContractPausedBehavior(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		run  func(*testing.T, scheduleBackend)
	}{
		{"DropsCatchup", testPausedDropsCatchup},
		{"RecentActionsAdvance", testRecentActionsAdvanceWhilePaused},
		{"FutureActionTimesAdvance", testFutureActionTimesAdvanceWhilePaused},
		{"BackfillDrains", testBackfillOnPausedSchedule},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); tc.run(t, schedule2Backend) })
	}
}

func TestSchedule2ContractWeeklyTimeSkipping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		policy  enumspb.ScheduleOverlapPolicy
		initial bool
		timeout time.Duration
	}{
		{"SkipCreateWithPatch", enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, true, 0},
		{"AllowAllPatch", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, false, 0},
		{"WorkflowExecutionTimeout", enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, true, 2 * 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testScheduleTimeSkippingWeeklyForBackend(t, schedule2Backend, tc.policy, tc.initial, tc.timeout)
		})
	}
}

func TestSchedule2ContractContinueAsNewCompletion(t *testing.T) {
	t.Parallel()
	testScheduledWorkflowContinueAsNewCompletion(t, schedule2Backend)
}
