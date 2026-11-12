package schedulertest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestActionBudgetSharedAcrossPhases pins a bug found by
// TestSchedulerProperties. InvokerExecuteTask.Execute used to pass its
// invokerTaskHandlerContext by value to terminateWorkflows, cancelWorkflows and
// startWorkflows, so each phase spent from its own copy of the action budget.
// Here one execution has a cancel, a terminate and more ALLOW_ALL starts than
// the default MaxActionsPerExecution allows; together they must stay within it.
func TestActionBudgetSharedAcrossPhases(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s := schedulertest.DefaultSchedule()
	s.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	s.State.Paused = true
	var vs []schedulertest.Violation
	d := schedulertest.NewDriver(t,
		schedulertest.WithStartTime(start),
		schedulertest.WithSchedule(s),
		schedulertest.WithAfterStep(collectViolations(&vs)),
	)
	backfill := func(policy enumspb.ScheduleOverlapPolicy, points int) {
		from := start.Add(-time.Hour)
		require.NoError(t, d.Backfill(&schedulepb.BackfillRequest{
			StartTime:     timestamppb.New(from),
			EndTime:       timestamppb.New(from.Add(time.Duration(points-1) * schedulertest.DefaultInterval)),
			OverlapPolicy: policy,
		}))
	}

	require.NoError(t, d.TriggerImmediately())
	require.True(t, d.StepSideEffect(), "start the triggered workflow")
	backfill(enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 1)
	backfill(enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, 1)
	backfill(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, scheduler.DefaultTweakables.MaxActionsPerExecution)
	for range 4 {
		d.Step()
	}

	require.Empty(t, violationNames(vs))
}
