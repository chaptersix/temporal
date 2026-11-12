package schedulertest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestActionBudgetSharedAcrossPhases reproduces a bug found by
// TestSchedulerProperties. InvokerExecuteTask.Execute passes its
// invokerTaskHandlerContext by value to terminateWorkflows, cancelWorkflows and
// startWorkflows, so each phase spends from its own copy of the action budget.
// A cancel from a CANCEL_OTHER start and a terminate from a later
// TERMINATE_OTHER start both target the same running workflow, and one
// execution makes two RPCs although MaxActionsPerExecution is 1.
func TestActionBudgetSharedAcrossPhases(t *testing.T) {
	t.Skip("InvokerExecuteTask spends the MaxActionsPerExecution budget separately in the terminate, cancel and start phases (invoker_tasks.go Execute passes ictx by value)")

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s := schedulertest.DefaultSchedule()
	s.Spec.Interval[0].Interval = durationpb.New(10 * time.Second)
	s.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	var vs []schedulertest.Violation
	d := schedulertest.NewDriver(t,
		schedulertest.WithStartTime(start),
		schedulertest.WithSchedule(s),
		schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) { tw.MaxActionsPerExecution = 1 })),
		schedulertest.WithAfterStep(collectViolations(&vs)),
	)
	backfill := func(policy enumspb.ScheduleOverlapPolicy) {
		at := timestamppb.New(start.Add(-time.Minute))
		require.NoError(t, d.Backfill(&schedulepb.BackfillRequest{StartTime: at, EndTime: at, OverlapPolicy: policy}))
	}

	require.NoError(t, d.TriggerImmediately())
	backfill(enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER)
	require.True(t, d.StepSideEffect(), "start the triggered workflow")
	backfill(enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
	require.True(t, d.StepSideEffect(), "cancel and terminate the running workflow")

	require.Empty(t, violationNames(vs))
}

// TestBackfillRespectsSmallMaxBufferSize reproduces a bug found by
// TestSchedulerProperties. The backfiller's capacity calculation subtracts the
// constant recentActionCount (10) from the buffer length instead of the number
// of completed starts actually retained, so with a MaxBufferSize below 20 it
// keeps adding starts until the buffer holds about recentActionCount plus
// MaxBufferSize/2 entries.
func TestBackfillRespectsSmallMaxBufferSize(t *testing.T) {
	t.Skip("backfillerBufferCapacity assumes recentActionCount completed starts are retained, so a backfill overfills a MaxBufferSize below 20 (backfiller_tasks.go allowedBufferedStarts)")

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s := schedulertest.DefaultSchedule()
	s.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	s.State.Paused = true
	var vs []schedulertest.Violation
	d := schedulertest.NewDriver(t,
		schedulertest.WithStartTime(start.Add(10*time.Minute)),
		schedulertest.WithSchedule(s),
		schedulertest.WithConfig(configWith(func(tw *scheduler.Tweakables) {
			tw.MaxBufferSize = 2
			tw.GeneratorBufferReserveSize = 0
		})),
		schedulertest.WithAfterStep(collectViolations(&vs)),
	)
	require.NoError(t, d.Backfill(&schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(start),
		EndTime:       timestamppb.New(start.Add(10 * time.Minute)),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	}))
	for range 12 {
		d.Step()
	}

	require.Empty(t, violationNames(vs))
}
