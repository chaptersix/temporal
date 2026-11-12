package schedulertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStalePhysicalTaskIsNotProgress(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.Paused = true
	schedule.Spec.Interval = nil
	d := NewDriver(t, WithSchedule(schedule))
	require.NoError(t, d.TriggerImmediately())
	require.NotEmpty(t, d.runner.PendingSideEffectTasks())
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			c.(*scheduler.Scheduler).Invoker.Get(ctx).BufferedStarts = nil
			return nil
		})
	require.NoError(t, err)
	require.NotEmpty(t, d.runner.PendingSideEffectTasks(), "physical deliveries survive logical invalidation")
	require.False(t, d.HasPendingTask())
	require.False(t, d.StepSideEffect())
	require.Empty(t, d.Snapshot().AcceptedStarts)
}

func TestOldExecuteTaskCanProcessCurrentWork(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.Paused = true
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	d := NewDriver(t, WithSchedule(schedule))
	require.NoError(t, d.TriggerImmediately())
	tasks := d.runner.PendingSideEffectTasks()
	require.NotEmpty(t, tasks)
	old := tasks[0]
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			inv := c.(*scheduler.Scheduler).Invoker.Get(ctx)
			current := common.CloneProto(inv.BufferedStarts[0])
			current.RequestId = "current-request"
			current.WorkflowId = "current-workflow"
			inv.BufferedStarts = append(inv.BufferedStarts, current)
			inv.BufferedStarts[0].RunId = "original-run"
			return nil
		})
	require.NoError(t, err)
	inTree, valid := d.runner.ValidateSideEffectTask(old)
	require.True(t, inTree)
	require.True(t, valid)
	require.True(t, d.StepSideEffect())
	require.Contains(t, d.Snapshot().AcceptedStarts, "current-request")
}

// TestIdleDeadlineRequiresValidIdleTask corrupts the advertised idle deadline so
// the armed idle task no longer matches it, and asserts the shared state
// invariant (which validates armed idle tasks through the handler) reports it.
// The driver is built WithoutStateInvariant, since the engine would otherwise
// fail the test the moment the corrupting transition commits.
func TestIdleDeadlineRequiresValidIdleTask(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.LimitedActions = true
	d := NewDriver(t, WithSchedule(schedule), WithoutStateInvariant())
	require.Empty(t, d.StateViolations())
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			s := c.(*scheduler.Scheduler)
			s.LastEventTime = timestamppb.New(d.Now().Add(time.Hour))
			s.IdleCloseTime = timestamppb.New(s.GetIdleCloseTime().AsTime().Add(time.Hour))
			// Deliberately omit Generator.Generate, which publishes the replacement.
			return nil
		})
	require.NoError(t, err)
	violations := d.StateViolations()
	require.Len(t, violations, 1)
	require.Contains(t, violations[0], "no valid SchedulerIdleTask is armed")
}

func TestRetryTaskEqualityAndRearming(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.Paused = true
	d := NewDriver(t, WithSchedule(schedule))
	require.NoError(t, d.TriggerImmediately())
	deadline := d.Now().Add(time.Minute)
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			inv := c.(*scheduler.Scheduler).Invoker.Get(ctx)
			inv.BufferedStarts[0].Attempt = 2
			inv.BufferedStarts[0].BackoffTime = timestamppb.New(deadline)
			ctx.AddTask(inv, chasm.TaskAttributes{ScheduledTime: deadline}, &schedulerpb.InvokerProcessBufferTask{})
			return nil
		})
	require.NoError(t, err)
	require.False(t, d.StepSideEffect())
	require.True(t, d.StepPure())
	require.Equal(t, deadline, d.Now())
	require.True(t, d.StepSideEffect())
	require.Len(t, d.Snapshot().AcceptedStarts, 1)
}
