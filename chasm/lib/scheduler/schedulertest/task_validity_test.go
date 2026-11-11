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
	require.NotEmpty(t, d.pendingSideEffectTasks())
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			c.(*scheduler.Scheduler).Invoker.Get(ctx).BufferedStarts = nil
			return nil
		})
	require.NoError(t, err)
	require.NotEmpty(t, d.pendingSideEffectTasks(), "physical deliveries survive logical invalidation")
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
	tasks := d.pendingSideEffectTasks()
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
	inTree, valid, err := d.node().ValidateSideEffectTask(context.Background(), old)
	require.NoError(t, err)
	require.True(t, inTree)
	require.True(t, valid)
	require.True(t, d.StepSideEffect())
	require.Contains(t, d.Snapshot().AcceptedStarts, "current-request")
}

func TestIdleRouteRequiresMatchingLogicalTask(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.LimitedActions = true
	d := NewDriver(t, WithSchedule(schedule))
	require.True(t, d.Snapshot().HasValidIdleTask)
	_, err := d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			s := c.(*scheduler.Scheduler)
			s.LastEventTime = timestamppb.New(d.Now().Add(time.Hour))
			s.IdleCloseTime = timestamppb.New(s.GetIdleCloseTime().AsTime().Add(time.Hour))
			// Deliberately omit Generator.Generate, which publishes the replacement.
			return nil
		})
	require.NoError(t, err)
	snap := d.Snapshot()
	require.False(t, snap.HasValidIdleTask)
	violations := CheckInvariants(nil, snap)
	require.Contains(t, violations, Violation{Name: "idle-route", Message: "advertised idle deadline " + snap.IdleCloseTime.String() + " has no valid logical idle task"})
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
