package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

func TestSchedulerStartFailureRetryAndRedeliveryProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		env := newSchedulerPropertyEnv(t, false)
		_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
		require.NoError(t, err)
		behavior := rpcgen.Draw(t, "StartWorkflowExecution retry behavior",
			schedulerRPCProfiles{}.startRetryableWithCode(codes.Unavailable),
			schedulerRPCProfiles{}.startRetryableWithCode(codes.ResourceExhausted),
		)
		behavior.Queue(&env.services.Start)
		env.trigger(t)

		failedTask := deliverUntilStartCall(t, env)
		calls := env.services.Start.Calls()
		require.Len(t, calls, 1)
		require.Equal(t, behavior.Label, calls[0].Name)
		require.Equal(t, status.Code(behavior.Err), status.Code(calls[0].Err))
		requestID := calls[0].Request.GetRequestId()
		require.NotEmpty(t, requestID)

		redelivery, err := env.engine.ExecuteTask(t.Context(), env.ref, failedTask)
		require.NoError(t, err)
		require.True(t, redelivery.Dropped)
		require.Len(t, env.services.Start.Calls(), 1)

		beforeReload := env.describe(t)
		require.NoError(t, env.engine.ReloadExecution(t.Context(), env.ref))
		protorequire.ProtoEqual(t, beforeReload, env.describe(t))

		env.timeSource.Update(nextSchedulerTaskTime(t, env))
		_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
		require.NoError(t, err)
		calls = env.services.Start.Calls()
		require.Len(t, calls, 2)
		require.NoError(t, calls[1].Err)
		require.Equal(t, requestID, calls[1].Request.GetRequestId())

		description := env.describe(t)
		require.Equal(t, int64(1), description.GetInfo().GetActionCount())
		require.Len(t, description.GetInfo().GetRunningWorkflows(), 1)
		require.Equal(t, int64(0), description.GetInfo().GetBufferSize())
	})
}

type schedulerPropertyFatalT interface {
	schedulerPropertyTestingT
	Context() context.Context
	Fatal(...any)
}

func deliverUntilStartCall(t schedulerPropertyFatalT, env *schedulerPropertyEnv) tasks.Task {
	t.Helper()
	for range schedulerConformanceDrainLimit {
		runnable, err := env.engine.RunnableTasks(env.ref)
		require.NoError(t, err)
		require.NotEmpty(t, runnable)
		task := runnable[0]
		_, err = env.engine.ExecuteTask(t.Context(), env.ref, task)
		require.NoError(t, err)
		if len(env.services.Start.Calls()) > 0 {
			return task
		}
	}
	t.Fatal("start RPC was not reached within drain limit")
	return nil
}

func nextSchedulerTaskTime(t schedulerPropertyTestingT, env *schedulerPropertyEnv) time.Time {
	t.Helper()
	queued, err := env.engine.Tasks(env.ref)
	require.NoError(t, err)
	now := env.timeSource.Now()
	var next time.Time
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			visibility := task.GetVisibilityTime()
			if visibility.After(now) && (next.IsZero() || visibility.Before(next)) {
				next = visibility
			}
		}
	}
	require.False(t, next.IsZero())
	return next
}
