package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestSchedulerMigrationFailureReloadAndRetryProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	env.services.Migrate.Push(
		"unavailable",
		rpctest.Fail[
			*historyservice.StartWorkflowExecutionRequest,
			*historyservice.StartWorkflowExecutionResponse,
		](serviceerror.NewUnavailable("injected migration failure")),
	)
	_, err = env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID, ScheduleId: scheduleID,
	})
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	migrationTask := runnable[0]

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.Error(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	require.True(t, env.describe(t).GetSchedule().GetState().GetPaused())
	require.NoError(t, env.engine.ReloadExecution(t.Context(), env.ref))

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.NoError(t, err)
	calls := env.services.Migrate.Calls()
	require.Len(t, calls, 2)
	require.Equal(t, calls[0].Request.GetStartRequest().GetWorkflowId(), calls[1].Request.GetStartRequest().GetWorkflowId())
	require.NotEqual(t, calls[0].Request.GetStartRequest().GetRequestId(), calls[1].Request.GetStartRequest().GetRequestId())

	redelivery, err := env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.NoError(t, err)
	require.True(t, redelivery.Dropped)
	require.Len(t, env.services.Migrate.Calls(), 2)
}

func TestSchedulerCallbackRecoveryUsesGeneratedClientsProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	env.trigger(t)
	_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	initialCall := env.services.Start.Calls()[0]

	_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		schedule := component.(*scheduler.Scheduler)
		start := schedule.Invoker.Get(ctx).GetBufferedStarts()[0]
		start.HasCallback = false
		ctx.AddTask(schedule, chasm.TaskAttributes{}, &schedulerpb.SchedulerCallbacksTask{})
		return nil
	})
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	callbackTask := runnable[0]
	_, err = env.engine.ExecuteTask(t.Context(), env.ref, callbackTask)
	require.NoError(t, err)

	require.Len(t, env.services.Describe.Calls(), 1)
	starts := env.services.Start.Calls()
	require.Len(t, starts, 2)
	require.Equal(t, initialCall.Request.GetRequestId(), starts[1].Request.GetRequestId())
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, starts[1].Request.GetWorkflowIdConflictPolicy())

	require.NoError(t, env.engine.ReloadExecution(t.Context(), env.ref))
	redelivery, err := env.engine.ExecuteTask(t.Context(), env.ref, callbackTask)
	require.NoError(t, err)
	require.True(t, redelivery.Dropped)
	require.Len(t, env.services.Describe.Calls(), 1)
	require.Len(t, env.services.Start.Calls(), 2)
}
