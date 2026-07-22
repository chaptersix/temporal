package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"pgregory.net/rapid"
)

func TestSchedulerMigrationFailureReloadAndRetryProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	schedulerRPCProfiles{}.migrationRetryable().Queue(&env.services.Migrate)
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

func TestSchedulerMigrationTerminalFailureRestoresScheduleProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	schedulerRPCProfiles{}.migrationTerminal().Queue(&env.services.Migrate)
	_, err = env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID, ScheduleId: scheduleID,
	})
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, runnable[0])
	require.NoError(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	require.False(t, env.describe(t).GetSchedule().GetState().GetPaused())
	runnable, err = env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Empty(t, runnable)
}

func TestSchedulerCallbackRecoveryUsesGeneratedClientsProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		env := newSchedulerPropertyEnv(t, false)
		profiles := schedulerRPCProfiles{}
		behavior := rpcgen.Draw(t, "DescribeWorkflowExecution callback behavior",
			profiles.describeRunning(),
			profiles.describeCompleted(),
		)
		behavior.Queue(&env.services.Describe)
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

		describes := env.services.Describe.Calls()
		require.Len(t, describes, 1)
		require.Equal(t, behavior.Label, describes[0].Name)
		require.False(t, describes[0].Deadline.IsZero())
		starts := env.services.Start.Calls()
		wantStartCalls := 1
		if behavior.Label == "running" {
			wantStartCalls = 2
			require.Equal(t, initialCall.Request.GetRequestId(), starts[1].Request.GetRequestId())
			require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, starts[1].Request.GetWorkflowIdConflictPolicy())
		}
		require.Len(t, starts, wantStartCalls)
		for _, start := range starts {
			require.False(t, start.Deadline.IsZero())
		}

		require.NoError(t, env.engine.ReloadExecution(t.Context(), env.ref))
		redelivery, err := env.engine.ExecuteTask(t.Context(), env.ref, callbackTask)
		require.NoError(t, err)
		require.True(t, redelivery.Dropped)
		require.Len(t, env.services.Describe.Calls(), 1)
		require.Len(t, env.services.Start.Calls(), wantStartCalls)
	})
}
