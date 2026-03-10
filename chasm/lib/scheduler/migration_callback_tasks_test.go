package scheduler_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type migrationCallbackTestEnv struct {
	*testEnv
	executor           *scheduler.MigrationCallbackTaskExecutor
	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
}

func newMigrationCallbackTestEnv(t *testing.T) *migrationCallbackTestEnv {
	env := newTestEnv(t, withMockEngine())

	mockFrontendClient := workflowservicemock.NewMockWorkflowServiceClient(env.Ctrl)

	executor := scheduler.NewMigrationCallbackTaskExecutor(scheduler.MigrationCallbackTaskExecutorOptions{
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		FrontendClient: mockFrontendClient,
	})

	return &migrationCallbackTestEnv{
		testEnv:            env,
		executor:           executor,
		mockFrontendClient: mockFrontendClient,
	}
}

func (e *migrationCallbackTestEnv) addRunningWorkflows(ctx chasm.MutableContext, workflows ...[2]string) {
	invoker := e.Scheduler.Invoker.Get(ctx)
	startTime := timestamppb.New(e.TimeSource.Now())
	for _, wf := range workflows {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:   wf[0] + "-req",
			WorkflowId:  wf[0],
			RunId:       wf[1],
			Attempt:     1,
			NominalTime: startTime,
			ActualTime:  startTime,
			StartTime:   startTime,
		})
	}
}

func TestMigrationCallbackTask_AttachesToRunningWorkflows(t *testing.T) {
	env := newMigrationCallbackTestEnv(t)
	ctx := env.MutableContext()

	env.addRunningWorkflows(ctx, [2]string{"wf-1", "run-1"}, [2]string{"wf-2", "run-2"})
	env.ExpectReadComponent(ctx, env.Scheduler)

	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(_ interface{}, req *workflowservice.StartWorkflowExecutionRequest, _ ...interface{}) (*workflowservice.StartWorkflowExecutionResponse, error) {
			require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, req.WorkflowIdConflictPolicy)
			require.NotNil(t, req.OnConflictOptions)
			require.True(t, req.OnConflictOptions.AttachCompletionCallbacks)
			require.True(t, req.OnConflictOptions.AttachRequestId)
			require.Len(t, req.CompletionCallbacks, 1)
			require.Equal(t, namespace, req.Namespace)
			return &workflowservice.StartWorkflowExecutionResponse{Started: false}, nil
		})

	engineCtx := env.EngineContext()
	err := env.executor.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
	require.NoError(t, err)
}

func TestMigrationCallbackTask_NoRunningWorkflows(t *testing.T) {
	env := newMigrationCallbackTestEnv(t)
	ctx := env.MutableContext()
	_ = ctx

	env.ExpectReadComponent(ctx, env.Scheduler)

	engineCtx := env.EngineContext()
	err := env.executor.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
	require.NoError(t, err)
}

func TestMigrationCallbackTask_PartialFailure(t *testing.T) {
	env := newMigrationCallbackTestEnv(t)
	ctx := env.MutableContext()

	env.addRunningWorkflows(ctx, [2]string{"wf-1", "run-1"}, [2]string{"wf-2", "run-2"})
	env.ExpectReadComponent(ctx, env.Scheduler)

	callCount := 0
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(_ interface{}, req *workflowservice.StartWorkflowExecutionRequest, _ ...interface{}) (*workflowservice.StartWorkflowExecutionResponse, error) {
			callCount++
			if callCount == 2 {
				return nil, fmt.Errorf("transient error")
			}
			return &workflowservice.StartWorkflowExecutionResponse{Started: false}, nil
		})

	engineCtx := env.EngineContext()
	err := env.executor.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to attach callbacks to 1 of 2 running workflows")
}

func TestMigrationCallbackTask_SkipsCompletedWorkflows(t *testing.T) {
	env := newMigrationCallbackTestEnv(t)
	ctx := env.MutableContext()

	invoker := env.Scheduler.Invoker.Get(ctx)
	startTime := timestamppb.New(env.TimeSource.Now())
	invoker.BufferedStarts = []*schedulespb.BufferedStart{
		// Running workflow - should get callback.
		{
			RequestId: "req1", WorkflowId: "wf-1", RunId: "run-1",
			Attempt: 1, NominalTime: startTime, ActualTime: startTime, StartTime: startTime,
		},
		// Completed workflow - should be skipped.
		{
			RequestId: "req2", WorkflowId: "wf-2", RunId: "run-2",
			Attempt: 1, NominalTime: startTime, ActualTime: startTime, StartTime: startTime,
			Completed: &schedulespb.CompletedResult{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		},
		// Pending workflow (no RunId) - should be skipped.
		{
			RequestId: "req3", WorkflowId: "wf-3",
			Attempt: 1, NominalTime: startTime, ActualTime: startTime,
		},
	}

	env.ExpectReadComponent(ctx, env.Scheduler)

	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(_ interface{}, req *workflowservice.StartWorkflowExecutionRequest, _ ...interface{}) (*workflowservice.StartWorkflowExecutionResponse, error) {
			require.Equal(t, "wf-1", req.WorkflowId)
			return &workflowservice.StartWorkflowExecutionResponse{Started: false}, nil
		})

	engineCtx := env.EngineContext()
	err := env.executor.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
	require.NoError(t, err)
}

// TestMigrationCallbackTask_TerminatesAccidentallyStartedWorkflow validates that
// when a workflow has already completed before the callback attachment, the
// USE_EXISTING conflict policy starts a new workflow (Started=true). The executor
// should detect this and terminate the unintended workflow.
func TestMigrationCallbackTask_TerminatesAccidentallyStartedWorkflow(t *testing.T) {
	env := newMigrationCallbackTestEnv(t)
	ctx := env.MutableContext()

	env.addRunningWorkflows(ctx, [2]string{"wf-1", "run-1"})
	env.ExpectReadComponent(ctx, env.Scheduler)

	// Simulate the workflow having already completed: USE_EXISTING starts a new one.
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			Started: true,
			RunId:   "new-run-id",
		}, nil)

	// Expect the accidental workflow to be terminated.
	env.mockFrontendClient.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(_ interface{}, req *workflowservice.TerminateWorkflowExecutionRequest, _ ...interface{}) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
			require.Equal(t, "wf-1", req.WorkflowExecution.WorkflowId)
			require.Equal(t, "new-run-id", req.WorkflowExecution.RunId)
			require.Contains(t, req.Reason, "started unintentionally")
			return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
		})

	engineCtx := env.EngineContext()
	err := env.executor.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
	require.NoError(t, err)
}

func TestMigrationCallbackTask_Validate(t *testing.T) {
	env := newMigrationCallbackTestEnv(t)

	t.Run("valid with running workflows", func(t *testing.T) {
		ctx := env.MutableContext()
		env.addRunningWorkflows(ctx, [2]string{"wf-1", "run-1"})

		valid, err := env.executor.Validate(ctx, env.Scheduler, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("invalid without running workflows", func(t *testing.T) {
		ctx := env.MutableContext()
		invoker := env.Scheduler.Invoker.Get(ctx)
		invoker.BufferedStarts = nil

		valid, err := env.executor.Validate(ctx, env.Scheduler, chasm.TaskAttributes{}, &schedulerpb.MigrationCallbackTask{})
		require.NoError(t, err)
		require.False(t, valid)
	})
}
