package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecuteTask_SharedBudgetPreservesActionCategoryOrder(t *testing.T) {
	var order []string
	record := func(action string) {
		order = append(order, action)
	}

	env := newInvokerExecuteTestEnv(t, func(config *scheduler.Config) {
		tweakables := scheduler.DefaultTweakables
		tweakables.MaxActionsPerExecution = 3
		config.Tweakables = func(string) scheduler.Tweakables { return tweakables }
	})
	env.mockHistoryClient.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.TerminateWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			record("terminate")
			return nil, nil
		})
	env.mockHistoryClient.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *historyservice.RequestCancelWorkflowExecutionRequest, ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			record("cancel")
			return nil, nil
		})
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *workflowservice.StartWorkflowExecutionRequest, ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			record("start")
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "run"}, nil
		})

	now := timestamppb.New(env.TimeSource.Now())
	runExecuteTestCase(t, env, &executeTestCase{
		InitialTerminateWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: "terminate", RunId: "terminate-run"}},
		InitialCancelWorkflows:    []*commonpb.WorkflowExecution{{WorkflowId: "cancel", RunId: "cancel-run"}},
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: now,
			ActualTime:  now,
			DesiredTime: now,
			RequestId:   "start",
			WorkflowId:  "start-workflow",
			Attempt:     1,
		}},
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
	})
	require.Equal(t, []string{"terminate", "cancel", "start"}, order)
}

func TestExecuteTask_SharedBudgetStopsBeforeStarts(t *testing.T) {
	env := newInvokerExecuteTestEnv(t, func(config *scheduler.Config) {
		tweakables := scheduler.DefaultTweakables
		tweakables.MaxActionsPerExecution = 2
		config.Tweakables = func(string) scheduler.Tweakables { return tweakables }
	})
	env.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	env.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)

	now := timestamppb.New(env.TimeSource.Now())
	runExecuteTestCase(t, env, &executeTestCase{
		InitialTerminateWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: "terminate", RunId: "terminate-run"}},
		InitialCancelWorkflows:    []*commonpb.WorkflowExecution{{WorkflowId: "cancel", RunId: "cancel-run"}},
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: now,
			ActualTime:  now,
			DesiredTime: now,
			RequestId:   "waiting",
			WorkflowId:  "waiting-workflow",
			Attempt:     1,
		}},
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 0,
	})
	valid, reason := scheduler.EvaluateInvokerExecuteTaskValidityForTest(
		env.Scheduler.Invoker.Get(env.ReadContext()), env.Scheduler)
	require.True(t, valid)
	require.Equal(t, "none", reason)
}

func TestExecuteTask_CommitMergesCompletionBeforeStartAcknowledgment(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := timestamppb.New(env.TimeSource.Now())
	invoker.LastProcessedTime = now
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime: now,
		ActualTime:  now,
		DesiredTime: now,
		RequestId:   "completion-race",
		WorkflowId:  "workflow",
		Attempt:     1,
	}}

	env.ExpectReadComponent(ctx, invoker)
	batch, err := env.handler.LoadExecutionBatchForTest(env.EngineContext(), chasm.ComponentRef{})
	require.NoError(t, err)
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("completion-race")).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "loaded-run"}, nil)
	result := env.handler.ExecuteBatchForTest(context.Background(), batch)

	completed := &schedulespb.CompletedResult{
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		CloseTime: now,
	}
	invoker.BufferedStarts[0].Completed = completed
	env.ExpectUpdateComponent(ctx, invoker)
	executeTaskScheduled, err := env.handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.Equal(t, "loaded-run", invoker.BufferedStarts[0].GetRunId())
	require.Equal(t, completed, invoker.BufferedStarts[0].GetCompleted())
	require.EqualValues(t, 1, env.Scheduler.Info.ActionCount)
	require.False(t, executeTaskScheduled)
}

func TestExecuteTask_CommitInvalidatesChangedWorkflowTarget(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.CancelWorkflows = []*commonpb.WorkflowExecution{{WorkflowId: "workflow", RunId: "loaded-run"}}

	env.ExpectReadComponent(ctx, invoker)
	batch, err := env.handler.LoadExecutionBatchForTest(env.EngineContext(), chasm.ComponentRef{})
	require.NoError(t, err)
	env.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	result := env.handler.ExecuteBatchForTest(context.Background(), batch)

	invoker.CancelWorkflows[0].RunId = "replacement-run"
	env.ExpectUpdateComponent(ctx, invoker)
	executeTaskScheduled, err := env.handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.Equal(t, "replacement-run", invoker.CancelWorkflows[0].GetRunId())
	require.True(t, executeTaskScheduled)
}

func TestExecuteTask_CommitPreservesFirstWriterAndRecordsInvalidation(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	recorder := metricstest.NewCaptureHandler()
	capture := recorder.StartCapture()
	defer recorder.StopCapture(capture)
	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: recorder,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		HistoryClient:  env.mockHistoryClient,
		FrontendClient: env.mockFrontendClient,
	})
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := timestamppb.New(env.TimeSource.Now())
	invoker.LastProcessedTime = now
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime: now,
		ActualTime:  now,
		DesiredTime: now,
		RequestId:   "request",
		WorkflowId:  "workflow",
		Attempt:     1,
	}}

	env.ExpectReadComponent(ctx, invoker)
	batch, err := handler.LoadExecutionBatchForTest(env.EngineContext(), chasm.ComponentRef{})
	require.NoError(t, err)
	env.mockFrontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "losing-run"}, nil)
	result := handler.ExecuteBatchForTest(context.Background(), batch)

	winningStartTime := timestamppb.New(now.AsTime().Add(time.Second))
	invoker.BufferedStarts[0].RunId = "winning-run"
	invoker.BufferedStarts[0].StartTime = winningStartTime
	invoker.BufferedStarts[0].HasCallback = true
	env.ExpectUpdateComponent(ctx, invoker)
	executeTaskScheduled, err := handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.False(t, executeTaskScheduled)
	require.Equal(t, "winning-run", invoker.BufferedStarts[0].GetRunId())
	require.Equal(t, winningStartTime, invoker.BufferedStarts[0].GetStartTime())
	require.Zero(t, env.Scheduler.Info.ActionCount)

	var invalidation *metricstest.CapturedRecording
	for _, recording := range capture.Snapshot()[metrics.ScheduleInvokerExecuteTask.Name()] {
		if recording.Tags["outcome"] == "invalidated" {
			invalidation = recording
		}
	}
	require.NotNil(t, invalidation)
	require.Equal(t, "already_recorded", invalidation.Tags["reason"])
	require.Equal(t, int64(1), invalidation.Value)
}

func TestExecuteTask_DuplicateRequestIDsCommitToExactLoadedStarts(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	conflictToken := env.Scheduler.GetConflictToken()
	now := timestamppb.New(env.TimeSource.Now())
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			return &workflowservice.StartWorkflowExecutionResponse{RunId: request.GetWorkflowId() + "-run"}, nil
		})

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{
			{NominalTime: now, ActualTime: now, DesiredTime: now, RequestId: "duplicate", WorkflowId: "first-workflow", Attempt: 1},
			{NominalTime: now, ActualTime: now, DesiredTime: now, RequestId: "duplicate", WorkflowId: "second-workflow", Attempt: 1},
		},
		ExpectedBufferedStarts:   2,
		ExpectedRunningWorkflows: 2,
		ExpectedActionCount:      2,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker, _ *invokerExecuteTestEnv) {
			require.Equal(t, "first-workflow-run", invoker.BufferedStarts[0].GetRunId())
			require.Equal(t, "second-workflow-run", invoker.BufferedStarts[1].GetRunId())
		},
	})
	require.Equal(t, conflictToken, env.Scheduler.GetConflictToken())
}

func TestExecuteTask_EngineCharacterizesCurrentWorkValidity(t *testing.T) {
	t.Run("old task executes different current work", func(t *testing.T) {
		env := newInvokerExecuteEngine(t)
		now := env.timeSource.Now()

		err := env.updateScheduler(
			func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				invoker := s.Invoker.Get(ctx)
				invoker.LastProcessedTime = timestamppb.New(now)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					RequestId:  "original-work",
					WorkflowId: "original-workflow",
					Attempt:    1,
				}}
				ctx.AddTask(invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
				return nil
			})
		require.NoError(t, err)

		var invoker *scheduler.Invoker
		err = env.updateScheduler(
			func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				invoker = s.Invoker.Get(ctx)
				s.Info.OverlapSkipped++ // unrelated scheduler state
				invoker.BufferedStarts[0].RunId = "original-run"
				invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
					RequestId:  "current-work",
					WorkflowId: "current-workflow",
					Attempt:    1,
				})
				return nil
			})
		require.NoError(t, err)

		env.frontendClient.EXPECT().
			StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("current-work")).
			Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "current-run"}, nil)

		dropped, err := chasmtest.ExecuteSideEffectTask(
			context.Background(), env.engine, invoker, env.handler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
		require.NoError(t, err)
		require.False(t, dropped)

		err = env.readScheduler(
			func(s *scheduler.Scheduler, ctx chasm.Context) error {
				starts := s.Invoker.Get(ctx).GetBufferedStarts()
				require.Equal(t, "original-run", starts[0].GetRunId())
				require.Equal(t, "current-run", starts[1].GetRunId())
				return nil
			})
		require.NoError(t, err)
	})

	t.Run("completion before start remains execute eligible", func(t *testing.T) {
		env := newInvokerExecuteEngine(t)
		now := env.timeSource.Now()
		completed := &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(now),
		}

		var invoker *scheduler.Invoker
		err := env.updateScheduler(
			func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				invoker = s.Invoker.Get(ctx)
				invoker.LastProcessedTime = timestamppb.New(now)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					NominalTime: timestamppb.New(now),
					ActualTime:  timestamppb.New(now),
					DesiredTime: timestamppb.New(now),
					RequestId:   "racing-work",
					WorkflowId:  "racing-workflow",
					Attempt:     1,
					Completed:   completed,
				}}
				ctx.AddTask(invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
				return nil
			})
		require.NoError(t, err)

		env.frontendClient.EXPECT().
			StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("racing-work")).
			Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "racing-run"}, nil)

		dropped, err := chasmtest.ExecuteSideEffectTask(
			context.Background(), env.engine, invoker, env.handler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
		require.NoError(t, err)
		require.False(t, dropped)

		err = env.readScheduler(
			func(s *scheduler.Scheduler, ctx chasm.Context) error {
				start := s.Invoker.Get(ctx).GetBufferedStarts()[0]
				require.Equal(t, "racing-run", start.GetRunId())
				require.Equal(t, completed, start.GetCompleted())
				return nil
			})
		require.NoError(t, err)
	})
}
