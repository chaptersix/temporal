package scheduler_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	workflowservicemock "go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMigrationImportedBufferNativeControl(t *testing.T) {
	testMigrationImportedBuffer(t, true, false, false, 0, false)
}

func TestMigrationImportedBufferCounterexample(t *testing.T) {
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") != "1" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1")
	}
	testMigrationImportedBuffer(t, false, false, false, 0, false)
}

func testMigrationImportedBuffer(t *testing.T, native, scheduled, empty bool, attempt int64, backingOff bool) {
	t.Helper()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	ts := clock.NewEventTimeSource().Update(now)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	engine, engineCtx := newTestEngineContext(t, logger, withEngineTimeSource(ts))
	spec := defaultSchedule()
	spec.State.Paused = !scheduled
	if !scheduled {
		spec.Spec = &schedulepb.ScheduleSpec{}
	}
	start := &schedulespb.BufferedStart{
		NominalTime: timestamppb.New(now), ActualTime: timestamppb.New(now), DesiredTime: timestamppb.New(now),
		Manual: true, RequestId: "import-request", WorkflowId: "import-workflow",
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, Attempt: attempt,
	}
	if backingOff {
		start.BackoffTime = timestamppb.New(now.Add(time.Minute))
	}
	var buffered []*schedulespb.BufferedStart
	if !empty {
		buffered = append(buffered, start)
	}
	req := &schedulerpb.CreateFromMigrationStateRequest{NamespaceId: namespaceID, State: &schedulerpb.SchedulerMigrationState{
		SchedulerState: &schedulerpb.SchedulerState{Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID, Schedule: spec, Info: &schedulepb.ScheduleInfo{}, ConflictToken: 1},
		GeneratorState: &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now)},
		InvokerState:   &schedulerpb.InvokerState{LastProcessedTime: timestamppb.New(now), BufferedStarts: buffered},
	}}
	if native {
		req.State.InvokerState.BufferedStarts = nil
	}
	result, err := chasm.StartExecution(engineCtx, chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID}, scheduler.CreateSchedulerFromMigration, req)
	require.NoError(t, err)
	ref := chasm.NewComponentRef[*scheduler.Scheduler](result.ExecutionKey)
	var invoker *scheduler.Invoker
	_, _, err = chasm.UpdateComponent(engineCtx, ref, func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
		invoker = s.Invoker.Get(ctx)
		if native {
			invoker.EnqueueBufferedStarts(ctx, buffered)
		}
		return struct{}{}, nil
	}, struct{}{})
	require.NoError(t, err)
	_, err = engine.FirePureTasks(ref, now)
	require.NoError(t, err)
	physical, err := engine.Tasks(ref)
	require.NoError(t, err)
	if empty || backingOff {
		require.Empty(t, physical[tasks.CategoryTransfer])
		if empty {
			return
		}
		ts.Update(now.Add(time.Minute))
		_, err = engine.FirePureTasks(ref, ts.Now())
		require.NoError(t, err)
		physical, err = engine.Tasks(ref)
		require.NoError(t, err)
	}
	require.NotEmpty(t, physical[tasks.CategoryTransfer], "seed=import-buffer: immediate pure tasks drained; imported action has no execution task")
	client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
		require.Equal(t, "import-request", request.RequestId)
		require.Equal(t, "import-workflow", request.WorkflowId)
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "import-run"}, nil
	}).Times(1)
	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, FrontendClient: client})
	dropped, err := chasmtest.ExecuteSideEffectTask(context.Background(), engine, invoker, handler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, dropped)
	dropped, err = chasmtest.ExecuteSideEffectTask(context.Background(), engine, invoker, handler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.True(t, dropped)
}
