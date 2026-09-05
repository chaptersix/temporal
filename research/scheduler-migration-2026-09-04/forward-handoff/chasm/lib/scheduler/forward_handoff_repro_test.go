package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestForwardHandoffTargetCommitAndReplayControl(t *testing.T) {
	now := time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC)
	timeSource := clock.NewEventTimeSource().Update(now)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	engine, engineCtx := newTestEngineContext(t, logger, withEngineTimeSource(timeSource))
	handler := scheduler.NewTestHandler(logger)
	definition := defaultSchedule()
	definition.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	req := migration.LegacyToCreateFromMigrationStateRequest(definition, &schedulepb.ScheduleInfo{}, &schedulespb.InternalState{
		Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID, ConflictToken: 1,
		LastProcessedTime: timestamppb.New(now),
		BufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: timestamppb.New(now), ActualTime: timestamppb.New(now), Manual: true,
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}},
	}, nil, nil, now)
	_, err := handler.TestCreateFromMigrationState(engineCtx, req)
	require.NoError(t, err)
	_, err = handler.TestCreateFromMigrationState(engineCtx, req)
	var alreadyExists *serviceerror.AlreadyExists
	require.ErrorAs(t, err, &alreadyExists)
	changed := common.CloneProto(req)
	changed.State.SchedulerState.Schedule.State.Notes = "accepted by V1 after ambiguous commit"
	changed.State.SchedulerState.ConflictToken++
	_, err = handler.TestCreateFromMigrationState(engineCtx, changed)
	require.ErrorAs(t, err, &alreadyExists)

	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})
	var generator *scheduler.Generator
	var invoker *scheduler.Invoker
	_, err = chasm.ReadComponent(engineCtx, rootRef, func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
		require.Empty(t, s.Schedule.State.Notes)
		require.Equal(t, int64(1), s.ConflictToken)
		generator = s.Generator.Get(ctx)
		invoker = s.Invoker.Get(ctx)
		return struct{}{}, nil
	}, struct{}{})
	require.NoError(t, err)
	timeSource.Update(now.Add(time.Minute))
	config := defaultConfig()
	specBuilder := newLegacySpecBuilder(0, 0)
	specProcessor := scheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler, logger, specBuilder)
	generatorHandler := scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
		Config: config, BaseLogger: logger, MetricsHandler: metrics.NoopMetricsHandler,
		SpecBuilder: specBuilder, SpecProcessor: specProcessor,
	})
	dropped, err := chasmtest.ExecutePureTask(context.Background(), engine, generator, generatorHandler, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)
	require.False(t, dropped)
	bufferHandler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config: config, BaseLogger: logger, MetricsHandler: metrics.NoopMetricsHandler, SpecProcessor: specProcessor,
	})
	dropped, err = chasmtest.ExecutePureTask(context.Background(), engine, invoker, bufferHandler, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.False(t, dropped)
	frontend := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	manualStarts := 0
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(_ context.Context, start *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			if start.WorkflowId == req.State.InvokerState.BufferedStarts[0].WorkflowId {
				manualStarts++
				require.Equal(t, req.State.InvokerState.BufferedStarts[0].RequestId, start.RequestId)
				require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, start.WorkflowIdReusePolicy)
			}
			return &workflowservice.StartWorkflowExecutionResponse{RunId: start.RequestId}, nil
		})
	executeHandler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config: config, BaseLogger: logger, MetricsHandler: metrics.NoopMetricsHandler, FrontendClient: frontend,
	})
	dropped, err = chasmtest.ExecuteSideEffectTask(context.Background(), engine, invoker, executeHandler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, dropped)
	require.Equal(t, 1, manualStarts)
}
