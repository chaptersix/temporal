package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecuteResultReconcilesDuplicateRequestIDsAfterBufferReorder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		activity bool
	}{
		{name: "workflow"},
		{name: "activity", activity: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newInvokerExecuteTestEnv(t)
			if tc.activity {
				env.Scheduler.Schedule = activitySchedule()
			}
			ctx := env.MutableContext()
			now := timestamppb.New(env.TimeSource.Now())
			starts := []*schedulespb.BufferedStart{
				duplicateExecutionStart(now, "one", "target-one", tc.activity),
				duplicateExecutionStart(now, "two", "target-two", tc.activity),
			}
			invoker := env.Scheduler.Invoker.Get(ctx)
			invoker.BufferedStarts = starts
			invoker.LastProcessedTime = now

			calls := 0
			runIDs := make(map[string]string)
			if tc.activity {
				env.mockFrontendClient.EXPECT().StartActivityExecution(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *workflowservice.StartActivityExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartActivityExecutionResponse, error) {
						runID := []string{"activity-run-one", "activity-run-two"}[calls]
						runIDs[req.ActivityId] = runID
						calls++
						return &workflowservice.StartActivityExecutionResponse{RunId: runID, Started: true}, nil
					}).Times(2)
			} else {
				env.mockFrontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
						runID := []string{"workflow-run-one", "workflow-run-two"}[calls]
						runIDs[req.WorkflowId] = runID
						calls++
						return &workflowservice.StartWorkflowExecutionResponse{RunId: runID}, nil
					}).Times(2)
			}

			env.ExpectReadComponent(ctx, invoker)
			batch, err := env.handler.LoadExecutionBatchForTest(env.EngineContext(), chasm.ComponentRef{})
			require.NoError(t, err)
			result := env.handler.ExecuteBatchForTest(env.EngineContext(), batch)

			invoker.BufferedStarts[0], invoker.BufferedStarts[1] = invoker.BufferedStarts[1], invoker.BufferedStarts[0]
			env.ExpectUpdateComponent(ctx, invoker)
			_, err = env.handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
			require.NoError(t, err)
			require.Equal(t, int64(2), env.Scheduler.Info.ActionCount)
			require.Equal(t, "two", invoker.BufferedStarts[0].GetOccurrenceId())
			require.Equal(t, runIDs[invoker.BufferedStarts[0].GetExecution().GetBusinessId()], runID(invoker.BufferedStarts[0]))
			require.Equal(t, "one", invoker.BufferedStarts[1].GetOccurrenceId())
			require.Equal(t, runIDs[invoker.BufferedStarts[1].GetExecution().GetBusinessId()], runID(invoker.BufferedStarts[1]))

			env.ExpectUpdateComponent(ctx, invoker)
			_, err = env.handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
			require.NoError(t, err)
			require.Equal(t, int64(2), env.Scheduler.Info.ActionCount)
		})
	}
}

func TestLegacyRecordExecuteResultUsesFirstRequestMatch(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{
		duplicateExecutionStart(timestamppb.Now(), "first", "target-first", false),
		duplicateExecutionStart(timestamppb.Now(), "second", "target-second", false),
	}

	newlyStarted, dropped, _ := invoker.RecordExecuteResult(ctx, []*schedulespb.BufferedStart{{RequestId: "same-request", RunId: "first-run"}}, nil)
	require.Equal(t, 1, newlyStarted)
	require.Zero(t, dropped)
	require.Equal(t, "first-run", invoker.BufferedStarts[0].GetExecution().GetRunId())
	require.Empty(t, invoker.BufferedStarts[1].GetExecution().GetRunId())
}

func duplicateExecutionStart(now *timestamppb.Timestamp, occurrenceID, targetID string, activity bool) *schedulespb.BufferedStart {
	kind := enumspb.EXECUTION_TYPE_WORKFLOW
	if activity {
		kind = enumspb.EXECUTION_TYPE_ACTIVITY
	}
	start := &schedulespb.BufferedStart{
		NominalTime: now, ActualTime: now, DesiredTime: now,
		RequestId: "same-request", OccurrenceId: occurrenceID, Attempt: 1,
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		Execution:     &commonpb.Execution{Type: kind, BusinessId: targetID},
	}
	if !activity {
		start.WorkflowId = targetID
	}
	return start
}

func runID(start *schedulespb.BufferedStart) string {
	return start.GetExecution().GetRunId()
}
