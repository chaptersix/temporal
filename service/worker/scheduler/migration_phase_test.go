package scheduler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMigrationPhaseV1NativeControl(t *testing.T) { testMigrationPhaseV1(t, false) }

func TestMigrationPhaseV1Counterexample(t *testing.T) {
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") != "1" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1")
	}
	testMigrationPhaseV1(t, true)
}

func testMigrationPhaseV1(t *testing.T, importedRetry bool) {
	t.Helper()
	previous := CurrentTweakablePolicies
	t.Cleanup(func() { CurrentTweakablePolicies = previous })
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 1
	CurrentTweakablePolicies.Version = MigrationHandoffFixes
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(baseStartTime)
	when := timestamppb.New(baseStartTime)
	start := &schedulespb.BufferedStart{NominalTime: when, ActualTime: when, DesiredTime: when, Manual: true, RequestId: "phase-request", WorkflowId: "phase-workflow", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	if importedRetry {
		start.Attempt = 3
		start.BackoffTime = timestamppb.New(baseStartTime.Add(time.Hour))
	}
	var startedAt time.Time
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Once().Return(func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		require.Equal(t, start.RequestId, req.Request.RequestId)
		require.Equal(t, start.WorkflowId, req.Request.WorkflowId)
		startedAt = env.Now()
		return &schedulespb.StartWorkflowResponse{RunId: "phase-run", RealStartTime: timestamppb.New(startedAt)}, nil
	})
	env.ExecuteWorkflow(SchedulerWorkflow, &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{}, Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}, Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "configured", WorkflowType: &commonpb.WorkflowType{Name: "action"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "queue"}}}}},
		State:    &schedulespb.InternalState{Namespace: "ns", NamespaceId: "ns-id", ScheduleId: "schedule", ConflictToken: InitialConflictToken, BufferedStarts: []*schedulespb.BufferedStart{start}},
	})
	require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
	env.AssertExpectations(t)
	require.False(t, startedAt.IsZero())
	if importedRetry {
		require.False(t, startedAt.Before(start.BackoffTime.AsTime()), "seed=rollback-retry: V1 bypasses the persisted CHASM retry deadline")
	}
}
