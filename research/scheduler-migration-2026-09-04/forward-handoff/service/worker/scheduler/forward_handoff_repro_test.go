package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type responseLossSchedulerClient struct {
	schedulerpb.SchedulerServiceClient
	committed   *schedulerpb.CreateFromMigrationStateRequest
	lastRequest *schedulerpb.CreateFromMigrationStateRequest
	loseReply   bool
	calls       int
}

func (c *responseLossSchedulerClient) CreateFromMigrationState(
	_ context.Context,
	req *schedulerpb.CreateFromMigrationStateRequest,
	_ ...grpc.CallOption,
) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	c.calls++
	c.lastRequest = common.CloneProto(req)
	if c.committed != nil {
		return nil, serviceerror.NewAlreadyExists("schedule already registered")
	}
	c.committed = common.CloneProto(req)
	if c.loseReply {
		return nil, serviceerror.NewUnavailable("create committed; response lost")
	}
	return &schedulerpb.CreateFromMigrationStateResponse{}, nil
}

func TestForwardHandoffResponseLoss(t *testing.T) {
	for _, version := range []SchedulerWorkflowVersion{TriggerImmediatelyTimestamp, MigrationHandoffFixes} {
		for _, tc := range []struct {
			name      string
			migrate   bool
			loseReply bool
			rollback  bool
			pause     bool
		}{
			{name: "native_control"},
			{name: "acknowledged_control", migrate: true},
			{name: "lost_reply_retry", migrate: true, loseReply: true},
			{name: "lost_reply_rollback", migrate: true, loseReply: true, rollback: true},
			{name: "lost_reply_pause", migrate: true, loseReply: true, pause: true},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				previous := CurrentTweakablePolicies
				t.Cleanup(func() { CurrentTweakablePolicies = previous })
				CurrentTweakablePolicies.Version = version
				CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 100
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment()
				env.SetStartTime(baseStartTime)
				client := &responseLossSchedulerClient{loseReply: tc.loseReply}
				a := newTestActivities(client, testNamespaceID)
				enabled := tc.migrate
				a.migrationEnabled = func() bool { return enabled }
				var sourceStarts []*schedulespb.StartWorkflowRequest
				env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).
					Maybe().Return(a.MigrateScheduleToChasm)
				env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Maybe().Return(
					func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
						sourceStarts = append(sourceStarts, common.CloneProto(req))
						return &schedulespb.StartWorkflowResponse{
							RunId: "source-run", RealStartTime: timestamppb.New(baseStartTime),
						}, nil
					})
				env.RegisterDelayedCallback(func() {
					if tc.rollback {
						enabled = false
					}
					if tc.pause {
						env.SignalWorkflow(SignalNamePatch, &schedulepb.SchedulePatch{Pause: "pause after response loss"})
					} else {
						env.SignalWorkflow(SignalNameRefresh, nil)
					}
				}, time.Second)
				env.RegisterDelayedCallback(func() {
					env.SignalWorkflow(SignalNameForceCAN, nil)
				}, 2*time.Second)
				env.ExecuteWorkflow(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
					return schedulerWorkflowWithSpecBuilder(ctx, args, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
						enableCHASMMigration: func() bool { return enabled },
					})
				}, &schedulespb.StartScheduleArgs{
					Schedule: &schedulepb.Schedule{
						Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}}},
						Policies: &schedulepb.SchedulePolicies{
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
						},
						Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
							StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
								WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "action"},
								TaskQueue: &taskqueuepb.TaskQueue{Name: "action"},
							},
						}},
					},
					State: &schedulespb.InternalState{
						Namespace: "test-namespace", NamespaceId: testNamespaceID, ScheduleId: "schedule",
						BufferedStarts: []*schedulespb.BufferedStart{{
							NominalTime: timestamppb.New(baseStartTime), ActualTime: timestamppb.New(baseStartTime),
							Manual: true, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
						}},
					},
				})
				if !tc.migrate {
					require.Len(t, sourceStarts, 1)
					require.Nil(t, client.committed)
					require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
					return
				}
				require.NotNil(t, client.committed)
				require.Len(t, client.committed.State.InvokerState.BufferedStarts, 1)
				if tc.rollback {
					var canErr *workflow.ContinueAsNewError
					require.ErrorAs(t, env.GetWorkflowError(), &canErr)
					var args schedulespb.StartScheduleArgs
					require.NoError(t, payloads.Decode(canErr.Input, &args))
					t.Logf("committed target, RPC calls=%d, continued V1 pending=%v", client.calls, args.State.PendingMigration)
				} else {
					require.NoError(t, env.GetWorkflowError())
				}
				if tc.pause {
					require.True(t, client.lastRequest.State.SchedulerState.Schedule.State.Paused)
					require.True(t, client.committed.State.SchedulerState.Schedule.State.Paused,
						"V1 applied the acknowledged pause before its successful migration retry; the target must retain it")
				}
				if len(sourceStarts) > 0 {
					target := client.committed.State.InvokerState.BufferedStarts[0]
					source := sourceStarts[0].Request
					require.Equal(t, target.WorkflowId, source.WorkflowId)
					require.NotEqual(t, target.RequestId, source.RequestId)
					require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, source.WorkflowIdReusePolicy)
					t.Logf("same manual action: workflow=%s target request=%s source request=%s", source.WorkflowId, target.RequestId, source.RequestId)
				}
				require.Empty(t, sourceStarts, "a committed target owns the transferred buffer; V1 must not start its actions")
			})
		}
	}
}
