package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestResearchMigrationSignalWindow(t *testing.T) {
	original := CurrentTweakablePolicies
	t.Cleanup(func() { CurrentTweakablePolicies = original })
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 100

	for _, operation := range []string{"pause", "update", "trigger", "backfill"} {
		for _, boundary := range []string{"native", "before_snapshot", "after_snapshot", "failed_migration", "committed_response_lost"} {
			t.Run(operation+"/"+boundary, func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment()
				env.SetStartTime(baseStartTime)
				schedule := &schedulepb.Schedule{
					Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}},
					Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
						StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
							WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "action"},
							TaskQueue: &taskqueuepb.TaskQueue{Name: "actions"},
						},
					}},
					Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
				}
				signalName := SignalNamePatch
				var signalValue any
				switch operation {
				case "pause":
					signalValue = &schedulepb.SchedulePatch{Pause: "accepted mutation"}
				case "update":
					updated := common.CloneProto(schedule)
					updated.State = &schedulepb.ScheduleState{Paused: true, Notes: "accepted mutation"}
					signalName = SignalNameUpdate
					signalValue = &schedulespb.FullUpdateRequest{Schedule: updated}
				case "trigger":
					signalValue = &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}}
				case "backfill":
					signalValue = &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						StartTime: timestamppb.New(baseStartTime.Add(-time.Minute)), EndTime: timestamppb.New(baseStartTime.Add(time.Minute)),
					}}}
				default:
					require.FailNow(t, "unknown operation", operation)
				}
				var snapshot *schedulerpb.CreateFromMigrationStateRequest
				var retrySnapshot *schedulerpb.CreateFromMigrationStateRequest
				var finalState *schedulespb.StartScheduleArgs
				var unhandled []string
				migrationEnabled := false
				startCount := 0
				migrationCalls := 0
				env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Maybe().Return(
					func(context.Context, *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
						startCount++
						return &schedulespb.StartWorkflowResponse{RunId: "action-run", RealStartTime: timestamppb.New(env.Now())}, nil
					})
				if boundary != "native" {
					expectedCalls := 1
					if boundary == "committed_response_lost" {
						expectedCalls = 2
					}
					env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Times(expectedCalls).Return(
						func(_ context.Context, req *schedulerpb.CreateFromMigrationStateRequest) error {
							migrationCalls++
							if migrationCalls == 1 {
								snapshot = common.CloneProto(req)
							} else {
								retrySnapshot = common.CloneProto(req)
							}
							if boundary == "committed_response_lost" && migrationCalls == 1 {
								return errors.New("create committed but response was lost")
							}
							if boundary == "failed_migration" {
								return errors.New("migration did not commit")
							}
							return nil
						})
				}
				env.SetOnLocalActivityStartedListener(func(info *activity.Info, _ context.Context, _ []any) {
					if info.ActivityType.Name != "MigrateScheduleToChasm" || boundary == "before_snapshot" || migrationCalls > 0 {
						return
					}
					env.SignalWorkflow(signalName, signalValue)
					if boundary == "failed_migration" {
						migrationEnabled = false
					}
				})
				env.RegisterDelayedCallback(func() {
					migrationEnabled = boundary != "native"
					if boundary == "native" || boundary == "before_snapshot" {
						env.SignalWorkflow(signalName, signalValue)
					} else {
						env.SignalWorkflow(SignalNameMigrateToChasm, nil)
					}
				}, time.Second)
				env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalNameForceCAN, nil) }, 3*time.Second)
				env.ExecuteWorkflow(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
					err := schedulerWorkflowWithSpecBuilder(ctx, args, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
						enableCHASMMigration:        func() bool { return migrationEnabled },
						migrateWithRunningWorkflows: func() bool { return true },
						versionOverride:             func() int { return int(LatestSchedulerWorkflowVersion) },
					})
					finalState = args
					unhandled = workflow.GetUnhandledSignalNames(ctx)
					return err
				}, &schedulespb.StartScheduleArgs{
					Schedule: schedule,
					State:    &schedulespb.InternalState{Namespace: "research", NamespaceId: "research-id", ScheduleId: "signal-window"},
				})
				require.True(t, env.IsWorkflowCompleted())
				env.AssertExpectations(t)
				if boundary == "native" || boundary == "failed_migration" {
					require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
					require.Empty(t, unhandled)
					if operation == "pause" || operation == "update" {
						require.Equal(t, "accepted mutation", finalState.Schedule.State.Notes)
						require.True(t, finalState.Schedule.State.Paused)
					} else {
						require.Equal(t, 1, startCount)
					}
					return
				}
				require.NoError(t, env.GetWorkflowError())
				require.NotNil(t, snapshot)
				if boundary == "after_snapshot" || boundary == "committed_response_lost" {
					if boundary == "after_snapshot" {
						require.Equal(t, []string{signalName}, unhandled)
					} else {
						require.Empty(t, unhandled)
						require.NotNil(t, retrySnapshot)
						switch operation {
						case "pause", "update":
							require.Equal(t, "accepted mutation", retrySnapshot.State.SchedulerState.Schedule.State.Notes)
						case "trigger":
							require.Len(t, retrySnapshot.State.InvokerState.BufferedStarts, 1)
						case "backfill":
							require.Len(t, retrySnapshot.State.Backfillers, 1)
						default:
							require.FailNow(t, "unknown operation", operation)
						}
					}
					require.Empty(t, snapshot.State.SchedulerState.Schedule.State.Notes)
					require.Empty(t, snapshot.State.InvokerState.BufferedStarts)
					require.Empty(t, snapshot.State.Backfillers)
					require.Zero(t, startCount)
					return
				}
				require.Empty(t, unhandled)
				switch operation {
				case "pause", "update":
					require.Equal(t, "accepted mutation", snapshot.State.SchedulerState.Schedule.State.Notes)
				case "trigger":
					require.Len(t, snapshot.State.InvokerState.BufferedStarts, 1)
				case "backfill":
					require.Len(t, snapshot.State.Backfillers, 1)
				default:
					require.FailNow(t, "unknown operation", operation)
				}
			})
		}
	}
}
