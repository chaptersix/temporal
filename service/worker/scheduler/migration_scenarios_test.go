package scheduler

import (
	"context"
	"fmt"
	"os"
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
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newMigrationScenario(t *testing.T) (*testsuite.TestWorkflowEnvironment, *schedulespb.StartScheduleArgs) {
	t.Helper()
	previous := CurrentTweakablePolicies
	t.Cleanup(func() { CurrentTweakablePolicies = previous })
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 3
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(baseStartTime)
	t.Cleanup(func() { env.AssertExpectations(t) })
	return env, &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "action"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "queue"},
				},
			}},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
			State:    &schedulepb.ScheduleState{},
		},
		Info:  &schedulepb.ScheduleInfo{},
		State: &schedulespb.InternalState{Namespace: "ns", NamespaceId: testNamespaceID, ScheduleId: "schedule", ConflictToken: InitialConflictToken},
	}
}

func requireMigrationCounterexamples(t *testing.T) {
	t.Helper()
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") == "" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1 to run known failing migration repros")
	}
}

func TestMigrationScenario_TriggerBeforeSnapshot(t *testing.T) {
	checkMigrationSignalAccounting(t, false)
}

func TestMigrationCounterexample_TriggerDuringSnapshot(t *testing.T) {
	requireMigrationCounterexamples(t)
	checkMigrationSignalAccounting(t, true)
}

func checkMigrationSignalAccounting(t *testing.T, duringMigration bool) {
	t.Helper()
	env, args := newMigrationScenario(t)
	enabled := duringMigration
	var destination *schedulerpb.SchedulerMigrationState
	starts := 0
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			starts++
			return &schedulespb.StartWorkflowResponse{RunId: "run", RealStartTime: timestamppb.New(env.Now())}, nil
		})
	env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Once().After(2 * time.Second).Return(
		func(_ context.Context, req *schedulerpb.CreateFromMigrationStateRequest) error {
			destination = common.CloneProto(req.State)
			return nil
		})
	delivered := false
	env.RegisterDelayedCallback(func() {
		require.False(t, env.IsWorkflowCompleted())
		env.SignalWorkflow(SignalNamePatch, &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
		})
		delivered = true
	}, time.Second)
	if !duringMigration {
		env.RegisterDelayedCallback(func() {
			enabled = true
			env.SignalWorkflow(SignalNameMigrateToChasm, nil)
		}, 3*time.Second)
	}
	env.ExecuteWorkflow(func(ctx workflow.Context, input *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, input, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
			enableCHASMMigration: func() bool { return enabled }, migrateWithRunningWorkflows: func() bool { return true },
			versionOverride: func() int { return int(MigrationHandoffFixes) },
		})
	}, args)
	require.NoError(t, env.GetWorkflowError())
	require.True(t, delivered)
	require.NotNil(t, destination)
	pending := 0
	for _, start := range destination.InvokerState.BufferedStarts {
		if start.Manual && start.RunId == "" && start.Completed == nil {
			pending++
		}
	}
	for _, backfill := range destination.Backfillers {
		if backfill.GetTriggerRequest() != nil {
			pending++
		}
	}
	require.Equal(t, 1, starts+pending, "a delivered trigger must execute or remain actionable at the destination")
}

type migrationCommitClient struct {
	schedulerpb.SchedulerServiceClient
	destination *schedulerpb.SchedulerMigrationState
	commit      bool
	calls       int
}

func (c *migrationCommitClient) CreateFromMigrationState(
	_ context.Context, req *schedulerpb.CreateFromMigrationStateRequest, _ ...grpc.CallOption,
) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	c.calls++
	if c.calls == 1 {
		if c.commit {
			c.destination = common.CloneProto(req.State)
		}
		return nil, serviceerror.NewUnavailable("injected response loss")
	}
	if c.destination != nil {
		return nil, serviceerror.NewAlreadyExists("destination committed on first attempt")
	}
	c.destination = common.CloneProto(req.State)
	return &schedulerpb.CreateFromMigrationStateResponse{}, nil
}

func TestMigrationScenario_FailureBeforeCommit(t *testing.T) {
	checkMigrationCommitAccounting(t, false, false, MigrationHandoffFixes)
}

func TestMigrationScenario_NativeBackfillIncludesStart(t *testing.T) {
	checkRollbackBackfillStart(t, false)
}

func TestMigrationCounterexample_RollbackFreshBackfill(t *testing.T) {
	requireMigrationCounterexamples(t)
	checkRollbackBackfillStart(t, true)
}

func TestMigrationCounterexample_ForwardDestinationCollision(t *testing.T) {
	requireMigrationCounterexamples(t)
	env, args := newMigrationScenario(t)
	when := timestamppb.New(baseStartTime)
	args.State.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime: when, ActualTime: when, Manual: true,
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}}
	client := &mockSchedulerClient{migrateErr: serviceerror.NewAlreadyExists("unrelated CHASM schedule owns this ID")}
	a := newTestActivities(client, testNamespaceID)
	env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *schedulerpb.CreateFromMigrationStateRequest) error {
			return a.MigrateScheduleToChasm(ctx, req)
		})
	env.ExecuteWorkflow(func(ctx workflow.Context, input *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, input, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
			enableCHASMMigration: func() bool { return true }, migrateWithRunningWorkflows: func() bool { return true },
		})
	}, args)
	require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()),
		"the source must remain active when the destination ID belongs to an unrelated schedule")
}

func checkRollbackBackfillStart(t *testing.T, rollback bool) {
	t.Helper()
	env, args := newMigrationScenario(t)
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 1
	when := timestamppb.New(baseStartTime.Add(-time.Hour))
	backfill := &schedulepb.BackfillRequest{
		StartTime: when, EndTime: when, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	if rollback {
		args = migration.CHASMToLegacyStartScheduleArgs(&schedulerpb.SchedulerState{
			Schedule: args.Schedule, Info: args.Info, Namespace: args.State.Namespace,
			NamespaceId: args.State.NamespaceId, ScheduleId: args.State.ScheduleId,
		}, &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(baseStartTime)}, &schedulerpb.InvokerState{},
			map[string]*schedulerpb.BackfillerState{"fresh": {
				Request: &schedulerpb.BackfillerState_BackfillRequest{BackfillRequest: backfill},
			}}, nil, nil, nil, baseStartTime)
	} else {
		args.InitialPatch = &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{backfill}}
	}
	starts := 0
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			starts++
			return &schedulespb.StartWorkflowResponse{RunId: "backfilled-run", RealStartTime: timestamppb.New(env.Now())}, nil
		})
	env.ExecuteWorkflow(SchedulerWorkflow, args)
	require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
	require.Equal(t, 1, starts, "the inclusive start of a fresh CHASM backfill must remain actionable after rollback")
}

func TestMigrationCounterexample_LostCreateResponse(t *testing.T) {
	requireMigrationCounterexamples(t)
	for _, version := range []SchedulerWorkflowVersion{TriggerImmediatelyTimestamp, MigrationHandoffFixes} {
		for _, disable := range []bool{false, true} {
			t.Run(fmt.Sprintf("version_%d_disable_%t", version, disable), func(t *testing.T) {
				checkMigrationCommitAccounting(t, true, disable, version)
			})
		}
	}
}

func checkMigrationCommitAccounting(t *testing.T, commit, disable bool, version SchedulerWorkflowVersion) {
	t.Helper()
	env, args := newMigrationScenario(t)
	when := timestamppb.New(baseStartTime)
	args.State.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime: when, ActualTime: when, Manual: true, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}}
	client := &migrationCommitClient{commit: commit}
	a := newTestActivities(client, testNamespaceID)
	enabled := true
	a.migrationEnabled = func() bool { return enabled }
	env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, req *schedulerpb.CreateFromMigrationStateRequest) error {
			err := a.MigrateScheduleToChasm(ctx, req)
			if disable {
				enabled = false
			}
			return err
		})
	var sourceRequests []*schedulespb.StartWorkflowRequest
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			sourceRequests = append(sourceRequests, common.CloneProto(req))
			return &schedulespb.StartWorkflowResponse{RunId: fmt.Sprintf("run-%d", len(sourceRequests)), RealStartTime: timestamppb.New(env.Now())}, nil
		})
	env.ExecuteWorkflow(func(ctx workflow.Context, input *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, input, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
			enableCHASMMigration: func() bool { return enabled }, migrateWithRunningWorkflows: func() bool { return true },
			versionOverride: func() int { return int(version) }, versionCeiling: func() int { return int(version) },
		})
	}, args)
	if disable {
		require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
	} else {
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, 2, client.calls)
	}
	require.NotEmpty(t, sourceRequests)
	require.NotNil(t, client.destination)
	manualAtDestination := 0
	for _, start := range client.destination.InvokerState.BufferedStarts {
		if start.Manual && start.RunId == "" && start.Completed == nil {
			manualAtDestination++
			t.Logf("destination workflow/request=%s/%s; source workflow/request=%s/%s", start.WorkflowId, start.RequestId,
				sourceRequests[0].Request.WorkflowId, sourceRequests[0].Request.RequestId)
		}
	}
	require.Zero(t, manualAtDestination, "source executed the manual action after migration returned an error; the committed destination must not still own it")
}
