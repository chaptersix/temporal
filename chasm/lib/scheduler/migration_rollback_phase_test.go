package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRollbackPhaseNativeControl(t *testing.T) {
	for _, phase := range []string{"ready", "retry", "cancel", "terminate", "mixed"} {
		t.Run(phase, func(t *testing.T) {
			s, ctx, _ := setupSchedulerForTest(t)
			i := s.Invoker.Get(ctx)
			i.InvokerState = rollbackPhaseState(phase)
			h := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{Config: defaultConfig()})
			if phase == "retry" {
				i.LastProcessedTime = i.BufferedStarts[0].BackoffTime
			}
			valid, err := h.Validate(ctx, i, chasm.TaskInvocation{}, &schedulerpb.InvokerExecuteTask{})
			require.NoError(t, err)
			require.True(t, valid)
		})
	}
}

func TestRollbackPhaseGate(t *testing.T) {
	testRollbackPhaseGate(t)
}

func testRollbackPhaseGate(t *testing.T) {
	t.Helper()
	for _, phase := range []string{"ready", "retry", "cancel", "terminate", "mixed"} {
		t.Run(phase, func(t *testing.T) {
			s, ctx, _ := setupSchedulerForTest(t)
			i := s.Invoker.Get(ctx)
			i.InvokerState = rollbackPhaseState(phase)
			before := common.CloneProto(i.InvokerState)
			_, err := s.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
			require.ErrorIs(t, err, scheduler.ErrMigrationActionsPending)
			require.Nil(t, s.WorkflowMigration)
			require.False(t, s.Schedule.State.Paused)
			protorequire.ProtoEqual(t, before, i.InvokerState)
		})
	}
}

func rollbackPhaseState(phase string) *schedulerpb.InvokerState {
	now := timestamppb.New(time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC))
	i := &schedulerpb.InvokerState{LastProcessedTime: now}
	if phase == "ready" || phase == "retry" || phase == "mixed" {
		i.BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "pending-request", WorkflowId: "pending-workflow", NominalTime: now, ActualTime: now, DesiredTime: now, Manual: true, Attempt: 1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}
	}
	if phase == "retry" {
		i.BufferedStarts[0].Attempt = 3
		i.BufferedStarts[0].BackoffTime = timestamppb.New(now.AsTime().Add(time.Hour))
	}
	if phase == "cancel" || phase == "mixed" {
		i.CancelWorkflows = []*commonpb.WorkflowExecution{{WorkflowId: "cancel-workflow", RunId: "cancel-run"}}
	}
	if phase == "terminate" || phase == "mixed" {
		i.TerminateWorkflows = []*commonpb.WorkflowExecution{{WorkflowId: "terminate-workflow", RunId: "terminate-run"}}
	}
	return i
}

func TestRollbackPhaseConversionPreservesFieldsButLosesQueues(t *testing.T) {
	s, ctx, _ := setupSchedulerForTest(t)
	i := rollbackPhaseState("mixed")
	args := migration.CHASMToLegacyStartScheduleArgs(s.SchedulerState, s.Generator.Get(ctx).GeneratorState, i, nil, nil, nil, nil, ctx.Now(s))
	protorequire.ProtoEqual(t, i.BufferedStarts[0], args.State.BufferedStarts[0])
	require.Empty(t, args.Info.RunningWorkflows)
	require.Len(t, i.CancelWorkflows, 1)
	require.Len(t, i.TerminateWorkflows, 1)
}

func TestRollbackPhaseTransferable(t *testing.T) {
	for _, phase := range []string{"unprocessed", "deferred", "running", "completed", "drained"} {
		t.Run(phase, func(t *testing.T) {
			s, ctx, _ := setupSchedulerForTest(t)
			i := s.Invoker.Get(ctx)
			i.InvokerState = rollbackPhaseState("ready")
			start := i.BufferedStarts[0]
			switch phase {
			case "unprocessed":
				start.Attempt = 0
			case "deferred":
				start.Attempt = -1
			case "running":
				start.RunId = "run"
			case "completed":
				start.RunId = "run"
				start.Completed = &schedulespb.CompletedResult{}
			case "drained":
				i.BufferedStarts = nil
			default:
				t.Fatalf("unknown phase %q", phase)
			}
			_, err := s.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
			require.NoError(t, err)
			require.NotNil(t, s.WorkflowMigration)
		})
	}
}

func TestRollbackPhaseDrainThenRetry(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	ctx := env.MutableContext()
	i := env.Scheduler.Invoker.Get(ctx)
	i.InvokerState = rollbackPhaseState("ready")
	i.LastProcessedTime = timestamppb.New(env.TimeSource.Now())
	_, err := env.Scheduler.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
	require.ErrorIs(t, err, scheduler.ErrMigrationActionsPending)
	env.mockFrontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "phase-run"}, nil).Times(1)
	executeTaskOnce(t, env, ctx, i)
	_, err = env.Scheduler.MigrateToWorkflow(env.MutableContext(), &schedulerpb.MigrateToWorkflowRequest{})
	require.NoError(t, err)
}

func TestRollbackPhaseAlreadyPendingFailsClosed(t *testing.T) {
	e := newSchedulerTestEngine(t, defaultSchedule())
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).InvokerState = rollbackPhaseState("mixed")
		s.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}
		s.Schedule.State.Paused = true
		return nil
	}))
	h := scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: e.logger})
	err := h.Execute(e.engineCtx, e.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{})
	require.ErrorIs(t, err, scheduler.ErrMigrationActionsPending)
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed)
		require.NotNil(t, s.WorkflowMigration)
		return nil
	}))
}
