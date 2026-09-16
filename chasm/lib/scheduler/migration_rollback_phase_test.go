package scheduler_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
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

func TestRollbackPhaseCounterexample(t *testing.T) {
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") != "1" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1")
	}
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
			require.Error(t, err, "seed=rollback-%s: V1 cannot execute persisted invoker phase; retain native task ownership", phase)
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
