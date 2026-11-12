package tests

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This uses the explicit scheduler2 service binding and real History execution
// clocks. A completed one-day child timer proves configuration propagation;
// persisted initial skipped duration proves scheduler-to-child clock propagation.
func TestSchedule2TimeSkippingPropagation(t *testing.T) {
	for _, initial := range []bool{true, false} {
		name := "patch"
		if initial {
			name = "initial-patch"
		}
		t.Run(name, func(t *testing.T) {
			env := newScheduleEnv(t, []scheduleBackend{schedule2Backend},
				testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true),
				testcore.WithDynamicConfig(dynamicconfig.CHASMSchedulerTimeSkippingEnabled, true))
			ctx := t.Context()
			sid := "scheduler2-time-" + uuid.NewString()
			wt := "timer-" + sid
			var started, completed atomic.Int32
			registerOneDayTimerWorkflow(env, wt, &started, &completed)
			now := time.Now().UTC()
			first := now.Add(12 * time.Hour).Truncate(time.Second)
			week := 7 * 24 * time.Hour
			schedule := &schedulepb.Schedule{
				Spec:     &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(week), Phase: durationpb.New(time.Duration(first.UnixNano() % week.Nanoseconds()))}}},
				Action:   startWorkflowAction(env, "workflow-"+sid, wt),
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
				State:    &schedulepb.ScheduleState{Paused: !initial},
			}
			ffID := uuid.NewString()
			patch := &schedulepb.SchedulePatch{TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Id: ffID, Duration: durationpb.New(2 * 24 * time.Hour)}}}
			req := &workflowservice.CreateScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid, Schedule: schedule, RequestId: uuid.NewString()}
			if initial {
				req.InitialPatch = patch
			}
			_, err := env.FrontendClient().CreateSchedule(ctx, req)
			require.NoError(t, err)
			if !initial {
				patch.Unpause = "start virtual clock"
				patchSchedule(ctx, t, env, sid, patch)
			}
			var execution *commonpb.WorkflowExecution
			await.Require(ctx, t, func(at *await.T) {
				desc, err := env.FrontendClient().DescribeSchedule(at.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
				require.NoError(at, err)
				info := desc.GetInfo()
				require.Equal(at, int64(1), info.GetActionCount())
				require.Len(at, info.GetRecentActions(), 1)
				require.Equal(at, ffID, info.GetTimeSkippingInfo().GetFastForwardInfo().GetFastForwardId())
				require.True(at, info.GetTimeSkippingInfo().GetFastForwardInfo().GetHasCompleted())
				require.False(at, info.GetTimeSkippingInfo().GetEffectiveConfig().GetEnabled(), "fast-forward disables the parent at its target")
				action := info.GetRecentActions()[0]
				require.Equal(at, first, action.GetScheduleTime().AsTime())
				execution = action.GetStartWorkflowResult()
				require.NotEmpty(at, execution.GetRunId())
				require.Equal(at, int32(1), completed.Load())
				child, err := env.FrontendClient().DescribeWorkflowExecution(at.Context(), &workflowservice.DescribeWorkflowExecutionRequest{Namespace: env.Namespace().String(), Execution: execution})
				require.NoError(at, err)
				require.Equal(at, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, child.GetWorkflowExecutionInfo().GetStatus())
			}, 15*time.Second, pollInterval)
			require.Equal(t, int32(1), started.Load())
			ms, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{Namespace: env.Namespace().String(), Execution: execution, Archetype: chasm.WorkflowArchetype})
			require.NoError(t, err)
			tsi := ms.GetDatabaseMutableState().GetExecutionInfo().GetTimeSkippingInfo()
			require.True(t, tsi.GetConfig().GetEnabled())
			require.Nil(t, tsi.GetConfig().GetFastForwardConfig(), "parent fast-forward policy must not propagate to child")
			require.Greater(t, tsi.GetInitialSkippedDuration().AsDuration(), 11*time.Hour)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, ms.GetDatabaseMutableState().GetExecutionState().GetStatus())
		})
	}
}
