package tests

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
)

// This exercises actual History mutable-state accounting with near-limit input,
// successful result and subsequent failure retained together. It does not raise
// blob or mutable-state limits or infer arbitrary concurrent-payload capacity.
func TestSchedule2LargePayloadCarryoverCapacity(t *testing.T) {
	for _, tc := range []struct {
		name            string
		size            int
		transportReject bool
	}{
		{"within_transport_limit", 1280 * 1024, false},
		{"near_blob_limit_exceeds_aggregate_transport", 2*1024*1024 - 16*1024, true},
	} {
		t.Run(tc.name, func(t *testing.T) { runLargePayloadCarryover(t, tc.size, tc.transportReject) })
	}
}

func runLargePayloadCarryover(t *testing.T, size int, transportReject bool) {
	t.Helper()
	outcomes := make(chan workflowStartOutcome, 64)
	backend := schedule2Backend
	backend.options = []testcore.TestOption{testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
		runtime.FrontendClient = &observedWorkflowStarter{WorkflowServiceClient: runtime.FrontendClient, outcomes: outcomes}
		return scheduler2.NewPublicExecutionTestFrontend(runtime)
	}, scheduler2.NewNilLibrary())}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	var lastStartError error
	large := strings.Repeat("p", size)
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context, input string) (string, error) {
		if len(input) != len(large) {
			return "", fmt.Errorf("input length changed: %d", len(input))
		}
		if !workflow.HasLastCompletionResult(ctx) {
			return input, nil
		}
		var previous string
		if err := workflow.GetLastCompletionResult(ctx, &previous); err != nil {
			return "", err
		}
		if previous != input {
			return "", errors.New("successful result changed")
		}
		if prior := workflow.GetLastError(ctx); prior != nil {
			application, ok := errors.AsType[*temporal.ApplicationError](prior)
			if !ok {
				return "", errors.New("previous failure type changed")
			}
			var detail string
			if err := application.Details(&detail); err != nil {
				return "", err
			}
			if detail != input {
				return "", errors.New("previous failure detail changed")
			}
			return "verified", nil
		}
		return "", temporal.NewNonRetryableApplicationError("expected large failure", "carryover", nil, input)
	}, workflow.RegisterOptions{Name: "large-carryover"})
	sid := "large-" + uuid.NewString()
	action := startWorkflowAction(env, "workflow-"+sid, "large-carryover")
	input, err := payloads.Encode(large)
	require.NoError(t, err)
	action.GetStartWorkflow().Input = input
	createSchedule(t.Context(), t, env, sid, &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, Action: action, State: &schedulepb.ScheduleState{Paused: true}})
	ref := chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: sid})
	for stage := int64(1); stage <= 3; stage++ {
		patchSchedule(t.Context(), t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
		await.Require(t.Context(), t, func(t *await.T) {
		drainOutcomes:
			for {
				select {
				case outcome := <-outcomes:
					lastStartError = outcome.err
				default:
					break drainOutcomes
				}
			}
			engineCtx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
			require.NoError(t, err)
			counters, err := chasm.ReadComponent(engineCtx, ref, func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) ([2]int64, error) {
				return [2]int64{s.Started, s.Unfinished}, nil
			}, struct{}{})
			require.NoError(t, err)
			wantCounters := [2]int64{stage, 0}
			historyStage := stage
			if transportReject && stage == 3 {
				require.ErrorContains(t, lastStartError, "larger than max")
				wantCounters, historyStage = [2]int64{2, 1}, 2
			}
			if counters != wantCounters {
				summary, readErr := chasm.ReadComponent(engineCtx, ref, func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (string, error) {
					return s.String(), nil
				}, struct{}{})
				require.NoError(t, readErr)
				require.Equal(t, wantCounters, counters, "last start error: %v; durable control state: %s", lastStartError, summary)
			}
			description, err := env.FrontendClient().DescribeSchedule(t.Context(), &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid})
			require.NoError(t, err)
			require.Positive(t, description.Info.StateSizeBytes)
			require.Less(t, description.Info.StateSizeBytes, int64(8*1024*1024))
			require.Len(t, description.Info.RecentActions, int(historyStage))
			want := enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			if historyStage == 2 {
				want = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
			}
			require.Equal(t, want, description.Info.RecentActions[historyStage-1].StartWorkflowStatus)
			var describedInput string
			require.NoError(t, payloads.Decode(description.Schedule.Action.GetStartWorkflow().Input, &describedInput))
			require.Equal(t, large, describedInput)
		}, 20*time.Second, 100*time.Millisecond)
	}
}
