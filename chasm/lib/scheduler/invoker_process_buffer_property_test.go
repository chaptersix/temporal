//go:build property_test

package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

func TestPropertyLegacyPlannerComparison(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	}
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(1, 4).Draw(rt, "count")
		starts := make([]*schedulespb.BufferedStart, count)
		for index := range starts {
			start := newBufferProcessingComparisonStart(now, fmt.Sprintf("request-%d", index), rapid.SampledFrom(policies).Draw(rt, fmt.Sprintf("policy-%d", index)))
			start.WorkflowId = fmt.Sprintf("workflow-%d", index)
			starts[index] = start
		}
		input := bufferProcessingComparisonInput{schedule: defaultSchedule(), bufferedStarts: starts, lastProcessedTime: now, initialConflictToken: 17}
		legacy := runBufferProcessing(t, input, false)
		planned := runBufferProcessing(t, input, true)
		require.True(rt, proto.Equal(legacy.schedulerState, planned.schedulerState))
		require.True(rt, proto.Equal(legacy.invokerState, planned.invokerState))
		require.Equal(rt, legacy.tasks, planned.tasks)
		require.Equal(rt, legacy.actionRequests, planned.actionRequests)
		require.Equal(rt, legacy.metrics, planned.metrics)
		require.Equal(rt, legacy.outcomes, planned.outcomes)
		require.Equal(rt, legacy.remainingDelta, planned.remainingDelta)
		require.Equal(rt, legacy.conflictTokenDelta, planned.conflictTokenDelta)
	})
}

func TestPropertyCompletedHistoryDoesNotConsumeCapacity(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		completed := rapid.IntRange(0, 10).Draw(rt, "completed")
		env := newTestEnv(t)
		ctx := env.MutableContext()
		env.Scheduler.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{StartTime: timestamppb.New(env.TimeSource.Now()), EndTime: timestamppb.New(env.TimeSource.Now().Add(time.Hour))})
		invoker := env.Scheduler.Invoker.Get(ctx)
		for index := range completed {
			invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{RequestId: fmt.Sprintf("completed-%d", index), Completed: &schedulespb.CompletedResult{CloseTime: timestamppb.New(env.TimeSource.Now())}})
		}
		tweakables := scheduler.DefaultTweakables
		tweakables.MaxBufferSize = 20
		tweakables.GeneratorBufferReserveSize = 0
		handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: env.Logger, SpecProcessor: env.SpecProcessor})
		limit, err := handler.AllowedBufferedStarts(ctx, env.Scheduler, invoker, tweakables)
		require.NoError(rt, err)
		require.Positive(rt, limit)
	})
}
