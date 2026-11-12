package scheduler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestProcessTimeRange_ManualPaginationPreservesOccurrences(t *testing.T) {
	for _, jitter := range []time.Duration{0, 15 * time.Second, 59 * time.Second, 2 * time.Minute} {
		for _, pageSize := range []int{1, 2, 5, 20} {
			t.Run(fmt.Sprintf("jitter=%s/page=%d", jitter, pageSize), func(t *testing.T) {
				env := newTestEnv(t)
				ctx := chasm.NewMutableContext(context.Background(), env.Node)
				schedule := defaultSchedule()
				schedule.Spec.Jitter = durationpb.New(jitter)
				sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)
				require.NoError(t, err)
				processor := newTestSpecProcessor(env.Ctrl)
				start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
				end := start.Add(17*time.Minute + 59*time.Second)
				whole, err := processor.ProcessTimeRange(sched, start, end,
					enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "backfill", true, nil)
				require.NoError(t, err)
				require.Len(t, whole.BufferedStarts, 17)
				for i, action := range whole.BufferedStarts {
					nominal := start.Add(time.Duration(i+1) * time.Minute)
					require.Equal(t, nominal, action.NominalTime.AsTime())
					require.False(t, action.ActualTime.AsTime().Before(nominal))
					require.False(t, action.ActualTime.AsTime().After(nominal.Add(jitter)))
					require.True(t, action.ActualTime.AsTime().Before(nominal.Add(time.Minute)), "jitter must leave the successor enumerable")
				}

				var paged []*schedulespb.BufferedStart
				cursor := start
				for page := 0; ; page++ {
					require.LessOrEqual(t, page, len(whole.BufferedStarts), "pagination failed to terminate")
					limit := pageSize
					part, err := processor.ProcessTimeRange(sched, cursor, end,
						enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "backfill", true, &limit)
					require.NoError(t, err)
					require.Zero(t, part.DroppedCount)
					require.Equal(t, pageSize-len(part.BufferedStarts), limit)
					paged = append(paged, part.BufferedStarts...)
					if len(part.BufferedStarts) == 0 {
						break
					}
					require.True(t, part.LastActionTime.After(cursor), "cursor must make progress")
					require.Equal(t, part.BufferedStarts[len(part.BufferedStarts)-1].ActualTime.AsTime(), part.LastActionTime)
					cursor = part.LastActionTime
				}

				require.Len(t, paged, len(whole.BufferedStarts))
				for i, action := range whole.BufferedStarts {
					require.True(t, proto.Equal(action, paged[i]), "occurrence %d changed during pagination", i)
				}
			})
		}
	}
}

func TestProcessTimeRange_AutomatedCapacityAccounting(t *testing.T) {
	for _, occurrences := range []int{1, 2, 7, 17} {
		for _, capacity := range []int{-1, 0, 1, 3, 20} {
			t.Run(fmt.Sprintf("occurrences=%d/capacity=%d", occurrences, capacity), func(t *testing.T) {
				env := newTestEnv(t)
				ctx := chasm.NewMutableContext(context.Background(), env.Node)
				schedule := defaultSchedule()
				schedule.Policies.CatchupWindow = durationpb.New(time.Hour)
				sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)
				require.NoError(t, err)
				processor := newTestSpecProcessor(env.Ctrl)
				start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
				end := start.Add(time.Duration(occurrences) * time.Minute)
				limit := capacity
				result, err := processor.ProcessTimeRange(sched, start, end,
					enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, &limit)
				require.NoError(t, err)
				buffered := min(occurrences, max(capacity, 0))
				require.Len(t, result.BufferedStarts, buffered)
				require.Equal(t, int64(occurrences-buffered), result.DroppedCount)
				require.Equal(t, capacity-buffered, limit)
				require.Equal(t, end, result.LastActionTime)
				require.Equal(t, end.Add(time.Minute), result.NextWakeupTime)
				for i, action := range result.BufferedStarts {
					want := start.Add(time.Duration(i+1) * time.Minute)
					require.Equal(t, want, action.NominalTime.AsTime())
					require.Equal(t, want, action.ActualTime.AsTime())
				}

				resumed, err := processor.ProcessTimeRange(sched, result.LastActionTime, end.Add(time.Minute),
					enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "", false, nil)
				require.NoError(t, err)
				require.Len(t, resumed.BufferedStarts, 1)
				require.Equal(t, end.Add(time.Minute), resumed.BufferedStarts[0].ActualTime.AsTime())
			})
		}
	}
}

func TestProcessTimeRange_JitterCursorBoundaries(t *testing.T) {
	env := newTestEnv(t)
	ctx := chasm.NewMutableContext(context.Background(), env.Node)
	schedule := defaultSchedule()
	schedule.Spec.Jitter = durationpb.New(59 * time.Second)
	sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)
	require.NoError(t, err)
	processor := newTestSpecProcessor(env.Ctrl)
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	first, err := processor.NextTime(sched, start)
	require.NoError(t, err)
	require.Equal(t, start.Add(time.Minute), first.Nominal)
	require.True(t, first.Next.After(first.Nominal), "fixture must exercise positive jitter")
	require.True(t, first.Next.Before(start.Add(2*time.Minute)))

	t.Run("actual end is inclusive", func(t *testing.T) {
		result, err := processor.ProcessTimeRange(sched, start, first.Next,
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "backfill", true, nil)
		require.NoError(t, err)
		require.Len(t, result.BufferedStarts, 1)
		require.Equal(t, first.Nominal, result.BufferedStarts[0].NominalTime.AsTime())
		require.Equal(t, first.Next, result.BufferedStarts[0].ActualTime.AsTime())
		require.Equal(t, first.Next, result.LastActionTime)

		resumed, err := processor.ProcessTimeRange(sched, result.LastActionTime, start.Add(3*time.Minute),
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "backfill", true, nil)
		require.NoError(t, err)
		require.Len(t, resumed.BufferedStarts, 1)
		require.Equal(t, start.Add(2*time.Minute), resumed.BufferedStarts[0].NominalTime.AsTime())
	})

	t.Run("unvisited jittered occurrence remains enumerable", func(t *testing.T) {
		boundary := first.Next.Add(-time.Nanosecond)
		before, err := processor.ProcessTimeRange(sched, start, boundary,
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "backfill", true, nil)
		require.NoError(t, err)
		require.Empty(t, before.BufferedStarts)
		require.Equal(t, start, before.LastActionTime)
		require.Equal(t, first.Next, before.NextWakeupTime)

		resumed, err := processor.ProcessTimeRange(sched, before.LastActionTime, first.Next,
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, sched.WorkflowID(), "backfill", true, nil)
		require.NoError(t, err)
		require.Len(t, resumed.BufferedStarts, 1)
		require.Equal(t, first.Nominal, resumed.BufferedStarts[0].NominalTime.AsTime())
		require.Equal(t, first.Next, resumed.BufferedStarts[0].ActualTime.AsTime())
	})
}

func TestGeneratorTask_JitterPendingOccurrenceSurvivesImmediateGeneration(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts := clock.NewEventTimeSource()
	ts.Update(base)
	schedule := defaultSchedule()
	schedule.Spec.Jitter = durationpb.New(59 * time.Second)
	e := newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts))
	_, err := e.engine.FirePureTasks(e.rootRef, base)
	require.NoError(t, err)
	processor := newRealSpecProcessor(gomock.NewController(t), e.logger)
	var actual time.Time
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		next, err := processor.NextTime(s, base)
		if err != nil {
			return err
		}
		require.Equal(t, base.Add(time.Minute), next.Nominal)
		require.True(t, next.Next.After(next.Nominal))
		actual = next.Next
		return nil
	}))
	ts.Update(actual.Add(-time.Nanosecond))
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Generator.Get(ctx).Generate(ctx)
		return nil
	}))
	_, err = e.engine.FirePureTasks(e.rootRef, ts.Now())
	require.NoError(t, err)
	ts.Update(actual)
	_, err = e.engine.FirePureTasks(e.rootRef, actual)
	require.NoError(t, err)
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		starts := s.Invoker.Get(ctx).BufferedStarts
		require.Len(t, starts, 1, "immediate generation must retain the pending jittered occurrence")
		require.Equal(t, base.Add(time.Minute), starts[0].NominalTime.AsTime())
		require.Equal(t, actual, starts[0].ActualTime.AsTime())
		return nil
	}))
}
