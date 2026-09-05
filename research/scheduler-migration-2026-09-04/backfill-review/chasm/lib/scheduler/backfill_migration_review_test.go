package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReviewBackfillMigrationCursor(t *testing.T) {
	boundary := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		cursor    time.Time
		clone     bool
		wantExtra int
	}{
		{"continued baseline", boundary, false, 1},
		{"continued cloned cursor", boundary, true, 0},
		{"initial adjusted baseline", boundary.Add(-time.Millisecond), false, 0},
		{"initial adjusted cloned cursor", boundary.Add(-time.Millisecond), true, 0},
		{"epoch baseline", time.Unix(0, 0).UTC(), false, 1},
		{"epoch cloned cursor", time.Unix(0, 0).UTC(), true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := defaultSchedule()
			spec.Spec.Interval = nil
			spec.Spec.CronString = []string{"* * * * *"}
			env := newTestEnv(t, withSchedule(spec))
			request := migration.LegacyToCreateFromMigrationStateRequest(
				env.Scheduler.Schedule, &schedulepb.ScheduleInfo{},
				&schedulespb.InternalState{
					Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
					OngoingBackfills: []*schedulepb.BackfillRequest{{
						StartTime: timestamppb.New(tc.cursor), EndTime: timestamppb.New(tc.cursor.Add(time.Minute)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					}},
				}, nil, nil, boundary.Add(time.Hour))
			var state *schedulerpb.BackfillerState
			for _, state = range request.State.Backfillers {
				if tc.clone {
					state.LastProcessedTime = common.CloneProto(state.GetBackfillRequest().StartTime)
				}
			}
			handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
				Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler,
				BaseLogger: env.Logger, SpecProcessor: env.SpecProcessor,
			})
			native, err := env.SpecProcessor.ProcessTimeRange(env.Scheduler, tc.cursor, tc.cursor.Add(time.Minute),
				enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, env.Scheduler.WorkflowID(), "native", true, nil)
			require.NoError(t, err)
			got, err := handler.ProcessBackfill(env.Scheduler, &scheduler.Backfiller{BackfillerState: state}, 100)
			require.NoError(t, err)
			require.Len(t, got.BufferedStarts, len(native.BufferedStarts)+tc.wantExtra)
			if tc.wantExtra > 0 {
				require.Equal(t, tc.cursor, got.BufferedStarts[0].ActualTime.AsTime())
			}
			for _, start := range got.BufferedStarts[tc.wantExtra:] {
				require.True(t, start.ActualTime.AsTime().After(tc.cursor))
				require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, start.OverlapPolicy)
			}
		})
	}
}

func TestReviewBackfillMigrationAttempt(t *testing.T) {
	boundary := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, attempt := range []int64{0, 1} {
		t.Run(time.Duration(attempt).String(), func(t *testing.T) {
			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(boundary.Add(time.Hour))
			engine, engineCtx := newTestEngineContext(t, logger, withEngineTimeSource(timeSource))
			spec := defaultSchedule()
			spec.State.Paused = true
			request := migration.LegacyToCreateFromMigrationStateRequest(spec, &schedulepb.ScheduleInfo{},
				&schedulespb.InternalState{
					Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
					LastProcessedTime: timestamppb.New(timeSource.Now()),
					OngoingBackfills: []*schedulepb.BackfillRequest{{
						StartTime: timestamppb.New(boundary), EndTime: timestamppb.New(boundary.Add(time.Minute)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					}},
				}, nil, nil, timeSource.Now())
			for _, state := range request.State.Backfillers {
				state.LastProcessedTime = common.CloneProto(state.GetBackfillRequest().StartTime)
				state.Attempt = attempt
			}
			_, err := scheduler.NewTestHandler(logger).TestCreateFromMigrationState(engineCtx, request)
			require.NoError(t, err)
			ref := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})
			check := func() {
				_, err := chasm.ReadComponent(engineCtx, ref, func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
					if attempt == 0 {
						require.Empty(t, s.Backfillers)
						require.Len(t, s.Invoker.Get(ctx).BufferedStarts, 1)
					} else {
						require.Len(t, s.Backfillers, 1)
						require.Empty(t, s.Invoker.Get(ctx).BufferedStarts)
						for _, field := range s.Backfillers {
							b := field.Get(ctx)
							require.Equal(t, int64(1), b.TaskStamp)
							require.Equal(t, int64(1), b.Attempt)
							require.Equal(t, boundary, b.LastProcessedTime.AsTime())
						}
					}
					return struct{}{}, nil
				}, struct{}{})
				require.NoError(t, err)
			}
			check()
			timeSource.Advance(time.Hour)
			_, err = engine.FirePureTasks(ref, timeSource.Now())
			require.NoError(t, err)
			check()
		})
	}
}

func TestReviewBackfillRollbackBoundary(t *testing.T) {
	boundary := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, attempt := range []int64{0, 3} {
		t.Run(time.Duration(attempt).String(), func(t *testing.T) {
			env := newTestEnv(t)
			state := &schedulerpb.BackfillerState{
				BackfillId: "pending", Attempt: attempt,
				Request: &schedulerpb.BackfillerState_BackfillRequest{BackfillRequest: &schedulepb.BackfillRequest{
					StartTime: timestamppb.New(boundary), EndTime: timestamppb.New(boundary),
				}},
			}
			args := migration.CHASMToLegacyStartScheduleArgs(env.Scheduler.SchedulerState, nil, nil,
				map[string]*schedulerpb.BackfillerState{"pending": state}, nil, nil, nil, boundary.Add(time.Hour))
			require.Len(t, args.State.OngoingBackfills, 1)
			request := args.State.OngoingBackfills[0]
			got, err := env.SpecProcessor.ProcessTimeRange(env.Scheduler, request.StartTime.AsTime(), request.EndTime.AsTime(),
				request.OverlapPolicy, env.Scheduler.WorkflowID(), "legacy", true, nil)
			require.NoError(t, err)
			require.Empty(t, got.BufferedStarts)
			native, err := env.SpecProcessor.ProcessTimeRange(env.Scheduler, boundary.Add(-time.Millisecond), boundary,
				request.OverlapPolicy, env.Scheduler.WorkflowID(), "native", true, nil)
			require.NoError(t, err)
			require.Len(t, native.BufferedStarts, 1)
		})
	}
}

func TestReviewBackfillBypassesCallbackBarrier(t *testing.T) {
	boundary := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, includeBackfill := range []bool{false, true} {
		name := "without backfill"
		if includeBackfill {
			name = "with backfill"
		}
		t.Run(name, func(t *testing.T) {
			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(boundary.Add(time.Hour))
			_, engineCtx := newTestEngineContext(t, logger, withEngineTimeSource(timeSource))
			spec := defaultSchedule()
			spec.State.Paused = true
			spec.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
			state := &schedulespb.InternalState{
				Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
				LastProcessedTime: timestamppb.New(timeSource.Now()),
				BufferedStarts: []*schedulespb.BufferedStart{{
					NominalTime: timestamppb.New(boundary), ActualTime: timestamppb.New(boundary),
					Manual: true, RequestId: "pending", WorkflowId: "pending-wf",
				}},
			}
			if includeBackfill {
				state.OngoingBackfills = []*schedulepb.BackfillRequest{{
					StartTime: timestamppb.New(boundary), EndTime: timestamppb.New(boundary.Add(time.Minute)),
				}}
			}
			request := migration.LegacyToCreateFromMigrationStateRequest(spec,
				&schedulepb.ScheduleInfo{RunningWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: "stale-wf", RunId: "stale-run"}}},
				state, nil, nil, timeSource.Now())
			for _, backfill := range request.State.Backfillers {
				backfill.LastProcessedTime = common.CloneProto(backfill.GetBackfillRequest().StartTime)
			}
			_, err := scheduler.NewTestHandler(logger).TestCreateFromMigrationState(engineCtx, request)
			require.NoError(t, err)
			ref := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})
			_, err = chasm.ReadComponent(engineCtx, ref, func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
				starts := s.Invoker.Get(ctx).BufferedStarts
				if includeBackfill {
					require.Len(t, starts, 1)
					require.Equal(t, int64(2), s.Info.OverlapSkipped)
				} else {
					require.Len(t, starts, 2)
					require.Zero(t, s.Info.OverlapSkipped)
					require.Equal(t, "pending", starts[0].RequestId)
				}
				running := starts[len(starts)-1]
				require.Equal(t, "stale-run", running.RunId)
				require.False(t, running.HasCallback)
				return struct{}{}, nil
			}, struct{}{})
			require.NoError(t, err)
		})
	}
}

func TestReviewBackfillCapacityThreshold(t *testing.T) {
	for _, count := range []int{10, 450, 451, 1000} {
		t.Run(time.Duration(count).String(), func(t *testing.T) {
			env := newTestEnv(t)
			ctx := env.MutableContext()
			for range count {
				env.Scheduler.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{})
			}
			handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
				Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler,
				BaseLogger: env.Logger, SpecProcessor: env.SpecProcessor,
			})
			limit, err := handler.AllowedBufferedStarts(ctx, env.Scheduler, env.Scheduler.Invoker.Get(ctx), scheduler.DefaultTweakables)
			require.NoError(t, err)
			if count > 450 {
				require.Zero(t, limit)
			} else {
				require.Positive(t, limit)
			}
		})
	}
}
