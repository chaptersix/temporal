package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTaskValidityEngine(
	t *testing.T,
	schedule *schedulepb.Schedule,
	opts ...engineTestOption,
) (*chasmtest.Engine, context.Context, chasm.ComponentRef, log.Logger) {
	t.Helper()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	engine, engineCtx := newTestEngineContext(t, logger, opts...)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})
	_, err := scheduler.NewTestHandler(logger).CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   schedule,
			RequestId:  "create-request",
		},
	})
	require.NoError(t, err)
	return engine, engineCtx, rootRef, logger
}

func TestGeneratorTask_EngineHighWaterMarkInvalidation(t *testing.T) {
	engine, engineCtx, rootRef, logger := newTaskValidityEngine(t, defaultSchedule())
	handler := newGeneratorTaskHandlerForValidity(t, logger)
	now := time.Now()
	taskTime := now.Add(time.Minute)
	task := &schedulerpb.GeneratorTask{}

	var generator *scheduler.Generator
	_, _, err := chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			generator = s.Generator.Get(ctx)
			generator.LastProcessedTime = timestamppb.New(now)
			ctx.AddTask(generator, chasm.TaskAttributes{ScheduledTime: taskTime}, task)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			generator = s.Generator.Get(ctx)
			s.Info.OverlapSkipped++ // unrelated scheduler state
			valid, err := handler.Validate(ctx, generator,
				chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: taskTime}}, task)
			if err != nil {
				return struct{}{}, err
			}
			require.True(t, valid)
			generator.LastProcessedTime = timestamppb.New(taskTime)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	dropped, err := chasmtest.ExecutePureTask(
		context.Background(), engine, generator, handler, chasm.TaskAttributes{ScheduledTime: taskTime}, task)
	require.NoError(t, err)
	require.True(t, dropped)

	dropped, err = chasmtest.ExecutePureTask(
		context.Background(), engine, generator, handler, chasm.TaskAttributes{}, task)
	require.NoError(t, err)
	require.False(t, dropped, "immediate Generator tasks ignore the high water mark")
}

func newGeneratorTaskHandlerForValidity(t *testing.T, logger log.Logger) *scheduler.GeneratorTaskHandler {
	t.Helper()
	specProcessor := newRealSpecProcessor(gomock.NewController(t), logger)
	return scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		SpecBuilder:    newLegacySpecBuilder(0, 0),
	})
}

func TestIdleTask_EngineActivityInvalidatesAndRearms(t *testing.T) {
	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)
	schedule := defaultSchedule()
	schedule.State.LimitedActions = true
	schedule.State.RemainingActions = 0
	engine, engineCtx, rootRef, logger := newTaskValidityEngine(
		t, schedule, withEngineTimeSource(timeSource))
	handler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
	})

	var oldDeadline time.Time
	_, err := chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (struct{}, error) {
			oldDeadline = s.GetIdleCloseTime().AsTime()
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.False(t, oldDeadline.IsZero())
	idleTime := oldDeadline.Sub(now)
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, _ chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Info.OverlapSkipped++ // unrelated scheduler state
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			valid, err := handler.Validate(ctx, s,
				chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: oldDeadline}},
				&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTime)})
			if err != nil {
				return struct{}{}, err
			}
			require.True(t, valid)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	timeSource.Update(now.Add(time.Minute))
	_, err = scheduler.NewTestHandler(logger).UpdateSchedule(engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   schedule,
		},
	})
	require.NoError(t, err)

	var sched *scheduler.Scheduler
	var newDeadline time.Time
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			sched = s
			newDeadline = s.GetIdleCloseTime().AsTime()
			valid, err := handler.Validate(ctx, s,
				chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: newDeadline}},
				&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTime)})
			if err != nil {
				return struct{}{}, err
			}
			require.True(t, valid)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.True(t, newDeadline.After(oldDeadline))

	dropped, err := chasmtest.ExecutePureTask(
		context.Background(), engine, sched, handler,
		chasm.TaskAttributes{ScheduledTime: oldDeadline},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTime)})
	require.NoError(t, err)
	require.True(t, dropped)
}

func TestCallbacksTask_EngineCurrentTupleInvalidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*schedulespb.BufferedStart)
	}{
		{name: "callback recorded", mutate: func(start *schedulespb.BufferedStart) { start.HasCallback = true }},
		{name: "run ID cleared", mutate: func(start *schedulespb.BufferedStart) { start.RunId = "" }},
		{name: "completion recorded", mutate: func(start *schedulespb.BufferedStart) {
			start.Completed = &schedulespb.CompletedResult{}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine, engineCtx, rootRef, _ := newTaskValidityEngine(t, defaultSchedule())
			handler := scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
				Config: defaultConfig(),
			})
			task := &schedulerpb.SchedulerCallbacksTask{}

			var sched *scheduler.Scheduler
			_, _, err := chasm.UpdateComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
					sched = s
					s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
						RequestId:  "running-work",
						WorkflowId: "running-workflow",
						RunId:      "running-run",
						Attempt:    1,
					}}
					ctx.AddTask(s, chasm.TaskAttributes{}, task)
					return struct{}{}, nil
				}, struct{}{})
			require.NoError(t, err)

			_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
					sched = s
					s.Info.OverlapSkipped++ // unrelated scheduler state
					valid, err := handler.Validate(ctx, s, chasm.TaskInvocation{}, task)
					if err != nil {
						return struct{}{}, err
					}
					require.True(t, valid)
					test.mutate(s.Invoker.Get(ctx).BufferedStarts[0])
					return struct{}{}, nil
				}, struct{}{})
			require.NoError(t, err)

			dropped, err := chasmtest.ExecuteSideEffectTask(
				context.Background(), engine, sched, handler, chasm.TaskAttributes{}, task)
			require.NoError(t, err)
			require.True(t, dropped)
		})
	}
}

func TestMigrationTask_EngineStateInvalidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scheduler.Scheduler)
	}{
		{
			name: "migration cleared",
			mutate: func(s *scheduler.Scheduler) {
				s.WorkflowMigration = nil
			},
		},
		{
			name: "scheduler closed",
			mutate: func(s *scheduler.Scheduler) {
				s.Closed = true
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine, engineCtx, rootRef, logger := newTaskValidityEngine(t, defaultSchedule())
			handler := scheduler.NewSchedulerMigrateToWorkflowTaskHandler(
				scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
					Config:         defaultConfig(),
					MetricsHandler: metrics.NoopMetricsHandler,
					BaseLogger:     logger,
				})
			task := &schedulerpb.SchedulerMigrateToWorkflowTask{}

			var sched *scheduler.Scheduler
			_, _, err := chasm.UpdateComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
					sched = s
					s.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}
					ctx.AddTask(s, chasm.TaskAttributes{}, task)
					return struct{}{}, nil
				}, struct{}{})
			require.NoError(t, err)

			_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
					sched = s
					s.Info.OverlapSkipped++ // unrelated scheduler state
					valid, err := handler.Validate(ctx, s, chasm.TaskInvocation{}, task)
					if err != nil {
						return struct{}{}, err
					}
					require.True(t, valid)
					test.mutate(s)
					return struct{}{}, nil
				}, struct{}{})
			require.NoError(t, err)

			dropped, err := chasmtest.ExecuteSideEffectTask(
				context.Background(), engine, sched, handler, chasm.TaskAttributes{}, task)
			require.NoError(t, err)
			require.True(t, dropped)
		})
	}
}
