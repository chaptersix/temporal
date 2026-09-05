package scheduler_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Validate that ProcessBufferTask is invalidated by a later high water mark.
func TestProcessBufferTask_Validate(t *testing.T) {
	cases := []struct {
		name                   string
		initialLastProcessedAt *timestamppb.Timestamp
		currentLastProcessedAt *timestamppb.Timestamp
		scheduledTime          func(time.Time) time.Time
		expectedValid          bool
	}{
		{
			name:                   "immediate always valid",
			initialLastProcessedAt: timestamppb.New(time.Time{}),
			currentLastProcessedAt: timestamppb.New(time.Time{}),
			scheduledTime:          func(time.Time) time.Time { return time.Time{} },
			expectedValid:          true,
		},
		{
			name:                   "nil LPT always valid",
			initialLastProcessedAt: nil,
			currentLastProcessedAt: nil,
			scheduledTime:          func(now time.Time) time.Time { return now },
			expectedValid:          true,
		},
		{
			name:                   "scheduled after LPT is valid",
			initialLastProcessedAt: timestamppb.New(time.Unix(1, 0)),
			currentLastProcessedAt: timestamppb.New(time.Unix(1, 0)),
			scheduledTime:          func(now time.Time) time.Time { return now },
			expectedValid:          true,
		},
		{
			name:                   "scheduled equal to LPT is stale",
			initialLastProcessedAt: timestamppb.New(time.Unix(1, 0)),
			currentLastProcessedAt: nil,
			scheduledTime:          func(now time.Time) time.Time { return now },
			expectedValid:          false,
		},
		{
			name:                   "scheduled before LPT is stale",
			initialLastProcessedAt: timestamppb.New(time.Unix(1, 0)),
			currentLastProcessedAt: nil,
			scheduledTime:          func(now time.Time) time.Time { return now },
			expectedValid:          false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newSchedulerTestEngine(t, defaultSchedule())
			handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
				Config:         defaultConfig(),
				MetricsHandler: metrics.NoopMetricsHandler,
				BaseLogger:     env.logger,
			})
			now := env.timeSource.Now()
			scheduledTime := c.scheduledTime(now)
			currentLastProcessedAt := c.currentLastProcessedAt
			switch c.name {
			case "scheduled equal to LPT is stale":
				currentLastProcessedAt = timestamppb.New(scheduledTime)
			case "scheduled before LPT is stale":
				currentLastProcessedAt = timestamppb.New(scheduledTime.Add(time.Second))
			default:
			}

			var invoker *scheduler.Invoker
			err := env.updateScheduler(
				func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
					invoker = s.Invoker.Get(ctx)
					invoker.LastProcessedTime = c.initialLastProcessedAt
					if !scheduledTime.IsZero() {
						ctx.AddTask(invoker, chasm.TaskAttributes{ScheduledTime: scheduledTime}, &schedulerpb.InvokerProcessBufferTask{})
					}
					return nil
				})
			require.NoError(t, err)

			err = env.updateScheduler(
				func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
					invoker = s.Invoker.Get(ctx)
					s.Info.OverlapSkipped++ // unrelated scheduler state
					invoker.LastProcessedTime = currentLastProcessedAt
					return nil
				})
			require.NoError(t, err)

			dropped, err := chasmtest.ExecutePureTask(
				context.Background(), env.engine, invoker, handler,
				chasm.TaskAttributes{ScheduledTime: scheduledTime}, &schedulerpb.InvokerProcessBufferTask{})
			require.NoError(t, err)
			require.Equal(t, !c.expectedValid, dropped)
		})
	}
}

func TestProcessBufferTask_Validate_MigrationPending(t *testing.T) {
	env := newSchedulerTestEngine(t, defaultSchedule())
	handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.logger,
	})
	now := env.timeSource.Now()
	taskTime := now.Add(time.Minute)
	var invoker *scheduler.Invoker
	err := env.updateScheduler(
		func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			invoker = s.Invoker.Get(ctx)
			invoker.LastProcessedTime = timestamppb.New(now)
			ctx.AddTask(invoker, chasm.TaskAttributes{ScheduledTime: taskTime}, &schedulerpb.InvokerProcessBufferTask{})
			return nil
		})
	require.NoError(t, err)

	err = env.updateScheduler(
		func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			invoker = s.Invoker.Get(ctx)
			s.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}
			return nil
		})
	require.NoError(t, err)

	dropped, err := chasmtest.ExecutePureTask(
		context.Background(), env.engine, invoker, handler,
		chasm.TaskAttributes{ScheduledTime: taskTime}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.True(t, dropped)
}

// A buffer of only deferred starts (Attempt=-1) must NOT start a workflow or
// emit a ProcessBufferTask. Deferred starts wait on completion events, not on a
// wall-clock deadline, so emitting an immediate ProcessBufferTask would
// spin-loop on a no-op processBuffer call. (A visibility-update task may fire
// because the buffered count changed; the assertions below are scoped to
// exclude it.)
func TestInvoker_AddTasks_AllDeferredEmitsNothing(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	now := env.TimeSource.Now()
	invoker.LastProcessedTime = timestamppb.New(now)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:  "deferred",
		WorkflowId: "wf-deferred",
		Attempt:    -1,
		ActualTime: timestamppb.New(now),
	}}

	env.NodeBackend.TasksByCategory = nil
	invoker.EnqueueBufferedStarts(ctx, nil)
	require.NoError(t, env.CloseTransaction())

	require.False(t, env.HasTaskInCategory(&tasks.ChasmTask{}, tasks.CategoryTransfer, chasm.TaskScheduledTimeImmediate),
		"all-deferred buffer must not emit an immediate workflow-start (execute) task")
	require.False(t, env.HasTask(&tasks.ChasmTaskPure{}, chasm.TaskScheduledTimeImmediate),
		"all-deferred buffer must not emit an immediate ProcessBufferTask")
}

// After ProcessBuffer fires for a backed-off start whose BackoffTime has just
// elapsed, the HWM advance must re-arm processing so the retry actually runs.
// Regression for the addTasks gate that previously suppressed re-arm when
// processBuffer found nothing to process (no Attempt==0 starts).
func TestProcessBufferTask_RearmsBackedOffRetry(t *testing.T) {
	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)
	env := newSchedulerTestEngine(
		t, defaultSchedule(), withEngineTimeSource(timeSource))

	// Set LPT to the past so the start (with BackoffTime at now) becomes
	// eligible only after the HWM advance in this run.
	err := env.updateScheduler(
		func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			invoker := s.Invoker.Get(ctx)
			invoker.LastProcessedTime = timestamppb.New(now.Add(-time.Minute))
			invoker.BufferedStarts = []*schedulespb.BufferedStart{{
				NominalTime:   timestamppb.New(now),
				ActualTime:    timestamppb.New(now),
				DesiredTime:   timestamppb.New(now),
				RequestId:     "retry-ready",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				Attempt:       2,
				BackoffTime:   timestamppb.New(now),
			}}
			ctx.AddTask(invoker, chasm.TaskAttributes{ScheduledTime: now}, &schedulerpb.InvokerProcessBufferTask{})
			return nil
		})
	require.NoError(t, err)

	before, err := env.engine.Tasks(env.rootRef)
	require.NoError(t, err)
	beforeInvokerTasks := countInvokerSideEffectTasks(before)
	executed, err := env.engine.FirePureTasks(env.rootRef, now)
	require.NoError(t, err)
	require.Equal(t, 1, executed)

	// Precondition for re-arm: the HWM advance in this Execute must have made
	// the previously-backing-off start eligible (BackoffTime <= LPT).
	err = env.readScheduler(
		func(s *scheduler.Scheduler, ctx chasm.Context) error {
			invoker := s.Invoker.Get(ctx)
			require.False(t, invoker.BufferedStarts[0].BackoffTime.AsTime().After(invoker.LastProcessedTime.AsTime()),
				"task execution must advance the HWM through the backoff boundary")
			return nil
		})
	require.NoError(t, err)

	after, err := env.engine.Tasks(env.rootRef)
	require.NoError(t, err)
	require.Equal(t, beforeInvokerTasks+1, countInvokerSideEffectTasks(after),
		"elapsed retry must emit an immediate Invoker Execute task")
}

func countInvokerSideEffectTasks(tasksByCategory map[tasks.Category][]tasks.Task) int {
	count := 0
	for _, task := range tasksByCategory[tasks.CategoryTransfer] {
		chasmTask, ok := task.(*tasks.ChasmTask)
		if ok && slices.Equal(chasmTask.Info.GetPath(), []string{"Invoker"}) {
			count++
		}
	}
	return count
}

func newProcessBufferHandler(env *testEnv) *scheduler.InvokerProcessBufferTaskHandler {
	return scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
}

type processBufferTestCase struct {
	InitialBufferedStarts     []*schedulespb.BufferedStart
	InitialCancelWorkflows    []*commonpb.WorkflowExecution
	InitialTerminateWorkflows []*commonpb.WorkflowExecution
	InitialRunningWorkflows   []*commonpb.WorkflowExecution

	ExpectedBufferedStarts      int
	ExpectedRunningWorkflows    int
	ExpectedTerminateWorkflows  int
	ExpectedCancelWorkflows     int
	ExpectedOverlapSkipped      int64
	ExpectedMissedCatchupWindow int64

	ValidateInvoker func(t *testing.T, invoker *scheduler.Invoker)
}

func runProcessBufferTestCase(t *testing.T, env *testEnv, c *processBufferTestCase) {
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	// Set up initial state. Note: InitialRunningWorkflows is now represented by
	// BufferedStarts that have RunId set but no Completed field.
	invoker.BufferedStarts = c.InitialBufferedStarts
	invoker.CancelWorkflows = c.InitialCancelWorkflows
	invoker.TerminateWorkflows = c.InitialTerminateWorkflows

	// Add initial running workflows as BufferedStarts with RunId set.
	for _, wf := range c.InitialRunningWorkflows {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:  wf.WorkflowId + "-req",
			WorkflowId: wf.WorkflowId,
			RunId:      wf.RunId,
			Attempt:    1,
		})
	}

	// Set LastProcessedTime to current time to ensure time checks pass.
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	handler := newProcessBufferHandler(env)
	err := handler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	// Validate the results.
	// Count BufferedStarts (excluding running ones added from InitialRunningWorkflows).
	require.Len(t, invoker.GetBufferedStarts(), c.ExpectedBufferedStarts+len(c.InitialRunningWorkflows))

	// Count running workflows from BufferedStarts (has RunId but no Completed).
	runningCount := 0
	for _, start := range invoker.GetBufferedStarts() {
		if start.GetRunId() != "" && start.GetCompleted() == nil {
			runningCount++
		}
	}
	require.Equal(t, c.ExpectedRunningWorkflows, runningCount)

	require.Len(t, invoker.TerminateWorkflows, c.ExpectedTerminateWorkflows)
	require.Len(t, invoker.CancelWorkflows, c.ExpectedCancelWorkflows)
	require.Equal(t, c.ExpectedOverlapSkipped, env.Scheduler.Info.OverlapSkipped)
	require.Equal(t, c.ExpectedMissedCatchupWindow, env.Scheduler.Info.MissedCatchupWindow)

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker)
	}
}

// ProcessBuffer attempts all buffered starts with ALLOW_ALL policy.
func TestProcessBufferTask_AllowAll(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req3",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:  bufferedStarts,
		ExpectedBufferedStarts: 3,
		ExpectedOverlapSkipped: 0,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 3)
		},
	})
}

// ProcessBuffer processes a start that missed the catchup window.
func TestProcessBufferTask_MissedCatchupWindow(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	startTime := now.Add(-defaultCatchupWindow * 2)
	startTimestamp := timestamppb.New(startTime)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTimestamp,
			ActualTime:    startTimestamp,
			DesiredTime:   startTimestamp,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:       bufferedStarts,
		ExpectedBufferedStarts:      0,
		ExpectedOverlapSkipped:      0,
		ExpectedMissedCatchupWindow: 1,
	})
}

// ProcessBuffer defers a start (from overlap policy) by placing it into NewBuffer.
func TestProcessBufferTask_BufferOne(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req3",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: bufferedStarts,
		// Because no workflows are running, we'll immediately kick off one
		// BufferedStart, and then buffer the next. This leaves us with 1 ready start,
		// and 1 still buffered.
		ExpectedBufferedStarts: 2,
		ExpectedOverlapSkipped: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			// Only one start should be set for execution (Attempt > 0).
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 1)
		},
	})
}

func TestProcessBufferTask_BufferOneKeepsExistingDeferredStart(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				RequestId:     "deferred-first",
				WorkflowId:    "deferred-first",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
				Attempt:       -1,
			},
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				RequestId:     "new-later",
				WorkflowId:    "new-later",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			},
		},
		InitialRunningWorkflows:  []*commonpb.WorkflowExecution{{WorkflowId: "running", RunId: "running-run"}},
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedOverlapSkipped:   1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Equal(t, "deferred-first", invoker.GetBufferedStarts()[0].GetRequestId())
			require.Equal(t, int64(-1), invoker.GetBufferedStarts()[0].GetAttempt())
		},
	})
}

func TestProcessBufferTask_BufferOneDropsDeferredStartPastCatchupWindow(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule.Policies.CatchupWindow = durationpb.New(10 * time.Minute)
	startTime := timestamppb.New(env.TimeSource.Now().Add(-15 * time.Minute))
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			RequestId:     "deferred-expired",
			WorkflowId:    "deferred-expired",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			Attempt:       -1,
		}},
		ExpectedMissedCatchupWindow: 1,
	})
}

// ProcessBuffer is scheduled with an empty buffer.
func TestProcessBufferTask_Empty(t *testing.T) {
	env := newTestEnv(t)
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: nil,
	})
}

// ProcessBuffer is scheduled with a buffer of starts all backing off.
func TestProcessBufferTask_BackingOff(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	backoffTime := startTime.AsTime().Add(30 * time.Minute)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
			BackoffTime:   timestamppb.New(backoffTime),
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       3,
			BackoffTime:   timestamppb.New(backoffTime),
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:  bufferedStarts,
		ExpectedBufferedStarts: 2,
	})
}

// ProcessBuffer is scheduled with a start that was backing off, but ready to retry.
func TestProcessBufferTask_BackingOffReady(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	backoffTime := env.TimeSource.Now().Add(-1 * time.Minute)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
			BackoffTime:   timestamppb.New(backoffTime),
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:  bufferedStarts,
		ExpectedBufferedStarts: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			// The start should be ready for execution (Attempt > 0).
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 1)
		},
	})
}

// A buffered start with an overlap policy to terminate other workflows is processed.
func TestProcessBufferTask_NeedsTerminate(t *testing.T) {
	env := newTestEnv(t)

	// Add a running workflow to the Scheduler.
	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	// Set up the BufferedStart with a policy that will terminate existing workflows.
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:   bufferedStarts,
		InitialRunningWorkflows: initialRunningWorkflows,
		// Buffer should still contain the buffered start. The existing workflow will still
		// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
		// after termination/cancelation takes effect.
		ExpectedBufferedStarts:     1,
		ExpectedRunningWorkflows:   1,
		ExpectedTerminateWorkflows: 1,
	})
}

// Past-catchup automated starts must drop WITHOUT consuming a LimitedActions
// slot. Regression for the order-of-checks bug where action capacity consumption
// fired before the catchup-window check, decrementing RemainingActions for
// starts that never ran.
//
// Not reachable via the public API today: backfill/TriggerImmediately starts
// are Manual=true (see backfiller_tasks.go) and bypass the catchup-window
// check. The bug only manifests for automated Generator starts that age past
// their catchup window while processing is stalled.
func TestProcessBufferTask_MissedCatchupPreservesRemainingActions(t *testing.T) {
	schedule := defaultSchedule()
	schedule.State.LimitedActions = true
	schedule.State.RemainingActions = 3
	env := newSchedulerTestEngine(t, schedule)

	now := env.timeSource.Now()
	startTime := timestamppb.New(now.Add(-defaultCatchupWindow * 2))
	err := env.updateScheduler(
		func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			invoker := s.Invoker.Get(ctx)
			invoker.BufferedStarts = []*schedulespb.BufferedStart{{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				Manual:        false,
				RequestId:     "expired",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}}
			ctx.AddTask(invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
			return nil
		})
	require.NoError(t, err)

	err = env.readScheduler(
		func(s *scheduler.Scheduler, ctx chasm.Context) error {
			require.Empty(t, s.Invoker.Get(ctx).GetBufferedStarts())
			require.Equal(t, int64(1), s.Info.GetMissedCatchupWindow())
			require.Equal(t, int64(3), s.Schedule.State.GetRemainingActions(),
				"RemainingActions must not be consumed by a start that was dropped for missing the catchup window")
			return nil
		})
	require.NoError(t, err)
}

// Paused schedules drop automated buffered starts during processBuffer (but
// must keep manual ones). Guards against accidental promotion of automated
// starts while paused.
func TestProcessBufferTask_PausedDropsAutomatedKeepsManual(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule.State.Paused = true

	startTime := timestamppb.New(env.TimeSource.Now())
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				Manual:        false,
				RequestId:     "auto",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				Manual:        true,
				RequestId:     "manual",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
		ExpectedBufferedStarts: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			kept := invoker.GetBufferedStarts()[0]
			require.Equal(t, "manual", kept.RequestId)
			require.Equal(t, int64(1), kept.Attempt,
				"manual start must be promoted to Attempt=1 even when schedule is paused")
		},
	})
}

type bufferProcessingComparisonInput struct {
	name                 string
	schedule             *schedulepb.Schedule
	bufferedStarts       []*schedulespb.BufferedStart
	cancelWorkflows      []*commonpb.WorkflowExecution
	terminateWorkflows   []*commonpb.WorkflowExecution
	lastProcessedTime    time.Time
	workflowMigration    bool
	initialConflictToken int64
}

type normalizedBufferProcessing struct {
	schedulerState     *schedulerpb.SchedulerState
	invokerState       *schedulerpb.InvokerState
	tasks              []string
	actionRequests     []string
	metrics            []string
	outcomes           []string
	remainingDelta     int64
	conflictTokenDelta int64
}

func TestProcessBufferTask_LegacyAndPlannerDifferentialCorpus(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	makeStart := func(id string, policy enumspb.ScheduleOverlapPolicy) *schedulespb.BufferedStart {
		return &schedulespb.BufferedStart{
			NominalTime:   timestamppb.New(now),
			ActualTime:    timestamppb.New(now),
			DesiredTime:   timestamppb.New(now),
			RequestId:     id,
			WorkflowId:    "workflow-" + id,
			OverlapPolicy: policy,
		}
	}
	running := makeStart("running", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	running.Attempt = 1
	running.RunId = "running-run"

	var corpus []bufferProcessingComparisonInput
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		corpus = append(corpus, bufferProcessingComparisonInput{
			name:              "overlap " + policy.String(),
			schedule:          defaultSchedule(),
			bufferedStarts:    []*schedulespb.BufferedStart{running, makeStart("pending", policy)},
			lastProcessedTime: now,
		})
	}

	pausedSchedule := defaultSchedule()
	pausedSchedule.State.Paused = true
	pausedManual := makeStart("manual", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	pausedManual.Manual = true
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "paused automatic and manual", schedule: pausedSchedule,
		bufferedStarts: []*schedulespb.BufferedStart{
			makeStart("automatic", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL), pausedManual,
		}, lastProcessedTime: now,
	})
	for _, remaining := range []int64{0, 1, 3} {
		limited := defaultSchedule()
		limited.State.LimitedActions = true
		limited.State.RemainingActions = remaining
		corpus = append(corpus, bufferProcessingComparisonInput{
			name: fmt.Sprintf("limited actions %d", remaining), schedule: limited,
			bufferedStarts: []*schedulespb.BufferedStart{
				makeStart("first", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL),
				makeStart("second", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL),
			}, lastProcessedTime: now, initialConflictToken: 17,
		})
	}
	unlimitedManual := makeStart("manual", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	unlimitedManual.Manual = true
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "unlimited and manual", schedule: defaultSchedule(),
		bufferedStarts: []*schedulespb.BufferedStart{
			makeStart("automatic", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL), unlimitedManual,
		}, lastProcessedTime: now,
	})
	expired := makeStart("expired", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	expired.ActualTime = timestamppb.New(now.Add(-2 * defaultCatchupWindow))
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "expired catchup window", schedule: defaultSchedule(),
		bufferedStarts: []*schedulespb.BufferedStart{expired}, lastProcessedTime: now,
	})
	deferred := makeStart("deferred", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
	deferred.Attempt = -1
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "running and deferred", schedule: defaultSchedule(),
		bufferedStarts: []*schedulespb.BufferedStart{running, deferred}, lastProcessedTime: now,
	})
	for _, backoffDelta := range []time.Duration{-time.Second, 0, time.Second} {
		retry := makeStart(fmt.Sprintf("retry-%s", backoffDelta), enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
		retry.Attempt = 2
		retry.BackoffTime = timestamppb.New(now.Add(backoffDelta))
		corpus = append(corpus, bufferProcessingComparisonInput{
			name: fmt.Sprintf("retry boundary %s", backoffDelta), schedule: defaultSchedule(),
			bufferedStarts: []*schedulespb.BufferedStart{retry}, lastProcessedTime: now,
		})
	}
	completed := makeStart("duplicate", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	completed.Attempt = 1
	completed.RunId = "completed-run"
	completed.Completed = &schedulespb.CompletedResult{}
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "duplicate and completed entries", schedule: defaultSchedule(),
		bufferedStarts:    []*schedulespb.BufferedStart{completed, proto.Clone(completed).(*schedulespb.BufferedStart)},
		lastProcessedTime: now,
	})
	duplicatePending := makeStart("duplicate-pending", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "duplicate pending entries", schedule: defaultSchedule(),
		bufferedStarts: []*schedulespb.BufferedStart{
			running,
			duplicatePending,
			proto.Clone(duplicatePending).(*schedulespb.BufferedStart),
		},
		lastProcessedTime: now,
	})
	corpus = append(corpus,
		bufferProcessingComparisonInput{
			name: "existing cancel work", schedule: defaultSchedule(),
			bufferedStarts:    []*schedulespb.BufferedStart{running},
			cancelWorkflows:   []*commonpb.WorkflowExecution{{WorkflowId: "cancel", RunId: "cancel-run"}},
			lastProcessedTime: now,
		},
		bufferProcessingComparisonInput{
			name: "existing terminate work", schedule: defaultSchedule(),
			bufferedStarts:     []*schedulespb.BufferedStart{running},
			terminateWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: "terminate", RunId: "terminate-run"}},
			lastProcessedTime:  now,
		},
		bufferProcessingComparisonInput{
			name: "migration state", schedule: defaultSchedule(),
			bufferedStarts:    []*schedulespb.BufferedStart{makeStart("migration", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)},
			lastProcessedTime: now, workflowMigration: true,
		},
	)
	completionRace := makeStart("completion-race", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	completionRace.Attempt = 1
	completionRace.Completed = &schedulespb.CompletedResult{}
	corpus = append(corpus, bufferProcessingComparisonInput{
		name: "completion before start result", schedule: defaultSchedule(),
		bufferedStarts: []*schedulespb.BufferedStart{completionRace}, lastProcessedTime: now,
	})

	for _, input := range corpus {
		t.Run(input.name, func(t *testing.T) {
			compareBufferProcessing(t, input)
		})
	}
}

func TestProcessBufferTask_LegacyAndPlannerDifferentialOverlapStateSpace(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	capacityCases := []struct {
		name      string
		limited   bool
		remaining int64
	}{
		{name: "unlimited"},
		{name: "exhausted", limited: true},
		{name: "one", limited: true, remaining: 1},
		{name: "two", limited: true, remaining: 2},
		{name: "three", limited: true, remaining: 3},
	}

	comparisons := 0
	for length := 0; length <= 4; length++ {
		forEachOverlapPolicySequence(policies, length, func(sequence []enumspb.ScheduleOverlapPolicy) {
			for _, running := range []bool{false, true} {
				for _, capacity := range capacityCases {
					schedule := defaultSchedule()
					schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
					schedule.State.LimitedActions = capacity.limited
					schedule.State.RemainingActions = capacity.remaining
					starts := make([]*schedulespb.BufferedStart, 0, len(sequence)+1)
					if running {
						start := newBufferProcessingComparisonStart(now, "running", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
						start.Attempt = 1
						start.RunId = "running-run"
						starts = append(starts, start)
					}
					for index, policy := range sequence {
						starts = append(starts, newBufferProcessingComparisonStart(now, fmt.Sprintf("pending-%d", index), policy))
					}
					compareBufferProcessing(t, bufferProcessingComparisonInput{
						name: fmt.Sprintf(
							"overlap state length=%d policies=%v running=%t capacity=%s",
							length,
							sequence,
							running,
							capacity.name,
						),
						schedule:             schedule,
						bufferedStarts:       starts,
						lastProcessedTime:    now,
						initialConflictToken: 17,
					})
					comparisons++
				}
			}
		})
	}
	require.Equal(t, 15550, comparisons)
}

func TestProcessBufferTask_LegacyAndPlannerDifferentialDuplicateIdentityStateSpace(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	requestIDPatterns := []struct {
		name string
		id   func(int) string
	}{
		{name: "same", id: func(int) string { return "duplicate" }},
		{name: "alternating", id: func(index int) string { return fmt.Sprintf("duplicate-%d", index%2) }},
	}
	comparisons := 0
	for length := 1; length <= 3; length++ {
		forEachOverlapPolicySequence(policies, length, func(sequence []enumspb.ScheduleOverlapPolicy) {
			for _, requestIDs := range requestIDPatterns {
				for _, running := range []bool{false, true} {
					for remaining := int64(0); remaining <= 3; remaining++ {
						schedule := defaultSchedule()
						schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
						schedule.State.LimitedActions = true
						schedule.State.RemainingActions = remaining
						starts := make([]*schedulespb.BufferedStart, 0, len(sequence)+1)
						if running {
							start := newBufferProcessingComparisonStart(now, "running", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
							start.Attempt = 1
							start.RunId = "running-run"
							starts = append(starts, start)
						}
						for index, policy := range sequence {
							starts = append(starts, newBufferProcessingComparisonStart(now, requestIDs.id(index), policy))
						}
						compareBufferProcessing(t, bufferProcessingComparisonInput{
							name: fmt.Sprintf(
								"duplicate identities pattern=%s policies=%v running=%t remaining=%d",
								requestIDs.name,
								sequence,
								running,
								remaining,
							),
							schedule:             schedule,
							bufferedStarts:       starts,
							lastProcessedTime:    now,
							initialConflictToken: 17,
						})
						comparisons++
					}
				}
			}
		})
	}
	require.Equal(t, 6384, comparisons)
}

func TestProcessBufferTask_LegacyAndPlannerDifferentialDefaultPolicyStateSpace(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	comparisons := 0
	for _, defaultPolicy := range policies {
		for unspecifiedIndex := range 3 {
			forEachOverlapPolicySequence(policies, 2, func(surrounding []enumspb.ScheduleOverlapPolicy) {
				sequence := make([]enumspb.ScheduleOverlapPolicy, 3)
				sequence[unspecifiedIndex] = enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
				for index, policy := range surrounding {
					if index >= unspecifiedIndex {
						index++
					}
					sequence[index] = policy
				}
				for _, running := range []bool{false, true} {
					for remaining := int64(0); remaining <= 3; remaining++ {
						schedule := defaultSchedule()
						schedule.Policies.OverlapPolicy = defaultPolicy
						schedule.State.LimitedActions = true
						schedule.State.RemainingActions = remaining
						starts := make([]*schedulespb.BufferedStart, 0, len(sequence)+1)
						if running {
							start := newBufferProcessingComparisonStart(now, "running", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
							start.Attempt = 1
							start.RunId = "running-run"
							starts = append(starts, start)
						}
						for index, policy := range sequence {
							starts = append(starts, newBufferProcessingComparisonStart(now, fmt.Sprintf("pending-%d", index), policy))
						}
						compareBufferProcessing(t, bufferProcessingComparisonInput{
							name: fmt.Sprintf(
								"default policy=%s policies=%v running=%t remaining=%d",
								defaultPolicy,
								sequence,
								running,
								remaining,
							),
							schedule:             schedule,
							bufferedStarts:       starts,
							lastProcessedTime:    now,
							initialConflictToken: 17,
						})
						comparisons++
					}
				}
			})
		}
	}
	require.Equal(t, 5184, comparisons)
}

func TestProcessBufferTask_LegacyAndPlannerDifferentialTimeCapacityStateSpace(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	catchupWindows := []time.Duration{time.Second, 5 * time.Second, time.Hour}
	timeRelations := []struct {
		name   string
		offset time.Duration
	}{
		{name: "before", offset: time.Nanosecond},
		{name: "equal"},
		{name: "after", offset: -time.Nanosecond},
	}
	capacityCases := []struct {
		name      string
		limited   bool
		remaining int64
	}{
		{name: "unlimited"},
		{name: "exhausted", limited: true},
		{name: "one", limited: true, remaining: 1},
		{name: "two", limited: true, remaining: 2},
	}
	policyCases := []struct {
		name          string
		policy        enumspb.ScheduleOverlapPolicy
		defaultPolicy enumspb.ScheduleOverlapPolicy
	}{
		{name: "explicit allow all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
		{name: "default allow all", defaultPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
	}

	comparisons := 0
	for _, policy := range policyCases {
		for _, catchupWindow := range catchupWindows {
			effectiveWindow := max(catchupWindow, 5*time.Second)
			for _, deadlineRelation := range timeRelations {
				actualTime := now.Add(-effectiveWindow + deadlineRelation.offset)
				deadline := actualTime.Add(effectiveWindow)
				for _, desiredRelation := range timeRelations {
					desiredTime := deadline.Add(-desiredRelation.offset)
					for _, manual := range []bool{false, true} {
						for _, paused := range []bool{false, true} {
							for _, running := range []bool{false, true} {
								for _, capacity := range capacityCases {
									schedule := defaultSchedule()
									schedule.Policies.OverlapPolicy = policy.defaultPolicy
									schedule.Policies.CatchupWindow = durationpb.New(catchupWindow)
									schedule.State.Paused = paused
									schedule.State.LimitedActions = capacity.limited
									schedule.State.RemainingActions = capacity.remaining
									start := newBufferProcessingComparisonStart(now, "pending", policy.policy)
									start.ActualTime = timestamppb.New(actualTime)
									start.DesiredTime = timestamppb.New(desiredTime)
									start.Manual = manual
									starts := []*schedulespb.BufferedStart{start}
									if running {
										runningStart := newBufferProcessingComparisonStart(now, "running", enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
										runningStart.Attempt = 1
										runningStart.RunId = "running-run"
										starts = append([]*schedulespb.BufferedStart{runningStart}, starts...)
									}
									compareBufferProcessing(t, bufferProcessingComparisonInput{
										name: fmt.Sprintf(
											"time capacity policy=%s catchup=%s deadline=%s desired=%s manual=%t paused=%t running=%t capacity=%s",
											policy.name,
											catchupWindow,
											deadlineRelation.name,
											desiredRelation.name,
											manual,
											paused,
											running,
											capacity.name,
										),
										schedule:             schedule,
										bufferedStarts:       starts,
										lastProcessedTime:    now,
										initialConflictToken: 17,
									})
									comparisons++
								}
							}
						}
					}
				}
			}
		}
	}
	require.Equal(t, 1728, comparisons)
}

func TestProcessBufferTask_LegacyAndPlannerDifferentialLifecycleStateSpace(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	defaultPolicies := policies[1:]
	lifecycleCases := []struct {
		name      string
		attempt   int64
		runID     string
		completed bool
	}{
		{name: "deferred", attempt: -1},
		{name: "unprocessed"},
		{name: "ready", attempt: 1},
		{name: "retry", attempt: 2},
		{name: "running", attempt: 1, runID: "run"},
		{name: "completed", attempt: 1, runID: "run", completed: true},
		{name: "completion before start", attempt: 1, completed: true},
	}

	comparisons := 0
	for _, defaultPolicy := range defaultPolicies {
		for _, policy := range policies {
			for _, lifecycle := range lifecycleCases {
				schedule := defaultSchedule()
				schedule.Policies.OverlapPolicy = defaultPolicy
				start := newBufferProcessingComparisonStart(now, "start", policy)
				start.Attempt = lifecycle.attempt
				start.RunId = lifecycle.runID
				if lifecycle.completed {
					start.Completed = &schedulespb.CompletedResult{}
				}
				compareBufferProcessing(t, bufferProcessingComparisonInput{
					name: fmt.Sprintf(
						"lifecycle=%s policy=%s default=%s",
						lifecycle.name,
						policy,
						defaultPolicy,
					),
					schedule:          schedule,
					bufferedStarts:    []*schedulespb.BufferedStart{start},
					lastProcessedTime: now,
				})
				comparisons++
			}
		}
	}
	require.Equal(t, 294, comparisons)
}

func forEachOverlapPolicySequence(
	policies []enumspb.ScheduleOverlapPolicy,
	length int,
	visit func([]enumspb.ScheduleOverlapPolicy),
) {
	sequence := make([]enumspb.ScheduleOverlapPolicy, length)
	var generate func(int)
	generate = func(index int) {
		if index == len(sequence) {
			visit(sequence)
			return
		}
		for _, policy := range policies {
			sequence[index] = policy
			generate(index + 1)
		}
	}
	generate(0)
}

func newBufferProcessingComparisonStart(
	now time.Time,
	requestID string,
	policy enumspb.ScheduleOverlapPolicy,
) *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{
		NominalTime:   timestamppb.New(now),
		ActualTime:    timestamppb.New(now),
		DesiredTime:   timestamppb.New(now),
		RequestId:     requestID,
		WorkflowId:    "workflow-" + requestID,
		OverlapPolicy: policy,
	}
}

func TestApplyBufferPlan_RevalidatesRequestBeforeMutation(t *testing.T) {
	schedule := defaultSchedule()
	schedule.State.LimitedActions = true
	schedule.State.RemainingActions = 1
	env := newSchedulerTestEngine(t, schedule)
	now := env.timeSource.Now()
	config := defaultConfig()
	handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: env.logger,
	})
	var apply func(chasm.MutableContext, *scheduler.Scheduler, *scheduler.Invoker) int64
	var initialConflictToken int64
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		invoker := s.Invoker.Get(ctx)
		invoker.BufferedStarts = []*schedulespb.BufferedStart{{
			NominalTime: timestamppb.New(now), ActualTime: timestamppb.New(now), DesiredTime: timestamppb.New(now),
			RequestId: "request", WorkflowId: "workflow", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}}
		invoker.LastProcessedTime = timestamppb.New(now)
		apply = handler.PlanBufferProcessingForTest(invoker, s, now)
		initialConflictToken = s.ConflictToken
		return nil
	}))

	var invalidatedDecisions int64
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		invoker := s.Invoker.Get(ctx)
		invoker.BufferedStarts[0].Attempt = 1
		invalidatedDecisions = apply(ctx, s, invoker)
		return nil
	}))

	require.Equal(t, int64(1), invalidatedDecisions)
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Equal(t, int64(1), s.Invoker.Get(ctx).BufferedStarts[0].GetAttempt())
		require.Equal(t, int64(1), s.Schedule.State.GetRemainingActions())
		require.Equal(t, initialConflictToken, s.ConflictToken)
		return nil
	}))
}

func compareBufferProcessing(t *testing.T, input bufferProcessingComparisonInput) {
	t.Helper()
	legacy := runBufferProcessing(t, input, false)
	planned := runBufferProcessing(t, input, true)
	require.True(t, proto.Equal(legacy.schedulerState, planned.schedulerState), "%s: Scheduler state mismatch\nlegacy: %v\nplanned: %v", input.name, legacy.schedulerState, planned.schedulerState)
	require.True(t, proto.Equal(legacy.invokerState, planned.invokerState), "%s: Invoker state mismatch\nlegacy: %v\nplanned: %v", input.name, legacy.invokerState, planned.invokerState)
	require.Equal(t, legacy.tasks, planned.tasks, input.name)
	require.Equal(t, legacy.actionRequests, planned.actionRequests, input.name)
	require.Equal(t, legacy.metrics, planned.metrics, input.name)
	require.Equal(t, legacy.outcomes, planned.outcomes, input.name)
	require.Equal(t, legacy.remainingDelta, planned.remainingDelta, input.name)
	require.Equal(t, legacy.conflictTokenDelta, planned.conflictTokenDelta, input.name)
}

func runBufferProcessing(t *testing.T, input bufferProcessingComparisonInput, planner bool) normalizedBufferProcessing {
	t.Helper()
	env := newTestEnv(t)
	env.TimeSource.Update(input.lastProcessedTime)
	ctx := env.MutableContext()
	env.Scheduler.Schedule = proto.Clone(input.schedule).(*schedulepb.Schedule)
	env.Scheduler.Info.CreateTime = timestamppb.New(input.lastProcessedTime)
	env.Scheduler.LastEventTime = timestamppb.New(input.lastProcessedTime)
	initialConflictToken := input.initialConflictToken
	if initialConflictToken == 0 {
		initialConflictToken = env.Scheduler.ConflictToken
	}
	env.Scheduler.ConflictToken = initialConflictToken
	if input.workflowMigration {
		env.Scheduler.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}
	}
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.InvokerState = &schedulerpb.InvokerState{
		BufferedStarts:     cloneBufferedStarts(input.bufferedStarts),
		CancelWorkflows:    cloneWorkflowExecutions(input.cancelWorkflows),
		TerminateWorkflows: cloneWorkflowExecutions(input.terminateWorkflows),
		LastProcessedTime:  timestamppb.New(input.lastProcessedTime),
	}
	env.NodeBackend.TasksByCategory = nil

	config := defaultConfig()
	recorder := metricstest.NewCaptureHandler()
	capture := recorder.StartCapture()
	defer recorder.StopCapture(capture)
	handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config: config, MetricsHandler: recorder, BaseLogger: env.Logger,
	})
	initialRemaining := env.Scheduler.Schedule.State.GetRemainingActions()
	if planner {
		require.NoError(t, handler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{}))
	} else {
		require.NoError(t, handler.ExecuteProcessBufferLegacyForTest(ctx, invoker))
	}
	require.NoError(t, env.CloseTransaction())

	return normalizedBufferProcessing{
		schedulerState:     proto.Clone(env.Scheduler.SchedulerState).(*schedulerpb.SchedulerState),
		invokerState:       proto.Clone(invoker.InvokerState).(*schedulerpb.InvokerState),
		tasks:              normalizeTasks(env.NodeBackend.TasksByCategory),
		actionRequests:     normalizeActionRequests(invoker),
		metrics:            normalizeMetrics(capture.Snapshot()),
		outcomes:           normalizeBufferOutcomes(input.bufferedStarts, invoker.GetBufferedStarts()),
		remainingDelta:     env.Scheduler.Schedule.State.GetRemainingActions() - initialRemaining,
		conflictTokenDelta: env.Scheduler.ConflictToken - initialConflictToken,
	}
}

func cloneBufferedStarts(starts []*schedulespb.BufferedStart) []*schedulespb.BufferedStart {
	cloned := make([]*schedulespb.BufferedStart, 0, len(starts))
	for _, start := range starts {
		cloned = append(cloned, proto.Clone(start).(*schedulespb.BufferedStart))
	}
	return cloned
}

func cloneWorkflowExecutions(executions []*commonpb.WorkflowExecution) []*commonpb.WorkflowExecution {
	cloned := make([]*commonpb.WorkflowExecution, 0, len(executions))
	for _, execution := range executions {
		cloned = append(cloned, proto.Clone(execution).(*commonpb.WorkflowExecution))
	}
	return cloned
}

func normalizeTasks(tasksByCategory map[tasks.Category][]tasks.Task) []string {
	var categories []tasks.Category
	for category := range tasksByCategory {
		categories = append(categories, category)
	}
	slices.SortFunc(categories, func(a, b tasks.Category) int { return a.ID() - b.ID() })
	var normalized []string
	for _, category := range categories {
		for index, task := range tasksByCategory[category] {
			identity := ""
			if chasmTask, ok := task.(*tasks.ChasmTask); ok {
				identity = fmt.Sprintf("path=%s,type=%d,data=%x", strings.Join(chasmTask.Info.GetPath(), "/"), chasmTask.Info.GetTypeId(), chasmTask.Info.GetData().GetData())
			}
			normalized = append(normalized, fmt.Sprintf("%d:%d:%s:%s:%s", category.ID(), index, task.GetType(), task.GetVisibilityTime().UTC().Format(time.RFC3339Nano), identity))
		}
	}
	return normalized
}

func normalizeActionRequests(invoker *scheduler.Invoker) []string {
	requests := make([]string, 0, len(invoker.GetTerminateWorkflows())+len(invoker.GetCancelWorkflows()))
	for _, execution := range invoker.GetTerminateWorkflows() {
		requests = append(requests, "terminate:"+execution.GetWorkflowId()+":"+execution.GetRunId())
	}
	for _, execution := range invoker.GetCancelWorkflows() {
		requests = append(requests, "cancel:"+execution.GetWorkflowId()+":"+execution.GetRunId())
	}
	return requests
}

func normalizeMetrics(snapshot metricstest.CaptureSnapshot) []string {
	var normalized []string
	for name, recordings := range snapshot {
		for _, recording := range recordings {
			var tags []string
			for key, value := range recording.Tags {
				tags = append(tags, key+"="+value)
			}
			slices.Sort(tags)
			normalized = append(normalized, fmt.Sprintf("%s:%v:%s", name, recording.Value, strings.Join(tags, ",")))
		}
	}
	slices.Sort(normalized)
	return normalized
}

func normalizeBufferOutcomes(before, after []*schedulespb.BufferedStart) []string {
	afterByRequestID := make(map[string][]*schedulespb.BufferedStart)
	for _, start := range after {
		afterByRequestID[start.GetRequestId()] = append(afterByRequestID[start.GetRequestId()], start)
	}
	outcomes := make([]string, 0, len(before))
	for _, start := range before {
		matches := afterByRequestID[start.GetRequestId()]
		if len(matches) == 0 {
			outcomes = append(outcomes, start.GetRequestId()+":discard")
			continue
		}
		current := matches[0]
		afterByRequestID[start.GetRequestId()] = matches[1:]
		switch {
		case start.GetAttempt() == 0 && current.GetAttempt() > 0:
			outcomes = append(outcomes, start.GetRequestId()+":execute")
		case current.GetAttempt() == -1:
			outcomes = append(outcomes, start.GetRequestId()+":defer")
		case current.GetCompleted() != nil:
			outcomes = append(outcomes, start.GetRequestId()+":completed")
		case current.GetRunId() != "":
			outcomes = append(outcomes, start.GetRequestId()+":running")
		case current.GetAttempt() > 1:
			outcomes = append(outcomes, start.GetRequestId()+":retry")
		default:
			outcomes = append(outcomes, start.GetRequestId()+":retain")
		}
	}
	return outcomes
}

// A buffered start with an overlap policy to cancel other workflows is processed.
func TestProcessBufferTask_NeedsCancel(t *testing.T) {
	env := newTestEnv(t)

	// Add a running workflow to the Scheduler.
	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	// Set up the BufferedStart with a policy that will cancel existing workflows.
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:   bufferedStarts,
		InitialRunningWorkflows: initialRunningWorkflows,
		// Buffer should still contain the buffered start. The existing workflow will still
		// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
		// after termination/cancelation takes effect.
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedCancelWorkflows:  1,
	})
}
