package scheduler_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/tasks"
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
			if c.name == "scheduled equal to LPT is stale" {
				currentLastProcessedAt = timestamppb.New(scheduledTime)
			} else if c.name == "scheduled before LPT is stale" {
				currentLastProcessedAt = timestamppb.New(scheduledTime.Add(time.Second))
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
// slot. Regression for the order-of-checks bug where useScheduledAction(true)
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
