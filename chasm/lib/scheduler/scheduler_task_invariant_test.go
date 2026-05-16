package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// singleShotSchedule returns a schedule with a one-minute interval and
// LimitedActions=true, RemainingActions=1. After the single action is consumed,
// the schedule has no more work but the spec still produces future times.
func singleShotSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(defaultInterval)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "scheduled-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{},
		State: &schedulepb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 1,
		},
	}
}

// emptySpecSchedule returns a schedule with no intervals or calendar entries.
// The spec produces no future scheduled actions, so the scheduler goes idle
// immediately after the first generator task fires.
func emptySpecSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "scheduled-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{},
		State:    &schedulepb.ScheduleState{},
	}
}

// TestPatchTriggerImmediately_IdleSchedule_LosesAllTasks reproduces a bug where
// Patch(TriggerImmediately) on an idle schedule invalidates the pending idle task
// (by updating updateTime, which shifts the idle expiration) but does not re-kick
// the generator. After the backfiller completes and the idle task is dropped, the
// schedule has neither a generator task nor a valid idle task and is stuck forever.
//
// Compare with Update(), which correctly calls generator.Generate(ctx) after any
// mutation.
func TestPatchTriggerImmediately_IdleSchedule_LosesAllTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	ts := clock.NewEventTimeSource()
	ts.Update(time.Now().Truncate(time.Second))

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	testEngine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)

	executionKey := chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	}

	// Step 1: Create a scheduler with an empty spec (no future actions).
	// During StartExecution's CloseTransaction, the initial GeneratorTask fires
	// inline, sees zero NextWakeupTime, and creates an idle task scheduled at
	// createTime + idleTime.
	createTime := ts.Now()
	_, err := chasm.StartExecution(
		engineCtx,
		executionKey,
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, emptySpecSchedule(), nil)
		},
		struct{}{},
	)
	require.NoError(t, err)

	// Step 2: Advance time so the Patch's updateTime > createTime.
	// This ensures the idle expiration shifts after Patch, making the old idle
	// task stale.
	ts.Update(createTime.Add(time.Minute))

	// Step 3: Patch with TriggerImmediately.
	// - handlePatch creates a backfiller (fires inline during CloseTransaction)
	// - updateTime is set to the advanced time
	// - BUG: Patch does NOT call generator.Generate(ctx) for non-unpause patches
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](executionKey)
	_, _, err = chasm.UpdateComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			_, patchErr := s.Patch(ctx, &schedulerpb.PatchScheduleRequest{
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Patch: &schedulepb.SchedulePatch{
						TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
					},
				},
			})
			return nil, patchErr
		},
		struct{}{},
	)
	require.NoError(t, err)

	// Step 4: Read the scheduler component pointer for ExecutePureTask.
	sched, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (*scheduler.Scheduler, error) {
			return s, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	// Step 5: Fire the idle task with the original scheduled time.
	// The idle task was created at createTime + idleTime. After Patch set
	// updateTime to createTime + 1min, the idle expiration shifted to
	// (createTime + 1min) + idleTime. The scheduled time no longer matches,
	// so Validate drops the task.
	idleTime := scheduler.DefaultTweakables.IdleTime
	originalIdleScheduledTime := createTime.Add(idleTime)

	idleHandler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config: defaultConfig(),
	})
	dropped, err := chasmtest.ExecutePureTask(
		context.Background(),
		testEngine,
		sched,
		idleHandler,
		chasm.TaskAttributes{ScheduledTime: originalIdleScheduledTime},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTime)},
	)
	require.NoError(t, err)
	require.True(t, dropped, "idle task should be stale: Patch updated updateTime, shifting idle expiration")

	// Step 6: Verify the schedule is stuck.
	// The schedule is running (not closed, not paused) but has no pending
	// generator task or valid idle task. It will never fire another action
	// or be cleaned up.
	require.False(t, sched.Closed, "schedule should still be open")
	require.False(t, sched.Schedule.State.Paused, "schedule should not be paused")

	// Verify that kicking the generator manually would create a new idle task,
	// proving Patch should have done this but didn't.
	generator, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (*scheduler.Generator, error) {
			return s.Generator.Get(ctx), nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	genHandler := scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  newRealSpecProcessor(ctrl, logger),
		SpecBuilder:    legacyscheduler.NewSpecBuilder(),
	})
	dropped, err = chasmtest.ExecutePureTask(
		context.Background(),
		testEngine,
		generator,
		genHandler,
		chasm.TaskAttributes{},
		&schedulerpb.GeneratorTask{},
	)
	require.NoError(t, err)
	require.False(t, dropped, "manually kicked generator task should execute (proving the fix path)")
}

// TestPatchBackfillRequest_IdleSchedule_LosesAllTasks is the same bug as
// TriggerImmediately but via a BackfillRequest patch.
func TestPatchBackfillRequest_IdleSchedule_LosesAllTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	ts := clock.NewEventTimeSource()
	ts.Update(time.Now().Truncate(time.Second))

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	testEngine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)

	executionKey := chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	}

	createTime := ts.Now()
	_, err := chasm.StartExecution(
		engineCtx,
		executionKey,
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, emptySpecSchedule(), nil)
		},
		struct{}{},
	)
	require.NoError(t, err)

	ts.Update(createTime.Add(time.Minute))

	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](executionKey)
	_, _, err = chasm.UpdateComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			_, patchErr := s.Patch(ctx, &schedulerpb.PatchScheduleRequest{
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Patch: &schedulepb.SchedulePatch{
						BackfillRequest: []*schedulepb.BackfillRequest{
							{
								StartTime: timestamppb.New(createTime.Add(-time.Hour)),
								EndTime:   timestamppb.New(createTime),
							},
						},
					},
				},
			})
			return nil, patchErr
		},
		struct{}{},
	)
	require.NoError(t, err)

	sched, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (*scheduler.Scheduler, error) {
			return s, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	idleTime := scheduler.DefaultTweakables.IdleTime
	originalIdleScheduledTime := createTime.Add(idleTime)

	idleHandler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config: defaultConfig(),
	})
	dropped, err := chasmtest.ExecutePureTask(
		context.Background(),
		testEngine,
		sched,
		idleHandler,
		chasm.TaskAttributes{ScheduledTime: originalIdleScheduledTime},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTime)},
	)
	require.NoError(t, err)
	require.True(t, dropped, "idle task should be stale after BackfillRequest patch updates updateTime")

	require.False(t, sched.Closed, "schedule should still be open")
	require.False(t, sched.Schedule.State.Paused, "schedule should not be paused")
}

// TestCreateFromMigration_NoRunningWorkflows_NoGeneratorTask reproduces a bug
// where CreateSchedulerFromMigration with no running workflows results in the
// SchedulerCallbacksTask self-invalidating (Validate returns false), which means
// generator.Generate(ctx) is never called and the schedule starts with no
// generator task or idle task.
func TestCreateFromMigration_NoRunningWorkflows_NoGeneratorTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	testEngine := chasmtest.NewEngine(t, registry)
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)

	executionKey := chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	}

	now := time.Now().Truncate(time.Second)

	// Create a scheduler from V1 migration state with no running workflows.
	// All BufferedStarts either have no RunId (waiting) or are already completed.
	// This is the common case: schedule fires hourly, workflow completes in 5min,
	// migration happens during the 55min idle window.
	_, err := chasm.StartExecution(
		engineCtx,
		executionKey,
		func(ctx chasm.MutableContext, req *schedulerpb.CreateFromMigrationStateRequest) (*scheduler.Scheduler, error) {
			return scheduler.CreateSchedulerFromMigration(ctx, req)
		},
		&schedulerpb.CreateFromMigrationStateRequest{
			NamespaceId: namespaceID,
			State: &schedulerpb.SchedulerMigrationState{
				SchedulerState: &schedulerpb.SchedulerState{
					Schedule:      defaultSchedule(),
					Info:          &schedulepb.ScheduleInfo{},
					Namespace:     namespace,
					NamespaceId:   namespaceID,
					ScheduleId:    scheduleID,
					ConflictToken: 42,
				},
				GeneratorState: &schedulerpb.GeneratorState{
					LastProcessedTime: timestamppb.New(now),
				},
				InvokerState: &schedulerpb.InvokerState{
					// No running workflows: all starts are completed.
					BufferedStarts: []*schedulespb.BufferedStart{
						{
							RequestId:   "req-1",
							WorkflowId:  "wf-1",
							RunId:       "run-1",
							HasCallback: true, // already has callback
							Completed: &schedulespb.CompletedResult{
								CloseTime: timestamppb.New(now.Add(-time.Minute)),
							},
						},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	// Read the scheduler to get the component pointer.
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](executionKey)
	sched, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (*scheduler.Scheduler, error) {
			return s, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	// Fire the SchedulerCallbacksTask. Since no starts need callbacks
	// (all have HasCallback=true or Completed!=nil), Validate returns false
	// and the task is dropped. This means generator.Generate(ctx) in Execute
	// is never called.
	callbacksHandler := scheduler.NewSchedulerCallbacksTaskHandler(
		scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config: defaultConfig(),
			// HistoryClient and FrontendClient are nil because Validate
			// returns false, so Execute (which uses them) never runs.
		},
	)
	dropped, err := chasmtest.ExecuteSideEffectTask(
		context.Background(),
		testEngine,
		sched,
		callbacksHandler,
		chasm.TaskAttributes{},
		&schedulerpb.SchedulerCallbacksTask{},
	)
	require.NoError(t, err)
	require.True(t, dropped, "callbacks task should be dropped: no starts need callbacks")

	// The schedule is now stuck: CreateSchedulerFromMigration intentionally
	// did not create generator tasks (to avoid overlap policy issues with stale
	// running workflow state), relying on SchedulerCallbacksTask.Execute to
	// kick the generator. But since Validate returned false, Execute never ran.
	require.False(t, sched.Closed, "schedule should still be open")
	require.False(t, sched.Schedule.State.Paused, "schedule should not be paused")
}

// TestExecuteTask_ShiftsIdleExpiration_InvalidatesIdleTask reproduces a bug
// where the InvokerExecuteTask (which starts workflows) sets RunId and StartTime
// on a BufferedStart. This causes recentActions() to include the action, which
// shifts getLastEventTime() and thus the idle expiration. The pending idle task
// (created by the generator before the workflow was started) becomes stale because
// its scheduled time no longer matches the new idle expiration. No replacement
// idle task is created, leaving the schedule stuck open forever.
//
// This is the production bug for single-shot schedules: the generator goes idle
// and creates an idle task at createTime + idleTime. Then ExecuteTask starts the
// workflow and sets StartTime > createTime. The idle expiration shifts to
// startTime + idleTime, but the pending idle task is at createTime + idleTime.
func TestExecuteTask_ShiftsIdleExpiration_InvalidatesIdleTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	ts := clock.NewEventTimeSource()
	createTime := time.Now().Truncate(time.Second)
	ts.Update(createTime)

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	testEngine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)

	executionKey := chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	}

	// Step 1: Create a single-shot schedule (LimitedActions=1, interval spec).
	// During StartExecution's CloseTransaction:
	// - GeneratorTask fires inline: buffers one action, spec has a next wakeup
	//   time so the generator schedules another generator task (remaining=1
	//   hasn't been decremented yet).
	// - InvokerProcessBufferTask fires inline: decrements remaining to 0, marks
	//   start ready.
	// - InvokerExecuteTask is persisted (side-effect).
	_, err := chasm.StartExecution(
		engineCtx,
		executionKey,
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, singleShotSchedule(), nil)
		},
		struct{}{},
	)
	require.NoError(t, err)

	// Step 2: Fire the second generator task. Now remaining=0, so:
	// - ProcessTimeRange returns early (useScheduledAction=false)
	// - The schedule is idle (nextWakeup non-zero but useScheduledAction=false)
	// - Idle task created at getLastEventTime() + idleTime
	// At this point, no workflow has been started yet (ExecuteTask hasn't run),
	// so recentActions() is empty and getLastEventTime()=createTime.
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](executionKey)
	generator, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (*scheduler.Generator, error) {
			return s.Generator.Get(ctx), nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	genHandler := scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  newRealSpecProcessor(ctrl, logger),
		SpecBuilder:    legacyscheduler.NewSpecBuilder(),
	})

	// The second generator task was scheduled at the next interval time.
	// Advance time to that point and fire it.
	ts.Update(createTime.Add(defaultInterval))
	dropped, err := chasmtest.ExecutePureTask(
		context.Background(),
		testEngine,
		generator,
		genHandler,
		chasm.TaskAttributes{ScheduledTime: createTime.Add(defaultInterval)},
		&schedulerpb.GeneratorTask{},
	)
	require.NoError(t, err)
	require.False(t, dropped, "second generator task should fire (remaining=0 -> idle)")

	// Record the idle task's scheduled time. At this point getLastEventTime()
	// returns createTime (no recentActions yet), so idle expiration =
	// createTime + idleTime.
	idleTime := scheduler.DefaultTweakables.IdleTime
	originalIdleScheduledTime := createTime.Add(idleTime)

	// Step 3: Simulate InvokerExecuteTask by setting RunId and StartTime on
	// the BufferedStart. This is what happens when the side-effect task starts
	// the workflow. The key mutation: start.StartTime is set, which makes
	// recentActions() include this action, shifting getLastEventTime().
	workflowStartTime := createTime.Add(100 * time.Millisecond)
	_, _, err = chasm.UpdateComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			invoker := s.Invoker.Get(ctx)
			for _, start := range invoker.BufferedStarts {
				if start.GetRunId() == "" && start.Attempt > 0 {
					start.RunId = "run-id-1"
					start.StartTime = timestamppb.New(workflowStartTime)
				}
			}
			return nil, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	// Step 4: Fire the idle task at the original scheduled time.
	// getLastEventTime() now returns workflowStartTime (from recentActions),
	// so idleExpiration = workflowStartTime + idleTime != createTime + idleTime.
	// Validate drops the task.
	sched, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (*scheduler.Scheduler, error) {
			return s, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	idleHandler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config: defaultConfig(),
	})
	dropped, err = chasmtest.ExecutePureTask(
		context.Background(),
		testEngine,
		sched,
		idleHandler,
		chasm.TaskAttributes{ScheduledTime: originalIdleScheduledTime},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(idleTime)},
	)
	require.NoError(t, err)
	require.True(t, dropped,
		"idle task should be stale: ExecuteTask set StartTime on the BufferedStart, "+
			"which shifted getLastEventTime() and thus the idle expiration")

	// The schedule is stuck: not closed, not paused, no valid idle task,
	// no generator task.
	require.False(t, sched.Closed, "schedule should still be open")
	require.False(t, sched.Schedule.State.Paused, "schedule should not be paused")
}
