package scheduler_test

import (
	"slices"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

var modelBoundaryOffsets = []time.Duration{-time.Nanosecond, 0, time.Nanosecond}

func TestSchedulerTimeBoundaryModel(t *testing.T) {
	t.Run("schedule start and end", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = time.Minute
			config.specStart = config.startTime.Add(2 * config.interval)
			config.specEnd = config.startTime.Add(4 * config.interval)
			boundaries := []time.Time{config.specStart, config.specEnd}
			boundary := boundaries[rapid.IntRange(0, 1).Draw(t, "boundary")]
			offset := modelBoundaryOffsets[rapid.IntRange(0, 2).Draw(t, "offset")]
			target := boundary.Add(offset)
			env := newSchedulerModelEnv(t, config)
			env.timeSource.Update(target)
			env.drain(t)
			expected := expectedIntervalTimes(
				config.interval, config.phase, config.specStart, config.specEnd, config.startTime, target,
			)
			if len(env.workflows.snapshot().starts) != len(expected) {
				t.Fatalf("starts at %s%+v: got %d, want %d", boundary, offset,
					len(env.workflows.snapshot().starts), len(expected))
			}
		})
	})

	t.Run("catchup window", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = time.Minute
			if rapid.Bool().Draw(t, "zero catchup window") {
				config.catchupWindow = 0
			} else {
				config.catchupWindow = scheduler.DefaultTweakables.MinCatchupWindow
			}
			effectiveCatchup := max(config.catchupWindow, scheduler.DefaultTweakables.MinCatchupWindow)
			offset := modelBoundaryOffsets[rapid.IntRange(0, 2).Draw(t, "offset")]
			nominal := config.startTime.Add(config.interval)
			env := newSchedulerModelEnv(t, config)
			env.timeSource.Update(nominal.Add(effectiveCatchup).Add(offset))
			env.drain(t)
			missed := offset > 0
			starts := len(env.workflows.snapshot().starts)
			missedCount := env.describe(t).GetInfo().GetMissedCatchupWindow()
			if missed && (starts != 0 || missedCount != 1) {
				t.Fatalf("late catchup boundary: starts=%d missed=%d", starts, missedCount)
			}
			if !missed && (starts != 1 || missedCount != 0) {
				t.Fatalf("eligible catchup boundary: starts=%d missed=%d", starts, missedCount)
			}
		})
	})

	t.Run("retry backoff", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			env := newSchedulerModelEnv(t, config)
			env.workflows.pushStartError(serviceerror.NewDeadlineExceeded("retry boundary"))
			triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			_, result := env.executeOne(t)
			if result.Dropped {
				t.Fatalf("retryable start task was dropped")
			}
			internal := env.internal(t)
			if len(internal.buffered) != 1 || internal.buffered[0].backoffTime.IsZero() {
				t.Fatalf("retry did not set a backoff: %+v", internal.buffered)
			}
			offset := modelBoundaryOffsets[rapid.IntRange(0, 2).Draw(t, "offset")]
			env.timeSource.Update(internal.buffered[0].backoffTime.Add(offset))
			if offset < 0 {
				if len(env.runnableTasks(t)) != 0 {
					t.Fatalf("retry runnable before backoff")
				}
				return
			}
			env.workflows.pushStartError(nil)
			env.drain(t)
			if len(env.workflows.snapshot().starts) != 1 {
				t.Fatalf("retry did not run at backoff boundary")
			}
		})
	})

	t.Run("idle closure", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = time.Minute
			config.specEnd = config.startTime.Add(-config.interval)
			config.idleTime = time.Minute
			env := newSchedulerModelEnv(t, config)
			offset := modelBoundaryOffsets[rapid.IntRange(0, 2).Draw(t, "offset")]
			env.timeSource.Update(config.startTime.Add(config.idleTime).Add(offset))
			env.drain(t)
			if env.internal(t).closed != (offset >= 0) {
				t.Fatalf("idle closed=%v at offset %v", env.internal(t).closed, offset)
			}
		})
	})

	t.Run("sentinel expiry", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			env := newSentinelModelEnv(t)
			deadline := env.internal(t).createTime.Add(scheduler.SentinelIdleTime)
			offset := modelBoundaryOffsets[rapid.IntRange(0, 2).Draw(t, "offset")]
			env.timeSource.Update(deadline.Add(offset))
			env.drain(t)
			if env.internal(t).closed != (offset >= 0) {
				t.Fatalf("sentinel closed=%v at offset %v", env.internal(t).closed, offset)
			}
		})
	})
}

func TestSchedulerCardinalityBoundaryModel(t *testing.T) {
	t.Run("attempts", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			target := []int64{9, 10, 11}[rapid.IntRange(0, 2).Draw(t, "attempt")]
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			env := newSchedulerModelEnv(t, config)
			env.workflows.pushStartError(serviceerror.NewDeadlineExceeded("attempt boundary"))
			triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			env.executeOne(t)
			for {
				internal := env.internal(t)
				if len(internal.buffered) == 0 {
					if target != 11 {
						t.Fatalf("start dropped before attempt %d", target)
					}
					break
				}
				attempt := internal.buffered[0].attempt
				if attempt == target {
					break
				}
				if attempt == 10 && target == 11 {
					env.timeSource.Update(internal.buffered[0].backoffTime)
					env.executeOne(t)
					env.executeOne(t)
					continue
				}
				env.workflows.pushStartError(serviceerror.NewDeadlineExceeded("attempt boundary"))
				env.timeSource.Update(internal.buffered[0].backoffTime)
				env.executeOne(t)
				env.executeOne(t)
			}
			calls := len(env.workflows.snapshot().startCalls)
			expectedCalls := int(min(target-1, 9))
			if calls != expectedCalls {
				t.Fatalf("attempt %d made %d calls, want %d", target, calls, expectedCalls)
			}
		})
	})

	t.Run("recent actions", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			count := []int{9, 10, 11}[rapid.IntRange(0, 2).Draw(t, "actions")]
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			config.maxActions = 100
			env := newSchedulerModelEnv(t, config)
			for i := 0; i < count; i++ {
				triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
				env.drain(t)
				running := env.runningRequestIDs(t)
				requestID := running[len(running)-1]
				env.complete(t, requestID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte(requestID))
				env.drain(t)
			}
			description := env.describe(t)
			if got := len(description.GetInfo().GetRecentActions()); got != min(count, 10) {
				t.Fatalf("recent actions: got %d, want %d", got, min(count, 10))
			}
			if description.GetInfo().GetActionCount() != int64(count) {
				t.Fatalf("action count: got %d, want %d", description.GetInfo().GetActionCount(), count)
			}
		})
	})

	t.Run("remaining actions and manual work", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			remaining := int64(rapid.IntRange(0, 2).Draw(t, "remaining actions"))
			config := defaultModelEnvConfig()
			config.interval = time.Minute
			config.limitedActions = true
			config.remainingAction = remaining
			env := newSchedulerModelEnv(t, config)
			env.timeSource.Update(config.startTime.Add(3 * config.interval))
			env.drain(t)
			if len(env.workflows.snapshot().starts) != int(remaining) ||
				env.describe(t).GetSchedule().GetState().GetRemainingActions() != 0 {
				t.Fatalf("scheduled action limit was not enforced")
			}
			triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			env.drain(t)
			if len(env.workflows.snapshot().starts) != int(remaining)+1 ||
				env.describe(t).GetSchedule().GetState().GetRemainingActions() != 0 {
				t.Fatalf("manual action changed or obeyed exhausted scheduled limit")
			}
		})
	})

	t.Run("buffer capacity and generator reserve", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.paused = true
			config.limitedActions = true
			config.remainingAction = 0
			config.maxBufferSize = 20
			config.generatorReserve = rapid.IntRange(0, 2).Draw(t, "generator reserve")
			capacity := config.maxBufferSize/2 - config.generatorReserve
			requested := capacity + []int{-1, 0, 1}[rapid.IntRange(0, 2).Draw(t, "capacity offset")]
			env := newSchedulerModelEnv(t, config)
			end := env.timeSource.Now()
			start := end.Add(-time.Duration(requested-1) * config.interval)
			patchModelBackfill(t, env, start, end, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			env.drain(t)
			if len(env.workflows.snapshot().starts) != min(requested, capacity) {
				t.Fatalf("capacity starts: got %d, want %d", len(env.workflows.snapshot().starts),
					min(requested, capacity))
			}
		})
	})

	t.Run("concurrent backfillers", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.paused = true
			config.limitedActions = true
			config.remainingAction = 0
			config.maxBufferSize = 40
			config.generatorReserve = 2
			capacity := config.maxBufferSize/2 - config.generatorReserve
			requested := capacity * 2
			env := newSchedulerModelEnv(t, config)
			end := env.timeSource.Now()
			patchModelBackfill(t, env, end.Add(-time.Duration(requested-1)*config.interval), end,
				enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			secondEnd := end.Add(-time.Duration(requested) * config.interval)
			patchModelBackfill(t, env, secondEnd.Add(-time.Duration(requested-1)*config.interval), secondEnd,
				enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			if env.internal(t).backfillers != 2 {
				t.Fatalf("concurrent backfillers: got %d, want 2", env.internal(t).backfillers)
			}
			env.drain(t)
			if len(env.workflows.snapshot().starts) != capacity || env.internal(t).backfillers != 2 {
				t.Fatalf("concurrent backfill progress: starts=%d backfillers=%d",
					len(env.workflows.snapshot().starts), env.internal(t).backfillers)
			}
		})
	})

	t.Run("shared task visibility", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			count := rapid.IntRange(2, 6).Draw(t, "task count")
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			config.maxActions = 100
			env := newSchedulerModelEnv(t, config)
			for i := 0; i < count; i++ {
				triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			}
			var runnable []tasks.Task
			for _, task := range env.runnableTasks(t) {
				if _, ok := task.(*tasks.ChasmTask); ok {
					runnable = append(runnable, task)
				}
			}
			if len(runnable) < count {
				t.Fatalf("same-time runnable tasks: got %d, want at least %d", len(runnable), count)
			}
			visibility := runnable[0].GetVisibilityTime()
			for _, task := range runnable[1:] {
				if !task.GetVisibilityTime().Equal(visibility) {
					t.Fatalf("task visibility: got %s, want %s", task.GetVisibilityTime(), visibility)
				}
			}
			env.drain(t)
			if len(env.workflows.snapshot().starts) != count {
				t.Fatalf("same-time starts: got %d, want %d", len(env.workflows.snapshot().starts), count)
			}
		})
	})
}

func TestSchedulerCrossFeatureFailureModel(t *testing.T) {
	t.Run("start outcomes", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			outcome := rapid.IntRange(0, 5).Draw(t, "start outcome")
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			env := newSchedulerModelEnv(t, config)
			env.workflows.pushStartError(modelServiceOutcomeError(outcome))
			triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			env.executeOne(t)
			workflow := env.workflows.snapshot()
			if len(workflow.startCalls) != 1 {
				t.Fatalf("start outcome made %d calls", len(workflow.startCalls))
			}
			internal := env.internal(t)
			switch outcome {
			case 0:
				if len(workflow.starts) != 1 {
					t.Fatalf("successful start was not recorded")
				}
			case 1, 2:
				if len(internal.buffered) != 1 || internal.buffered[0].backoffTime.IsZero() {
					t.Fatalf("retryable start was not retained: %+v", internal.buffered)
				}
			case 3, 4, 5:
				if len(internal.buffered) != 0 || len(workflow.starts) != 0 {
					t.Fatalf("non-retryable start was retained: %+v", internal.buffered)
				}
			}
		})
	})

	t.Run("describe outcomes", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			outcome := rapid.IntRange(0, 5).Draw(t, "describe outcome")
			model := newCallbackRecoveryModel(t)
			task := callbackModelTask(t, model.env)
			err := modelServiceOutcomeError(outcome)
			if outcome == 0 {
				model.env.history.pushDescribeOutcome(callbackDescribeResponse(
					model.workflowID, model.runID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Time{},
				), nil)
				model.env.workflows.pushAttachError(nil)
			} else {
				model.env.history.pushDescribeOutcome(nil, err)
			}
			result, executeErr := model.env.engine.ExecuteTask(model.env.engineCtx, model.env.ref, task)
			if outcome == 0 || outcome == 5 {
				mustNoError(t, executeErr)
				if result.Dropped {
					t.Fatalf("terminal describe outcome dropped recovery task")
				}
				return
			}
			if executeErr == nil || !slices.Contains(model.env.runnableTasks(t), task) {
				t.Fatalf("describe failure did not retain task: %v", executeErr)
			}
			model.env.history.pushDescribeOutcome(callbackDescribeResponse(
				model.workflowID, model.runID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Time{},
			), nil)
			model.env.workflows.pushAttachError(nil)
			_, executeErr = model.env.engine.ExecuteTask(model.env.engineCtx, model.env.ref, task)
			mustNoError(t, executeErr)
		})
	})

	t.Run("attach outcomes", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			outcome := rapid.IntRange(0, 5).Draw(t, "attach outcome")
			model := newCallbackRecoveryModel(t)
			task := callbackModelTask(t, model.env)
			model.env.history.pushDescribeOutcome(callbackDescribeResponse(
				model.workflowID, model.runID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Time{},
			), nil)
			model.env.workflows.pushAttachError(modelServiceOutcomeError(outcome))
			result, executeErr := model.env.engine.ExecuteTask(model.env.engineCtx, model.env.ref, task)
			if outcome == 0 || outcome == 4 {
				mustNoError(t, executeErr)
				if result.Dropped {
					t.Fatalf("terminal attach outcome dropped recovery task")
				}
				return
			}
			if executeErr == nil || !slices.Contains(model.env.runnableTasks(t), task) {
				t.Fatalf("attach failure did not retain task: %v", executeErr)
			}
			model.env.history.pushDescribeOutcome(callbackDescribeResponse(
				model.workflowID, model.runID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Time{},
			), nil)
			model.env.workflows.pushAttachError(nil)
			_, executeErr = model.env.engine.ExecuteTask(model.env.engineCtx, model.env.ref, task)
			mustNoError(t, executeErr)
		})
	})

	t.Run("update during backfill", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = time.Minute
			env := newSchedulerModelEnv(t, config)
			patchModelBackfill(t, env, config.startTime.Add(-2*config.interval), config.startTime,
				enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			description := env.describe(t)
			updated := modelSchedule(config)
			updated.State.Notes = "updated during backfill"
			_, err := env.handler.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.UpdateScheduleRequest{
					Namespace: namespace, ScheduleId: env.scheduleID, Schedule: updated,
					ConflictToken: slices.Clone(description.GetConflictToken()),
				},
			})
			mustNoError(t, err)
			env.drain(t)
			if env.describe(t).GetSchedule().GetState().GetNotes() != "updated during backfill" {
				t.Fatalf("update was lost during backfill")
			}
			env.checkRequestIDs(t)
		})
	})

	t.Run("pause with buffered retry", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			env := newSchedulerModelEnv(t, config)
			env.workflows.pushStartError(serviceerror.NewDeadlineExceeded("pause retry"))
			triggerModelAction(t, env, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
			env.executeOne(t)
			_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Namespace: namespace, ScheduleId: env.scheduleID,
					Patch: &schedulepb.SchedulePatch{Pause: "pause with retry"},
				},
			})
			mustNoError(t, err)
			backoff := env.internal(t).buffered[0].backoffTime
			env.timeSource.Update(backoff)
			env.workflows.pushStartError(nil)
			env.drain(t)
			if len(env.workflows.snapshot().starts) != 1 || !env.describe(t).GetSchedule().GetState().GetPaused() {
				t.Fatalf("paused buffered retry did not finish")
			}
		})
	})

	t.Run("cancel then completion", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			config.overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
			env := newSchedulerModelEnv(t, config)
			triggerModelAction(t, env, config.overlapPolicy)
			env.drain(t)
			first := env.runningRequestIDs(t)[0]
			outcome := modelServiceOutcomeError(rapid.IntRange(0, 5).Draw(t, "cancel outcome"))
			env.history.cancelOutcomes = append(env.history.cancelOutcomes, outcome)
			triggerModelAction(t, env, config.overlapPolicy)
			env.drain(t)
			if len(env.history.snapshot().cancels) != 1 {
				t.Fatalf("cancel overlap made %d calls", len(env.history.snapshot().cancels))
			}
			env.complete(t, first, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, []byte("canceled"))
			env.drain(t)
			if len(env.workflows.snapshot().starts) != 2 {
				t.Fatalf("completion did not release canceled overlap")
			}
		})
	})

	t.Run("terminate then completion", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			config.overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
			env := newSchedulerModelEnv(t, config)
			triggerModelAction(t, env, config.overlapPolicy)
			env.drain(t)
			first := env.runningRequestIDs(t)[0]
			outcome := modelServiceOutcomeError(rapid.IntRange(0, 5).Draw(t, "terminate outcome"))
			env.history.terminateOutcomes = append(env.history.terminateOutcomes, outcome)
			triggerModelAction(t, env, config.overlapPolicy)
			env.drain(t)
			if len(env.history.snapshot().terminates) != 1 {
				t.Fatalf("terminate overlap made %d calls", len(env.history.snapshot().terminates))
			}
			env.complete(t, first, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, []byte("terminated"))
			env.drain(t)
			if len(env.workflows.snapshot().starts) != 2 {
				t.Fatalf("completion did not release terminated overlap")
			}
		})
	})

	t.Run("migration with running and buffered work", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			outcome := rapid.IntRange(0, 5).Draw(t, "migration outcome")
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			config.overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
			env := newSchedulerModelEnv(t, config)
			triggerModelAction(t, env, config.overlapPolicy)
			env.drain(t)
			triggerModelAction(t, env, config.overlapPolicy)
			env.drain(t)
			if len(env.runningRequestIDs(t)) != 1 || env.describe(t).GetInfo().GetBufferSize() != 1 {
				t.Fatalf("migration setup lacks running and buffered work")
			}
			_, err := env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
				NamespaceId: namespaceID, ScheduleId: env.scheduleID,
			})
			mustNoError(t, err)
			task := callbackModelTask(t, env)
			env.history.pushMigrationError(modelServiceOutcomeError(outcome))
			result, executeErr := env.engine.ExecuteTask(env.engineCtx, env.ref, task)
			if outcome == 0 || outcome == 4 {
				mustNoError(t, executeErr)
				if result.Dropped || !env.internal(t).closed {
					t.Fatalf("terminal migration outcome: result=%+v closed=%v", result, env.internal(t).closed)
				}
				return
			}
			if executeErr == nil || !slices.Contains(env.runnableTasks(t), task) {
				t.Fatalf("migration failure did not retain task: %v", executeErr)
			}
			env.history.pushMigrationError(nil)
			_, executeErr = env.engine.ExecuteTask(env.engineCtx, env.ref, task)
			mustNoError(t, executeErr)
			if !env.internal(t).closed {
				t.Fatalf("migration retry did not close schedule")
			}
		})
	})

	t.Run("delete during callback recovery", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			model := newCallbackRecoveryModel(t)
			var task tasks.Task
			for _, candidate := range model.env.runnableTasks(t) {
				if _, ok := candidate.(*tasks.ChasmTask); ok {
					task = candidate
					break
				}
			}
			if task == nil {
				t.Fatalf("callback recovery has no side-effect task")
			}
			_, err := model.env.handler.DeleteSchedule(model.env.engineCtx, &schedulerpb.DeleteScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.DeleteScheduleRequest{
					Namespace: namespace, ScheduleId: model.env.scheduleID,
				},
			})
			mustNoError(t, err)
			result := model.env.redeliver(t, task)
			if !result.Dropped || len(model.env.history.snapshot().describes) != 0 ||
				len(model.env.workflows.snapshot().startCalls) != 0 {
				t.Fatalf("deleted callback recovery task: %+v", result)
			}
			checkClosedAPIs(t, model.env)
		})
	})
}

func triggerModelAction(t *rapid.T, env *schedulerModelEnv, policy enumspb.ScheduleOverlapPolicy) {
	t.Helper()
	_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: env.scheduleID,
			Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
				OverlapPolicy: policy,
			}},
		},
	})
	mustNoError(t, err)
}

func patchModelBackfill(
	t *rapid.T,
	env *schedulerModelEnv,
	start time.Time,
	end time.Time,
	policy enumspb.ScheduleOverlapPolicy,
) {
	t.Helper()
	_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: env.scheduleID,
			Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
				StartTime: timestamppb.New(start), EndTime: timestamppb.New(end), OverlapPolicy: policy,
			}}},
		},
	})
	mustNoError(t, err)
}

func modelServiceOutcomeError(outcome int) error {
	switch outcome {
	case 0:
		return nil
	case 1:
		return serviceerror.NewDeadlineExceeded("retryable failure")
	case 2:
		return scheduler.NewTestRateLimitedError(time.Second)
	case 3:
		return serviceerror.NewInvalidArgument("non-retryable failure")
	case 4:
		return serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", "")
	case 5:
		return serviceerror.NewNotFound("not found")
	default:
		panic("invalid service outcome")
	}
}

func callbackModelTask(t *rapid.T, env *schedulerModelEnv) tasks.Task {
	t.Helper()
	for _, task := range env.runnableTasks(t) {
		if _, ok := task.(*tasks.ChasmTask); ok {
			return task
		}
	}
	t.Fatalf("expected a callback or migration side-effect task")
	return nil
}
