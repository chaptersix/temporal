package scheduler_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

type (
	deliveryStage int

	deliveryModel struct {
		env            *schedulerModelEnv
		savedTasks     []tasks.Task
		observedStages map[deliveryStage]bool
	}
)

const (
	deliveryStageEligible deliveryStage = iota
	deliveryStageBuffered
	deliveryStageStarting
	deliveryStageRunning
	deliveryStageCompleted
	deliveryStageDropped
	deliveryStageStale
)

func TestSchedulerTaskInterleavingModel(t *testing.T) {
	policies := []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	}
	for _, policy := range policies {
		t.Run(policy.String(), func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				config := defaultModelEnvConfig()
				config.overlapPolicy = policy
				config.maxBufferSize = 10_000
				config.maxActions = 10_000
				model := &deliveryModel{
					env:            newSchedulerModelEnv(t, config),
					observedStages: make(map[deliveryStage]bool),
				}
				model.check(t)
				t.Repeat(map[string]func(*rapid.T){
					"deliver one task":      model.deliverOne,
					"drain runnable tasks":  model.drain,
					"advance intervals":     model.advanceIntervals,
					"advance to next timer": model.advanceToNextTimer,
					"trigger immediately":   model.triggerImmediately,
					"range backfill":        model.rangeBackfill,
					"toggle pause":          model.togglePause,
					"retry workflow start":  model.retryWorkflowStart,
					"complete workflow":     model.completeWorkflow,
					"redeliver stale task":  model.redeliverStaleTask,
					"query APIs":            model.queryAPIs,
					"reload execution":      model.reload,
					"":                      model.check,
				})
			})
		})
	}
}

func (m *deliveryModel) reload(t *rapid.T) {
	m.env.reload(t)
	m.check(t)
}

func TestSchedulerTerminalTaskInterleavingModel(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = time.Minute
			config.specEnd = config.startTime.Add(-config.interval)
			config.idleTime = time.Duration(rapid.IntRange(1, 5).Draw(t, "idle minutes")) * time.Minute
			env := newSchedulerModelEnv(t, config)
			task := firstQueuedModelTask(t, env)
			env.timeSource.Update(task.GetVisibilityTime())
			result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, task)
			mustNoError(t, err)
			if result.Dropped || !env.internal(t).closed {
				t.Fatalf("idle delivery: result=%+v closed=%v", result, env.internal(t).closed)
			}
			redelivery := env.redeliver(t, task)
			if !redelivery.Dropped || redelivery.Executed != 0 {
				t.Fatalf("idle task redelivery: %+v", redelivery)
			}
			checkClosedAPIs(t, env)
		})
	})

	t.Run("callback recovery", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			model := newCallbackRecoveryModel(t)
			model.recoverCallback(t)
			if len(model.savedTasks) != 0 {
				model.redeliverRecovery(t)
			}
			if model.state == callbackStateAttached {
				model.completeAttached(t)
			}
			model.check(t)
		})
	})

	t.Run("migration", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = 24 * time.Hour
			config.paused = true
			env := newSchedulerModelEnv(t, config)
			_, err := env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
				NamespaceId: namespaceID, ScheduleId: env.scheduleID,
			})
			mustNoError(t, err)
			task := env.selectRunnableTask(t)
			outcome := rapid.IntRange(0, 2).Draw(t, "migration outcome")
			switch outcome {
			case 0:
				env.history.pushMigrationError(nil)
			case 1:
				env.history.pushMigrationError(serviceerror.NewWorkflowExecutionAlreadyStarted(
					"migration already started", "", "",
				))
			case 2:
				injected := serviceerror.NewInternal("migration retry")
				env.history.pushMigrationError(injected)
				_, err = env.engine.ExecuteTask(env.engineCtx, env.ref, task)
				if !errors.Is(err, injected) || !slices.Contains(env.runnableTasks(t), task) {
					t.Fatalf("migration retry did not retain task: %v", err)
				}
				env.history.pushMigrationError(nil)
			default:
				t.Fatalf("unexpected migration outcome %d", outcome)
			}
			result, err := env.engine.ExecuteTask(env.engineCtx, env.ref, task)
			mustNoError(t, err)
			if result.Dropped || !env.internal(t).closed {
				t.Fatalf("migration terminal delivery: result=%+v closed=%v", result, env.internal(t).closed)
			}
			beforeCalls := len(env.history.snapshot().migrationStarts)
			redelivery := env.redeliver(t, task)
			if !redelivery.Dropped || len(env.history.snapshot().migrationStarts) != beforeCalls {
				t.Fatalf("migration redelivery: result=%+v calls=%d/%d", redelivery,
					len(env.history.snapshot().migrationStarts), beforeCalls)
			}
			checkClosedAPIs(t, env)
		})
	})

	t.Run("delete", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			config := defaultModelEnvConfig()
			config.interval = time.Duration(rapid.IntRange(1, 5).Draw(t, "interval minutes")) * time.Minute
			env := newSchedulerModelEnv(t, config)
			task := firstQueuedModelTask(t, env)
			_, err := env.handler.DeleteSchedule(env.engineCtx, &schedulerpb.DeleteScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.DeleteScheduleRequest{
					Namespace: namespace, ScheduleId: env.scheduleID,
				},
			})
			mustNoError(t, err)
			if !env.internal(t).closed {
				t.Fatal("delete did not close schedule")
			}
			env.timeSource.Update(task.GetVisibilityTime())
			result := env.redeliver(t, task)
			if !result.Dropped || result.Executed != 0 {
				t.Fatalf("pre-delete task redelivery: %+v", result)
			}
			checkClosedAPIs(t, env)
		})
	})
}

func firstQueuedModelTask(t *rapid.T, env *schedulerModelEnv) tasks.Task {
	t.Helper()
	queued, err := env.engine.Tasks(env.ref)
	mustNoError(t, err)
	var candidates []tasks.Task
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility {
			continue
		}
		candidates = append(candidates, categoryTasks...)
	}
	if len(candidates) == 0 {
		t.Fatal("expected a queued task")
	}
	slices.SortFunc(candidates, func(a, b tasks.Task) int {
		return a.GetVisibilityTime().Compare(b.GetVisibilityTime())
	})
	return candidates[0]
}

func (m *deliveryModel) deliverOne(t *rapid.T) {
	runnable := m.env.runnableTasks(t)
	if len(runnable) == 0 {
		t.Skip("no runnable tasks")
	}
	task := runnable[rapid.IntRange(0, len(runnable)-1).Draw(t, "runnable task")]
	result, err := m.env.engine.ExecuteTask(m.env.engineCtx, m.env.ref, task)
	mustNoError(t, err)
	if result.Dropped {
		m.observedStages[deliveryStageDropped] = true
	} else {
		m.observedStages[deliveryStageStarting] = true
	}
	if _, ok := task.(*tasks.ChasmTask); ok {
		m.savedTasks = append(m.savedTasks, task)
	}
	m.check(t)
}

func (m *deliveryModel) drain(t *rapid.T) {
	m.env.drain(t)
	m.check(t)
}

func (m *deliveryModel) advanceIntervals(t *rapid.T) {
	count := rapid.IntRange(1, 3).Draw(t, "interval count")
	m.env.timeSource.Update(m.env.timeSource.Now().Add(time.Duration(count) * m.env.config.interval))
	m.observedStages[deliveryStageEligible] = true
	m.check(t)
}

func (m *deliveryModel) advanceToNextTimer(t *rapid.T) {
	m.env.advanceToNextTask(t)
	m.observedStages[deliveryStageEligible] = true
	m.check(t)
}

func (m *deliveryModel) triggerImmediately(t *rapid.T) {
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID,
			Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
				OverlapPolicy: m.env.config.overlapPolicy,
			}},
		},
	})
	mustNoError(t, err)
	m.observedStages[deliveryStageBuffered] = true
	m.check(t)
}

func (m *deliveryModel) rangeBackfill(t *rapid.T) {
	intervals := rapid.IntRange(0, 3).Draw(t, "backfill intervals")
	end := m.env.timeSource.Now()
	start := end.Add(-time.Duration(intervals) * m.env.config.interval)
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID,
			Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
				StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
				OverlapPolicy: m.env.config.overlapPolicy,
			}}},
		},
	})
	mustNoError(t, err)
	m.observedStages[deliveryStageEligible] = true
	m.check(t)
}

func (m *deliveryModel) togglePause(t *rapid.T) {
	paused := m.env.describe(t).GetSchedule().GetState().GetPaused()
	patch := &schedulepb.SchedulePatch{}
	if paused {
		patch.Unpause = "delivery model unpause"
	} else {
		patch.Pause = "delivery model pause"
	}
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID, Patch: patch,
		},
	})
	mustNoError(t, err)
	m.check(t)
}

func (m *deliveryModel) retryWorkflowStart(t *rapid.T) {
	m.env.drain(t)
	if len(m.env.runningRequestIDs(t)) != 0 {
		t.Skip("running workflow would change overlap processing")
	}
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID,
			Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}},
		},
	})
	mustNoError(t, err)
	var sideEffects []tasks.Task
	for _, task := range m.env.runnableTasks(t) {
		if _, ok := task.(*tasks.ChasmTask); ok {
			sideEffects = append(sideEffects, task)
		}
	}
	if len(sideEffects) == 0 {
		t.Fatal("trigger produced no workflow-start task")
	}
	m.env.workflows.pushStartError(serviceerror.NewDeadlineExceeded("interleaved start retry"))
	beforeCalls := len(m.env.workflows.snapshot().startCalls)
	task := sideEffects[rapid.IntRange(0, len(sideEffects)-1).Draw(t, "start task")]
	result, err := m.env.engine.ExecuteTask(m.env.engineCtx, m.env.ref, task)
	mustNoError(t, err)
	if result.Dropped || len(m.env.workflows.snapshot().startCalls) != beforeCalls+1 {
		t.Fatalf("retryable start delivery: result=%+v calls=%d/%d", result,
			len(m.env.workflows.snapshot().startCalls), beforeCalls+1)
	}
	m.savedTasks = append(m.savedTasks, task)
	m.observedStages[deliveryStageStarting] = true
	m.check(t)
}

func (m *deliveryModel) completeWorkflow(t *rapid.T) {
	running := m.env.runningRequestIDs(t)
	if len(running) == 0 {
		t.Skip("no running workflows")
	}
	requestID := running[rapid.IntRange(0, len(running)-1).Draw(t, "workflow")]
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}
	status := statuses[rapid.IntRange(0, len(statuses)-1).Draw(t, "status")]
	m.env.complete(t, requestID, status, []byte("delivery completion"))
	m.observedStages[deliveryStageCompleted] = true
	m.check(t)
}

func (m *deliveryModel) redeliverStaleTask(t *rapid.T) {
	if len(m.savedTasks) == 0 {
		t.Skip("no saved side-effect tasks")
	}
	task := m.savedTasks[rapid.IntRange(0, len(m.savedTasks)-1).Draw(t, "saved task")]
	beforeWorkflow := m.env.workflows.snapshot()
	beforeHistory := m.env.history.snapshot()
	result := m.env.redeliver(t, task)
	if !result.Dropped || result.Executed != 0 {
		t.Fatalf("stale redelivery: got %+v", result)
	}
	afterWorkflow := m.env.workflows.snapshot()
	afterHistory := m.env.history.snapshot()
	if len(beforeWorkflow.startCalls) != len(afterWorkflow.startCalls) ||
		len(beforeHistory.cancels) != len(afterHistory.cancels) ||
		len(beforeHistory.terminates) != len(afterHistory.terminates) {
		t.Fatal("stale task made a service call")
	}
	m.observedStages[deliveryStageStale] = true
	m.check(t)
}

func (m *deliveryModel) queryAPIs(t *rapid.T) {
	beforeTasks := m.env.tasks(t)
	beforeWorkflow := m.env.workflows.snapshot()
	beforeHistory := m.env.history.snapshot()
	before := m.env.timeSource.Now().Add(-3 * m.env.config.interval)
	after := m.env.timeSource.Now().Add(3 * m.env.config.interval)
	_, err := m.env.listMatching(t, before, after)
	mustNoError(t, err)
	_ = m.env.describe(t)
	if !slices.Equal(beforeTasks, m.env.tasks(t)) ||
		len(beforeWorkflow.startCalls) != len(m.env.workflows.snapshot().startCalls) ||
		len(beforeHistory.cancels) != len(m.env.history.snapshot().cancels) ||
		len(beforeHistory.terminates) != len(m.env.history.snapshot().terminates) {
		t.Fatal("read-only APIs changed scheduler work")
	}
	m.check(t)
}

func (m *deliveryModel) check(t *rapid.T) {
	t.Helper()
	internal := m.env.internal(t)
	if internal.closed || internal.sentinel || internal.migrationPending {
		t.Fatal("delivery model unexpectedly entered terminal state")
	}
	description := m.env.describe(t)
	info := description.GetInfo()
	workflow := m.env.workflows.snapshot()
	if info.GetActionCount() != int64(len(workflow.starts)) {
		t.Fatalf("action count: got %d, want %d successful starts", info.GetActionCount(), len(workflow.starts))
	}
	if len(workflow.startCalls) < len(workflow.starts) {
		t.Fatalf("workflow start calls=%d successful starts=%d", len(workflow.startCalls), len(workflow.starts))
	}

	internalRunning := make(map[string]struct{})
	internalRecent := make(map[string]enumspb.WorkflowExecutionStatus)
	buffered := int64(0)
	for _, start := range internal.buffered {
		if start.runID == "" {
			buffered++
		}
		switch {
		case start.completed != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
			m.observedStages[deliveryStageCompleted] = true
		case start.runID != "":
			m.observedStages[deliveryStageRunning] = true
		case start.attempt == -1:
			m.observedStages[deliveryStageBuffered] = true
		case start.attempt > 0:
			m.observedStages[deliveryStageStarting] = true
		default:
			m.observedStages[deliveryStageDropped] = true
		}
		if start.runID == "" {
			continue
		}
		status := start.completed
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
			status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			internalRunning[start.workflowID+"/"+start.runID] = struct{}{}
		}
		internalRecent[start.runID] = status
	}
	if info.GetBufferSize() != buffered {
		t.Fatalf("buffer size: got %d, want %d", info.GetBufferSize(), buffered)
	}

	visibleRunning := make(map[string]struct{})
	for _, running := range info.GetRunningWorkflows() {
		key := running.GetWorkflowId() + "/" + running.GetRunId()
		if _, duplicate := visibleRunning[key]; duplicate {
			t.Fatalf("duplicate visible running workflow %q", key)
		}
		visibleRunning[key] = struct{}{}
	}
	if !mapsEqual(visibleRunning, internalRunning) {
		t.Fatalf("running workflows: got %v, want %v", visibleRunning, internalRunning)
	}
	completedRecent := 0
	for _, recent := range info.GetRecentActions() {
		runID := recent.GetStartWorkflowResult().GetRunId()
		if status, ok := internalRecent[runID]; !ok || status != recent.GetStartWorkflowStatus() {
			t.Fatalf("recent action %q status %v missing from internal state %v", runID,
				recent.GetStartWorkflowStatus(), internalRecent)
		}
		if recent.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			completedRecent++
		}
	}
	if completedRecent > 10 {
		t.Fatalf("completed recent action limit exceeded: %d", completedRecent)
	}
	if info.GetOverlapSkipped() < 0 || info.GetBufferDropped() < 0 || info.GetMissedCatchupWindow() < 0 {
		t.Fatalf("negative scheduler counter: %+v", info)
	}

	for _, request := range workflow.startCalls {
		if request.GetRequestId() == "" || request.GetWorkflowId() == "" || len(request.GetCompletionCallbacks()) != 1 {
			t.Fatalf("invalid workflow start request: %+v", request)
		}
	}
	for _, request := range m.env.history.snapshot().cancels {
		if request.GetCancelRequest().GetWorkflowExecution().GetWorkflowId() == "" {
			t.Fatal("cancel request has no workflow ID")
		}
	}
	for _, request := range m.env.history.snapshot().terminates {
		if request.GetTerminateRequest().GetWorkflowExecution().GetWorkflowId() == "" {
			t.Fatal("terminate request has no workflow ID")
		}
	}
	m.env.checkRequestIDs(t)
}
