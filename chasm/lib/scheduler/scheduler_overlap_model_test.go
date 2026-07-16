package scheduler_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

type (
	overlapModelItem struct {
		actualTime time.Time
		manual     bool
	}

	overlapModelAction struct {
		requestID  string
		workflowID string
		runID      string
		status     enumspb.WorkflowExecutionStatus
	}

	overlapModel struct {
		env              *schedulerModelEnv
		policy           enumspb.ScheduleOverlapPolicy
		cursor           time.Time
		running          map[string]overlapModelAction
		completed        map[string]overlapModelAction
		completedOrder   []string
		deferred         []overlapModelItem
		actionCount      int64
		overlapSkipped   int64
		cancelTargets    []string
		terminateTargets []string
	}
)

func TestSchedulerOverlapBackfillModel(t *testing.T) {
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
				model := &overlapModel{
					env: newSchedulerModelEnv(t, config), policy: policy, cursor: config.startTime,
					running: make(map[string]overlapModelAction), completed: make(map[string]overlapModelAction),
				}
				model.check(t)
				t.Repeat(map[string]func(*rapid.T){
					"advance intervals":   model.advanceIntervals,
					"trigger immediately": model.triggerImmediately,
					"range backfill":      model.rangeBackfill,
					"complete workflow":   model.completeWorkflow,
					"":                    model.check,
				})
			})
		})
	}
}

func TestSchedulerBackfillCapacityModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		config := defaultModelEnvConfig()
		config.paused = true
		config.limitedActions = true
		config.remainingAction = 0
		config.maxBufferSize = rapid.IntRange(32, 60).Draw(t, "max buffer size")
		config.generatorReserve = rapid.IntRange(0, 5).Draw(t, "generator reserve")
		capacity := config.maxBufferSize/2 - config.generatorReserve
		requested := rapid.IntRange(1, capacity*2).Draw(t, "backfill actions")
		env := newSchedulerModelEnv(t, config)
		end := env.timeSource.Now()
		start := end.Add(-time.Duration(requested-1) * config.interval)
		expectedTimes := expectedIntervalTimes(
			config.interval, config.phase, config.specStart, config.specEnd,
			start.Add(-time.Nanosecond), end,
		)
		if len(expectedTimes) != requested {
			t.Fatalf("generated backfill times: got %d, want %d", len(expectedTimes), requested)
		}

		before := env.workflows.snapshot()
		_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace: namespace, ScheduleId: scheduleID,
				Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}}},
			},
		})
		mustNoError(t, err)

		remaining := requested
		attempt := int64(0)
		backfillActive := true
		var actualTimes []time.Time
		for backfillActive {
			for {
				env.drain(t)
				internal := env.internal(t)
				if internal.backfillers == 0 ||
					(len(internal.backfillerStates) == 1 && internal.backfillerStates[0].attempt > attempt) {
					break
				}
				var next time.Time
				for _, task := range env.tasks(t) {
					if task.visibilityTime.After(env.timeSource.Now()) && (next.IsZero() || task.visibilityTime.Before(next)) {
						next = task.visibilityTime
					}
				}
				if next.IsZero() {
					t.Fatal("pending backfill has no retry task")
				}
				env.timeSource.Update(next)
			}
			after := env.workflows.snapshot()
			var newRequestIDs []string
			for requestID := range after.starts {
				if _, existed := before.starts[requestID]; !existed {
					newRequestIDs = append(newRequestIDs, requestID)
				}
			}
			slices.Sort(newRequestIDs)

			retained := min(requested-remaining, 10)
			available := capacity - retained
			expectedBatch := min(remaining, available)
			if len(newRequestIDs) != expectedBatch {
				t.Fatalf("backfill batch: got %d starts, want %d (remaining=%d capacity=%d retained=%d)",
					len(newRequestIDs), expectedBatch, remaining, capacity, retained)
			}
			internalByID := make(map[string]modelBufferedStart)
			for _, buffered := range env.internal(t).buffered {
				internalByID[buffered.requestID] = buffered
			}
			for _, requestID := range newRequestIDs {
				buffered, ok := internalByID[requestID]
				if !ok || !buffered.manual || buffered.runID == "" {
					t.Fatalf("backfill request %q was not started as manual work", requestID)
				}
				actualTimes = append(actualTimes, buffered.actualTime)
			}
			remaining -= expectedBatch
			attempt++
			backfillActive = remaining > 0 || expectedBatch == available
			internal := env.internal(t)
			if !backfillActive {
				if internal.backfillers != 0 {
					t.Fatalf("completed backfill retained %d components", internal.backfillers)
				}
			} else {
				if len(internal.backfillerStates) != 1 || internal.backfillerStates[0].attempt != attempt {
					t.Fatalf("backfiller attempt: got %v, want %d", internal.backfillerStates, attempt)
				}
			}

			for _, requestID := range newRequestIDs {
				env.complete(t, requestID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte(requestID))
				env.drain(t)
			}
			if !backfillActive {
				break
			}

			before = env.workflows.snapshot()
		}

		slices.SortFunc(actualTimes, time.Time.Compare)
		if !slices.Equal(actualTimes, expectedTimes) {
			t.Fatalf("inclusive backfill times: got %v, want %v", actualTimes, expectedTimes)
		}
		description := env.describe(t)
		if description.GetInfo().GetActionCount() != int64(requested) {
			t.Fatalf("backfill action count: got %d, want %d", description.GetInfo().GetActionCount(), requested)
		}
		if !description.GetSchedule().GetState().GetPaused() ||
			description.GetSchedule().GetState().GetRemainingActions() != 0 {
			t.Fatal("manual backfill changed paused or limited-action state")
		}
		if len(env.workflows.snapshot().starts) != requested {
			t.Fatalf("unique backfill starts: got %d, want %d", len(env.workflows.snapshot().starts), requested)
		}
		env.requireNoRunnableTasks(t)
	})
}

func (m *overlapModel) advanceIntervals(t *rapid.T) {
	count := rapid.IntRange(1, 3).Draw(t, "interval count")
	now := m.env.timeSource.Now().Add(time.Duration(count) * m.env.config.interval)
	times := expectedIntervalTimes(
		m.env.config.interval, m.env.config.phase,
		m.env.config.specStart, m.env.config.specEnd,
		m.cursor, now,
	)
	items := make([]overlapModelItem, len(times))
	for i, value := range times {
		items[i] = overlapModelItem{actualTime: value}
	}
	before := m.env.workflows.snapshot()
	expectedStarts := m.process(items)
	m.env.timeSource.Update(now)
	m.env.drain(t)
	m.recordNewStarts(t, before, expectedStarts)
	m.cursor = now
	m.check(t)
}

func (m *overlapModel) triggerImmediately(t *rapid.T) {
	before := m.env.workflows.snapshot()
	item := overlapModelItem{actualTime: m.env.timeSource.Now(), manual: true}
	expectedStarts := m.process([]overlapModelItem{item})
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
				OverlapPolicy: m.policy,
			}},
		},
	})
	mustNoError(t, err)
	m.env.drain(t)
	m.recordNewStarts(t, before, expectedStarts)
	m.check(t)
}

func (m *overlapModel) rangeBackfill(t *rapid.T) {
	beforeIntervals := rapid.IntRange(0, 3).Draw(t, "backfill start intervals")
	start := m.env.timeSource.Now().Add(-time.Duration(beforeIntervals) * m.env.config.interval)
	end := m.env.timeSource.Now()
	times := expectedIntervalTimes(
		m.env.config.interval, m.env.config.phase,
		m.env.config.specStart, m.env.config.specEnd,
		start.Add(-time.Nanosecond), end,
	)
	items := make([]overlapModelItem, len(times))
	for i, value := range times {
		items[i] = overlapModelItem{actualTime: value, manual: true}
	}
	before := m.env.workflows.snapshot()
	expectedStarts := m.process(items)
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
				StartTime: timestamppb.New(start), EndTime: timestamppb.New(end), OverlapPolicy: m.policy,
			}}},
		},
	})
	mustNoError(t, err)
	m.env.drain(t)
	m.recordNewStarts(t, before, expectedStarts)
	m.check(t)
}

func (m *overlapModel) completeWorkflow(t *rapid.T) {
	requestIDs := make([]string, 0, len(m.running))
	for requestID := range m.running {
		requestIDs = append(requestIDs, requestID)
	}
	slices.Sort(requestIDs)
	if len(requestIDs) == 0 {
		t.Skip("no running workflows")
	}
	requestID := requestIDs[rapid.IntRange(0, len(requestIDs)-1).Draw(t, "workflow")]
	before := m.env.workflows.snapshot()
	action := m.running[requestID]
	delete(m.running, requestID)
	action.status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	m.completed[requestID] = action
	m.completedOrder = append(m.completedOrder, requestID)
	if len(m.completedOrder) > 10 {
		delete(m.completed, m.completedOrder[0])
		m.completedOrder = m.completedOrder[1:]
	}

	deferred := slices.Clone(m.deferred)
	m.deferred = nil
	expectedStarts := m.process(deferred)
	m.env.complete(t, requestID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte(requestID))
	m.env.drain(t)
	m.recordNewStarts(t, before, expectedStarts)
	m.check(t)
}

func (m *overlapModel) process(items []overlapModelItem) []overlapModelItem {
	if len(items) == 0 {
		return nil
	}
	hasRunning := len(m.running) > 0
	switch m.policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
		return slices.Clone(items)
	case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
		if hasRunning {
			m.overlapSkipped += int64(len(items))
			return nil
		}
		m.overlapSkipped += int64(len(items) - 1)
		return items[:1]
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
		if hasRunning {
			m.deferred = append(m.deferred, items[0])
			m.overlapSkipped += int64(len(items) - 1)
			return nil
		}
		if len(items) > 1 {
			m.deferred = append(m.deferred, items[1])
			m.overlapSkipped += int64(len(items) - 2)
		}
		return items[:1]
	case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
		if hasRunning {
			m.deferred = append(m.deferred, items...)
			return nil
		}
		m.deferred = append(m.deferred, items[1:]...)
		return items[:1]
	case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
		if hasRunning {
			m.deferred = append(m.deferred, items...)
			for _, action := range m.running {
				m.cancelTargets = append(m.cancelTargets, action.workflowID+"/"+action.runID)
			}
			return nil
		}
		return items[len(items)-1:]
	case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		if hasRunning {
			m.deferred = append(m.deferred, items...)
			for _, action := range m.running {
				m.terminateTargets = append(m.terminateTargets, action.workflowID+"/"+action.runID)
			}
			return nil
		}
		return items[len(items)-1:]
	default:
		panic(fmt.Sprintf("unsupported overlap policy %v", m.policy))
	}
}

func (m *overlapModel) recordNewStarts(
	t *rapid.T,
	before modelWorkflowSnapshot,
	expected []overlapModelItem,
) {
	t.Helper()
	after := m.env.workflows.snapshot()
	var requestIDs []string
	for requestID := range after.starts {
		if _, existed := before.starts[requestID]; !existed {
			requestIDs = append(requestIDs, requestID)
		}
	}
	slices.Sort(requestIDs)
	if len(requestIDs) != len(expected) {
		t.Fatalf("new starts: got %d, want %d", len(requestIDs), len(expected))
	}
	internal := make(map[string]modelBufferedStart)
	for _, start := range m.env.internal(t).buffered {
		internal[start.requestID] = start
	}
	var actualItems []overlapModelItem
	for _, requestID := range requestIDs {
		request := after.starts[requestID]
		start, ok := internal[requestID]
		if !ok {
			t.Fatalf("started request %q missing from scheduler state", requestID)
		}
		actualItems = append(actualItems, overlapModelItem{actualTime: start.actualTime, manual: start.manual})
		m.running[requestID] = overlapModelAction{
			requestID: requestID, workflowID: request.GetWorkflowId(), runID: modelRunID(requestID),
			status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}
	}
	sortOverlapItems(actualItems)
	sortOverlapItems(expected)
	if !slices.Equal(actualItems, expected) {
		t.Fatalf("started items: got %v, want %v", actualItems, expected)
	}
	m.actionCount += int64(len(expected))
}

func (m *overlapModel) check(t *rapid.T) {
	t.Helper()
	description := m.env.describe(t)
	info := description.GetInfo()
	if info.GetActionCount() != m.actionCount {
		t.Fatalf("action count: got %d, want %d", info.GetActionCount(), m.actionCount)
	}
	if info.GetOverlapSkipped() != m.overlapSkipped {
		t.Fatalf("overlap skipped: got %d, want %d", info.GetOverlapSkipped(), m.overlapSkipped)
	}
	if info.GetBufferDropped() != 0 || info.GetMissedCatchupWindow() != 0 {
		t.Fatalf("unexpected drop counters: dropped=%d missed=%d", info.GetBufferDropped(), info.GetMissedCatchupWindow())
	}
	if info.GetBufferSize() != int64(len(m.deferred)) {
		t.Fatalf("buffer size: got %d, want %d", info.GetBufferSize(), len(m.deferred))
	}

	var expectedRunning []string
	for _, action := range m.running {
		expectedRunning = append(expectedRunning, action.workflowID+"/"+action.runID)
	}
	var actualRunning []string
	for _, action := range info.GetRunningWorkflows() {
		actualRunning = append(actualRunning, action.GetWorkflowId()+"/"+action.GetRunId())
	}
	slices.Sort(expectedRunning)
	slices.Sort(actualRunning)
	if !slices.Equal(actualRunning, expectedRunning) {
		t.Fatalf("running workflows: got %v, want %v", actualRunning, expectedRunning)
	}

	expectedRecent := make(map[string]enumspb.WorkflowExecutionStatus)
	for _, action := range m.running {
		expectedRecent[action.runID] = action.status
	}
	for _, action := range m.completed {
		expectedRecent[action.runID] = action.status
	}
	actualRecent := make(map[string]enumspb.WorkflowExecutionStatus)
	for _, action := range info.GetRecentActions() {
		actualRecent[action.GetStartWorkflowResult().GetRunId()] = action.GetStartWorkflowStatus()
	}
	if !mapsEqual(actualRecent, expectedRecent) {
		t.Fatalf("recent actions: got %v, want %v", actualRecent, expectedRecent)
	}

	var actualDeferred []overlapModelItem
	for _, start := range m.env.internal(t).buffered {
		if start.attempt == -1 && start.runID == "" {
			actualDeferred = append(actualDeferred, overlapModelItem{
				actualTime: start.actualTime, manual: start.manual,
			})
		}
	}
	expectedDeferred := slices.Clone(m.deferred)
	sortOverlapItems(actualDeferred)
	sortOverlapItems(expectedDeferred)
	if !slices.Equal(actualDeferred, expectedDeferred) {
		t.Fatalf("deferred starts: got %v, want %v", actualDeferred, expectedDeferred)
	}

	history := m.env.history.snapshot()
	actualCancels := make([]string, len(history.cancels))
	for i, request := range history.cancels {
		workflow := request.GetCancelRequest().GetWorkflowExecution()
		actualCancels[i] = workflow.GetWorkflowId() + "/" + request.GetCancelRequest().GetFirstExecutionRunId()
	}
	actualTerminates := make([]string, len(history.terminates))
	for i, request := range history.terminates {
		workflow := request.GetTerminateRequest().GetWorkflowExecution()
		actualTerminates[i] = workflow.GetWorkflowId() + "/" + request.GetTerminateRequest().GetFirstExecutionRunId()
	}
	expectedCancels := slices.Clone(m.cancelTargets)
	expectedTerminates := slices.Clone(m.terminateTargets)
	slices.Sort(actualCancels)
	slices.Sort(actualTerminates)
	slices.Sort(expectedCancels)
	slices.Sort(expectedTerminates)
	if !slices.Equal(actualCancels, expectedCancels) || !slices.Equal(actualTerminates, expectedTerminates) {
		t.Fatalf("overlap service calls: cancels=%v/%v terminates=%v/%v",
			actualCancels, expectedCancels, actualTerminates, expectedTerminates)
	}

	workflows := m.env.workflows.snapshot()
	if int64(len(workflows.starts)) != m.actionCount || len(workflows.startCalls) != len(workflows.starts) {
		t.Fatalf("workflow starts: unique=%d calls=%d actions=%d",
			len(workflows.starts), len(workflows.startCalls), m.actionCount)
	}
	m.env.checkRequestIDs(t)
	m.env.requireNoRunnableTasks(t)
}

func sortOverlapItems(items []overlapModelItem) {
	slices.SortFunc(items, func(a, b overlapModelItem) int {
		if order := a.actualTime.Compare(b.actualTime); order != 0 {
			return order
		}
		if a.manual == b.manual {
			return 0
		}
		if !a.manual {
			return -1
		}
		return 1
	})
}
