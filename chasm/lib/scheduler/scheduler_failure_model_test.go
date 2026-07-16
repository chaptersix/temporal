package scheduler_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	"pgregory.net/rapid"
)

type (
	failureOutcome   int
	failureItemState int

	failureModelItem struct {
		requestID    string
		workflowID   string
		runID        string
		state        failureItemState
		attempt      int64
		backoffTime  time.Time
		status       enumspb.WorkflowExecutionStatus
		serviceCalls int
	}

	failureModel struct {
		env            *schedulerModelEnv
		pauseOnFailure bool
		paused         bool
		notes          string
		items          map[string]*failureModelItem
		completedOrder []string
		savedTasks     []tasks.Task
		actionCount    int64
		lastSuccess    []byte
		lastFailure    string
	}
)

const (
	failureOutcomeSuccess failureOutcome = iota
	failureOutcomeRetryable
	failureOutcomeRateLimited
	failureOutcomeNonRetryable
	failureOutcomeAlreadyStarted
)

const (
	failureItemBackingOff failureItemState = iota
	failureItemRunning
	failureItemCompleted
	failureItemDropped
	failureItemEvicted
)

func TestSchedulerFailureIdempotencyModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		config := defaultModelEnvConfig()
		config.interval = 24 * time.Hour
		config.pauseOnFailure = rapid.Bool().Draw(t, "pause on failure")
		model := &failureModel{
			env: newSchedulerModelEnv(t, config), pauseOnFailure: config.pauseOnFailure,
			items: make(map[string]*failureModelItem),
		}
		model.check(t)
		t.Repeat(map[string]func(*rapid.T){
			"trigger with outcome": model.triggerWithOutcome,
			"advance retry":        model.advanceRetry,
			"redeliver saved task": model.redeliverSavedTask,
			"complete workflow":    model.completeWorkflow,
			"duplicate completion": model.duplicateCompletion,
			"reload execution":     model.reload,
			"":                     model.check,
		})
	})
}

func (m *failureModel) reload(t *rapid.T) {
	m.env.reload(t)
	m.check(t)
}

func (m *failureModel) triggerWithOutcome(t *rapid.T) {
	if m.backingOffItem() != nil {
		t.Skip("a start is already backing off")
	}
	outcome := failureOutcome(rapid.IntRange(
		int(failureOutcomeSuccess), int(failureOutcomeAlreadyStarted),
	).Draw(t, "outcome"))
	m.pushOutcome(outcome, 5*time.Second)

	beforeIDs := make(map[string]struct{}, len(m.items))
	for requestID := range m.items {
		beforeIDs[requestID] = struct{}{}
	}
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}},
		},
	})
	mustNoError(t, err)

	var item *failureModelItem
	for _, start := range m.env.internal(t).buffered {
		if _, exists := beforeIDs[start.requestID]; exists {
			continue
		}
		if item != nil {
			t.Fatal("trigger created more than one buffered start")
		}
		item = &failureModelItem{
			requestID: start.requestID, workflowID: start.workflowID, attempt: 1,
		}
	}
	if item == nil {
		t.Fatal("trigger did not create a buffered start")
		return
	}
	m.items[item.requestID] = item

	task, _ := m.env.executeOne(t)
	if _, ok := task.(*tasks.ChasmTask); !ok {
		t.Fatalf("trigger execution task has type %T", task)
	}
	m.savedTasks = append(m.savedTasks, task)
	item.serviceCalls++
	m.applyOutcome(item, outcome, 5*time.Second)
	m.env.drain(t)
	m.check(t)
}

func (m *failureModel) advanceRetry(t *rapid.T) {
	item := m.backingOffItem()
	if item == nil {
		t.Skip("no start is backing off")
		return
	}

	var outcome failureOutcome
	if item.attempt < scheduler.InvokerMaxStartAttempts {
		outcome = failureOutcome(rapid.IntRange(
			int(failureOutcomeSuccess), int(failureOutcomeAlreadyStarted),
		).Draw(t, "retry outcome"))
		m.pushOutcome(outcome, 5*time.Second)
	}
	m.env.timeSource.Update(item.backoffTime)
	pureTask, _ := m.env.executeOne(t)
	if _, ok := pureTask.(*tasks.ChasmTaskPure); !ok {
		t.Fatalf("retry wakeup task has type %T", pureTask)
	}
	executeTask, _ := m.env.executeOne(t)
	if _, ok := executeTask.(*tasks.ChasmTask); !ok {
		t.Fatalf("retry execution task has type %T", executeTask)
	}
	m.savedTasks = append(m.savedTasks, executeTask)

	if item.attempt >= scheduler.InvokerMaxStartAttempts {
		item.state = failureItemDropped
		item.backoffTime = time.Time{}
	} else {
		item.serviceCalls++
		m.applyOutcome(item, outcome, 5*time.Second)
	}
	m.env.drain(t)
	m.check(t)
}

func (m *failureModel) redeliverSavedTask(t *rapid.T) {
	if len(m.savedTasks) == 0 {
		t.Skip("no saved tasks")
	}
	task := m.savedTasks[rapid.IntRange(0, len(m.savedTasks)-1).Draw(t, "saved task")]
	beforeCalls := len(m.env.workflows.snapshot().startCalls)
	beforeActions := m.env.describe(t).GetInfo().GetActionCount()
	result := m.env.redeliver(t, task)
	if !result.Dropped || result.Executed != 0 {
		t.Fatalf("redelivered task result: got %+v, want stale drop", result)
	}
	if len(m.env.workflows.snapshot().startCalls) != beforeCalls ||
		m.env.describe(t).GetInfo().GetActionCount() != beforeActions {
		t.Fatal("redelivered task produced side effects")
	}
	m.check(t)
}

func (m *failureModel) completeWorkflow(t *rapid.T) {
	requestIDs := m.requestIDsInState(failureItemRunning)
	if len(requestIDs) == 0 {
		t.Skip("no running workflows")
	}
	requestID := requestIDs[rapid.IntRange(0, len(requestIDs)-1).Draw(t, "workflow")]
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}
	status := statuses[rapid.IntRange(0, len(statuses)-1).Draw(t, "status")]
	payload := []byte("completion-" + requestID)
	m.env.complete(t, requestID, status, payload)
	m.env.drain(t)

	item := m.items[requestID]
	item.state = failureItemCompleted
	item.status = status
	m.completedOrder = append(m.completedOrder, requestID)
	if status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		m.lastSuccess = slices.Clone(payload)
		m.lastFailure = ""
	} else {
		m.lastFailure = string(payload)
		if m.pauseOnFailure && !m.paused {
			m.paused = true
			m.notes = fmt.Sprintf("paused, workflow %s: %s", strings.ToLower(status.String()), item.workflowID)
		}
	}
	if len(m.completedOrder) > 10 {
		evicted := m.completedOrder[0]
		m.completedOrder = m.completedOrder[1:]
		m.items[evicted].state = failureItemEvicted
	}
	m.check(t)
}

func (m *failureModel) duplicateCompletion(t *rapid.T) {
	var requestIDs []string
	for requestID, item := range m.items {
		if item.state == failureItemCompleted || item.state == failureItemEvicted {
			requestIDs = append(requestIDs, requestID)
		}
	}
	slices.Sort(requestIDs)
	if len(requestIDs) == 0 {
		t.Skip("no completed workflows")
	}
	requestID := requestIDs[rapid.IntRange(0, len(requestIDs)-1).Draw(t, "completed workflow")]
	item := m.items[requestID]
	m.env.complete(t, requestID, item.status, []byte("duplicate-"+requestID))
	m.env.drain(t)
	m.check(t)
}

func (m *failureModel) pushOutcome(outcome failureOutcome, rateLimitDelay time.Duration) {
	switch outcome {
	case failureOutcomeSuccess:
		m.env.workflows.pushStartError(nil)
	case failureOutcomeRetryable:
		m.env.workflows.pushStartError(serviceerror.NewDeadlineExceeded("retryable start failure"))
	case failureOutcomeRateLimited:
		m.env.workflows.pushStartError(scheduler.NewTestRateLimitedError(rateLimitDelay))
	case failureOutcomeNonRetryable:
		m.env.workflows.pushStartError(serviceerror.NewInvalidArgument("non-retryable start failure"))
	case failureOutcomeAlreadyStarted:
		m.env.workflows.pushStartError(serviceerror.NewWorkflowExecutionAlreadyStarted(
			"workflow already started", "", "",
		))
	default:
		panic(fmt.Sprintf("unknown failure outcome %d", outcome))
	}
}

func (m *failureModel) applyOutcome(
	item *failureModelItem,
	outcome failureOutcome,
	rateLimitDelay time.Duration,
) {
	switch outcome {
	case failureOutcomeSuccess:
		item.state = failureItemRunning
		item.runID = modelRunID(item.requestID)
		item.backoffTime = time.Time{}
		m.actionCount++
	case failureOutcomeRetryable, failureOutcomeRateLimited:
		delay := rateLimitDelay
		if outcome == failureOutcomeRetryable {
			delay = modelRetryPolicy{}.ComputeNextDelay(0, int(item.attempt), nil)
		}
		item.state = failureItemBackingOff
		item.backoffTime = m.env.timeSource.Now().Add(delay)
		item.attempt++
	case failureOutcomeNonRetryable, failureOutcomeAlreadyStarted:
		item.state = failureItemDropped
		item.backoffTime = time.Time{}
	default:
		panic(fmt.Sprintf("unknown failure outcome %d", outcome))
	}
}

func (m *failureModel) backingOffItem() *failureModelItem {
	for _, item := range m.items {
		if item.state == failureItemBackingOff {
			return item
		}
	}
	return nil
}

func (m *failureModel) requestIDsInState(state failureItemState) []string {
	var requestIDs []string
	for requestID, item := range m.items {
		if item.state == state {
			requestIDs = append(requestIDs, requestID)
		}
	}
	slices.Sort(requestIDs)
	return requestIDs
}

func (m *failureModel) check(t *rapid.T) {
	t.Helper()
	internal := m.env.internal(t)
	description := m.env.describe(t)
	info := description.GetInfo()
	if info.GetActionCount() != m.actionCount {
		t.Fatalf("action count: got %d, want %d", info.GetActionCount(), m.actionCount)
	}
	if info.GetOverlapSkipped() != 0 || info.GetBufferDropped() != 0 || info.GetMissedCatchupWindow() != 0 {
		t.Fatalf("unexpected failure-model counters: %+v", info)
	}
	if description.GetSchedule().GetState().GetPaused() != m.paused ||
		description.GetSchedule().GetState().GetNotes() != m.notes {
		t.Fatalf("pause state: got paused=%v notes=%q, want paused=%v notes=%q",
			description.GetSchedule().GetState().GetPaused(), description.GetSchedule().GetState().GetNotes(),
			m.paused, m.notes)
	}
	if !slices.Equal(internal.lastSuccess, m.lastSuccess) || internal.lastFailure != m.lastFailure {
		t.Fatalf("last completion: got success=%q failure=%q, want success=%q failure=%q",
			internal.lastSuccess, internal.lastFailure, m.lastSuccess, m.lastFailure)
	}

	expectedBuffered := make(map[string]*failureModelItem)
	for requestID, item := range m.items {
		switch item.state {
		case failureItemBackingOff, failureItemRunning, failureItemCompleted:
			expectedBuffered[requestID] = item
		case failureItemDropped, failureItemEvicted:
		default:
			t.Fatalf("unexpected failure item state %d", item.state)
		}
	}
	if len(internal.buffered) != len(expectedBuffered) {
		t.Fatalf("buffered starts: got %d, want %d", len(internal.buffered), len(expectedBuffered))
	}
	pendingCount := 0
	for _, actual := range internal.buffered {
		expected := expectedBuffered[actual.requestID]
		if expected == nil {
			t.Fatalf("unexpected buffered request %q", actual.requestID)
		} else {
			if actual.workflowID != expected.workflowID || actual.attempt != expected.attempt {
				t.Fatalf("buffered request %q: got workflow=%q attempt=%d, want workflow=%q attempt=%d",
					actual.requestID, actual.workflowID, actual.attempt, expected.workflowID, expected.attempt)
			}
			switch expected.state {
			case failureItemBackingOff:
				pendingCount++
				if actual.runID != "" || !actual.backoffTime.Equal(expected.backoffTime) {
					t.Fatalf("backoff request %q: got run=%q time=%s, want time=%s",
						actual.requestID, actual.runID, actual.backoffTime, expected.backoffTime)
				}
			case failureItemRunning:
				if actual.runID != expected.runID || actual.completed != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED ||
					!actual.hasCallback {
					t.Fatalf("running request %q has inconsistent state: %+v", actual.requestID, actual)
				}
			case failureItemCompleted:
				if actual.runID != expected.runID || actual.completed != expected.status {
					t.Fatalf("completed request %q has inconsistent state: %+v", actual.requestID, actual)
				}
			default:
				t.Fatalf("unexpected buffered request state %d", expected.state)
			}
		}
	}
	if info.GetBufferSize() != int64(pendingCount) {
		t.Fatalf("buffer size: got %d, want %d", info.GetBufferSize(), pendingCount)
	}

	var expectedRunning []string
	expectedRecent := make(map[string]enumspb.WorkflowExecutionStatus)
	for _, item := range m.items {
		switch item.state {
		case failureItemRunning:
			expectedRunning = append(expectedRunning, item.workflowID+"/"+item.runID)
			expectedRecent[item.runID] = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		case failureItemCompleted:
			expectedRecent[item.runID] = item.status
		case failureItemBackingOff, failureItemDropped, failureItemEvicted:
		default:
			t.Fatalf("unexpected failure item state %d", item.state)
		}
	}
	var actualRunning []string
	for _, execution := range info.GetRunningWorkflows() {
		actualRunning = append(actualRunning, execution.GetWorkflowId()+"/"+execution.GetRunId())
	}
	slices.Sort(expectedRunning)
	slices.Sort(actualRunning)
	if !slices.Equal(actualRunning, expectedRunning) {
		t.Fatalf("running workflows: got %v, want %v", actualRunning, expectedRunning)
	}
	actualRecent := make(map[string]enumspb.WorkflowExecutionStatus)
	for _, action := range info.GetRecentActions() {
		actualRecent[action.GetStartWorkflowResult().GetRunId()] = action.GetStartWorkflowStatus()
	}
	if !mapsEqual(actualRecent, expectedRecent) {
		t.Fatalf("recent actions: got %v, want %v", actualRecent, expectedRecent)
	}

	workflowSnapshot := m.env.workflows.snapshot()
	actualCalls := make(map[string]int)
	for _, request := range workflowSnapshot.startCalls {
		actualCalls[request.GetRequestId()]++
		item := m.items[request.GetRequestId()]
		if item == nil || request.GetWorkflowId() != item.workflowID || len(request.GetCompletionCallbacks()) != 1 {
			t.Fatalf("invalid start call for request %q", request.GetRequestId())
		}
	}
	for requestID, item := range m.items {
		if actualCalls[requestID] != item.serviceCalls {
			t.Fatalf("service calls for %q: got %d, want %d", requestID, actualCalls[requestID], item.serviceCalls)
		}
	}
	if int64(len(workflowSnapshot.starts)) != m.actionCount {
		t.Fatalf("recorded starts: got %d, want %d", len(workflowSnapshot.starts), m.actionCount)
	}
	m.env.checkRequestIDs(t)
	m.env.requireNoRunnableTasks(t)
}
