package scheduler_test

import (
	"encoding/binary"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

type (
	timingModelAction struct {
		requestID  string
		workflowID string
		runID      string
		status     enumspb.WorkflowExecutionStatus
	}

	timingModel struct {
		env               *schedulerModelEnv
		config            modelEnvConfig
		cursor            time.Time
		paused            bool
		notes             string
		limitedActions    bool
		remainingActions  int64
		conflictToken     int64
		actionCount       int64
		futureActionLimit int
		memo              map[string][]byte
		searchAttributes  map[string][]byte
		actions           map[string]timingModelAction
		completedOrder    []string
	}
)

func TestSchedulerTimingAPIModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		interval := time.Duration(rapid.IntRange(1, 4).Draw(t, "initial interval minutes")) * time.Minute
		config := defaultModelEnvConfig()
		config.interval = interval
		config.phase = time.Duration(rapid.IntRange(0, int(interval/time.Second)-1).Draw(t, "initial phase seconds")) * time.Second
		config.specStart = modelStartTime.Add(time.Duration(rapid.IntRange(0, 2).Draw(t, "initial start intervals")) * interval)
		config.specEnd = config.specStart.Add(time.Duration(rapid.IntRange(5, 15).Draw(t, "initial end intervals")) * interval)
		config.paused = rapid.Bool().Draw(t, "initial paused")
		config.notes = "initial notes"
		config.limitedActions = rapid.Bool().Draw(t, "initial limited")
		if config.limitedActions {
			config.remainingAction = int64(rapid.IntRange(2, 12).Draw(t, "initial remaining"))
		}

		model := &timingModel{
			env:               newSchedulerModelEnv(t, config),
			config:            config,
			cursor:            config.startTime,
			paused:            config.paused,
			notes:             config.notes,
			limitedActions:    config.limitedActions,
			remainingActions:  config.remainingAction,
			conflictToken:     1,
			futureActionLimit: modelFutureActionLimit(config.limitedActions, config.remainingAction),
			memo:              map[string][]byte{"created-memo": []byte("created")},
			searchAttributes:  map[string][]byte{"CustomKeywordField": []byte("created")},
			actions:           make(map[string]timingModelAction),
		}
		model.check(t)
		t.Repeat(map[string]func(*rapid.T){
			"advance time":        model.advanceTime,
			"pause or unpause":    model.patchPaused,
			"trigger immediately": model.triggerImmediately,
			"update schedule":     model.updateSchedule,
			"reject stale update": model.rejectStaleUpdate,
			"list matching times": model.listMatchingTimes,
			"complete workflow":   model.completeWorkflow,
			"reload execution":    model.reload,
			"":                    model.check,
		})
	})
}

func (m *timingModel) reload(t *rapid.T) {
	m.env.reload(t)
	m.check(t)
}

func (m *timingModel) advanceTime(t *rapid.T) {
	steps := rapid.IntRange(1, 4).Draw(t, "advance intervals")
	delta := time.Duration(steps)*m.config.interval + time.Duration(rapid.IntRange(0, 2).Draw(t, "advance seconds"))*time.Second
	now := m.env.timeSource.Now().Add(delta)
	expected := expectedIntervalTimes(
		m.config.interval, m.config.phase, m.config.specStart, m.config.specEnd, m.cursor, now,
	)
	if m.paused || (m.limitedActions && m.remainingActions == 0) {
		expected = nil
	} else if m.limitedActions && int64(len(expected)) > m.remainingActions {
		expected = expected[:m.remainingActions]
	}

	before := m.env.workflows.snapshot()
	m.futureActionLimit = modelFutureActionLimit(m.limitedActions, m.remainingActions)
	m.env.timeSource.Update(now)
	m.env.drain(t)
	m.recordNewStarts(t, before, expected, false)
	if m.limitedActions {
		m.remainingActions -= int64(len(expected))
		m.conflictToken += int64(len(expected))
	}
	m.cursor = now
	m.check(t)
}

func (m *timingModel) patchPaused(t *rapid.T) {
	paused := rapid.Bool().Draw(t, "paused")
	notes := "unpaused by timing model"
	patch := &schedulepb.SchedulePatch{Unpause: notes}
	if paused {
		notes = "paused by timing model"
		patch = &schedulepb.SchedulePatch{Pause: notes}
	}
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID, Patch: patch,
		},
	})
	mustNoError(t, err)
	m.paused = paused
	m.notes = notes
	m.conflictToken++
	m.futureActionLimit = modelFutureActionLimit(m.limitedActions, m.remainingActions)
	m.env.drain(t)
	m.check(t)
}

func (m *timingModel) triggerImmediately(t *rapid.T) {
	before := m.env.workflows.snapshot()
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
	m.conflictToken++
	m.futureActionLimit = modelFutureActionLimit(m.limitedActions, m.remainingActions)
	m.env.drain(t)
	m.recordNewStarts(t, before, []time.Time{m.env.timeSource.Now()}, true)
	m.check(t)
}

func (m *timingModel) updateSchedule(t *rapid.T) {
	interval := time.Duration(rapid.IntRange(1, 4).Draw(t, "updated interval minutes")) * time.Minute
	config := m.config
	config.interval = interval
	config.phase = time.Duration(rapid.IntRange(0, int(interval/time.Second)-1).Draw(t, "updated phase seconds")) * time.Second
	config.specStart = m.env.timeSource.Now().Add(time.Duration(rapid.IntRange(0, 2).Draw(t, "updated start intervals")) * interval)
	config.specEnd = config.specStart.Add(time.Duration(rapid.IntRange(4, 12).Draw(t, "updated end intervals")) * interval)
	config.paused = rapid.Bool().Draw(t, "updated paused")
	config.notes = "updated notes"
	config.limitedActions = rapid.Bool().Draw(t, "updated limited")
	config.remainingAction = 0
	if config.limitedActions {
		config.remainingAction = int64(rapid.IntRange(1, 10).Draw(t, "updated remaining"))
	}
	memoValue := []byte{byte(rapid.IntRange(1, 250).Draw(t, "memo byte"))}
	searchValue := []byte{byte(rapid.IntRange(1, 250).Draw(t, "search byte"))}

	_, err := m.env.handler.UpdateSchedule(m.env.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			ConflictToken: encodeConflictToken(m.conflictToken),
			Schedule:      modelSchedule(config),
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				"updated-memo": {Data: slices.Clone(memoValue)},
			}},
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": {Data: slices.Clone(searchValue)},
			}},
		},
	})
	mustNoError(t, err)
	m.config = config
	m.cursor = m.env.timeSource.Now()
	m.paused = config.paused
	m.notes = config.notes
	m.limitedActions = config.limitedActions
	m.remainingActions = config.remainingAction
	m.conflictToken++
	m.futureActionLimit = modelFutureActionLimit(m.limitedActions, m.remainingActions)
	m.memo = map[string][]byte{"updated-memo": memoValue}
	m.searchAttributes = map[string][]byte{"CustomKeywordField": searchValue}
	m.env.drain(t)
	m.check(t)
}

func (m *timingModel) rejectStaleUpdate(t *rapid.T) {
	beforeDescription := proto.CloneOf(m.env.describe(t))
	beforeInternal := m.env.internal(t)
	beforeTasks := m.env.tasks(t)
	beforeWorkflows := m.env.workflows.snapshot()
	stale := m.conflictToken - 1
	_, err := m.env.handler.UpdateSchedule(m.env.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			ConflictToken: encodeConflictToken(stale),
			Schedule:      modelSchedule(defaultModelEnvConfig()),
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				"should-not-commit": {Data: []byte("stale")},
			}},
		},
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	if !errors.As(err, &failedPrecondition) {
		t.Fatalf("stale update error: got %v, want FailedPrecondition", err)
	}
	if !proto.Equal(beforeDescription, m.env.describe(t)) {
		t.Fatal("stale update changed DescribeSchedule response")
	}
	if !reflect.DeepEqual(beforeInternal, m.env.internal(t)) {
		t.Fatal("stale update changed internal state")
	}
	if !slices.Equal(beforeTasks, m.env.tasks(t)) {
		t.Fatal("stale update changed queued tasks")
	}
	afterWorkflows := m.env.workflows.snapshot()
	if len(beforeWorkflows.starts) != len(afterWorkflows.starts) || len(beforeWorkflows.startCalls) != len(afterWorkflows.startCalls) {
		t.Fatal("stale update changed workflow service calls")
	}
	m.check(t)
}

func (m *timingModel) listMatchingTimes(t *rapid.T) {
	start := m.env.timeSource.Now().Add(-time.Duration(rapid.IntRange(0, 3).Draw(t, "list past intervals")) * m.config.interval)
	end := start.Add(time.Duration(rapid.IntRange(0, 12).Draw(t, "list interval count")) * m.config.interval)
	actual, err := m.env.listMatching(t, start, end)
	mustNoError(t, err)
	expected := expectedIntervalTimes(
		m.config.interval, m.config.phase, m.config.specStart, m.config.specEnd, start, end,
	)
	if !slices.Equal(actual, expected) {
		t.Fatalf("matching times: got %v, want %v", actual, expected)
	}
	m.check(t)
}

func (m *timingModel) completeWorkflow(t *rapid.T) {
	var running []string
	for requestID, action := range m.actions {
		if action.status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			running = append(running, requestID)
		}
	}
	slices.Sort(running)
	if len(running) == 0 {
		t.Skip("no running workflows")
	}
	requestID := running[rapid.IntRange(0, len(running)-1).Draw(t, "workflow")]
	m.env.complete(t, requestID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte(requestID))
	action := m.actions[requestID]
	action.status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	m.actions[requestID] = action
	m.completedOrder = append(m.completedOrder, requestID)
	if len(m.completedOrder) > 10 {
		delete(m.actions, m.completedOrder[0])
		m.completedOrder = m.completedOrder[1:]
	}
	m.futureActionLimit = modelFutureActionLimit(m.limitedActions, m.remainingActions)
	m.env.drain(t)
	m.check(t)
}

func (m *timingModel) recordNewStarts(
	t *rapid.T,
	before modelWorkflowSnapshot,
	expectedTimes []time.Time,
	manual bool,
) {
	t.Helper()
	after := m.env.workflows.snapshot()
	var newRequestIDs []string
	for requestID := range after.starts {
		if _, existed := before.starts[requestID]; !existed {
			newRequestIDs = append(newRequestIDs, requestID)
		}
	}
	slices.Sort(newRequestIDs)
	if len(newRequestIDs) != len(expectedTimes) {
		t.Fatalf("new workflow starts: got %d, want %d", len(newRequestIDs), len(expectedTimes))
	}

	internalByID := make(map[string]modelBufferedStart)
	for _, start := range m.env.internal(t).buffered {
		internalByID[start.requestID] = start
	}
	var actualTimes []time.Time
	for _, requestID := range newRequestIDs {
		request := after.starts[requestID]
		start, ok := internalByID[requestID]
		if !ok {
			t.Fatalf("request %q missing from scheduler buffer", requestID)
		}
		if start.runID != modelRunID(requestID) || start.workflowID != request.GetWorkflowId() {
			t.Fatalf("request %q workflow/run mismatch", requestID)
		}
		if start.manual != manual {
			t.Fatalf("request %q manual: got %v, want %v", requestID, start.manual, manual)
		}
		actualTimes = append(actualTimes, start.actualTime)
		m.actions[requestID] = timingModelAction{
			requestID: requestID, workflowID: request.WorkflowId,
			runID: modelRunID(requestID), status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}
	}
	slices.SortFunc(actualTimes, time.Time.Compare)
	slices.SortFunc(expectedTimes, time.Time.Compare)
	if !slices.Equal(actualTimes, expectedTimes) {
		t.Fatalf("new workflow times: got %v, want %v", actualTimes, expectedTimes)
	}
	m.actionCount += int64(len(expectedTimes))
}

func (m *timingModel) check(t *rapid.T) {
	t.Helper()
	description := m.env.describe(t)
	schedule := description.GetSchedule()
	info := description.GetInfo()
	if schedule.GetState().GetPaused() != m.paused || schedule.GetState().GetNotes() != m.notes {
		t.Fatalf("schedule state: got paused=%v notes=%q, want paused=%v notes=%q",
			schedule.GetState().GetPaused(), schedule.GetState().GetNotes(), m.paused, m.notes)
	}
	if schedule.GetState().GetLimitedActions() != m.limitedActions ||
		schedule.GetState().GetRemainingActions() != m.remainingActions {
		t.Fatalf("remaining actions: got limited=%v remaining=%d, want limited=%v remaining=%d",
			schedule.GetState().GetLimitedActions(), schedule.GetState().GetRemainingActions(),
			m.limitedActions, m.remainingActions)
	}
	interval := schedule.GetSpec().GetInterval()[0]
	if interval.GetInterval().AsDuration() != m.config.interval || interval.GetPhase().AsDuration() != m.config.phase {
		t.Fatalf("interval: got %v/%v, want %v/%v",
			interval.GetInterval().AsDuration(), interval.GetPhase().AsDuration(), m.config.interval, m.config.phase)
	}
	if info.GetActionCount() != m.actionCount {
		t.Fatalf("action count: got %d, want %d", info.GetActionCount(), m.actionCount)
	}
	if actual := conflictTokenValue(description.GetConflictToken()); actual != m.conflictToken {
		t.Fatalf("conflict token: got %d, want %d", actual, m.conflictToken)
	}
	internal := m.env.internal(t)
	if internal.conflictToken != m.conflictToken {
		t.Fatalf("internal conflict token: got %d, want %d", internal.conflictToken, m.conflictToken)
	}
	checkPayloadMap(t, "memo", description.GetMemo().GetFields(), m.memo)
	checkPayloadMap(t, "search attributes", description.GetSearchAttributes().GetIndexedFields(), m.searchAttributes)

	var expectedRunning []string
	expectedRecent := make(map[string]enumspb.WorkflowExecutionStatus)
	for _, action := range m.actions {
		expectedRecent[action.runID] = action.status
		if action.status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			expectedRunning = append(expectedRunning, action.workflowID+"/"+action.runID)
		}
	}
	var actualRunning []string
	for _, workflow := range info.GetRunningWorkflows() {
		actualRunning = append(actualRunning, workflow.GetWorkflowId()+"/"+workflow.GetRunId())
	}
	slices.Sort(actualRunning)
	slices.Sort(expectedRunning)
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

	expectedFuture := expectedIntervalTimes(
		m.config.interval, m.config.phase, m.config.specStart, m.config.specEnd,
		m.env.timeSource.Now(), m.env.timeSource.Now().Add(50*m.config.interval),
	)
	if len(expectedFuture) > m.futureActionLimit {
		expectedFuture = expectedFuture[:m.futureActionLimit]
	}
	actualFuture := make([]time.Time, len(info.GetFutureActionTimes()))
	for i, value := range info.GetFutureActionTimes() {
		actualFuture[i] = value.AsTime()
	}
	if !slices.Equal(actualFuture, expectedFuture) {
		t.Fatalf("future action times: got %v, want %v", actualFuture, expectedFuture)
	}

	workflows := m.env.workflows.snapshot()
	if int64(len(workflows.starts)) != m.actionCount || len(workflows.startCalls) != len(workflows.starts) {
		t.Fatalf("workflow calls: unique=%d calls=%d actions=%d", len(workflows.starts), len(workflows.startCalls), m.actionCount)
	}
	m.env.checkRequestIDs(t)
	m.env.requireNoRunnableTasks(t)
}

func encodeConflictToken(value int64) []byte {
	token := make([]byte, 8)
	binary.LittleEndian.PutUint64(token, uint64(value))
	return token
}

func modelFutureActionLimit(limited bool, remaining int64) int {
	if limited {
		return min(int(remaining), 10)
	}
	return 10
}

func checkPayloadMap(t *rapid.T, name string, actual map[string]*commonpb.Payload, expected map[string][]byte) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("%s size: got %d, want %d", name, len(actual), len(expected))
	}
	for key, value := range expected {
		if !slices.Equal(actual[key].GetData(), value) {
			t.Fatalf("%s[%q]: got %v, want %v", name, key, actual[key].GetData(), value)
		}
	}
}

func mapsEqual[K comparable, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}
