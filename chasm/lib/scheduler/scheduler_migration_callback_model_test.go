package scheduler_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedmigration "go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

type (
	migrationOutcome int

	migrationModel struct {
		env              *schedulerModelEnv
		initialPaused    bool
		initialNotes     string
		paused           bool
		notes            string
		pending          bool
		closed           bool
		calls            int
		successfulStarts int
	}

	callbackOutcome int
	callbackState   int

	callbackRecoveryModel struct {
		env           *schedulerModelEnv
		requestID     string
		workflowID    string
		runID         string
		state         callbackState
		status        enumspb.WorkflowExecutionStatus
		describeCalls int
		attachCalls   int
		canCallback   bool
		savedTasks    []tasks.Task
	}
)

const (
	migrationOutcomeSuccess migrationOutcome = iota
	migrationOutcomeAlreadyStarted
	migrationOutcomeFailure
)

const (
	callbackOutcomeAttach callbackOutcome = iota
	callbackOutcomeDescribeClosed
	callbackOutcomeNotFound
	callbackOutcomeAttachAlreadyStarted
	callbackOutcomeDescribeFailure
	callbackOutcomeAttachFailure
)

const (
	callbackStatePending callbackState = iota
	callbackStateAttached
	callbackStateCompleted
)

func TestSchedulerMigrationModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		config := defaultModelEnvConfig()
		config.interval = 24 * time.Hour
		config.paused = rapid.Bool().Draw(t, "initial paused")
		config.notes = "pre-migration notes"
		model := &migrationModel{
			env: newSchedulerModelEnv(t, config), initialPaused: config.paused,
			initialNotes: config.notes, paused: config.paused, notes: config.notes,
		}
		model.check(t)
		t.Repeat(map[string]func(*rapid.T){
			"start migration":       model.startMigration,
			"repeat migration":      model.repeatMigration,
			"pause while pending":   model.pauseWhilePending,
			"reject pending change": model.rejectPendingChange,
			"execute migration":     model.executeMigration,
			"":                      model.check,
		})
	})
}

func TestSchedulerCallbackRecoveryModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		model := newCallbackRecoveryModel(t)
		model.check(t)
		t.Repeat(map[string]func(*rapid.T){
			"recover callback":     model.recoverCallback,
			"complete attached":    model.completeAttached,
			"duplicate completion": model.duplicateCompletion,
			"redeliver recovery":   model.redeliverRecovery,
			"":                     model.check,
		})
	})
}

func (m *migrationModel) startMigration(t *rapid.T) {
	if m.pending {
		t.Skip("migration is already pending")
	}
	_, err := m.env.handler.MigrateToWorkflow(m.env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID, ScheduleId: m.env.scheduleID,
	})
	if m.closed {
		requireErrorIs(t, err, scheduler.ErrClosed)
	} else {
		mustNoError(t, err)
		m.pending = true
		m.paused = true
		m.notes = "paused for migration to workflow-backed scheduler"
	}
	m.check(t)
}

func (m *migrationModel) repeatMigration(t *rapid.T) {
	if !m.pending {
		t.Skip("migration is not pending")
	}
	beforeTasks := m.env.tasks(t)
	_, err := m.env.handler.MigrateToWorkflow(m.env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID, ScheduleId: m.env.scheduleID,
	})
	mustNoError(t, err)
	if !slices.Equal(beforeTasks, m.env.tasks(t)) {
		t.Fatal("repeated migration scheduled another task")
	}
	m.check(t)
}

func (m *migrationModel) pauseWhilePending(t *rapid.T) {
	if !m.pending {
		t.Skip("migration is not pending")
	}
	note := fmt.Sprintf("pause while pending %d", m.calls)
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID,
			Patch: &schedulepb.SchedulePatch{Pause: note},
		},
	})
	mustNoError(t, err)
	m.paused = true
	m.notes = note
	m.check(t)
}

func (m *migrationModel) rejectPendingChange(t *rapid.T) {
	if !m.pending {
		t.Skip("migration is not pending")
	}
	before := m.env.describe(t)
	if rapid.Bool().Draw(t, "update") {
		schedule := proto.CloneOf(before.GetSchedule())
		schedule.State.Paused = false
		schedule.State.Notes = "rejected update"
		_, err := m.env.handler.UpdateSchedule(m.env.engineCtx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace: namespace, ScheduleId: m.env.scheduleID, Schedule: schedule,
				ConflictToken: slices.Clone(before.GetConflictToken()),
				Memo:          &commonpb.Memo{Fields: map[string]*commonpb.Payload{"rejected": {Data: []byte("memo")}}},
			},
		})
		requireErrorIs(t, err, scheduler.ErrMigrationPending)
	} else {
		_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace: namespace, ScheduleId: m.env.scheduleID,
				Patch: &schedulepb.SchedulePatch{Unpause: "rejected unpause"},
			},
		})
		requireErrorIs(t, err, scheduler.ErrMigrationPending)
	}
	after := m.env.describe(t)
	if !proto.Equal(before.GetSchedule(), after.GetSchedule()) ||
		!slices.Equal(before.GetConflictToken(), after.GetConflictToken()) ||
		!proto.Equal(before.GetMemo(), after.GetMemo()) ||
		!proto.Equal(before.GetSearchAttributes(), after.GetSearchAttributes()) {
		t.Fatal("rejected migration mutation was not atomic")
	}
	m.check(t)
}

func (m *migrationModel) executeMigration(t *rapid.T) {
	if !m.pending {
		t.Skip("migration is not pending")
	}
	outcome := migrationOutcome(rapid.IntRange(
		int(migrationOutcomeSuccess), int(migrationOutcomeFailure),
	).Draw(t, "migration outcome"))
	var injected error
	switch outcome {
	case migrationOutcomeSuccess:
		m.env.history.pushMigrationError(nil)
	case migrationOutcomeAlreadyStarted:
		injected = serviceerror.NewWorkflowExecutionAlreadyStarted("migration already started", "", "")
		m.env.history.pushMigrationError(injected)
	case migrationOutcomeFailure:
		injected = serviceerror.NewInternal("migration start failure")
		m.env.history.pushMigrationError(injected)
	default:
		t.Fatalf("unexpected migration outcome %d", outcome)
	}
	m.calls++
	if outcome == migrationOutcomeFailure {
		m.env.executeOneError(t, injected)
	} else {
		task, _ := m.env.executeOne(t)
		m.pending = false
		m.closed = true
		if outcome == migrationOutcomeSuccess {
			m.successfulStarts++
		}
		redelivery := m.env.redeliver(t, task)
		if !redelivery.Dropped || redelivery.Executed != 0 {
			t.Fatalf("completed migration task redelivery: %+v", redelivery)
		}
	}
	m.check(t)
}

func (m *migrationModel) check(t *rapid.T) {
	t.Helper()
	internal := m.env.internal(t)
	if internal.closed != m.closed || internal.migrationPending != m.pending {
		t.Fatalf("migration state: got closed=%v pending=%v, want closed=%v pending=%v",
			internal.closed, internal.migrationPending, m.closed, m.pending)
	}
	if m.pending {
		if internal.preMigrationPaused != m.initialPaused || internal.preMigrationNotes != m.initialNotes {
			t.Fatalf("saved pre-migration state: got paused=%v notes=%q, want paused=%v notes=%q",
				internal.preMigrationPaused, internal.preMigrationNotes, m.initialPaused, m.initialNotes)
		}
	}
	if m.closed {
		checkClosedAPIs(t, m.env)
	} else {
		description := m.env.describe(t)
		if description.GetSchedule().GetState().GetPaused() != m.paused ||
			description.GetSchedule().GetState().GetNotes() != m.notes {
			t.Fatalf("migration-visible pause: got paused=%v notes=%q, want paused=%v notes=%q",
				description.GetSchedule().GetState().GetPaused(), description.GetSchedule().GetState().GetNotes(),
				m.paused, m.notes)
		}
	}

	history := m.env.history.snapshot()
	if len(history.migrationStarts) != m.calls || history.migrationSuccesses != m.successfulStarts {
		t.Fatalf("migration service calls: got calls=%d successes=%d, want calls=%d successes=%d",
			len(history.migrationStarts), history.migrationSuccesses, m.calls, m.successfulStarts)
	}
	requestIDs := make(map[string]struct{}, len(history.migrationStarts))
	for _, request := range history.migrationStarts {
		start := request.GetStartRequest()
		if start.GetWorkflowId() == "" || start.GetRequestId() == "" ||
			start.GetWorkflowIdConflictPolicy() != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
			t.Fatalf("invalid migration start request: %+v", start)
		}
		if _, duplicate := requestIDs[start.GetRequestId()]; duplicate {
			t.Fatalf("duplicate migration request ID %q", start.GetRequestId())
		}
		requestIDs[start.GetRequestId()] = struct{}{}
		var args schedulespb.StartScheduleArgs
		if err := sdk.PreferProtoDataConverter.FromPayloads(start.GetInput(), &args); err != nil {
			t.Fatalf("decode migration input: %v", err)
		}
		if args.GetSchedule().GetState().GetPaused() != m.initialPaused ||
			args.GetSchedule().GetState().GetNotes() != m.initialNotes ||
			args.GetState().GetScheduleId() != m.env.scheduleID {
			t.Fatal("migration export did not restore pre-migration state")
		}
	}
	if m.pending {
		runnable, err := m.env.engine.RunnableTasks(m.env.ref)
		mustNoError(t, err)
		if len(runnable) != 1 {
			t.Fatalf("pending migration runnable tasks: got %d, want 1", len(runnable))
		}
	} else {
		m.env.requireNoRunnableTasks(t)
	}
}

func newCallbackRecoveryModel(t *rapid.T) *callbackRecoveryModel {
	config := defaultModelEnvConfig()
	config.interval = 24 * time.Hour
	env := newSchedulerModelEnv(t, config)
	env.scheduleID = "callback-recovery-schedule"
	env.ref = chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID, BusinessID: env.scheduleID,
	})
	workflowID := "migrated-running-workflow"
	runID := "migrated-running-run"
	legacyState := &schedulespb.InternalState{
		Namespace: namespace, NamespaceId: namespaceID, ScheduleId: env.scheduleID,
		ConflictToken: 7, LastProcessedTime: timestamppb.New(env.timeSource.Now()),
	}
	info := &schedulepb.ScheduleInfo{
		ActionCount: 1, CreateTime: timestamppb.New(env.timeSource.Now()),
		RunningWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: workflowID, RunId: runID}},
	}
	request := schedmigration.LegacyToCreateFromMigrationStateRequest(
		modelSchedule(config), info, legacyState,
		&commonpb.SearchAttributes{}, &commonpb.Memo{}, env.timeSource.Now(),
	)
	_, err := scheduler.CreateFromMigrationStateForTest(env.engineCtx, env.handler, request)
	mustNoError(t, err)
	internal := env.internal(t)
	if len(internal.buffered) != 1 {
		t.Fatalf("migrated callback starts: got %d, want 1", len(internal.buffered))
	}
	return &callbackRecoveryModel{
		env: env, requestID: internal.buffered[0].requestID,
		workflowID: workflowID, runID: runID, state: callbackStatePending,
	}
}

func (m *callbackRecoveryModel) recoverCallback(t *rapid.T) {
	if m.state != callbackStatePending {
		t.Skip("callback recovery already completed")
	}
	outcome := callbackOutcome(rapid.IntRange(
		int(callbackOutcomeAttach), int(callbackOutcomeAttachFailure),
	).Draw(t, "callback outcome"))
	closedStatuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}
	closedStatus := closedStatuses[rapid.IntRange(0, len(closedStatuses)-1).Draw(t, "closed status")]
	var injected error
	switch outcome {
	case callbackOutcomeAttach, callbackOutcomeAttachAlreadyStarted, callbackOutcomeAttachFailure:
		m.env.history.pushDescribeOutcome(callbackDescribeResponse(m.workflowID, m.runID,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Time{}), nil)
		m.describeCalls++
		m.attachCalls++
		switch outcome {
		case callbackOutcomeAttach:
			m.env.workflows.pushAttachError(nil)
		case callbackOutcomeAttachAlreadyStarted:
			m.env.workflows.pushAttachError(serviceerror.NewWorkflowExecutionAlreadyStarted(
				"workflow completed during callback attachment", "", "",
			))
		case callbackOutcomeAttachFailure:
			injected = serviceerror.NewInternal("callback attachment failure")
			m.env.workflows.pushAttachError(injected)
		default:
			t.Fatalf("unexpected attach outcome %d", outcome)
		}
	case callbackOutcomeDescribeClosed:
		m.env.history.pushDescribeOutcome(callbackDescribeResponse(
			m.workflowID, m.runID, closedStatus, m.env.timeSource.Now(),
		), nil)
		m.describeCalls++
	case callbackOutcomeNotFound:
		m.env.history.pushDescribeOutcome(nil, serviceerror.NewNotFound("migrated workflow not found"))
		m.describeCalls++
	case callbackOutcomeDescribeFailure:
		injected = serviceerror.NewInternal("callback describe failure")
		m.env.history.pushDescribeOutcome(nil, injected)
		m.describeCalls++
	default:
		t.Fatalf("unexpected callback outcome %d", outcome)
	}

	if outcome == callbackOutcomeDescribeFailure || outcome == callbackOutcomeAttachFailure {
		m.env.executeOneError(t, injected)
	} else {
		task, _ := m.env.executeOne(t)
		m.savedTasks = append(m.savedTasks, task)
		m.env.drain(t)
		switch outcome {
		case callbackOutcomeAttach:
			m.state = callbackStateAttached
			m.canCallback = true
		case callbackOutcomeDescribeClosed:
			m.state = callbackStateCompleted
			m.status = closedStatus
		case callbackOutcomeNotFound:
			m.state = callbackStateCompleted
			m.status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		case callbackOutcomeAttachAlreadyStarted:
			m.state = callbackStateCompleted
			m.status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		default:
			t.Fatalf("unexpected successful callback outcome %d", outcome)
		}
	}
	m.check(t)
}

func (m *callbackRecoveryModel) completeAttached(t *rapid.T) {
	if m.state != callbackStateAttached {
		t.Skip("workflow has no attached callback")
	}
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}
	m.status = statuses[rapid.IntRange(0, len(statuses)-1).Draw(t, "completion status")]
	m.env.complete(t, m.requestID, m.status, []byte("callback completion"))
	m.env.drain(t)
	m.state = callbackStateCompleted
	m.check(t)
}

func (m *callbackRecoveryModel) duplicateCompletion(t *rapid.T) {
	if m.state != callbackStateCompleted || !m.canCallback {
		t.Skip("no completed attached callback")
	}
	m.env.complete(t, m.requestID, m.status, []byte("duplicate callback completion"))
	m.env.drain(t)
	m.check(t)
}

func (m *callbackRecoveryModel) redeliverRecovery(t *rapid.T) {
	if len(m.savedTasks) == 0 {
		t.Skip("no completed recovery task")
	}
	task := m.savedTasks[rapid.IntRange(0, len(m.savedTasks)-1).Draw(t, "recovery task")]
	beforeDescribe := len(m.env.history.snapshot().describes)
	beforeAttach := len(m.env.workflows.snapshot().startCalls)
	result := m.env.redeliver(t, task)
	if !result.Dropped || result.Executed != 0 {
		t.Fatalf("recovery task redelivery: %+v", result)
	}
	if len(m.env.history.snapshot().describes) != beforeDescribe ||
		len(m.env.workflows.snapshot().startCalls) != beforeAttach {
		t.Fatal("stale recovery task made service calls")
	}
	m.check(t)
}

func (m *callbackRecoveryModel) check(t *rapid.T) {
	t.Helper()
	internal := m.env.internal(t)
	if len(internal.buffered) != 1 {
		t.Fatalf("callback buffered starts: got %d, want 1", len(internal.buffered))
	}
	start := internal.buffered[0]
	if start.requestID != m.requestID || start.workflowID != m.workflowID || start.runID != m.runID {
		t.Fatalf("callback start identity changed: %+v", start)
	}
	if start.hasCallback != (m.state != callbackStatePending) {
		t.Fatalf("callback attachment: got %v for state %d", start.hasCallback, m.state)
	}
	if m.state == callbackStateCompleted {
		if start.completed != m.status {
			t.Fatalf("recovered completion: got %v, want %v", start.completed, m.status)
		}
	} else if start.completed != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		t.Fatalf("running recovered workflow has completion %v", start.completed)
	}

	description := m.env.describe(t)
	if description.GetInfo().GetActionCount() != 1 || len(description.GetInfo().GetRecentActions()) != 1 {
		t.Fatal("callback recovery changed action count or recent action cardinality")
	}
	expectedStatus := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	if m.state == callbackStateCompleted {
		expectedStatus = m.status
	}
	if description.GetInfo().GetRecentActions()[0].GetStartWorkflowStatus() != expectedStatus {
		t.Fatalf("callback recent status: got %v, want %v",
			description.GetInfo().GetRecentActions()[0].GetStartWorkflowStatus(), expectedStatus)
	}
	expectedRunning := 1
	if m.state == callbackStateCompleted {
		expectedRunning = 0
	}
	if len(description.GetInfo().GetRunningWorkflows()) != expectedRunning {
		t.Fatalf("callback running workflows: got %d, want %d",
			len(description.GetInfo().GetRunningWorkflows()), expectedRunning)
	}

	history := m.env.history.snapshot()
	if len(history.describes) != m.describeCalls {
		t.Fatalf("callback describe calls: got %d, want %d", len(history.describes), m.describeCalls)
	}
	workflow := m.env.workflows.snapshot()
	if len(workflow.startCalls) != m.attachCalls {
		t.Fatalf("callback attach calls: got %d, want %d", len(workflow.startCalls), m.attachCalls)
	}
	for _, request := range workflow.startCalls {
		if request.GetRequestId() != m.requestID || request.GetWorkflowId() != m.workflowID ||
			request.GetWorkflowIdConflictPolicy() != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING ||
			len(request.GetCompletionCallbacks()) != 1 {
			t.Fatalf("invalid callback attachment request: %+v", request)
		}
	}
	if m.state == callbackStatePending {
		runnable, err := m.env.engine.RunnableTasks(m.env.ref)
		mustNoError(t, err)
		if len(runnable) != 1 {
			t.Fatalf("callback recovery runnable tasks: got %d, want 1", len(runnable))
		}
	} else {
		m.env.requireNoRunnableTasks(t)
	}
}

func callbackDescribeResponse(
	workflowID string,
	runID string,
	status enumspb.WorkflowExecutionStatus,
	closeTime time.Time,
) *historyservice.DescribeWorkflowExecutionResponse {
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Status:    status,
	}
	if !closeTime.IsZero() {
		info.CloseTime = timestamppb.New(closeTime)
	}
	return &historyservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}
}
