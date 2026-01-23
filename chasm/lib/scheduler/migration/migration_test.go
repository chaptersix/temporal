package migration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testNamespace   = "test-ns"
	testNamespaceID = "test-ns-id"
	testScheduleID  = "test-sched-id"
)

func testSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(time.Minute),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "test-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "test-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			CatchupWindow: durationpb.New(5 * time.Minute),
		},
		State: &schedulepb.ScheduleState{},
	}
}

func testLegacyState() *LegacyState {
	now := time.Now().UTC()
	return &LegacyState{
		Schedule: testSchedule(),
		Info: &schedulepb.ScheduleInfo{
			ActionCount: 5,
		},
		State: &schedulespb.InternalState{
			Namespace:         testNamespace,
			NamespaceId:       testNamespaceID,
			ScheduleId:        testScheduleID,
			LastProcessedTime: timestamppb.New(now),
			ConflictToken:     1,
		},
	}
}

// testLegacyToImportScheduleRequest is a test helper that calls LegacyToImportScheduleRequest
// with fields from a LegacyState struct for convenience in tests.
func testLegacyToImportScheduleRequest(v1 *LegacyState, migrationTime time.Time) (*schedulerpb.ImportScheduleRequest, error) {
	return LegacyToImportScheduleRequest(
		v1.Schedule,
		v1.Info,
		v1.State,
		v1.SearchAttributes,
		v1.Memo,
		migrationTime,
	)
}

// importReqToCHASMState converts an ImportScheduleRequest to CHASMState for round-trip testing.
// This simulates what CreateSchedulerFromMigration does internally.
func importReqToCHASMState(req *schedulerpb.ImportScheduleRequest) *CHASMState {
	return &CHASMState{
		Scheduler:            req.SchedulerState,
		Generator:            req.GeneratorState,
		Invoker:              req.InvokerState,
		Backfillers:          req.Backfillers,
		LastCompletionResult: req.LastCompletionResult,
		SearchAttributes:     req.SearchAttributes,
		Memo:                 req.Memo,
	}
}

func TestCHASMToLegacy_BasicState(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{ActionCount: 10},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 2,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{},
		},
	}
	migrationTime := time.Now().UTC()

	v1, err := CHASMToLegacy(v2, migrationTime)
	require.NoError(t, err)
	require.NotNil(t, v1)

	// Verify state
	require.Equal(t, testNamespace, v1.State.Namespace)
	require.Equal(t, testNamespaceID, v1.State.NamespaceId)
	require.Equal(t, testScheduleID, v1.State.ScheduleId)
	require.Equal(t, int64(2), v1.State.ConflictToken)
	require.Equal(t, now, v1.State.LastProcessedTime.AsTime())
	require.True(t, v1.State.NeedRefresh) // Should be set for V1 to refresh watchers
}

func TestCHASMToLegacy_BufferedStarts(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				{
					NominalTime:   timestamppb.New(now),
					ActualTime:    timestamppb.New(now.Add(time.Second)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					RequestId:     "req-1",
					WorkflowId:    "wf-1",
					Attempt:       2,
					BackoffTime:   timestamppb.New(now.Add(time.Minute)),
				},
			},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify V2-specific fields are dropped
	require.Len(t, v1.State.BufferedStarts, 1)
	start := v1.State.BufferedStarts[0]
	require.Empty(t, start.RequestId, "request_id should be cleared")
	require.Empty(t, start.WorkflowId, "workflow_id should be cleared")
	require.Equal(t, int64(0), start.Attempt, "attempt should be cleared")
	require.Nil(t, start.BackoffTime, "backoff_time should be cleared")

	// Verify other fields preserved
	require.Equal(t, now, start.NominalTime.AsTime())
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, start.OverlapPolicy)
}

func TestCHASMToLegacy_Backfillers(t *testing.T) {
	now := time.Now().UTC()
	progressTime := now.Add(-30 * time.Minute)

	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{},
		Backfillers: map[string]*schedulerpb.BackfillerState{
			"bf-1": {
				BackfillId: "bf-1",
				Request: &schedulerpb.BackfillerState_BackfillRequest{
					BackfillRequest: &schedulepb.BackfillRequest{
						StartTime:     timestamppb.New(now.Add(-time.Hour)),
						EndTime:       timestamppb.New(now),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					},
				},
				LastProcessedTime: timestamppb.New(progressTime),
				Attempt:           1,
			},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify backfillers converted to ongoing backfills
	require.Len(t, v1.State.OngoingBackfills, 1)
	backfill := v1.State.OngoingBackfills[0]

	// StartTime should be set to LastProcessedTime per design doc
	require.Equal(t, progressTime, backfill.StartTime.AsTime())
	require.Equal(t, now, backfill.EndTime.AsTime())
}

func TestCHASMToLegacy_LastCompletionResult(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{},
		LastCompletionResult: &schedulerpb.LastCompletionResult{
			Success: &commonpb.Payload{Data: []byte("success-data")},
			Failure: &failurepb.Failure{Message: "failure-msg"},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// V1 uses Payloads (plural)
	require.NotNil(t, v1.State.LastCompletionResult)
	require.Len(t, v1.State.LastCompletionResult.Payloads, 1)
	require.Equal(t, []byte("success-data"), v1.State.LastCompletionResult.Payloads[0].Data)
	require.NotNil(t, v1.State.ContinuedFailure)
	require.Equal(t, "failure-msg", v1.State.ContinuedFailure.Message)
}

func TestRoundTrip_LegacyToCHASMToLegacy(t *testing.T) {
	original := testLegacyState()
	now := time.Now().UTC()

	// Add some state
	original.State.LastCompletionResult = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{Data: []byte("data")}},
	}
	original.State.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   timestamppb.New(now),
			ActualTime:    timestamppb.New(now),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}

	// V1 -> ImportScheduleRequest -> CHASMState
	importReq, err := testLegacyToImportScheduleRequest(original, now)
	require.NoError(t, err)
	v2 := importReqToCHASMState(importReq)

	// V2 -> V1
	roundTripped, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify key fields preserved
	require.Equal(t, original.State.Namespace, roundTripped.State.Namespace)
	require.Equal(t, original.State.NamespaceId, roundTripped.State.NamespaceId)
	require.Equal(t, original.State.ScheduleId, roundTripped.State.ScheduleId)
	require.Equal(t, original.State.ConflictToken, roundTripped.State.ConflictToken)
	require.Equal(t, original.State.LastProcessedTime.AsTime(), roundTripped.State.LastProcessedTime.AsTime())
	require.Len(t, roundTripped.State.BufferedStarts, 1)
	require.NotNil(t, roundTripped.State.LastCompletionResult)
}

func TestLegacyToImportScheduleRequest_RecentActionsAndRunningWorkflows(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add RecentActions and RunningWorkflows to legacy state
	v1.Info.RecentActions = []*schedulepb.ScheduleActionResult{
		{
			ScheduleTime: timestamppb.New(now.Add(-5 * time.Minute)),
			ActualTime:   timestamppb.New(now.Add(-5 * time.Minute)),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: "wf-1",
				RunId:      "run-1",
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			ScheduleTime: timestamppb.New(now.Add(-3 * time.Minute)),
			ActualTime:   timestamppb.New(now.Add(-3 * time.Minute)),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: "wf-2",
				RunId:      "run-2",
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	v1.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
		{WorkflowId: "wf-2", RunId: "run-2"},
		{WorkflowId: "wf-3", RunId: "run-3"},
	}

	req, err := testLegacyToImportScheduleRequest(v1, now)
	require.NoError(t, err)

	// In V2, RecentActions and RunningWorkflows are computed from BufferedStarts, not stored in Info
	require.NotNil(t, req.SchedulerState.Info)
	require.Nil(t, req.SchedulerState.Info.RecentActions)
	require.Nil(t, req.SchedulerState.Info.RunningWorkflows)

	// Verify BufferedStarts contains converted RecentActions and RunningWorkflows
	bufferedStarts := req.InvokerState.GetBufferedStarts()
	require.GreaterOrEqual(t, len(bufferedStarts), 4) // 2 recent actions + 2 running workflows

	// Find completed workflow (wf-1) in BufferedStarts
	var completedStart *schedulespb.BufferedStart
	for _, start := range bufferedStarts {
		if start.GetWorkflowId() == "wf-1" && start.GetRunId() == "run-1" {
			completedStart = start
			break
		}
	}
	require.NotNil(t, completedStart, "completed workflow wf-1 should be in BufferedStarts")
	require.NotNil(t, completedStart.Completed, "wf-1 should have Completed field set")
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, completedStart.Completed.Status)

	// Find running workflows (wf-2, wf-3) in BufferedStarts
	runningCount := 0
	for _, start := range bufferedStarts {
		if (start.GetWorkflowId() == "wf-2" || start.GetWorkflowId() == "wf-3") &&
			start.GetRunId() != "" && start.GetCompleted() == nil {
			runningCount++
		}
	}
	require.Equal(t, 2, runningCount, "both running workflows should be in BufferedStarts without Completed field")
}

func TestCHASMToLegacy_RecentActionsAndRunningWorkflows(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
			Info: &schedulepb.ScheduleInfo{
				ActionCount: 5,
			},
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				// Completed workflow
				{
					NominalTime: timestamppb.New(now.Add(-10 * time.Minute)),
					ActualTime:  timestamppb.New(now.Add(-10 * time.Minute)),
					StartTime:   timestamppb.New(now.Add(-10 * time.Minute)),
					WorkflowId:  "chasm-wf-1",
					RunId:       "chasm-run-1",
					Completed: &schedulespb.CompletedResult{
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						CloseTime: timestamppb.New(now.Add(-9 * time.Minute)),
					},
				},
				// Running workflow
				{
					NominalTime: timestamppb.New(now.Add(-5 * time.Minute)),
					ActualTime:  timestamppb.New(now.Add(-5 * time.Minute)),
					StartTime:   timestamppb.New(now.Add(-5 * time.Minute)),
					WorkflowId:  "chasm-wf-2",
					RunId:       "chasm-run-2",
					Completed:   nil, // Still running
				},
				// Another running workflow
				{
					NominalTime: timestamppb.New(now.Add(-3 * time.Minute)),
					ActualTime:  timestamppb.New(now.Add(-3 * time.Minute)),
					StartTime:   timestamppb.New(now.Add(-3 * time.Minute)),
					WorkflowId:  "chasm-wf-3",
					RunId:       "chasm-run-3",
					Completed:   nil, // Still running
				},
			},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify RecentActions are preserved (only completed workflows)
	require.NotNil(t, v1.Info)
	require.Len(t, v1.Info.RecentActions, 1)
	require.Equal(t, "chasm-wf-1", v1.Info.RecentActions[0].StartWorkflowResult.WorkflowId)
	require.Equal(t, "chasm-run-1", v1.Info.RecentActions[0].StartWorkflowResult.RunId)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, v1.Info.RecentActions[0].StartWorkflowStatus)

	// Verify RunningWorkflows are preserved (workflows with RunId but no Completed)
	require.Len(t, v1.Info.RunningWorkflows, 2)
	require.Equal(t, "chasm-wf-2", v1.Info.RunningWorkflows[0].WorkflowId)
	require.Equal(t, "chasm-run-2", v1.Info.RunningWorkflows[0].RunId)
	require.Equal(t, "chasm-wf-3", v1.Info.RunningWorkflows[1].WorkflowId)
	require.Equal(t, "chasm-run-3", v1.Info.RunningWorkflows[1].RunId)

	// Verify NeedRefresh is set to re-arm watchers for running workflows
	require.True(t, v1.State.NeedRefresh)
}

func TestRoundTrip_PreservesRecentActionsAndRunningWorkflows(t *testing.T) {
	original := testLegacyState()
	now := time.Now().UTC()

	// Add RecentActions and RunningWorkflows
	original.Info.RecentActions = []*schedulepb.ScheduleActionResult{
		{
			ScheduleTime: timestamppb.New(now.Add(-15 * time.Minute)),
			ActualTime:   timestamppb.New(now.Add(-15 * time.Minute)),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: "original-wf-1",
				RunId:      "original-run-1",
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	}
	original.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
		{WorkflowId: "original-wf-2", RunId: "original-run-2"},
	}

	// V1 -> ImportScheduleRequest -> CHASMState
	importReq, err := testLegacyToImportScheduleRequest(original, now)
	require.NoError(t, err)
	v2 := importReqToCHASMState(importReq)

	// V2 -> V1
	roundTripped, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify RecentActions preserved through round trip
	require.Len(t, roundTripped.Info.RecentActions, 1)
	require.Equal(t, "original-wf-1", roundTripped.Info.RecentActions[0].StartWorkflowResult.WorkflowId)
	require.Equal(t, "original-run-1", roundTripped.Info.RecentActions[0].StartWorkflowResult.RunId)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, roundTripped.Info.RecentActions[0].StartWorkflowStatus)

	// Verify RunningWorkflows preserved through round trip
	require.Len(t, roundTripped.Info.RunningWorkflows, 1)
	require.Equal(t, "original-wf-2", roundTripped.Info.RunningWorkflows[0].WorkflowId)
	require.Equal(t, "original-run-2", roundTripped.Info.RunningWorkflows[0].RunId)
}

// Tests for LegacyToImportScheduleRequest - returns proto ready for ImportSchedule RPC

func TestLegacyToImportScheduleRequest_BasicState(t *testing.T) {
	v1 := testLegacyState()
	migrationTime := time.Now().UTC()

	req, err := testLegacyToImportScheduleRequest(v1, migrationTime)
	require.NoError(t, err)
	require.NotNil(t, req)

	// Verify top-level request fields
	require.Equal(t, testNamespaceID, req.NamespaceId)
	require.Equal(t, testNamespace, req.Namespace)
	require.Equal(t, testScheduleID, req.ScheduleId)

	// Verify scheduler state
	require.NotNil(t, req.SchedulerState)
	require.Equal(t, testNamespace, req.SchedulerState.Namespace)
	require.Equal(t, testNamespaceID, req.SchedulerState.NamespaceId)
	require.Equal(t, testScheduleID, req.SchedulerState.ScheduleId)
	require.False(t, req.SchedulerState.Closed)

	// Verify generator state
	require.NotNil(t, req.GeneratorState)
	require.Equal(t, v1.State.LastProcessedTime.AsTime(), req.GeneratorState.LastProcessedTime.AsTime())

	// Verify invoker state
	require.NotNil(t, req.InvokerState)
}

func TestLegacyToImportScheduleRequest_PreservesConflictToken(t *testing.T) {
	v1 := testLegacyState()
	v1.State.ConflictToken = 42 // Set specific conflict token
	migrationTime := time.Now().UTC()

	req, err := testLegacyToImportScheduleRequest(v1, migrationTime)
	require.NoError(t, err)

	// Conflict token must be preserved for client compatibility
	require.Equal(t, int64(42), req.SchedulerState.ConflictToken)
}

func TestLegacyToImportScheduleRequest_ConvertsBufferedStarts(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add buffered starts without V2-specific fields
	v1.State.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   timestamppb.New(now),
			ActualTime:    timestamppb.New(now.Add(time.Second)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Manual:        false,
		},
		{
			NominalTime:   timestamppb.New(now.Add(time.Minute)),
			ActualTime:    timestamppb.New(now.Add(time.Minute + time.Second)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Manual:        true,
		},
	}

	req, err := testLegacyToImportScheduleRequest(v1, now)
	require.NoError(t, err)

	// Verify buffered starts in invoker state have V2-specific fields populated
	require.Len(t, req.InvokerState.BufferedStarts, 2)
	for _, start := range req.InvokerState.BufferedStarts {
		require.NotEmpty(t, start.RequestId, "request_id should be populated")
		require.NotEmpty(t, start.WorkflowId, "workflow_id should be populated")
		require.Equal(t, int64(0), start.Attempt, "attempt should be 0")
		require.Nil(t, start.BackoffTime, "backoff_time should be nil")
	}
}

func TestLegacyToImportScheduleRequest_ConvertsRunningWorkflows(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	v1.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
		{WorkflowId: "wf-1", RunId: "run-1"},
		{WorkflowId: "wf-2", RunId: "run-2"},
	}

	req, err := testLegacyToImportScheduleRequest(v1, now)
	require.NoError(t, err)

	// Running workflows should be converted to BufferedStarts with RunId set
	runningCount := 0
	for _, start := range req.InvokerState.BufferedStarts {
		if start.RunId != "" && start.Completed == nil {
			runningCount++
		}
	}
	require.Equal(t, 2, runningCount, "both running workflows should be in BufferedStarts")
}

func TestLegacyToImportScheduleRequest_ConvertsBackfillers(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add ongoing backfills
	v1.State.OngoingBackfills = []*schedulepb.BackfillRequest{
		{
			StartTime:     timestamppb.New(now.Add(-time.Hour)),
			EndTime:       timestamppb.New(now),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			StartTime:     timestamppb.New(now.Add(-2 * time.Hour)),
			EndTime:       timestamppb.New(now.Add(-time.Hour)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}

	req, err := testLegacyToImportScheduleRequest(v1, now)
	require.NoError(t, err)

	// Verify backfillers map is populated
	require.Len(t, req.Backfillers, 2)
	for id, backfiller := range req.Backfillers {
		require.NotEmpty(t, id, "backfiller ID should not be empty")
		require.Equal(t, id, backfiller.BackfillId, "backfiller ID should match map key")
		require.NotNil(t, backfiller.GetBackfillRequest())
	}
}

func TestLegacyToImportScheduleRequest_ConvertsSearchAttributesAndMemo(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add search attributes and memo
	v1.SearchAttributes = map[string]*commonpb.Payload{
		"CustomAttribute": {Data: []byte("attr-value")},
	}
	v1.Memo = map[string]*commonpb.Payload{
		"MemoKey": {Data: []byte("memo-value")},
	}

	req, err := testLegacyToImportScheduleRequest(v1, now)
	require.NoError(t, err)

	// Verify search attributes
	require.NotNil(t, req.SearchAttributes)
	require.Contains(t, req.SearchAttributes, "CustomAttribute")
	require.Equal(t, []byte("attr-value"), req.SearchAttributes["CustomAttribute"].Data)

	// Verify memo
	require.NotNil(t, req.Memo)
	require.Contains(t, req.Memo, "MemoKey")
	require.Equal(t, []byte("memo-value"), req.Memo["MemoKey"].Data)
}

func TestLegacyToImportScheduleRequest_ConvertsLastCompletionResult(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add last completion result
	v1.State.LastCompletionResult = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{Data: []byte("result-data")},
		},
	}
	v1.State.ContinuedFailure = &failurepb.Failure{
		Message: "last failure",
	}

	req, err := testLegacyToImportScheduleRequest(v1, now)
	require.NoError(t, err)

	// Verify last completion result
	require.NotNil(t, req.LastCompletionResult)
	require.NotNil(t, req.LastCompletionResult.Success)
	require.Equal(t, []byte("result-data"), req.LastCompletionResult.Success.Data)
	require.NotNil(t, req.LastCompletionResult.Failure)
	require.Equal(t, "last failure", req.LastCompletionResult.Failure.Message)
}
