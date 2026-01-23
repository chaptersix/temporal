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

func testLegacyToMigrateScheduleRequest(v1 *LegacyState, migrationTime time.Time) *schedulerpb.MigrateScheduleRequest {
	return LegacyToMigrateScheduleRequest(
		v1.Schedule,
		v1.Info,
		v1.State,
		v1.SearchAttributes,
		v1.Memo,
		migrationTime,
	)
}

func TestLegacyToMigrateScheduleRequest_BasicState(t *testing.T) {
	v1 := testLegacyState()
	migrationTime := time.Now().UTC()

	req := testLegacyToMigrateScheduleRequest(v1, migrationTime)
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

func TestLegacyToMigrateScheduleRequest_PreservesConflictToken(t *testing.T) {
	v1 := testLegacyState()
	v1.State.ConflictToken = 42
	migrationTime := time.Now().UTC()

	req := testLegacyToMigrateScheduleRequest(v1, migrationTime)

	require.Equal(t, int64(42), req.SchedulerState.ConflictToken)
}

func TestLegacyToMigrateScheduleRequest_ConvertsBufferedStarts(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

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

	req := testLegacyToMigrateScheduleRequest(v1, now)

	require.Len(t, req.InvokerState.BufferedStarts, 2)
	for _, start := range req.InvokerState.BufferedStarts {
		require.NotEmpty(t, start.RequestId)
		require.NotEmpty(t, start.WorkflowId)
		require.Equal(t, int64(0), start.Attempt)
		require.Nil(t, start.BackoffTime)
	}
}

func TestLegacyToMigrateScheduleRequest_ConvertsRunningWorkflows(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	v1.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
		{WorkflowId: "wf-1", RunId: "run-1"},
		{WorkflowId: "wf-2", RunId: "run-2"},
	}

	req := testLegacyToMigrateScheduleRequest(v1, now)

	runningCount := 0
	for _, start := range req.InvokerState.BufferedStarts {
		if start.RunId != "" && start.Completed == nil {
			runningCount++
		}
	}
	require.Equal(t, 2, runningCount)
}

func TestLegacyToMigrateScheduleRequest_ConvertsBackfillers(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

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

	req := testLegacyToMigrateScheduleRequest(v1, now)

	require.Len(t, req.Backfillers, 2)
	for id, backfiller := range req.Backfillers {
		require.NotEmpty(t, id)
		require.Equal(t, id, backfiller.BackfillId)
		require.NotNil(t, backfiller.GetBackfillRequest())
	}
}

func TestLegacyToMigrateScheduleRequest_ConvertsSearchAttributesAndMemo(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	v1.SearchAttributes = map[string]*commonpb.Payload{
		"CustomAttribute": {Data: []byte("attr-value")},
	}
	v1.Memo = map[string]*commonpb.Payload{
		"MemoKey": {Data: []byte("memo-value")},
	}

	req := testLegacyToMigrateScheduleRequest(v1, now)

	require.NotNil(t, req.SearchAttributes)
	require.Contains(t, req.SearchAttributes, "CustomAttribute")
	require.Equal(t, []byte("attr-value"), req.SearchAttributes["CustomAttribute"].Data)

	require.NotNil(t, req.Memo)
	require.Contains(t, req.Memo, "MemoKey")
	require.Equal(t, []byte("memo-value"), req.Memo["MemoKey"].Data)
}

func TestLegacyToMigrateScheduleRequest_ConvertsLastCompletionResult(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	v1.State.LastCompletionResult = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{Data: []byte("result-data")},
		},
	}
	v1.State.ContinuedFailure = &failurepb.Failure{
		Message: "last failure",
	}

	req := testLegacyToMigrateScheduleRequest(v1, now)

	require.NotNil(t, req.LastCompletionResult)
	require.NotNil(t, req.LastCompletionResult.Success)
	require.Equal(t, []byte("result-data"), req.LastCompletionResult.Success.Data)
	require.NotNil(t, req.LastCompletionResult.Failure)
	require.Equal(t, "last failure", req.LastCompletionResult.Failure.Message)
}

func TestCHASMToMigrateScheduleRequest(t *testing.T) {
	now := time.Now().UTC()
	chasm := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{ActionCount: 10},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 42,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				{NominalTime: timestamppb.New(now)},
			},
		},
		SearchAttributes: map[string]*commonpb.Payload{
			"Attr": {Data: []byte("value")},
		},
		Memo: map[string]*commonpb.Payload{
			"Memo": {Data: []byte("memo")},
		},
	}

	req := CHASMToMigrateScheduleRequest(chasm)

	require.Equal(t, testNamespaceID, req.NamespaceId)
	require.Equal(t, testNamespace, req.Namespace)
	require.Equal(t, testScheduleID, req.ScheduleId)
	require.Equal(t, int64(42), req.SchedulerState.ConflictToken)
	require.NotNil(t, req.GeneratorState)
	require.NotNil(t, req.InvokerState)
	require.Len(t, req.InvokerState.BufferedStarts, 1)
	require.Contains(t, req.SearchAttributes, "Attr")
	require.Contains(t, req.Memo, "Memo")
}
