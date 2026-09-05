package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestActionCompletion(t *testing.T) {
	execution := &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "activity-id", RunId: "run-id"}
	activityCases := []struct {
		name   string
		info   *persistencespb.ChasmNexusCompletion
		status enumspb.ActivityExecutionStatus
		failed bool
	}{
		{name: "success", info: &persistencespb.ChasmNexusCompletion{Outcome: &persistencespb.ChasmNexusCompletion_Success{}}, status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
		{name: "application failure", info: completionWithFailure(&failurepb.Failure{}), status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, failed: true},
		{name: "timeout", info: completionWithFailure(&failurepb.Failure{FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{}}}), status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, failed: true},
		{name: "canceled", info: completionWithFailure(&failurepb.Failure{FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}}}), status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED},
		{name: "terminated", info: completionWithFailure(&failurepb.Failure{FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}}}), status: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED},
	}
	for _, tc := range activityCases {
		t.Run(tc.name, func(t *testing.T) {
			completion := (activityAction{}).Completion(tc.info, execution)
			require.Equal(t, tc.failed, completion.Failed)
			require.Equal(t, execution, completion.Result.GetExecution())
			require.Equal(t, tc.status, completion.Result.GetActivityStatus())
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, completion.Result.GetWorkflowStatus())
		})
	}

	workflowExecution := &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: "workflow-id", RunId: "run-id"}
	workflowCases := []struct {
		name   string
		info   *persistencespb.ChasmNexusCompletion
		status enumspb.WorkflowExecutionStatus
		failed bool
	}{
		{name: "success", info: &persistencespb.ChasmNexusCompletion{Outcome: &persistencespb.ChasmNexusCompletion_Success{}}, status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		{name: "application failure", info: completionWithFailure(&failurepb.Failure{}), status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, failed: true},
		{name: "timeout", info: completionWithFailure(&failurepb.Failure{FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{}}}), status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, failed: true},
		{name: "canceled", info: completionWithFailure(&failurepb.Failure{FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}}}), status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED},
		{name: "terminated", info: completionWithFailure(&failurepb.Failure{FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}}}), status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
	}
	for _, tc := range workflowCases {
		t.Run("workflow/"+tc.name, func(t *testing.T) {
			completion := (workflowAction{}).Completion(tc.info, workflowExecution)
			require.Equal(t, tc.failed, completion.Failed)
			require.Equal(t, workflowExecution, completion.Result.GetExecution())
			require.Equal(t, tc.status, completion.Result.GetWorkflowStatus())
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED, completion.Result.GetActivityStatus())
		})
	}
}

func completionWithFailure(failure *failurepb.Failure) *persistencespb.ChasmNexusCompletion {
	return &persistencespb.ChasmNexusCompletion{Outcome: &persistencespb.ChasmNexusCompletion_Failure{Failure: failure}}
}

func TestActionTargetID(t *testing.T) {
	nominal := time.Date(2024, time.January, 2, 3, 4, 5, 900000000, time.FixedZone("test", -6*60*60))
	want := "base-2024-01-02T09:04:05Z"
	occurrence := occurrenceContext{NominalTime: nominal}
	require.Equal(t, want, (workflowAction{}).GenerateTargetID("base", occurrence))
	require.Equal(t, want, (activityAction{}).GenerateTargetID("base", occurrence))
}
