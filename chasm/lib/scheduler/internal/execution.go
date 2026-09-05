package internal

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Execution reads old workflow state without storing activity data in workflow fields.
func Execution(start *schedulespb.BufferedStart) *commonpb.Execution {
	if start.GetExecution() != nil {
		return start.GetExecution()
	}
	return &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: start.GetWorkflowId(), RunId: start.GetRunId()}
}

func RunID(start *schedulespb.BufferedStart) string    { return Execution(start).GetRunId() }
func TargetID(start *schedulespb.BufferedStart) string { return Execution(start).GetBusinessId() }
func IsCompleted(start *schedulespb.BufferedStart) bool {
	return start.GetCompletion() != nil || start.GetCompleted() != nil
}
func CompletionTime(start *schedulespb.BufferedStart) *timestamppb.Timestamp {
	if start.GetCompletionTime() != nil {
		return start.GetCompletionTime()
	}
	return start.GetCompleted().GetCloseTime()
}
func TracksExecution(start *schedulespb.BufferedStart) bool {
	return start.GetOverlapPolicy() != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || start.GetCustomOverlapPolicy() != nil
}
func ExecutionResult(start *schedulespb.BufferedStart) *commonpb.ActionExecutionResult {
	if start.GetCompletion() != nil {
		return start.GetCompletion()
	}
	result := &commonpb.ActionExecutionResult{Execution: Execution(start)}
	if result.Execution.Type == enumspb.EXECUTION_TYPE_ACTIVITY {
		result.Status = &commonpb.ActionExecutionResult_ActivityStatus{ActivityStatus: enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING}
	} else {
		status := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		if start.GetCompleted() != nil {
			status = start.GetCompleted().GetStatus()
		}
		result.Status = &commonpb.ActionExecutionResult_WorkflowStatus{WorkflowStatus: status}
	}
	return result
}
