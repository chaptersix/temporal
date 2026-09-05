package scheduler

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common"
)

func actionResult(start *schedulespb.BufferedStart) *schedulepb.ScheduleActionResult {
	result := &schedulepb.ScheduleActionResult{ScheduleTime: start.GetActualTime(), ActualTime: start.GetStartTime(), ActionExecutionResult: common.CloneProto(internal.ExecutionResult(start)), CloseTime: internal.CompletionTime(start)}
	if result.ActionExecutionResult.Execution.GetType() == enumspb.EXECUTION_TYPE_WORKFLOW {
		result.StartWorkflowResult = &commonpb.WorkflowExecution{WorkflowId: internal.TargetID(start), RunId: internal.RunID(start)}
		result.StartWorkflowStatus = result.ActionExecutionResult.GetWorkflowStatus()
	}
	return result
}
