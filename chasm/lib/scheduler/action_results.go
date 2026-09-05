package scheduler

import (
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
)

func actionResult(start *schedulespb.BufferedStart) *schedulepb.ScheduleActionResult {
	result := &schedulepb.ScheduleActionResult{ScheduleTime: start.GetActualTime(), ActualTime: start.GetStartTime(), ActionExecutionResult: common.CloneProto(internal.ExecutionResult(start)), CloseTime: internal.CompletionTime(start)}
	if result.ActionExecutionResult.Execution.GetType() == enumspb.EXECUTION_TYPE_WORKFLOW {
		result.StartWorkflowResult = &commonpb.WorkflowExecution{WorkflowId: internal.TargetID(start), RunId: internal.RunID(start)}
		result.StartWorkflowStatus = result.ActionExecutionResult.GetWorkflowStatus()
	}
	return result
}

func (s *Scheduler) recordActivityCompletion(ctx chasm.MutableContext, invoker *Invoker, start *schedulespb.BufferedStart, info *persistencespb.ChasmNexusCompletion) error {
	execution := common.CloneProto(internal.Execution(start))
	for _, link := range info.GetLinks() {
		if activity := link.GetActivity(); activity != nil && activity.GetActivityId() == execution.BusinessId {
			if execution.RunId != "" && execution.RunId != activity.GetRunId() {
				return nil
			}
			execution.RunId = activity.GetRunId()
		}
	}
	completion := implementation(s.Schedule.GetAction()).Completion(info, execution)
	start.Completion = completion.Result
	start.CompletionTime = info.CloseTime
	if start.GetStartTime() == nil && info.StartTime != nil {
		start.StartTime = info.StartTime
	}
	if internal.TracksExecution(start) && completion.Failed && s.Schedule.GetPolicies().GetPauseOnFailure() && !s.Schedule.GetState().GetPaused() {
		s.Schedule.State.Paused = true
		s.Schedule.State.Notes = fmt.Sprintf("paused, activity %s: %s", strings.ToLower(completion.Result.GetActivityStatus().String()), execution.BusinessId)
		s.updateConflictToken()
	}
	if closeTime := info.GetCloseTime().AsTime(); !closeTime.IsZero() {
		newTaggedMetricsHandler(ctx.MetricsHandler(), s).Timer(metrics.ScheduleCallbackLatency.Name()).Record(max(0, ctx.Now(s).Sub(closeTime)))
	}
	invoker.releaseWaiting(ctx, info.CloseTime)
	s.Generator.Get(ctx).Generate(ctx)
	return nil
}
