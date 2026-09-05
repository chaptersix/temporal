package scheduler

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
)

type activityAction struct{}

func (activityAction) Metadata(action *schedulepb.ScheduleAction) actionMetadata {
	spec := action.GetStartActivity()
	return actionMetadata{Kind: enumspb.EXECUTION_TYPE_ACTIVITY, Type: spec.GetActivityType().GetName(), IDBase: spec.GetActivityId(), TaskQueue: spec.GetTaskQueue().GetName(), SearchAttributes: spec.GetSearchAttributes()}
}
func (activityAction) Validate(action *schedulepb.ScheduleAction) error {
	spec := action.GetStartActivity()
	if spec == nil || spec.GetActivityId() == "" || spec.GetActivityType().GetName() == "" || spec.GetTaskQueue().GetName() == "" {
		return serviceerror.NewInvalidArgument("scheduled activity requires ID, type, and task queue")
	}
	if spec.GetScheduleToCloseTimeout().AsDuration() <= 0 && spec.GetStartToCloseTimeout().AsDuration() <= 0 {
		return serviceerror.NewInvalidArgument("scheduled activity requires a positive schedule-to-close or start-to-close timeout")
	}
	return nil
}
func (activityAction) Policies() *internal.PolicyRegistry { return internal.ActivityPolicies() }
func (activityAction) GenerateTargetID(base string, occurrence occurrenceContext) string {
	return internal.GenerateTimestampTargetID(base, occurrence.NominalTime)
}
func (activityAction) ParticipatesInCompletionHistory() bool { return false }
func (activityAction) Start(ctx context.Context, clients actionClients, input actionStartInput) (string, error) {
	scheduler, start := input.Scheduler, input.Occurrence
	spec := scheduler.Schedule.GetAction().GetStartActivity()
	reuse := enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
	if start.Manual {
		reuse = enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	response, err := clients.Frontend.StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace: scheduler.Namespace, Identity: scheduler.identity(), RequestId: start.RequestId,
		ActivityId: internal.TargetID(start), ActivityType: spec.ActivityType, TaskQueue: spec.TaskQueue,
		ScheduleToCloseTimeout: spec.ScheduleToCloseTimeout, ScheduleToStartTimeout: spec.ScheduleToStartTimeout,
		StartToCloseTimeout: spec.StartToCloseTimeout, HeartbeatTimeout: spec.HeartbeatTimeout, RetryPolicy: spec.RetryPolicy,
		Input: spec.Input, IdReusePolicy: reuse, IdConflictPolicy: enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL,
		SearchAttributes: scheduler.startActionSearchAttributes(start.GetNominalTime().AsTime()), Header: spec.Header,
		UserMetadata: spec.UserMetadata, Priority: spec.Priority, CompletionCallbacks: []*commonpb.Callback{input.Callback}, StartDelay: spec.StartDelay,
	})
	if err != nil {
		return "", err
	}
	return response.RunId, nil
}
func (activityAction) Cancel(context.Context, actionClients, *Scheduler, *commonpb.Execution) error {
	return serviceerror.NewInvalidArgument("scheduled activities do not support cancellation overlap policies")
}
func (activityAction) Terminate(ctx context.Context, clients actionClients, scheduler *Scheduler, target *commonpb.Execution) error {
	_, err := clients.Frontend.TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{Namespace: scheduler.Namespace, ActivityId: target.BusinessId, RunId: target.RunId, Reason: "terminated by schedule overlap policy", Identity: scheduler.identity()})
	return err
}
func (activityAction) Completion(info *persistencespb.ChasmNexusCompletion, execution *commonpb.Execution) actionCompletion {
	status := enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	if _, ok := info.Outcome.(*persistencespb.ChasmNexusCompletion_Success); ok {
		status = enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	}
	if failure := info.GetFailure(); failure != nil {
		switch failure.FailureInfo.(type) {
		case *failurepb.Failure_CanceledFailureInfo:
			status = enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
		case *failurepb.Failure_TimeoutFailureInfo:
			status = enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		case *failurepb.Failure_TerminatedFailureInfo:
			status = enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
		default:
			status = enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
		}
	}
	failed := status == enumspb.ACTIVITY_EXECUTION_STATUS_FAILED || status == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	return actionCompletion{Result: &commonpb.ActionExecutionResult{Execution: execution, Status: &commonpb.ActionExecutionResult_ActivityStatus{ActivityStatus: status}}, Failed: failed}
}
