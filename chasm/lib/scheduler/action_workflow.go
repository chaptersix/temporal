package scheduler

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
)

type workflowAction struct{}

func (workflowAction) Metadata(action *schedulepb.ScheduleAction) actionMetadata {
	spec := action.GetStartWorkflow()
	return actionMetadata{Kind: enumspb.EXECUTION_TYPE_WORKFLOW, Type: spec.GetWorkflowType().GetName(), IDBase: spec.GetWorkflowId(), TaskQueue: spec.GetTaskQueue().GetName(), SearchAttributes: spec.GetSearchAttributes()}
}
func (workflowAction) Validate(action *schedulepb.ScheduleAction) error {
	if action.GetStartWorkflow() == nil {
		return serviceerror.NewInvalidArgument("schedule must have an action")
	}
	return nil
}
func (workflowAction) Policies() *internal.PolicyRegistry { return internal.WorkflowPolicies() }
func (workflowAction) GenerateTargetID(base string, occurrence occurrenceContext) string {
	return internal.GenerateTimestampTargetID(base, occurrence.NominalTime)
}
func (workflowAction) ParticipatesInCompletionHistory() bool { return true }
func (workflowAction) Start(ctx context.Context, clients actionClients, input actionStartInput) (string, error) {
	scheduler, start, lastCompletionState := input.Scheduler, input.Occurrence, input.Previous
	requestSpec := scheduler.Schedule.GetAction().GetStartWorkflow()
	reusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	if start.Manual {
		reusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}

	tracksCompletionResult := internal.TracksCompletionResult(start.GetOverlapPolicy())
	var lcr []*commonpb.Payload
	continuedFailure := lastCompletionState.Failure
	if !tracksCompletionResult {
		continuedFailure = nil
	}
	if tracksCompletionResult && lastCompletionState.Success != nil {
		lcr = append(lcr, lastCompletionState.Success)
	}
	request := &workflowservice.StartWorkflowExecutionRequest{
		CompletionCallbacks:      []*commonpb.Callback{input.Callback},
		Header:                   requestSpec.Header,
		Identity:                 scheduler.identity(),
		Input:                    requestSpec.Input,
		Memo:                     requestSpec.Memo,
		Namespace:                scheduler.Namespace,
		RequestId:                start.RequestId,
		RetryPolicy:              requestSpec.RetryPolicy,
		SearchAttributes:         scheduler.startActionSearchAttributes(start.NominalTime.AsTime()),
		TaskQueue:                requestSpec.TaskQueue,
		UserMetadata:             requestSpec.UserMetadata,
		WorkflowExecutionTimeout: requestSpec.WorkflowExecutionTimeout,
		WorkflowId:               internal.TargetID(start),
		WorkflowIdReusePolicy:    reusePolicy,
		WorkflowRunTimeout:       requestSpec.WorkflowRunTimeout,
		WorkflowTaskTimeout:      requestSpec.WorkflowTaskTimeout,
		WorkflowType:             requestSpec.WorkflowType,
		Priority:                 requestSpec.Priority,
		ContinuedFailure:         continuedFailure,
		LastCompletionResult: &commonpb.Payloads{
			Payloads: lcr,
		},
	}
	if input.EnableVersioningOverride {
		request.VersioningOverride = requestSpec.VersioningOverride
	}

	result, err := clients.Frontend.StartWorkflowExecution(ctx, request)
	if err != nil {
		return "", err
	}
	return result.RunId, nil
}
func (workflowAction) Cancel(ctx context.Context, clients actionClients, scheduler *Scheduler, target *commonpb.Execution) error {
	_, err := clients.History.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId, CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{Namespace: scheduler.Namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: target.BusinessId}, Reason: "cancelled by schedule overlap policy", Identity: scheduler.identity(), FirstExecutionRunId: target.RunId},
	})
	return err
}
func (workflowAction) Terminate(ctx context.Context, clients actionClients, scheduler *Scheduler, target *commonpb.Execution) error {
	_, err := clients.History.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId, TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{Namespace: scheduler.Namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: target.BusinessId}, Reason: "terminated by schedule overlap policy", Identity: scheduler.identity(), FirstExecutionRunId: target.RunId},
	})
	return err
}
func (workflowAction) Completion(info *persistencespb.ChasmNexusCompletion, execution *commonpb.Execution) actionCompletion {
	status := enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	if _, ok := info.Outcome.(*persistencespb.ChasmNexusCompletion_Success); ok {
		status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}
	if failure := info.GetFailure(); failure != nil {
		status = executionStatusFromFailure(failure)
	}
	return actionCompletion{Result: &commonpb.ActionExecutionResult{Execution: execution, Status: &commonpb.ActionExecutionResult_WorkflowStatus{WorkflowStatus: status}}, Failed: countsAsFailureForPause(status)}
}
