package scheduler

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

func workflowExecutions(executions []*commonpb.WorkflowExecution) []*commonpb.Execution {
	result := make([]*commonpb.Execution, 0, len(executions))
	for _, execution := range executions {
		result = append(result, &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: execution.WorkflowId, RunId: execution.RunId})
	}
	return result
}
func workflowProjection(executions []*commonpb.Execution) []*commonpb.WorkflowExecution {
	var result []*commonpb.WorkflowExecution
	for _, execution := range executions {
		if execution.Type == enumspb.EXECUTION_TYPE_WORKFLOW {
			result = append(result, &commonpb.WorkflowExecution{WorkflowId: execution.BusinessId, RunId: execution.RunId})
		}
	}
	return result
}
func (i *Invoker) terminationExecutions() []*commonpb.Execution {
	if i.TerminateExecutions != nil {
		return i.TerminateExecutions
	}
	return workflowExecutions(i.TerminateWorkflows)
}
func (i *Invoker) cancellationExecutions() []*commonpb.Execution {
	if i.CancelExecutions != nil {
		return i.CancelExecutions
	}
	return workflowExecutions(i.CancelWorkflows)
}
func (i *Invoker) setTerminationExecutions(executions []*commonpb.Execution) {
	i.TerminateExecutions = executions
	i.TerminateWorkflows = workflowProjection(executions)
}
func (i *Invoker) setCancellationExecutions(executions []*commonpb.Execution) {
	i.CancelExecutions = executions
	i.CancelWorkflows = workflowProjection(executions)
}
