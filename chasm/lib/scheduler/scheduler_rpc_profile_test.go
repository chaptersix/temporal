package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"google.golang.org/grpc/codes"
	"pgregory.net/rapid"
)

// schedulerRPCProfiles records the dependency outcomes the scheduler can
// reason about. It is intentionally separate from descriptor structure.
type schedulerRPCProfiles struct{}

func (schedulerRPCProfiles) startStarted() rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Success[*workflowservice.StartWorkflowExecutionRequest](
		&workflowservice.StartWorkflowExecutionResponse{RunId: "started-run"},
	)
}

func (schedulerRPCProfiles) startSucceeded() rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Derived("started", func(request *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-" + request.GetRequestId()}
	})
}

func (schedulerRPCProfiles) startRetryable() rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return schedulerRPCProfiles{}.startRetryableWithCode(codes.Unavailable)
}

func (schedulerRPCProfiles) startRetryableWithCode(code codes.Code) rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Retryable[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse](code)
}

func (schedulerRPCProfiles) startTerminal() rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Terminal[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse](codes.InvalidArgument)
}

func (schedulerRPCProfiles) startAmbiguousCommit() rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.AmbiguousCommit[*workflowservice.StartWorkflowExecutionRequest](
		&workflowservice.StartWorkflowExecutionResponse{RunId: "committed-run"},
	)
}

func (schedulerRPCProfiles) describeRunning() rpcgen.Behavior[
	*historyservice.DescribeWorkflowExecutionRequest,
	*historyservice.DescribeWorkflowExecutionResponse,
] {
	return rpcgen.Derived("running", func(request *historyservice.DescribeWorkflowExecutionRequest) *historyservice.DescribeWorkflowExecutionResponse {
		return &historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
				Execution: request.GetRequest().GetExecution(),
				Status:    enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		}
	})
}

func (schedulerRPCProfiles) cancelAccepted() rpcgen.Behavior[
	*historyservice.RequestCancelWorkflowExecutionRequest,
	*historyservice.RequestCancelWorkflowExecutionResponse,
] {
	return rpcgen.Success[*historyservice.RequestCancelWorkflowExecutionRequest](
		&historyservice.RequestCancelWorkflowExecutionResponse{},
	)
}

func (schedulerRPCProfiles) terminateAccepted() rpcgen.Behavior[
	*historyservice.TerminateWorkflowExecutionRequest,
	*historyservice.TerminateWorkflowExecutionResponse,
] {
	return rpcgen.Success[*historyservice.TerminateWorkflowExecutionRequest](
		&historyservice.TerminateWorkflowExecutionResponse{},
	)
}

func (schedulerRPCProfiles) cancelRetryable() rpcgen.Behavior[
	*historyservice.RequestCancelWorkflowExecutionRequest,
	*historyservice.RequestCancelWorkflowExecutionResponse,
] {
	return rpcgen.Retryable[*historyservice.RequestCancelWorkflowExecutionRequest, *historyservice.RequestCancelWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) terminateRetryable() rpcgen.Behavior[
	*historyservice.TerminateWorkflowExecutionRequest,
	*historyservice.TerminateWorkflowExecutionResponse,
] {
	return rpcgen.Retryable[*historyservice.TerminateWorkflowExecutionRequest, *historyservice.TerminateWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) migrationStarted() rpcgen.Behavior[
	*historyservice.StartWorkflowExecutionRequest,
	*historyservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Derived("migration-started", func(request *historyservice.StartWorkflowExecutionRequest) *historyservice.StartWorkflowExecutionResponse {
		return &historyservice.StartWorkflowExecutionResponse{RunId: "migration-" + request.GetStartRequest().GetRequestId()}
	})
}

func (schedulerRPCProfiles) migrationRetryable() rpcgen.Behavior[
	*historyservice.StartWorkflowExecutionRequest,
	*historyservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Retryable[*historyservice.StartWorkflowExecutionRequest, *historyservice.StartWorkflowExecutionResponse](codes.Unavailable)
}

func (schedulerRPCProfiles) migrationTerminal() rpcgen.Behavior[
	*historyservice.StartWorkflowExecutionRequest,
	*historyservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Behavior[
		*historyservice.StartWorkflowExecutionRequest,
		*historyservice.StartWorkflowExecutionResponse,
	]{Label: "terminal-invalid-argument", Err: serviceerror.NewInvalidArgument("injected terminal failure")}
}

func (schedulerRPCProfiles) describeCompleted() rpcgen.Behavior[
	*historyservice.DescribeWorkflowExecutionRequest,
	*historyservice.DescribeWorkflowExecutionResponse,
] {
	return rpcgen.Derived("completed", func(request *historyservice.DescribeWorkflowExecutionRequest) *historyservice.DescribeWorkflowExecutionResponse {
		return &historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
				Execution: request.GetRequest().GetExecution(),
				Status:    enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		}
	})
}

func TestSchedulerRPCProfilesQueueLabeledBehaviors(t *testing.T) {
	profiles := schedulerRPCProfiles{}
	var script rpctest.Script[
		*workflowservice.StartWorkflowExecutionRequest,
		*workflowservice.StartWorkflowExecutionResponse,
	]
	profiles.startRetryable().Queue(&script)
	profiles.startAmbiguousCommit().Queue(&script)

	_, err := script.Handle(t.Context(), &workflowservice.StartWorkflowExecutionRequest{})
	require.Error(t, err)
	response, err := script.Handle(t.Context(), &workflowservice.StartWorkflowExecutionRequest{})
	require.Error(t, err)
	require.Equal(t, "committed-run", response.GetRunId())
	calls := script.Calls()
	require.Equal(t, []string{"retryable-Unavailable", "ambiguous-commit"}, []string{calls[0].Name, calls[1].Name})
}

func TestSchedulerRPCProfilesReplayBehaviorSelection(t *testing.T) {
	profiles := schedulerRPCProfiles{}
	rapid.Check(t, func(t *rapid.T) {
		behavior := rpcgen.Draw(t, "StartWorkflowExecution behavior",
			profiles.startRetryable(),
			profiles.startTerminal(),
		)
		var script rpctest.Script[
			*workflowservice.StartWorkflowExecutionRequest,
			*workflowservice.StartWorkflowExecutionResponse,
		]
		behavior.Queue(&script)
		_, err := script.Handle(t.Context(), &workflowservice.StartWorkflowExecutionRequest{})
		require.Error(t, err)
		calls := script.Calls()
		require.Len(t, calls, 1)
		require.Equal(t, behavior.Label, calls[0].Name)
	})
}
