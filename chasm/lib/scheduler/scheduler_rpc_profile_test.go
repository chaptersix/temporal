package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
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

func (schedulerRPCProfiles) startRetryable() rpcgen.Behavior[
	*workflowservice.StartWorkflowExecutionRequest,
	*workflowservice.StartWorkflowExecutionResponse,
] {
	return rpcgen.Retryable[*workflowservice.StartWorkflowExecutionRequest, *workflowservice.StartWorkflowExecutionResponse](codes.Unavailable)
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
	return rpcgen.Success[*historyservice.DescribeWorkflowExecutionRequest](
		&historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING},
		},
	)
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
