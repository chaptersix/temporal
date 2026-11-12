package callback

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func TestInternalCompletionWorkflowChain(t *testing.T) {
	for _, firstRunID := range []string{"", "first-run-distinct-from-current"} {
		for _, failed := range []bool{false, true} {
			options := nexusrpc.CompleteOperationOptions{WorkflowFirstExecutionRunID: firstRunID, Result: &commonpb.Payload{Data: []byte("result")}}
			if failed {
				options.Error = &nexus.OperationError{State: nexus.OperationStateFailed, Cause: &nexus.FailureError{Failure: nexus.Failure{Message: "operation failed"}}}
			}
			invocation := invocableInternal{completion: options}
			request, err := invocation.getHistoryRequest([]byte("target-ref"), "operation-id")
			require.NoError(t, err)
			require.Equal(t, firstRunID, request.WorkflowFirstExecutionRunId)
			require.Equal(t, "operation-id", request.Completion.RequestId)
			require.Equal(t, []byte("target-ref"), request.Completion.ComponentRef)
			if failed {
				require.NotNil(t, request.GetFailure())
			} else {
				require.NotNil(t, request.GetSuccess())
			}
		}
	}
}
