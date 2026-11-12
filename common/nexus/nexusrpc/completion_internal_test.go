package nexusrpc

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowChainMetadataIsNotSerializedToHTTP(t *testing.T) {
	client := NewCompletionHTTPClient(CompletionHTTPClientOptions{})
	var baselineHeader http.Header
	var baselineBody []byte
	for _, firstRunID := range []string{"", "internal-first-run-id"} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://example.test/callback", nil)
		require.NoError(t, err)
		options := CompleteOperationOptions{Result: "result", WorkflowFirstExecutionRunID: firstRunID}
		require.NoError(t, options.applyToHTTPRequest(client, request))
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.NoError(t, request.Body.Close())
		if firstRunID == "" {
			baselineHeader = request.Header.Clone()
			baselineBody = body
		} else {
			require.Equal(t, baselineHeader, request.Header)
			require.Equal(t, baselineBody, body)
		}
	}
}
