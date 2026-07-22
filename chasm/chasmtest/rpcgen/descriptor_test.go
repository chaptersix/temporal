package rpcgen_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func TestUnaryMethodGeneratesTypedTransportMessages(t *testing.T) {
	descriptor := historyservice.File_temporal_server_api_historyservice_v1_service_proto.
		Services().ByName("HistoryService").Methods().ByName("DescribeWorkflowExecution")
	method, err := rpcgen.NewUnaryMethod(
		descriptor,
		func() *historyservice.DescribeWorkflowExecutionRequest {
			return &historyservice.DescribeWorkflowExecutionRequest{}
		},
		func() *historyservice.DescribeWorkflowExecutionResponse {
			return &historyservice.DescribeWorkflowExecutionResponse{}
		},
	)
	require.NoError(t, err)
	require.Equal(t, "/temporal.server.api.historyservice.v1.HistoryService/DescribeWorkflowExecution", method.FullMethodName())

	request := method.RequestGenerator().Example(1)
	response := method.ResponseGenerator().Example(2)
	require.NotNil(t, request)
	require.NotNil(t, response)
	_, err = proto.Marshal(request)
	require.NoError(t, err)
	_, err = proto.Marshal(response)
	require.NoError(t, err)
}

func TestUnaryMethodGeneratorReplaysRapidDraws(t *testing.T) {
	descriptor := historyservice.File_temporal_server_api_historyservice_v1_service_proto.
		Services().ByName("HistoryService").Methods().ByName("DescribeWorkflowExecution")
	method, err := rpcgen.NewUnaryMethod(
		descriptor,
		func() *historyservice.DescribeWorkflowExecutionRequest {
			return &historyservice.DescribeWorkflowExecutionRequest{}
		},
		func() *historyservice.DescribeWorkflowExecutionResponse {
			return &historyservice.DescribeWorkflowExecutionResponse{}
		},
	)
	require.NoError(t, err)
	rapid.Check(t, func(t *rapid.T) {
		request := method.RequestGenerator().Draw(t, method.FullMethodName()+" request")
		require.NotNil(t, request)
	})
}

func TestUnaryMethodGeneratesClassifiedInboundRequests(t *testing.T) {
	descriptor := historyservice.File_temporal_server_api_historyservice_v1_service_proto.
		Services().ByName("HistoryService").Methods().ByName("DescribeWorkflowExecution")
	method, err := rpcgen.NewUnaryMethod(
		descriptor,
		func() *historyservice.DescribeWorkflowExecutionRequest {
			return &historyservice.DescribeWorkflowExecutionRequest{}
		},
		func() *historyservice.DescribeWorkflowExecutionResponse {
			return &historyservice.DescribeWorkflowExecutionResponse{}
		},
	)
	require.NoError(t, err)

	classes := map[rpcgen.InboundRequestClass]bool{}
	rapid.Check(t, func(t *rapid.T) {
		request := method.InboundRequestGenerator(rpcgen.InboundRequestProfile[*historyservice.DescribeWorkflowExecutionRequest]{
			Invalid: rapid.Just(&historyservice.DescribeWorkflowExecutionRequest{}),
		}).Draw(t, "DescribeWorkflowExecution inbound request")
		classes[request.Class] = true
		require.NotNil(t, request.Value)
		_, err := proto.Marshal(request.Value)
		require.NoError(t, err)
	})
	require.NotEmpty(t, classes)
}

func TestUnaryMethodRejectsMismatchedTypes(t *testing.T) {
	descriptor := historyservice.File_temporal_server_api_historyservice_v1_service_proto.
		Services().ByName("HistoryService").Methods().ByName("DescribeWorkflowExecution")
	_, err := rpcgen.NewUnaryMethod(
		descriptor,
		func() *historyservice.DescribeWorkflowExecutionResponse {
			return &historyservice.DescribeWorkflowExecutionResponse{}
		},
		func() *historyservice.DescribeWorkflowExecutionRequest {
			return &historyservice.DescribeWorkflowExecutionRequest{}
		},
	)
	require.ErrorContains(t, err, "request type does not match")
}
