package rpctest_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestScriptConsumesOutcomesAndClonesMessages(t *testing.T) {
	var script rpctest.Script[*wrapperspb.StringValue, *wrapperspb.StringValue]
	queuedResponse := wrapperspb.String("queued")
	defaultResponse := wrapperspb.String("default")
	script.Push("success", rpctest.Return[*wrapperspb.StringValue](queuedResponse))
	script.SetDefault("fallback", rpctest.Return[*wrapperspb.StringValue](defaultResponse))

	request := wrapperspb.String("request")
	response, err := script.Handle(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "queued", response.Value)
	require.Equal(t, 0, script.Pending())

	request.Value = "mutated request"
	queuedResponse.Value = "mutated response"
	response.Value = "mutated returned response"
	calls := script.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, "success", calls[0].Name)
	require.Equal(t, "request", calls[0].Request.Value)
	require.Equal(t, "queued", calls[0].Response.Value)

	response, err = script.Handle(context.Background(), wrapperspb.String("second"))
	require.NoError(t, err)
	require.Equal(t, "default", response.Value)
	require.Equal(t, "fallback", script.Calls()[1].Name)
}

func TestScriptFailuresAndMissingOutcome(t *testing.T) {
	retryErr := errors.New("retry")
	var script rpctest.Script[*wrapperspb.StringValue, *wrapperspb.StringValue]
	script.Push("retryable", rpctest.Fail[*wrapperspb.StringValue, *wrapperspb.StringValue](retryErr))

	response, err := script.Handle(context.Background(), wrapperspb.String("first"))
	require.Nil(t, response)
	require.ErrorIs(t, err, retryErr)
	require.ErrorIs(t, script.Calls()[0].Err, retryErr)

	response, err = script.Handle(context.Background(), wrapperspb.String("second"))
	require.Nil(t, response)
	require.ErrorContains(t, err, "no response configured")
	require.Len(t, script.Calls(), 2)
}

func TestScriptDoesNotCallResponderUnderLock(t *testing.T) {
	var script rpctest.Script[*wrapperspb.StringValue, *wrapperspb.StringValue]
	script.Push("derived", func(request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		require.Equal(t, 0, script.Pending())
		return wrapperspb.String(request.Value + "-response"), nil
	})

	response, err := script.Handle(context.Background(), wrapperspb.String("request"))
	require.NoError(t, err)
	require.Equal(t, "request-response", response.Value)
}

func TestScriptConcurrentCallHistory(t *testing.T) {
	var script rpctest.Script[*wrapperspb.Int64Value, *wrapperspb.Int64Value]
	script.SetDefault("derived", func(request *wrapperspb.Int64Value) (*wrapperspb.Int64Value, error) {
		return wrapperspb.Int64(request.Value + 1), nil
	})

	var waitGroup sync.WaitGroup
	results := make(chan error, 20)
	for value := int64(0); value < 20; value++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			_, err := script.Handle(context.Background(), wrapperspb.Int64(value))
			results <- err
		}()
	}
	waitGroup.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Len(t, script.Calls(), 20)
}

func TestScriptBindsToGeneratedClientMock(t *testing.T) {
	controller := gomock.NewController(t)
	client := historyservicemock.NewMockHistoryServiceClient(controller)
	var script rpctest.Script[
		*historyservice.DescribeWorkflowExecutionRequest,
		*historyservice.DescribeWorkflowExecutionResponse,
	]
	script.Push(
		"success",
		rpctest.Return[*historyservice.DescribeWorkflowExecutionRequest](
			&historyservice.DescribeWorkflowExecutionResponse{},
		),
	)
	client.EXPECT().
		DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(script.Handle)

	response, err := client.DescribeWorkflowExecution(
		context.Background(),
		&historyservice.DescribeWorkflowExecutionRequest{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, script.Calls(), 1)
}
