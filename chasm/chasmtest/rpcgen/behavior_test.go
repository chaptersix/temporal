package rpcgen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/chasmtest/rpcgen"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"pgregory.net/rapid"
)

func TestBehaviorFamiliesRetainLabelsAndAmbiguousCommit(t *testing.T) {
	var script rpctest.Script[*wrapperspb.StringValue, *wrapperspb.StringValue]
	rpcgen.Retryable[*wrapperspb.StringValue, *wrapperspb.StringValue](codes.Unavailable).Queue(&script)
	rpcgen.AmbiguousCommit[*wrapperspb.StringValue](wrapperspb.String("committed")).Queue(&script)

	_, err := script.Handle(context.Background(), wrapperspb.String("first"))
	require.Equal(t, codes.Unavailable, status.Code(err))
	response, err := script.Handle(context.Background(), wrapperspb.String("second"))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "committed", response.GetValue())
	calls := script.Calls()
	require.Equal(t, []string{"retryable-Unavailable", "ambiguous-commit"}, []string{calls[0].Name, calls[1].Name})
}

func TestBehaviorDrawIsReplayable(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		behavior := rpcgen.Draw(t, "DescribeWorkflowExecution behavior",
			rpcgen.Retryable[*wrapperspb.StringValue, *wrapperspb.StringValue](codes.Unavailable),
			rpcgen.Terminal[*wrapperspb.StringValue, *wrapperspb.StringValue](codes.NotFound),
		)
		require.NotEmpty(t, behavior.Label)
	})
}

func TestRetrySequenceAndCancellation(t *testing.T) {
	var script rpctest.Script[*wrapperspb.StringValue, *wrapperspb.StringValue]
	rpcgen.QueueSequence(&script, rpcgen.RetrySequence(
		rpcgen.Retryable[*wrapperspb.StringValue, *wrapperspb.StringValue](codes.Unavailable),
		rpcgen.Success[*wrapperspb.StringValue](wrapperspb.String("success")),
	)...)
	_, err := script.Handle(context.Background(), wrapperspb.String("first"))
	require.Equal(t, codes.Unavailable, status.Code(err))
	response, err := script.Handle(context.Background(), wrapperspb.String("second"))
	require.NoError(t, err)
	require.Equal(t, "success", response.GetValue())

	var cancellation rpctest.Script[*wrapperspb.StringValue, *wrapperspb.StringValue]
	rpcgen.Cancellation[*wrapperspb.StringValue, *wrapperspb.StringValue]().Queue(&cancellation)
	_, err = cancellation.Handle(context.Background(), wrapperspb.String("third"))
	require.ErrorIs(t, err, context.Canceled)
}
