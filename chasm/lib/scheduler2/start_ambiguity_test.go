package scheduler2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

// A transport-level retry can hide acceptance and a lost response, then return
// a permanent error from its final attempt. That error cannot release authority.
func TestTaskPermanentErrorAfterUnknownAcceptanceRetainsAuthority(t *testing.T) {
	t.Parallel()
	var accepted *workflowservice.StartWorkflowExecutionRequest
	e := newTaskEngine(t, func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		accepted = proto.Clone(request).(*workflowservice.StartWorkflowExecutionRequest)
		return "", serviceerror.NewPermissionDenied("permission changed after a hidden accepted attempt", "")
	})
	require.NoError(t, e.execute(e.ctx, e.h.node, e.h.physical[0]))
	require.NotNil(t, accepted)
	s, ctx := e.h.root(false)
	op := s.Operations[s.OperationId].Get(ctx)
	require.EqualValues(t, decision.Pending, op.Phase)
	require.Zero(t, s.Started)
	require.EqualValues(t, 1, s.Unfinished)
	e.h.now = op.Deadline
	e.h.apply(decision.Event{Kind: decision.Expire, Incarnation: s.Incarnation, OperationID: op.Id}, true)
	s, ctx = e.h.root(false)
	op = s.Operations[op.Id].Get(ctx)
	require.EqualValues(t, decision.Ambiguous, op.Phase)
	require.EqualValues(t, 1, s.Unfinished)
	frozen, err := s.request(ctx, op.Id)
	require.NoError(t, err)
	require.Nil(t, frozen, "bounded issuance has expired, but unresolved authority remains")
	e.h.apply(decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: op.Id, ChainID: "accepted-first-run", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
	s, ctx = e.h.root(false)
	require.EqualValues(t, 1, s.Started)
	require.Zero(t, s.Unfinished)
	require.Equal(t, "accepted-first-run", s.Operations[op.Id].Get(ctx).ChainId)
}
