package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

// This checks the normalized first-execution identity contract. It does not run
// ContinueAsNew/Reset or translate a public callback's current run identifier.
func TestUpdateChainCompletionFence(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	coordinatorStep(h, true)
	s, _ := h.root(false)
	id := s.OperationId
	allowAck(h, id)
	s, ctx := h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), localUpdateConfig(t, s.Revision+1)))
	commitGeneration(h, true)
	for _, event := range []decision.Event{
		{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW},
		{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "different-first-run", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
	} {
		before, physical := cloneNodes(h.durable), len(h.physical)
		s, ctx = h.root(true)
		require.Error(t, s.Apply(ctx.(chasm.MutableContext), event))
		requireRejectedCoordinatorUnchanged(h, before, physical)
	}
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
	s, ctx = h.root(false)
	require.Equal(t, scheduler2pb.Phase(decision.Completed), s.Operations[id].Get(ctx).Phase)
	require.Equal(t, int64(1), s.Started)
	require.Zero(t, s.UnconfirmedStarts)
	before := proto.Clone(s.RootState)
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "different-first-run", Outcome: decision.Failure, Failure: []byte{0xff}}, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, before, s.RootState)
}
