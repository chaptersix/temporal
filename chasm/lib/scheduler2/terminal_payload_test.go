package scheduler2

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

func TestTerminalReclaimsPayloadRetainingDeliveryFences(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	request, _ := frozenRequest(t)
	request.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: bytes.Repeat([]byte("x"), 2*1024*1024-16384)}}}
	wire, err := proto.Marshal(request)
	require.NoError(t, err)
	h.apply(decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, CatchupDeadline: h.now, Request: wire}, true)
	id := request.RequestId
	s, ctx := h.root(false)
	require.Greater(t, len(s.Requests[id].Get(ctx).Data), 2*1024*1024-16384)
	before := cloneNodes(h.durable)
	event := decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: id, ChainID: "first-run"}
	h.apply(event, false)
	require.Equal(t, before, h.durable)
	h.apply(event, true)
	s, ctx = h.root(false)
	require.NotContains(t, s.Requests, id)
	require.EqualValues(t, decision.Completed, s.Operations[id].Get(ctx).Phase)
	require.Zero(t, logicalCount(h.durable))
	require.Nil(t, h.request())
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "first-run"}, true)
	h.apply(event, true)
	s, ctx = h.root(false)
	require.NotContains(t, s.Requests, id)
	require.EqualValues(t, 1, s.Started)
	require.Zero(t, s.Unfinished)
	require.Equal(t, "first-run", s.Operations[id].Get(ctx).ChainId)
}
