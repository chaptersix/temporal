package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
)

func sequentialBudget(h *treeHarness, count int64) {
	h.t.Helper()
	s, _ := h.root(true)
	s.Remaining = count
	h.commit(true)
}
func sequentialDue(h *treeHarness) *workflowservice.StartWorkflowExecutionRequest {
	h.t.Helper()
	_, wire := frozenRequest(h.t)
	h.apply(decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, CatchupDeadline: h.now, Request: wire}, true)
	return h.request()
}
func sequentialComplete(h *treeHarness, id string, result *commonpb.Payload) {
	h.t.Helper()
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, Result: completionWire(h.t, result)}, true)
}
func TestSequentialCarryoverRetentionAndLateResults(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	sequentialBudget(h, 6)
	var ids []string
	var previous *commonpb.Payload
	for index := 1; index <= 6; index++ {
		h.now = int64(index) * int64(time.Second)
		request := sequentialDue(h)
		require.NotNil(t, request)
		ids = append(ids, request.RequestId)
		if previous != nil {
			protorequire.ProtoEqual(t, &commonpb.Payloads{Payloads: []*commonpb.Payload{previous}}, request.LastCompletionResult)
			require.Nil(t, request.ContinuedFailure)
		}
		if index > 1 {
			h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: ids[index-2], ChainID: "late-old-chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: []byte{0xff}}, true)
			protorequire.ProtoEqual(t, request, h.request())
			current, _ := h.root(false)
			require.Equal(t, int64(index-1), current.Started)
			require.False(t, current.Paused)
		}
		previous = &commonpb.Payload{Data: []byte(fmt.Sprintf("result-%d", index))}
		sequentialComplete(h, request.RequestId, previous)
		s, _ := h.root(false)
		require.Len(t, s.Operations, min(index, 4))
		require.Equal(t, int64(index), s.Started)
		require.Equal(t, int64(6-index), s.Remaining)
	}
	s, _ := h.root(false)
	require.NotContains(t, s.Operations, ids[0])
	require.NotContains(t, s.Operations, ids[1])
	for _, id := range ids[2:] {
		require.Contains(t, s.Operations, id)
	}
	expected := completionWire(t, previous)
	for _, id := range ids[:5] {
		h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "stale-chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, Result: completionWire(t, &commonpb.Payload{Data: []byte("stale")})}, true)
		h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "stale-chain"}, true)
		s, ctx := h.root(false)
		require.Equal(t, ids[5], s.OperationId)
		require.Equal(t, int64(6), s.Started)
		require.Zero(t, s.Remaining)
		require.Equal(t, expected, s.Carryover.Get(ctx).Result)
	}
}
func TestSequentialSkipOccupiesUntilCompletion(t *testing.T) {
	t.Parallel()
	for _, phase := range []decision.Phase{decision.Pending, decision.Started, decision.Ambiguous} {
		t.Run(fmt.Sprintf("phase_%d", phase), func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			sequentialBudget(h, 2)
			first := sequentialDue(h)
			switch phase {
			case decision.Started:
				h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: first.RequestId + "/chain"}, true)
			case decision.Ambiguous:
				h.now += decision.RetryHorizon
				h.apply(decision.Event{Kind: decision.Expire, Incarnation: "incarnation", OperationID: first.RequestId}, true)
			case decision.Pending:
			default:
				t.Fatal("unsupported test phase")
			}
			h.now += int64(time.Second)
			skipped := h.now
			_, wire := frozenRequest(t)
			due := decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: skipped, CatchupDeadline: skipped, Request: wire}
			h.apply(due, true)
			s, ctx := h.root(false)
			require.Equal(t, first.RequestId, s.OperationId)
			require.Equal(t, int64(1), s.Remaining)
			require.Equal(t, skipped, s.Cursor)
			require.Equal(t, scheduler2pb.Phase(phase), s.Operations[s.OperationId].Get(ctx).Phase)
			sequentialComplete(h, first.RequestId, &commonpb.Payload{Data: []byte("completed")})
			h.apply(due, true)
			s, _ = h.root(false)
			require.Equal(t, first.RequestId, s.OperationId)
			require.Equal(t, int64(1), s.Remaining)
			h.now += int64(time.Second)
			second := sequentialDue(h)
			require.NotEqual(t, first.RequestId, second.RequestId)
			s, _ = h.root(false)
			require.Zero(t, s.Remaining)
			require.Equal(t, 1, logicalCount(h.durable))
		})
	}
}
func TestSequentialUnfinishedCapacityRejectsAtomically(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	sequentialBudget(h, 2)
	s, ctx := h.root(true)
	for index := 0; index < 4; index++ {
		id := fmt.Sprintf("unfinished-%d", index)
		s.Operations[id] = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.OperationState{Id: id, Phase: scheduler2pb.PHASE_STARTED})
	}
	h.commit(true)
	before := cloneNodes(h.durable)
	_, wire := frozenRequest(t)
	s, ctx = h.root(true)
	err := s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, CatchupDeadline: h.now, Request: wire})
	require.ErrorContains(t, err, "capacity")
	h.commit(true)
	require.Len(t, h.durable, len(before))
	for path, node := range before {
		protorequire.ProtoEqual(t, node.Data, h.durable[path].Data)
	}
	s, _ = h.root(false)
	require.Zero(t, s.Cursor)
	require.Equal(t, int64(2), s.Remaining)
	require.Empty(t, s.OperationId)
	require.Zero(t, logicalCount(h.durable))
}

func TestSequentialFailureCarriesPriorSuccessIntoThirdRequest(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	sequentialBudget(h, 3)
	first := sequentialDue(h)
	success := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("42")}
	sequentialComplete(h, first.RequestId, success)
	h.now += int64(time.Second)
	second := sequentialDue(h)
	failure := &failurepb.Failure{Message: "second failed", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "example"}}}
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: second.RequestId, ChainID: second.RequestId + "/chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: completionWire(t, failure)}, true)
	s, _ := h.root(false)
	require.False(t, s.Paused)
	h.now += int64(time.Second)
	third := sequentialDue(h)
	protorequire.ProtoEqual(t, &commonpb.Payloads{Payloads: []*commonpb.Payload{success}}, third.LastCompletionResult)
	protorequire.ProtoEqual(t, failure, third.ContinuedFailure)
	s, _ = h.root(false)
	require.Zero(t, s.Remaining)
	require.Equal(t, int64(2), s.Started)
	// Corrupt current carryover after admission; retries use the third snapshot.
	for path, node := range h.durable {
		if node.Data != nil && (path == "Carryover") {
			node.Data.Data = []byte{0xff}
		}
	}
	h.reload()
	protorequire.ProtoEqual(t, third, h.request())
}
