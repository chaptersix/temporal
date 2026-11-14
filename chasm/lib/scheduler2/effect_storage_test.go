package scheduler2

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func fixedEffectsState(target string, now int64) decision.State {
	return decision.State{Root: decision.Root{BlockingOperationID: target}, Effects: []decision.Effect{
		{ID: target + "/cancel", TargetID: target, Kind: decision.Cancel, Phase: decision.EffectPending, ChainID: "first-run", RetryAt: now, Deadline: now + decision.RetryHorizon},
		{ID: target + "/terminate", TargetID: target, Kind: decision.Terminate, Phase: decision.EffectPending, ChainID: "first-run", RetryAt: now, Deadline: now + decision.RetryHorizon},
	}}
}
func publishFixedEffects(t *testing.T, h *treeHarness, next decision.State, requests map[string][]byte, publish bool) {
	t.Helper()
	s, raw := h.root(true)
	before := decision.State{}
	for _, effect := range s.Effects {
		before.Effects = append(before.Effects, decodeEffect(effect))
	}
	s.BlockingOperationId = next.Root.BlockingOperationID
	s.Effects = nil
	s.publishEffects(raw.(chasm.MutableContext), before, next, requests)
	require.NoError(t, s.validateEffects())
	h.commit(publish)
}
func fixedEffectWires(t *testing.T, state decision.State) map[string][]byte {
	t.Helper()
	wires := make(map[string][]byte)
	for _, effect := range state.Effects {
		wire, err := freezeEffect(effect, &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace", WorkflowId: "workflow"})
		require.NoError(t, err)
		wires[effect.ID] = wire
	}
	return wires
}

func TestFixedEffectStoragePublicationAndRetry(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	state := fixedEffectsState("target", h.now)
	wires := fixedEffectWires(t, state)
	before := cloneNodes(h.durable)
	publishFixedEffects(t, h, state, wires, false)
	require.Equal(t, before, h.durable)
	publishFixedEffects(t, h, state, wires, true)
	s, ctx := h.root(false)
	require.Empty(t, s.EffectRequests)
	require.True(t, s.CancelRequest.IsSet())
	require.True(t, s.TerminateRequest.IsSet())
	require.Equal(t, 2, logicalCount(h.durable))
	for _, effect := range state.Effects {
		wire, err := s.effectRequestBytes(ctx, effect)
		require.NoError(t, err)
		require.Equal(t, wires[effect.ID], wire)
		wire[0] ^= 0xff
		again, err := s.effectRequestBytes(ctx, effect)
		require.NoError(t, err)
		require.Equal(t, wires[effect.ID], again)
	}
	cancel, err := s.effectRequest(ctx, state.Effects[0].ID)
	require.NoError(t, err)
	require.Equal(t, "first-run", cancel.Cancel.FirstExecutionRunId)
	require.Equal(t, state.Effects[0].ID, cancel.Cancel.RequestId)
	original := cancel.Cancel
	state.Effects[0].RetryAt++
	h.now++
	publishFixedEffects(t, h, state, nil, true)
	s, ctx = h.root(false)
	retry, err := s.effectRequest(ctx, state.Effects[0].ID)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, retry.Cancel)
	publishFixedEffects(t, h, decision.State{}, nil, true)
	s, _ = h.root(false)
	require.False(t, s.CancelRequest.IsSet())
	require.False(t, s.TerminateRequest.IsSet())
	require.Zero(t, logicalCount(h.durable))
}

func TestFixedEffectStorageMetadataDoesNotReadRequests(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	state := fixedEffectsState("target", h.now)
	publishFixedEffects(t, h, state, fixedEffectWires(t, state), true)
	for _, path := range []string{"CancelRequest", "TerminateRequest"} {
		require.Contains(t, h.durable, path)
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	s, ctx := h.root(false)
	require.NoError(t, s.validateEffects())
	_, err := s.stateFor(ctx, "")
	require.NoError(t, err)
	valid, err := (*EffectHandler)(nil).Validate(ctx, s, chasm.TaskInvocation{}, &pb.EffectTask{EffectId: state.Effects[0].ID, WakeTime: state.Effects[0].RetryAt})
	require.NoError(t, err)
	require.True(t, valid)
	// Retention and target cleanup use only ownership metadata, even when old RPC
	// bytes are corrupt. An addressed issuance still fails closed on those bytes.
	require.Panics(t, func() { _, _ = s.effectRequest(ctx, state.Effects[0].ID) })
	h.reload()
	next := fixedEffectsState("replacement", h.now)
	publishFixedEffects(t, h, next, fixedEffectWires(t, next), true)
	s, ctx = h.root(false)
	request, err := s.effectRequest(ctx, next.Effects[0].ID)
	require.NoError(t, err)
	require.Equal(t, next.Effects[0].ID, request.Cancel.RequestId)
}

func TestFixedEffectStorageMembershipAndEnvelope(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"missing", "orphan", "legacy", "owner", "encoding"} {
		t.Run(kind, func(t *testing.T) {
			h := newReadinessHarness(t)
			state := fixedEffectsState("target", h.now)
			publishFixedEffects(t, h, state, fixedEffectWires(t, state), true)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			switch kind {
			case "missing":
				s.CancelRequest = chasm.NewEmptyField[*pb.Payload]()
			case "orphan":
				s.Effects = s.Effects[1:]
			case "legacy":
				s.EffectRequests["legacy"] = chasm.NewDataField(ctx, &pb.Payload{})
			case "owner":
				s.CancelRequest = chasm.NewDataField(ctx, encodeEffectRequest("foreign", []byte{1}))
			case "encoding":
				s.CancelRequest = chasm.NewDataField(ctx, &pb.Payload{Data: []byte{1, 0x81, 0, 1}})
			default:
				t.Fatal("unknown corruption")
			}
			if kind == "owner" || kind == "encoding" {
				require.NoError(t, s.validateEffects())
				_, err := s.effectRequest(raw, state.Effects[0].ID)
				require.Error(t, err)
			} else {
				require.Error(t, s.validateEffects())
			}
		})
	}
}

func TestFixedEffectStorageNewTargetFreezesNewBytes(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	old := fixedEffectsState("old", h.now)
	publishFixedEffects(t, h, old, fixedEffectWires(t, old), true)
	next := fixedEffectsState("new", h.now)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	start := &workflowservice.StartWorkflowExecutionRequest{Namespace: "new-namespace", WorkflowId: "new-workflow"}
	wire, err := proto.Marshal(start)
	require.NoError(t, err)
	s.OperationRecords.Put(ctx, "new", &pb.OperationState{Id: "new", Phase: pb.PHASE_STARTED})
	s.RequestRecords.Put(ctx, "new", &pb.Payload{Data: wire})
	s.OperationStore.OperationCount = 1
	s.OperationStore.RequestCount = 1
	h.commit(true)
	s, raw = h.root(false)
	prepared, requests, err := s.prepareEffects(raw, next)
	require.NoError(t, err)
	require.Len(t, requests, 2)
	publishFixedEffects(t, h, prepared, requests, true)
	s, raw = h.root(false)
	request, err := s.effectRequest(raw, next.Effects[0].ID)
	require.NoError(t, err)
	require.Equal(t, "new-namespace", request.Cancel.Namespace)
	require.Equal(t, "new-workflow", request.Cancel.WorkflowExecution.WorkflowId)
	require.Equal(t, next.Effects[0].ID, request.Cancel.RequestId)
}

func TestFixedEffectStorageTaskChurnBound(t *testing.T) {
	t.Parallel()
	h := newReadinessHarness(t)
	for iteration := range 24 {
		state := fixedEffectsState(fmt.Sprintf("target/%d", iteration), h.now)
		publishFixedEffects(t, h, state, fixedEffectWires(t, state), true)
		for range 3 {
			h.now++
			for i := range state.Effects {
				state.Effects[i].RetryAt = h.now
			}
			publishFixedEffects(t, h, state, nil, true)
			s, _ := h.root(false)
			require.Empty(t, s.EffectRequests)
			require.Equal(t, 2, s.effectRequestCount())
			require.LessOrEqual(t, logicalCount(h.durable), 2)
		}
	}
	publishFixedEffects(t, h, decision.State{}, nil, true)
	require.Zero(t, logicalCount(h.durable))
}

func TestEffectRequestEnvelopeCanonical(t *testing.T) {
	t.Parallel()
	wire := bytes.Repeat([]byte{0x42}, 1024)
	record := encodeEffectRequest("owner", wire)
	decoded, err := decodeEffectRequest(record, "owner")
	require.NoError(t, err)
	require.Equal(t, wire, decoded)
	for _, data := range [][]byte{nil, {2}, {1, 0}, {1, 0xff}, {1, 0x81, 0, 'x'}, {1, 5, 'x'}} {
		_, err := decodeEffectRequest(&pb.Payload{Data: data}, "owner")
		require.Error(t, err)
	}
}
