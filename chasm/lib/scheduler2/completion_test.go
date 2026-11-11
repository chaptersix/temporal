package scheduler2

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func completionWire(t *testing.T, message proto.Message) []byte {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	require.NoError(t, err)
	return encoded
}
func seedCarryover(h *treeHarness, value *scheduler2pb.CarryoverState) {
	h.t.Helper()
	s, ctx := h.root(true)
	s.PauseOnFailure = true
	s.CarryoverSet = true
	s.Carryover = chasm.NewDataField(ctx.(chasm.MutableContext), value)
	h.commit(true)
}
func TestCallbackCarryoverMatrix(t *testing.T) {
	t.Parallel()
	priorResult := completionWire(t, &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("42")})
	priorFailure := completionWire(t, &failurepb.Failure{Message: "old"})
	nullResult := completionWire(t, &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}})
	valueResult := completionWire(t, &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("99")})
	canceled := completionWire(t, &failurepb.Failure{Message: "canceled", FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}}})
	timedOut := completionWire(t, &failurepb.Failure{Message: "timed out", FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{}}})
	terminated := completionWire(t, &failurepb.Failure{Message: "terminated", FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}}})
	for _, tc := range []struct {
		name            string
		outcome         decision.Outcome
		status          enumspb.WorkflowExecutionStatus
		result, failure []byte
		expected        *scheduler2pb.CarryoverState
		paused          bool
	}{
		{"absent", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil, nil, &scheduler2pb.CarryoverState{}, false},
		{"empty_wire", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte{0x12, 0}, nil, &scheduler2pb.CarryoverState{}, false},
		{"explicit_null", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, nullResult, nil, &scheduler2pb.CarryoverState{Result: nullResult}, false},
		{"value", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, valueResult, nil, &scheduler2pb.CarryoverState{Result: valueResult}, false},
		{"unknown_fields", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte{0x18, 1}, nil, &scheduler2pb.CarryoverState{Result: []byte{0x18, 1}}, false},
		{"empty_failure", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, nil, &scheduler2pb.CarryoverState{Result: priorResult, FailurePresent: true}, true},
		{"failed", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, priorFailure, &scheduler2pb.CarryoverState{Result: priorResult, Failure: priorFailure, FailurePresent: true}, true},
		{"canceled", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, nil, canceled, &scheduler2pb.CarryoverState{Result: priorResult, Failure: canceled, FailurePresent: true}, false},
		{"timed_out", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, nil, timedOut, &scheduler2pb.CarryoverState{Result: priorResult, Failure: timedOut, FailurePresent: true}, true},
		{"terminated", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, nil, terminated, &scheduler2pb.CarryoverState{Result: priorResult, Failure: terminated, FailurePresent: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			seedCarryover(h, &scheduler2pb.CarryoverState{Result: bytes.Clone(priorResult), Failure: bytes.Clone(priorFailure), FailurePresent: true})
			request := h.admit()
			event := decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: request.RequestId, ChainID: "chain", Outcome: tc.outcome, Status: tc.status, Result: bytes.Clone(tc.result), Failure: bytes.Clone(tc.failure)}
			h.apply(event, true)
			if len(event.Result) > 0 {
				event.Result[0] ^= 0xff
			}
			if len(event.Failure) > 0 {
				event.Failure[0] ^= 0xff
			}
			s, ctx := h.root(false)
			protorequire.ProtoEqual(t, tc.expected, s.Carryover.Get(ctx))
			require.Equal(t, tc.paused, s.Paused)
			require.Equal(t, int64(1), s.Started)
			require.Zero(t, s.Remaining)
			require.Zero(t, logicalCount(h.durable))
			// A valid but different late outcome cannot change either policy or carryover.
			h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: request.RequestId, ChainID: "chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: priorFailure}, true)
			s, ctx = h.root(false)
			protorequire.ProtoEqual(t, tc.expected, s.Carryover.Get(ctx))
			require.Equal(t, tc.paused, s.Paused)
		})
	}
}
func TestCallbackFailedPublicationAndPayloadLoading(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	seedCarryover(h, &scheduler2pb.CarryoverState{Result: completionWire(t, &commonpb.Payload{Data: []byte("old")})})
	request := h.admit()
	before := cloneNodes(h.durable)
	event := decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: request.RequestId, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}
	h.apply(event, false)
	require.Equal(t, before, h.durable)
	require.Equal(t, 1, logicalCount(h.durable))
	poisoned := 0
	for path, node := range h.durable {
		if strings.Contains(path, "Request") || strings.Contains(path, "Diagnostics") || path == "Carryover" {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 3, poisoned)
	h.reload()
	mutation := h.apply(event, true)
	for path := range mutation.UpdatedNodes {
		require.NotContains(t, path, "Request")
		require.NotContains(t, path, "Diagnostics")
	}
	s, ctx := h.root(false)
	protorequire.ProtoEqual(t, &scheduler2pb.CarryoverState{}, s.Carryover.Get(ctx))
	require.Panics(t, func() { s.Diagnostics.Get(ctx) })
	require.Panics(t, func() { s.Operations[s.OperationId].Get(ctx).Request.Get(ctx) })
	require.Equal(t, int64(1), s.Started)
}
func TestCarryoverFrozenAtAdmission(t *testing.T) {
	t.Parallel()
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "result_and_empty_failure", true: "cleared"}[empty], func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			carry := &scheduler2pb.CarryoverState{}
			payload := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}}
			if !empty {
				carry.Result = completionWire(t, payload)
				carry.FailurePresent = true
			}
			seedCarryover(h, carry)
			expected := h.admit()
			expected.LastCompletionResult = nil
			expected.ContinuedFailure = nil
			if !empty {
				expected.LastCompletionResult = &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
				expected.ContinuedFailure = &failurepb.Failure{}
			}
			protorequire.ProtoEqual(t, expected, h.request())
			for path, node := range h.durable {
				if path == "Carryover" {
					node.Data.Data = []byte{0xff}
				}
			}
			h.reload()
			// Retry request preparation must read only the admission snapshot.
			protorequire.ProtoEqual(t, expected, h.request())
			s, ctx := h.root(false)
			require.Panics(t, func() { s.Carryover.Get(ctx) })
		})
	}
}
func TestMalformedCallbackLeavesTreeUnchanged(t *testing.T) {
	t.Parallel()
	canceled := completionWire(t, &failurepb.Failure{FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}}})
	for _, tc := range []struct {
		name            string
		outcome         decision.Outcome
		status          enumspb.WorkflowExecutionStatus
		result, failure []byte
	}{
		{"result_wire", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, []byte{0xff}, nil},
		{"failure_wire", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, []byte{0xff}},
		{"failure_status_mismatch", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, canceled},
		{"result_limit", decision.Success, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, make([]byte, decision.MaxCompletionBytes+1), nil},
		{"failure_limit", decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, nil, make([]byte, decision.MaxCompletionBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			request := h.admit()
			before := cloneNodes(h.durable)
			s, ctx := h.root(true)
			err := s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: request.RequestId, ChainID: "chain", Outcome: tc.outcome, Status: tc.status, Result: tc.result, Failure: tc.failure})
			require.Error(t, err)
			// Commit the otherwise unchanged tree to expose any accidental partial mutation.
			h.commit(true)
			require.Len(t, h.durable, len(before))
			for path, node := range before {
				protorequire.ProtoEqual(t, node.Data, h.durable[path].Data)
			}
			require.Equal(t, logicalCount(before), logicalCount(h.durable))
		})
	}
}

func TestCallbackStaleDoesNotPrepareCarryover(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, incarnation, operation string }{
		{"old_incarnation", "old", "incarnation/automatic/1000000000/action/start"},
		{"unknown_operation", "incarnation", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			seedCarryover(h, &scheduler2pb.CarryoverState{})
			h.admit()
			for path, node := range h.durable {
				if path == "Carryover" || strings.Contains(path, "Request") || strings.Contains(path, "Diagnostics") {
					node.Data.Data = []byte{0xff}
				}
			}
			h.reload()
			h.apply(decision.Event{Kind: decision.Callback, Incarnation: tc.incarnation, OperationID: tc.operation, ChainID: "chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: []byte{0xff}}, true)
			s, ctx := h.root(false)
			require.Zero(t, s.Started)
			require.False(t, s.Paused)
			require.Equal(t, 1, logicalCount(h.durable))
			require.Panics(t, func() { s.Carryover.Get(ctx) })
		})
	}
}

func TestCompletionResourceBoundariesAndRecovery(t *testing.T) {
	t.Parallel()
	for _, outcome := range []decision.Outcome{decision.Success, decision.Failure} {
		for _, size := range []int{decision.MaxCompletionBytes - 1, decision.MaxCompletionBytes, decision.MaxCompletionBytes + 1} {
			name := fmt.Sprintf("outcome_%d_bytes_%d", outcome, size)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h := newTreeHarness(t)
				request := h.admit()
				event := decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: request.RequestId, ChainID: "chain", Outcome: outcome}
				wire := func(size int) []byte {
					if outcome == decision.Success {
						return completionWire(t, &commonpb.Payload{Data: bytes.Repeat([]byte("x"), size-4)})
					}
					return completionWire(t, &failurepb.Failure{Message: strings.Repeat("x", size-4)})
				}
				encoded := wire(size)
				require.Len(t, encoded, size, "wire size includes field tag and length")
				if outcome == decision.Success {
					event.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
					event.Result = encoded
				} else {
					event.Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
					event.Failure = encoded
				}
				if size > decision.MaxCompletionBytes {
					s, ctx := h.root(true)
					err := s.Apply(ctx.(chasm.MutableContext), event)
					require.ErrorContains(t, err, "prototype limit")
					h.commit(true)
					s, _ = h.root(false)
					require.Zero(t, s.Started)
					require.False(t, s.CarryoverSet)
					require.Equal(t, 1, logicalCount(h.durable))
					encoded = wire(decision.MaxCompletionBytes)
					if outcome == decision.Success {
						event.Result = encoded
					} else {
						event.Failure = encoded
					}
				}
				h.apply(event, true)
				s, ctx := h.root(false)
				expected := &scheduler2pb.CarryoverState{}
				if outcome == decision.Success {
					expected.Result = encoded
				} else {
					expected.Failure = encoded
					expected.FailurePresent = true
				}
				protorequire.ProtoEqual(t, expected, s.Carryover.Get(ctx))
				require.Equal(t, int64(1), s.Started)
				require.Zero(t, s.Remaining)
			})
		}
	}
}
