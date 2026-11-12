package scheduler2

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func replaceBundleInput(h *treeHarness, data []byte) {
	h.t.Helper()
	s, ctx := h.root(true)
	definition, err := s.publicDefinition(ctx)
	require.NoError(h.t, err)
	definition.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	definition.Action.GetStartWorkflow().Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: data}}}
	require.NoError(h.t, s.UpdatePublic(ctx.(chasm.MutableContext), &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition}, schedulerconfig.DefaultTweakables))
	h.commit(true)
}
func admitBundle(h *treeHarness) string {
	h.t.Helper()
	h.now += int64(time.Second)
	s, ctx := h.root(true)
	wire, err := s.generationRequest(ctx)
	require.NoError(h.t, err)
	require.NoError(h.t, s.applyTransition(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: h.now, Actual: h.now, CatchupDeadline: h.now, Request: wire, WorkflowID: "action"}))
	id := s.OperationId
	h.commit(true)
	return id
}
func TestSharedPayloadMultipleGenerationsAndGC(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	oldInput := bytes.Repeat([]byte("a"), 128*1024)
	replaceBundleInput(h, oldInput)
	first := admitBundle(h)
	s, ctx := h.root(false)
	oldRef := s.Requests[first].Get(ctx).ActionRef
	require.Equal(t, s.PublicActionRef, oldRef)
	require.Len(t, s.PayloadBlobs, 1)
	require.Less(t, len(s.Requests[first].Get(ctx).Data), 4096)
	require.Empty(t, s.PublicSchedule.Get(ctx).Action.GetStartWorkflow().Input)
	nextInput := bytes.Repeat([]byte("b"), 128*1024)
	replaceBundleInput(h, nextInput)
	second := admitBundle(h)
	h.reload()
	s, ctx = h.root(false)
	require.Len(t, s.PayloadBlobs, 2)
	oldRequest, err := s.assembleRequest(ctx, s.Requests[first].Get(ctx))
	require.NoError(t, err)
	newRequest, err := s.assembleRequest(ctx, s.Requests[second].Get(ctx))
	require.NoError(t, err)
	require.Equal(t, oldInput, oldRequest.Input.Payloads[0].Data)
	require.Equal(t, nextInput, newRequest.Input.Payloads[0].Data)
	oldRequest.Input.Payloads[0].Data[0] = 'x'
	reread, err := s.assembleRequest(ctx, s.Requests[first].Get(ctx))
	require.NoError(t, err)
	require.Equal(t, oldInput, reread.Input.Payloads[0].Data)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: first, ChainID: "chain"}, true)
	s, _ = h.root(false)
	require.NotContains(t, s.Requests, first)
	require.NotContains(t, s.PayloadBlobs, oldRef)
	require.Len(t, s.PayloadBlobs, 1)
}

func TestSharedPayloadCarryoverSnapshotAndPresence(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	replaceBundleInput(h, bytes.Repeat([]byte("i"), 128*1024))
	s, ctx := h.root(true)
	result := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: bytes.Repeat([]byte("r"), 128*1024)}
	failure := &failurepb.Failure{Message: "failure", EncodedAttributes: &commonpb.Payload{Data: bytes.Repeat([]byte("f"), 128*1024)}}
	result.ProtoReflect().SetUnknown([]byte{0xc0, 0x3e, 0x07})
	failure.ProtoReflect().SetUnknown([]byte{0xc0, 0x3e, 0x08})
	resultWire, err := payloadWire(result)
	require.NoError(t, err)
	failureWire, err := payloadWire(failure)
	require.NoError(t, err)
	stored, blobs, err := s.prepareCarryover(ctx, &scheduler2pb.CarryoverState{Result: resultWire, Failure: failureWire, FailurePresent: true})
	require.NoError(t, err)
	s.publishBlobs(ctx.(chasm.MutableContext), blobs)
	s.Carryover = chasm.NewDataField(ctx.(chasm.MutableContext), stored)
	s.CarryoverSet = true
	// SKIP tracks completion and therefore snapshots carryover at admission.
	s.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	h.commit(true)
	id := admitBundle(h)
	h.reload()
	s, ctx = h.root(false)
	request, err := s.assembleRequest(ctx, s.Requests[id].Get(ctx))
	require.NoError(t, err)
	protorequire.ProtoEqual(t, result, request.LastCompletionResult.Payloads[0])
	protorequire.ProtoEqual(t, failure, request.ContinuedFailure)
	require.Equal(t, s.Carryover.Get(ctx).ResultRef, s.Requests[id].Get(ctx).ResultRef)
	require.Equal(t, s.Carryover.Get(ctx).FailureRef, s.Requests[id].Get(ctx).FailureRef)
	require.Len(t, s.PayloadBlobs, 3)
	s, ctx = h.root(true)
	empty, blobs, err := s.prepareCarryover(ctx, &scheduler2pb.CarryoverState{})
	require.NoError(t, err)
	s.publishBlobs(ctx.(chasm.MutableContext), blobs)
	s.Carryover = chasm.NewDataField(ctx.(chasm.MutableContext), empty)
	s.collectPayloadBlobs(ctx)
	h.commit(true)
	s, ctx = h.root(false)
	request, err = s.assembleRequest(ctx, s.Requests[id].Get(ctx))
	require.NoError(t, err)
	protorequire.ProtoEqual(t, result, request.LastCompletionResult.Payloads[0])
	require.Len(t, s.PayloadBlobs, 3, "live issuance retains old carryover")
	for _, input := range []*commonpb.Payloads{nil, {}} {
		wire, err := payloadWire(&workflowservice.StartWorkflowExecutionRequest{Input: input})
		require.NoError(t, err)
		s, ctx = h.root(true)
		record, blobs, err := s.splitRequest(ctx, wire)
		require.NoError(t, err)
		s.publishBlobs(ctx.(chasm.MutableContext), blobs)
		assembled, err := s.assembleRequest(ctx, record)
		require.NoError(t, err)
		expected := &workflowservice.StartWorkflowExecutionRequest{Input: input}
		require.True(t, proto.Equal(expected, assembled))
	}
}

func TestSharedPayloadMissingReferenceRejects(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	id := admitBundle(h)
	s, ctx := h.root(true)
	record := s.Requests[id].Get(ctx)
	delete(s.PayloadBlobs, record.ActionRef)
	_, err := s.assembleRequest(ctx, record)
	require.ErrorContains(t, err, "referenced schedule payload is missing")
}

func TestSharedPayloadReplacementPublicationRollback(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	e.h = executingPublicTree(t, nil)
	replaceBundleInput(e.h, bytes.Repeat([]byte("a"), 128*1024))
	admitBundle(e.h)
	before := cloneNodes(e.h.durable)
	s, ctx := e.h.root(false)
	definition, err := s.publicDefinition(ctx)
	require.NoError(t, err)
	definition.Action.GetStartWorkflow().Input.Payloads[0].Data = bytes.Repeat([]byte("b"), 128*1024)
	request := &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition}
	e.failPublication = true
	_, _, err = chasm.UpdateComponent(e.ctx, e.ref, func(s *Scheduler, cc chasm.MutableContext, r *workflowservice.UpdateScheduleRequest) (chasm.NoValue, error) {
		return nil, s.UpdatePublic(cc, r, schedulerconfig.DefaultTweakables)
	}, request, chasm.WithRequestID("replacement"))
	require.ErrorIs(t, err, errManualPublication)
	require.Equal(t, before, e.h.durable)
	used, err := e.h.node.HasRequestID("replacement", e.maxAge)
	require.NoError(t, err)
	require.False(t, used)
}

func TestSharedPayloadUnknownEnvelopeFieldsSurvive(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(true)
	request := &workflowservice.StartWorkflowExecutionRequest{Input: &commonpb.Payloads{}}
	// Unknown field1000, varint7 must remain in the frozen request skeleton.
	request.ProtoReflect().SetUnknown([]byte{0xc0, 0x3e, 0x07})
	wire, err := payloadWire(request)
	require.NoError(t, err)
	record, blobs, err := s.splitRequest(ctx, wire)
	require.NoError(t, err)
	s.publishBlobs(ctx.(chasm.MutableContext), blobs)
	assembled, err := s.assembleRequest(ctx, record)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, request, assembled)
	encoded, err := payloadWire(assembled)
	require.NoError(t, err)
	require.Equal(t, wire, encoded)
	record.ActionRef = ""
	_, err = s.assembleRequest(ctx, record)
	require.ErrorContains(t, err, "reference is missing")
}

func TestSharedPayloadEmptyFailureAndExplicitNull(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(true)
	null := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}}
	nullWire, err := payloadWire(null)
	require.NoError(t, err)
	expected := &scheduler2pb.CarryoverState{Result: nullWire, FailurePresent: true}
	stored, blobs, err := s.prepareCarryover(ctx, expected)
	require.NoError(t, err)
	require.NotEmpty(t, stored.ResultRef)
	require.NotEmpty(t, stored.FailureRef)
	s.publishBlobs(ctx.(chasm.MutableContext), blobs)
	s.Carryover = chasm.NewDataField(ctx.(chasm.MutableContext), stored)
	s.CarryoverSet = true
	h.commit(true)
	h.reload()
	s, ctx = h.root(false)
	actual, err := s.carryoverValue(ctx)
	require.NoError(t, err)
	require.Equal(t, nullWire, actual.Result)
	require.True(t, actual.FailurePresent)
	require.Empty(t, actual.Failure)
	for _, kind := range []string{"result", "failure"} {
		t.Run(kind, func(t *testing.T) {
			s, ctx := h.root(true)
			saved := s.Carryover.Get(ctx)
			id := saved.ResultRef
			if kind == "failure" {
				id = saved.FailureRef
			}
			delete(s.PayloadBlobs, id)
			_, err := s.carryoverValue(ctx)
			require.ErrorContains(t, err, "referenced schedule payload is missing")
			h.reload()
		})
	}
	s, ctx = h.root(true)
	s.PublicActionRef = "missing"
	_, err = s.publicDefinition(ctx)
	require.ErrorContains(t, err, "referenced schedule payload is missing")
	h.reload()
	s, ctx = h.root(true)
	s.Generator.Get(ctx).Definition.Get(ctx).ActionRef = "missing"
	_, err = s.generationRequest(ctx)
	require.ErrorContains(t, err, "referenced schedule payload is missing")
}

func TestSharedPayloadThreeLargeCompletionStages(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	large := bytes.Repeat([]byte("p"), 2*1024*1024-16*1024)
	replaceBundleInput(h, large)
	s, _ := h.root(true)
	s.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	h.commit(true)
	result, err := payloadWire(&commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: large})
	require.NoError(t, err)
	failure, err := payloadWire(&failurepb.Failure{Message: "failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: large}}}}}})
	require.NoError(t, err)
	first := admitBundle(h)
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: first, ChainID: "first", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, Result: result}, true)
	second := admitBundle(h)
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: second, ChainID: "second", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: failure}, true)
	third := admitBundle(h)
	h.reload()
	s, ctx := h.root(false)
	request, err := s.assembleRequest(ctx, s.Requests[third].Get(ctx))
	require.NoError(t, err)
	require.Equal(t, large, request.Input.Payloads[0].Data)
	require.Equal(t, large, request.LastCompletionResult.Payloads[0].Data)
	require.Equal(t, large, request.ContinuedFailure.GetApplicationFailureInfo().Details.Payloads[0].Data)
	require.EqualValues(t, 2, s.Started)
	require.EqualValues(t, 1, s.Unfinished)
	require.Len(t, s.PayloadBlobs, 3)
}
