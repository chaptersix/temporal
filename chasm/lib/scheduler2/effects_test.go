package scheduler2

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
)

func effectSnapshot(h *treeHarness, id string) *EffectRequest {
	h.t.Helper()
	s, ctx := h.root(false)
	request, err := s.effectRequest(ctx, id)
	require.NoError(h.t, err)
	return request
}
func effectsResult(h *treeHarness, id string, kind decision.Kind, publish bool) {
	h.t.Helper()
	h.apply(decision.Event{Kind: kind, Incarnation: "incarnation", EffectID: id}, publish)
}
func assertEffectsPhase(h *treeHarness, index int, phase decision.EffectPhase) {
	h.t.Helper()
	s, _ := h.root(false)
	require.Greater(h.t, len(s.Effects), index)
	require.Equal(h.t, phase, decision.EffectPhase(s.Effects[index].Phase))
}

func TestEffectsUnknownTargetBindingAndFrozenRetry(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	id := first.RequestId + "/cancel"
	assertEffectsPhase(h, 0, decision.AwaitingTarget)
	require.Nil(t, effectSnapshot(h, id))
	s, _ := h.root(false)
	require.Empty(t, s.EffectRequests)
	require.Equal(t, 1, logicalCount(h.durable))
	// Waiting for chain evidence does not consume the bound retry horizon.
	h.now += int64(2 * time.Minute)
	snapshot := cloneNodes(h.durable)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: "first-run"}, false)
	require.Equal(t, snapshot, h.durable)
	require.Nil(t, effectSnapshot(h, id))
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: "first-run"}, true)
	assertEffectsPhase(h, 0, decision.EffectPending)
	request := effectSnapshot(h, id).Cancel
	require.Equal(t, first.Namespace, request.Namespace)
	require.Equal(t, first.WorkflowId, request.WorkflowExecution.WorkflowId)
	require.Empty(t, request.WorkflowExecution.RunId)
	require.Equal(t, "first-run", request.FirstExecutionRunId)
	require.Equal(t, id, request.RequestId)
	require.Equal(t, "cancelled by schedule overlap policy", request.Reason)
	require.Equal(t, "scheduler2", request.Identity)
	s, _ = h.root(false)
	require.Equal(t, h.now+decision.RetryHorizon, s.Effects[0].Deadline)
	require.Equal(t, 1, logicalCount(h.durable))
	replacement, _ := frozenRequest(t)
	replacement.Namespace = "new-namespace"
	replacement.WorkflowId = "new-workflow"
	configureBuffer(h, &schedulepb.ScheduleSpec{}, s.Revision+1, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, time.Hour, replacement)
	fireGeneration(h, true)
	effectsResult(h, id, decision.EffectFailed, true)
	s, _ = h.root(false)
	h.now = s.Effects[0].RetryAt
	retry := effectSnapshot(h, id).Cancel
	protorequire.ProtoEqual(t, request, retry)
	retry.WorkflowExecution.WorkflowId = "caller mutation"
	protorequire.ProtoEqual(t, request, effectSnapshot(h, id).Cancel)
	effectsResult(h, id, decision.EffectSucceeded, true)
	assertEffectsPhase(h, 0, decision.EffectAccepted)
	s, _ = h.root(false)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.Equal(t, int64(1), s.Unfinished)
	require.Len(t, s.Queue, 1)
	require.False(t, s.DrainArmed)
	require.Equal(t, int64(9), s.Remaining)
}

func TestEffectsTerminateSupersedesCancelAndLateEvidence(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	template, _ := frozenRequest(t)
	configureBuffer(h, &schedulepb.ScheduleSpec{}, 2, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, time.Hour, template)
	fireGeneration(h, true)
	allowAck(h, first.RequestId)
	cancelID := first.RequestId + "/cancel"
	cancel := effectSnapshot(h, cancelID).Cancel
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
	allowDue(h, true)
	terminateID := first.RequestId + "/terminate"
	assertEffectsPhase(h, 0, decision.EffectSuperseded)
	assertEffectsPhase(h, 1, decision.EffectPending)
	require.Nil(t, effectSnapshot(h, cancelID))
	terminate := effectSnapshot(h, terminateID).Terminate
	require.Equal(t, cancel.Namespace, terminate.Namespace)
	protorequire.ProtoEqual(t, cancel.WorkflowExecution, terminate.WorkflowExecution)
	require.Equal(t, cancel.FirstExecutionRunId, terminate.FirstExecutionRunId)
	require.Equal(t, "terminated by schedule overlap policy", terminate.Reason)
	require.Equal(t, "scheduler2", terminate.Identity)
	effectsResult(h, cancelID, decision.EffectSucceeded, true)
	assertEffectsPhase(h, 0, decision.EffectSuperseded)
	assertEffectsPhase(h, 1, decision.EffectPending)
	s, _ := h.root(false)
	h.now = s.Effects[1].Deadline
	effectsResult(h, terminateID, decision.EffectExpired, true)
	assertEffectsPhase(h, 1, decision.EffectAmbiguous)
	require.Nil(t, effectSnapshot(h, terminateID))
	require.Zero(t, logicalCount(h.durable))
	effectsResult(h, terminateID, decision.EffectNotFound, true)
	assertEffectsPhase(h, 1, decision.EffectAbsent)
	s, _ = h.root(false)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.Len(t, s.Queue, 2)
	sequentialComplete(h, first.RequestId, nil)
	s, _ = h.root(false)
	require.Empty(t, s.Effects)
	require.Empty(t, s.EffectRequests)
	require.True(t, s.DrainArmed)
	effectsResult(h, terminateID, decision.EffectSucceeded, true)
	s, _ = h.root(false)
	require.Empty(t, s.Effects)
	fireGeneration(h, true)
	s, _ = h.root(false)
	require.NotEqual(t, first.RequestId, s.OperationId)
	require.Equal(t, int64(1), s.OverlapSuperseded)
	effectsResult(h, terminateID, decision.EffectSucceeded, true)
	s, _ = h.root(false)
	require.Empty(t, s.Effects)
	require.Equal(t, int64(1), s.Unfinished)
}

func TestEffectsQueueOverflowCreatesNoDemand(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 5)
	allowAck(h, first.RequestId)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)
	allowDue(h, true)
	s, _ := h.root(false)
	require.Len(t, s.Queue, 4)
	require.Equal(t, int64(1), s.BufferDropped)
	require.Empty(t, s.Effects)
	require.Empty(t, s.EffectRequests)
	require.Zero(t, logicalCount(h.durable))
	require.Equal(t, int64(9), s.Remaining)
}

func TestEffectsMetadataAndUnrelatedPayloadIsolation(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	allowAck(h, first.RequestId)
	id := first.RequestId + "/cancel"
	expected := effectSnapshot(h, id).Cancel
	// Target metadata/start request, template and diagnostics are unnecessary once
	// immutable effect binding has committed. Only the selected effect payload loads.
	poisoned := 0
	for path, node := range h.durable {
		if node.Data == nil {
			continue
		}
		if strings.HasPrefix(path, "Operations#") || strings.HasPrefix(path, "Requests#") || strings.Contains(path, "Definition") || path == "Diagnostics" {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 4, poisoned)
	h.reload()
	protorequire.ProtoEqual(t, expected, effectSnapshot(h, id).Cancel)
	effectsResult(h, id, decision.EffectNotFound, true)
	assertEffectsPhase(h, 0, decision.EffectAbsent)
	s, ctx := h.root(false)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.Panics(t, func() { s.Operations[first.RequestId].Get(ctx) })
	h.reload()
	s, ctx = h.root(false)
	require.Panics(t, func() { s.Requests[first.RequestId].Get(ctx) })
}

func TestEffectsDelayedBindingAfterParallelAdmission(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	allowDue(h, true)
	s, _ := h.root(false)
	latest := s.OperationId
	require.NotEqual(t, first.RequestId, latest)
	poisoned := 0
	for path, node := range h.durable {
		parts, err := chasm.DefaultPathEncoder.Decode(path)
		require.NoError(t, err)
		if node.Data != nil && len(parts) == 2 && parts[1] == latest && (parts[0] == "Operations" || parts[0] == "Requests") {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 2, poisoned)
	h.reload()
	h.now += int64(2 * time.Minute)
	allowAck(h, first.RequestId)
	s, _ = h.root(false)
	require.Equal(t, latest, s.OperationId)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.Equal(t, int64(2), s.Unfinished)
	require.Len(t, s.Effects, 1)
	require.Equal(t, h.now+decision.RetryHorizon, s.Effects[0].Deadline)
	request := effectSnapshot(h, first.RequestId+"/cancel").Cancel
	require.Equal(t, first.WorkflowId, request.WorkflowExecution.WorkflowId)
	require.Equal(t, first.RequestId+"/chain", request.FirstExecutionRunId)
}

func TestEffectsClockRollbackPreservesObligation(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	allowAck(h, first.RequestId)
	id := first.RequestId + "/cancel"
	s, _ := h.root(false)
	boundAt, deadline := s.Effects[0].RetryAt, s.Effects[0].Deadline
	h.now -= int64(time.Second)
	s, ctx := h.root(false)
	_, err := s.effectRequest(ctx, id)
	require.ErrorContains(t, err, "not due")
	effectsResult(h, id, decision.EffectFailed, true)
	s, _ = h.root(false)
	require.Equal(t, boundAt+decision.RetryBackoff, s.Effects[0].RetryAt)
	require.Equal(t, deadline, s.Effects[0].Deadline)
	require.Equal(t, 1, logicalCount(h.durable))
	h.now = boundAt + decision.RetryBackoff
	require.NotNil(t, effectSnapshot(h, id))
}
