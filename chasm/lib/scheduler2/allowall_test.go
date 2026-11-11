package scheduler2

import (
	"fmt"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
)

func allowIDs(h *treeHarness) []string {
	h.t.Helper()
	assertAllowPendingStarts(h)
	s, _ := h.root(false)
	ids := make([]string, 0, len(s.Operations))
	for id := range s.Operations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func allowRequest(h *treeHarness, id string) *workflowservice.StartWorkflowExecutionRequest {
	h.t.Helper()
	s, ctx := h.root(false)
	request, err := s.request(ctx, id)
	require.NoError(h.t, err)
	return request
}
func allowUpdate(h *treeHarness, policy enumspb.ScheduleOverlapPolicy) {
	h.t.Helper()
	s, _ := h.root(false)
	h.apply(decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: s.Revision + 1, Limited: true, Remaining: s.Remaining, Policy: policy, PauseOnFailure: true}, true)
}
func allowDue(h *treeHarness, publish bool) {
	h.t.Helper()
	h.now++
	_, request := frozenRequest(h.t)
	h.apply(decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, Actual: h.now, CatchupDeadline: h.now, WorkflowID: fmt.Sprintf("direct-%d", h.now), Request: request}, publish)
}
func allowAck(h *treeHarness, id string) {
	h.t.Helper()
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain"}, true)
}

func TestAllowAllKeyedIssuanceCapacityAndRecovery(t *testing.T) {
	t.Parallel()
	h, _ := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, 5)
	ids := allowIDs(h)
	require.Len(t, ids, 4)
	s, _ := h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, int64(4), s.Unfinished)
	require.Equal(t, int64(1), s.CapacityDropped)
	require.Equal(t, int64(6), s.Remaining)
	require.Equal(t, 4, logicalCount(h.durable))
	require.Empty(t, s.BlockingOperationId)
	snapshots := make([]*workflowservice.StartWorkflowExecutionRequest, 4)
	for i, id := range ids {
		snapshots[i] = allowRequest(h, id)
		require.NotNil(t, snapshots[i])
		require.Nil(t, snapshots[i].LastCompletionResult)
		require.Nil(t, snapshots[i].ContinuedFailure)
	}
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	for i, id := range ids {
		protorequire.ProtoEqual(t, snapshots[i], allowRequest(h, id))
	}
	// Old acknowledgement is keyed, independently of the latest admitted record.
	allowAck(h, ids[0])
	s, ctx := h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, int64(3), s.Unfinished)
	require.Equal(t, int64(1), s.Started)
	require.Equal(t, decision.StartOnly, decision.Phase(s.Operations[ids[0]].Get(ctx).Phase))
	require.Equal(t, ids[3], s.OperationId)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	allowDue(h, false)
	require.Equal(t, ids, allowIDs(h))
	allowDue(h, true)
	s, _ = h.root(false)
	assertAllowPendingStarts(h)
	require.Len(t, s.Operations, 4)
	require.NotContains(t, s.Operations, ids[0])
	require.Equal(t, int64(4), s.Unfinished)
	require.Equal(t, int64(5), s.Remaining)
	// Reversed, duplicate, evicted, and foreign-incarnation results cannot recount.
	for i := 3; i >= 1; i-- {
		allowAck(h, ids[i])
		allowAck(h, ids[i])
	}
	allowAck(h, ids[0])
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "foreign", OperationID: s.OperationId, ChainID: "wrong"}, true)
	s, _ = h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, int64(4), s.Started)
	require.Equal(t, int64(1), s.Unfinished)
	require.Equal(t, 1, logicalCount(h.durable))
}

func TestAllowAllMixedBlockingAndFrozenQueuePolicy(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 2)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	allowDue(h, true)
	s, _ := h.root(false)
	assertAllowPendingStarts(h)
	parallel := s.OperationId
	require.NotEqual(t, first.RequestId, parallel)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.Len(t, s.Queue, 1)
	allowAck(h, parallel)
	s, _ = h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.False(t, s.DrainArmed)
	sequentialComplete(h, first.RequestId, nil)
	require.Equal(t, 1, pureObligations(h))
	fireGeneration(h, true)
	s, ctx := h.root(false)
	assertAllowPendingStarts(h)
	op := s.Operations[s.OperationId].Get(ctx)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, op.Policy)
	require.Equal(t, s.OperationId, s.BlockingOperationId)
	require.Empty(t, s.Queue)
	allowAck(h, s.OperationId)
	s, ctx = h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, decision.Started, decision.Phase(s.Operations[s.OperationId].Get(ctx).Phase))
	require.Equal(t, int64(1), s.Unfinished)
}

func TestAllowAllCapacityWaitingDrainWakesWithoutGenerator(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 2)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	for i := 0; i < 3; i++ {
		allowDue(h, true)
	}
	sequentialComplete(h, first.RequestId, nil)
	// A new parallel ingress may bypass the accepted queue and consume the last slot.
	allowDue(h, true)
	s, _ := h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, int64(4), s.Unfinished)
	require.Len(t, s.Queue, 1)
	require.False(t, s.DrainArmed)
	require.Zero(t, pureObligations(h))
	require.False(t, generationState(h).Armed)
	ids := allowIDs(h)
	allowAck(h, ids[0])
	s, _ = h.root(false)
	assertAllowPendingStarts(h)
	require.True(t, s.DrainArmed)
	require.Equal(t, 1, pureObligations(h))
	before := cloneNodes(h.durable)
	fireGeneration(h, false)
	require.Equal(t, before, h.durable)
	require.Equal(t, 1, pureObligations(h))
	fireGeneration(h, true)
	s, ctx := h.root(false)
	assertAllowPendingStarts(h)
	require.Empty(t, s.Queue)
	require.Equal(t, int64(4), s.Unfinished)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, s.Operations[s.OperationId].Get(ctx).Policy)
	require.Zero(t, pureObligations(h))
}

func TestAllowAllPayloadIsolationAndNoPause(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1)
	sequentialComplete(h, first.RequestId, nil)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	// A valid carryover was persisted by the tracked completion. Poison it and
	// unrelated serialized payloads; ALLOW_ALL must neither read nor overwrite it.
	poisoned := 0
	for path, node := range h.durable {
		if node.Data != nil && (path == "Carryover" || strings.Contains(path, "Request") || strings.Contains(path, "Definition") || strings.Contains(path, "Diagnostics")) {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 4, poisoned)
	h.reload()
	allowDue(h, true)
	s, ctx := h.root(false)
	id := s.OperationId
	request := allowRequest(h, id)
	require.Nil(t, request.LastCompletionResult)
	require.Nil(t, request.ContinuedFailure)
	require.Panics(t, func() { s.Carryover.Get(ctx) })
	h.reload()
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, Failure: completionWire(t, &failurepb.Failure{Message: "ignored failure"})}, true)
	s, ctx = h.root(false)
	require.False(t, s.Paused)
	require.True(t, s.PauseOnFailure)
	require.Equal(t, int64(0), s.Unfinished)
	require.Equal(t, decision.StartOnly, decision.Phase(s.Operations[id].Get(ctx).Phase))
	require.Panics(t, func() { s.Carryover.Get(ctx) })
	h.reload()
	// Once terminal, malformed duplicate and chain conflicts are true no-ops.
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "conflict", Outcome: decision.Failure, Failure: []byte{0xff}}, true)
	s, _ = h.root(false)
	require.Equal(t, int64(2), s.Started)
	require.False(t, s.Paused)
}

func TestAllowAllKeyedPayloadAndMetadataIsolation(t *testing.T) {
	t.Parallel()
	h, _ := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, 2)
	ids := allowIDs(h)
	oldRequest := allowRequest(h, ids[0])
	poisoned := 0
	for path, node := range h.durable {
		parts, err := chasm.DefaultPathEncoder.Decode(path)
		require.NoError(t, err)
		if node.Data != nil && len(parts) == 2 && (parts[0] == "Operations" || parts[0] == "Requests") && parts[1] == ids[1] {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 2, poisoned)
	h.reload()
	protorequire.ProtoEqual(t, oldRequest, allowRequest(h, ids[0]))
	allowAck(h, ids[0])
	s, ctx := h.root(false)
	require.Equal(t, int64(1), s.Started)
	require.Equal(t, int64(1), s.Unfinished)
	require.Equal(t, ids[1], s.OperationId)
	require.Len(t, s.PendingStarts, 1)
	require.Contains(t, s.PendingStarts, ids[1])
	require.NotContains(t, s.PendingStarts, ids[0])
	require.Panics(t, func() { s.Operations[ids[1]].Get(ctx) })
}

func TestAllowAllConfigDisposesCapacityWaitingQueue(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 2)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	for i := 0; i < 3; i++ {
		allowDue(h, true)
	}
	sequentialComplete(h, first.RequestId, nil)
	allowDue(h, true)
	s, _ := h.root(false)
	assertAllowPendingStarts(h)
	require.False(t, s.DrainArmed)
	require.Equal(t, int64(4), s.Unfinished)
	h.now += int64(time.Second)
	request, _ := frozenRequest(t)
	configureBuffer(h, &schedulepb.ScheduleSpec{}, s.Revision+1, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, 0, request)
	s, ctx := h.root(true)
	require.True(t, s.DrainArmed)
	// Configuration arms both generation and drain. Execute the drain directly;
	// no acceptance/completion or operation slot is needed for disposal.
	require.NoError(t, (&DrainHandler{}).Execute(ctx.(chasm.MutableContext), s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.DrainAt)}, &scheduler2pb.DrainTask{Token: s.DrainToken}))
	commitGeneration(h, true)
	s, _ = h.root(false)
	assertAllowPendingStarts(h)
	require.Empty(t, s.Queue)
	require.False(t, s.DrainArmed)
	require.Equal(t, int64(4), s.Unfinished)
	require.Equal(t, int64(5), s.Remaining)
}

func TestAllowAllBoundChainConflictIsAtomic(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1)
	allowAck(h, first.RequestId)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	allowDue(h, true)
	snapshot := cloneNodes(h.durable)
	s, ctx := h.root(true)
	err := s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: first.RequestId, ChainID: "conflict"})
	require.ErrorContains(t, err, "conflicting workflow chain")
	h.reload()
	require.Equal(t, snapshot, h.durable)
	s, _ = h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, first.RequestId, s.BlockingOperationId)
	require.Equal(t, int64(2), s.Unfinished)
}

func TestAllowAllOldRetrySurvivesDefinitionUpdate(t *testing.T) {
	t.Parallel()
	h, _ := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, 2)
	ids := allowIDs(h)
	expected := allowRequest(h, ids[0])
	s, _ := h.root(false)
	assertAllowPendingStarts(h)
	replacement, _ := frozenRequest(t)
	replacement.WorkflowId = "new-definition"
	replacement.WorkflowType.Name = "new-type"
	configureBuffer(h, &schedulepb.ScheduleSpec{}, s.Revision+1, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, time.Minute, replacement)
	fireGeneration(h, true)
	h.apply(decision.Event{Kind: decision.RetryableFailure, Incarnation: "incarnation", OperationID: ids[0]}, true)
	s, ctx := h.root(false)
	assertAllowPendingStarts(h)
	old := s.Operations[ids[0]].Get(ctx)
	h.now = old.RetryAt
	protorequire.ProtoEqual(t, expected, allowRequest(h, ids[0]))
	// The original physical task is stale after only this key's retry advances;
	// the other same-batch operation remains valid and issuable.
	validCount := 0
	for _, physical := range h.physical[:2] {
		_, valid, err := h.node.ValidateSideEffectTask(t.Context(), physical)
		require.NoError(t, err)
		if valid {
			validCount++
		}
	}
	require.Equal(t, 1, validCount)
	require.NotNil(t, allowRequest(h, ids[1]))
	h.apply(decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: ids[0], ChainID: "old-chain"}, true)
	s, ctx = h.root(false)
	assertAllowPendingStarts(h)
	require.Equal(t, ids[1], s.OperationId)
	require.Equal(t, decision.StartOnly, decision.Phase(s.Operations[ids[0]].Get(ctx).Phase))
	require.Equal(t, int64(1), s.Unfinished)
}

// The compact root projection is exactly the durable pending-task obligation.
func assertAllowPendingStarts(h *treeHarness) {
	h.t.Helper()
	s, ctx := h.root(false)
	expected := map[string]int64{}
	for id, field := range s.Operations {
		op := field.Get(ctx)
		if decision.Phase(op.Phase) == decision.Pending {
			expected[id] = min(op.RetryAt, op.Deadline)
		}
	}
	require.Len(h.t, s.PendingStarts, len(expected))
	for id, at := range expected {
		actual, ok := s.PendingStarts[id]
		require.True(h.t, ok, "pending operation %s", id)
		require.Equal(h.t, at, actual)
	}
}
