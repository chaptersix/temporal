package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUpdateCompositionLeasedHistoricalRebase(t *testing.T) {
	t.Parallel()
	base := generationStart()
	end := base.Add(4 * time.Second)
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, end, time.Second), now, now)
	coordinatorStep(h, true)
	coordinatorBackfill(h, base, end, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	coordinatorStep(h, true) // Discover nominal zero.
	coordinatorStep(h, true) // Admit zero, retaining its immutable request.
	coordinatorStep(h, true) // Discover and lease nominal one.
	s, ctx := h.root(false)
	require.Equal(t, base.UnixNano(), s.Backfills[0].Continuation.DisposedThrough)
	require.Equal(t, base.Add(time.Second).UnixNano(), s.Backfills[0].Continuation.Pending.Nominal)
	oldLease := s.Coordination.OwnerKey
	require.NotEmpty(t, oldLease)
	firstID := s.OperationId
	firstRequest := append([]byte(nil), s.Requests[firstID].Get(ctx).Data...)
	config := localUpdateConfig(t, 2)
	config.Generation.Spec = generationSpec(base, end, 2*time.Second)
	config.Generation.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	config.Remaining = 0
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.True(t, s.Paused)
	require.True(t, s.Limited)
	require.Zero(t, s.Remaining)
	require.True(t, s.PauseOnFailure)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, s.Policy)
	require.Equal(t, now.UnixNano(), s.Cursor, "automatic boundary uses execution now")
	require.Equal(t, now.UnixNano(), s.Generator.Get(ctx).Continuation.DisposedThrough)
	require.Empty(t, s.Coordination.OwnerKey, "old-revision candidate loses its concrete lease")
	require.Equal(t, end.UnixNano(), s.Backfills[0].End)
	require.Equal(t, base.UnixNano(), s.Backfills[0].Continuation.DisposedThrough, "historical disposition cannot jump to update time")
	// Wait for the revised candidate, without performing its handoff.
	for step := 0; step < 3; step++ {
		s, _ = h.root(false)
		b := s.Backfills[0]
		if b.Continuation.Revision == 2 && b.Continuation.Pending != nil {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Equal(t, base.Add(2*time.Second).UnixNano(), s.Backfills[0].Continuation.Pending.Nominal)
	require.Equal(t, int64(2), s.Backfills[0].Continuation.Revision)
	require.NotEmpty(t, s.Coordination.OwnerKey)
	require.NotEqual(t, oldLease, s.Coordination.OwnerKey)
	before, physical = cloneNodes(h.durable), len(h.physical)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	for step := 0; step < 8; step++ {
		s, _ = h.root(false)
		if len(s.Backfills) == 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx = h.root(false)
	require.Empty(t, s.Backfills)
	require.Len(t, s.Operations, 3)
	require.Equal(t, int64(3), s.Unfinished)
	require.Equal(t, int64(3), s.UnconfirmedStarts)
	require.Zero(t, s.Started)
	require.Zero(t, s.Remaining, "historical manual semantics bypass the replaced zero budget")
	require.Equal(t, now.UnixNano(), s.Cursor)
	require.Equal(t, firstRequest, s.Requests[firstID].Get(ctx).Data)
	for _, second := range []int{2, 4} {
		nominal := base.Add(time.Duration(second) * time.Second)
		id := fmt.Sprintf("incarnation/backfill/1/%d/action/start", nominal.UnixNano())
		require.Contains(t, s.Operations, id)
		op := s.Operations[id].Get(ctx)
		require.Equal(t, uint64(1), op.BackfillId)
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, op.Policy)
		expected := common.CloneProto(config.Generation.Request)
		expected.LastCompletionResult = nil
		expected.ContinuedFailure = nil
		expected.WorkflowId = "replacement-" + nominal.Format(time.RFC3339)
		expected.RequestId = id
		expected.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
		protorequire.ProtoEqual(t, expected, allowRequest(h, id))
	}
}

func TestUpdateCompositionQueuedIdentityCurrentCarryoverAndRetry(t *testing.T) {
	t.Parallel()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	allowAck(h, first.RequestId)
	effectID := first.RequestId + "/cancel"
	frozenEffect := effectSnapshot(h, effectID)
	require.NotNil(t, frozenEffect)
	nominal := generationStart().Add(-24 * time.Hour)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(nominal), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, true)
	transferManual(h, true)
	s, ctx := h.root(false)
	require.Len(t, s.Queue, 2)
	queued := common.CloneProto(s.Queue[1])
	oldStart := append([]byte(nil), s.Requests[first.RequestId].Get(ctx).Data...)
	config := localUpdateConfig(t, s.Revision+1)
	config.Generation.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	config.Generation.CatchupWindow = 0
	config.Generation.Request.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("new input")}}}
	config.Remaining = 0
	before := cloneNodes(h.durable)
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, false)
	require.Equal(t, before, h.durable)
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	protorequire.ProtoEqual(t, queued, s.Queue[1])
	protorequire.DeepEqual(t, frozenEffect, effectSnapshot(h, effectID))
	require.Equal(t, oldStart, s.Requests[first.RequestId].Get(ctx).Data)
	// First completion after the update supplies the payload adopted at admission.
	result := &commonpb.Payload{Data: []byte("completion after replacement")}
	sequentialComplete(h, first.RequestId, result)
	// Consume only the automatic empty-source scan if it wins before drain.
	for step := 0; step < 2; step++ {
		s, ctx = h.root(false)
		_, d, _, planErr := s.planWork(ctx)
		require.NoError(t, planErr)
		if d != nil && d.class == scheduler2pb.WORK_CLASS_DRAIN {
			break
		}
		coordinatorStep(h, true)
	}
	before, physical := cloneNodes(h.durable), len(h.physical)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	coordinatorStep(h, true)
	s, ctx = h.root(false)
	require.Empty(t, s.Queue, "paused automatic entry disposed; accepted manual entry admitted")
	require.Zero(t, s.Remaining)
	require.Equal(t, int64(1), s.Started, "admission does not recount the earlier completed workflow")
	op := s.Operations[s.OperationId].Get(ctx)
	require.Equal(t, queued.ManualId, op.ManualId)
	require.Equal(t, queued.Policy, op.Policy)
	require.Equal(t, queued.WorkflowId, op.WorkflowId)
	expected := common.CloneProto(config.Generation.Request)
	expected.WorkflowId = queued.WorkflowId
	expected.RequestId = s.OperationId
	expected.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	expected.LastCompletionResult = &commonpb.Payloads{Payloads: []*commonpb.Payload{result}}
	protorequire.ProtoEqual(t, expected, h.request())
	// Another atomic update cannot rewrite the already admitted request.
	next := localUpdateConfig(t, s.Revision+1)
	next.Paused = false
	next.Remaining = 5
	next.Generation.Request.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("later input")}}}
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), next))
	commitGeneration(h, true)
	h.apply(decision.Event{Kind: decision.RetryableFailure, Incarnation: "incarnation", OperationID: expected.RequestId}, true)
	s, ctx = h.root(false)
	h.now = s.Operations[expected.RequestId].Get(ctx).RetryAt
	protorequire.ProtoEqual(t, expected, allowRequest(h, expected.RequestId))
	require.Equal(t, int64(5), s.Remaining, "budget replacement does not debit an existing retry")
}
