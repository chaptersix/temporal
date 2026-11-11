package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func coordinatorHarness(t *testing.T, spec *schedulepb.ScheduleSpec, after, now time.Time) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	h.now = now.UnixNano()
	sequentialBudget(h, 100)
	request, _ := frozenRequest(t)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: spec, Revision: 1, After: after, CatchupWindow: time.Hour, Request: request, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
	commitGeneration(h, true)
	return h
}
func coordinatorStep(h *treeHarness, publish bool) {
	h.t.Helper()
	s, _ := h.root(false)
	require.NotNil(h.t, s.Coordination)
	require.True(h.t, s.Coordination.Armed)
	require.LessOrEqual(h.t, s.Coordination.At, h.now, "coordinator wake must be due")
	count, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(h.t, err)
	require.Equal(h.t, 1, count)
	commitGeneration(h, publish)
}
func coordinatorBackfill(h *treeHarness, start, end time.Time, policy enumspb.ScheduleOverlapPolicy) {
	h.t.Helper()
	s, ctx := h.root(true)
	require.NoError(h.t, s.AcceptBackfill(ctx.(chasm.MutableContext), &schedulepb.BackfillRequest{StartTime: timestamppb.New(start), EndTime: timestamppb.New(end), OverlapPolicy: policy}))
	commitGeneration(h, true)
}
func coordinatorManual(h *treeHarness) {
	h.t.Helper()
	s, ctx := h.root(true)
	require.NoError(h.t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
	commitGeneration(h, true)
}

func TestCoordinatorHistoricalBackfillAtomicHandoff(t *testing.T) {
	t.Parallel()
	base := generationStart()
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, base.Add(2*time.Second), time.Second), now, now)
	coordinatorStep(h, true) // automatic enumeration is already beyond this definition
	s, _ := h.root(true)
	s.Paused = true
	s.Remaining = 0
	cursor := s.Cursor
	h.commit(true)
	coordinatorBackfill(h, base, base.Add(2*time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	coordinatorStep(h, true) // bounded scan retains first candidate
	s, _ = h.root(false)
	require.Len(t, s.Backfills, 1)
	pending := proto.Clone(s.Backfills[0].Continuation)
	require.Equal(t, base.UnixNano(), s.Backfills[0].Continuation.Pending.Nominal)
	require.Equal(t, base.Add(-time.Nanosecond).UnixNano(), s.Backfills[0].Continuation.DisposedThrough)
	snapshot := cloneNodes(h.durable)
	physical := len(h.physical)
	coordinatorStep(h, false)
	require.Equal(t, snapshot, h.durable)
	require.Len(t, h.physical, physical)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, pending, s.Backfills[0].Continuation)
	h.reload()
	coordinatorStep(h, true)
	s, ctx := h.root(false)
	require.Len(t, s.Operations, 1)
	first := s.Operations[s.OperationId].Get(ctx)
	require.Equal(t, uint64(1), first.BackfillId)
	require.Equal(t, base.UnixNano(), first.Nominal)
	require.Equal(t, base.UnixNano(), s.Backfills[0].Continuation.DisposedThrough)
	for step := 0; step < 8; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx = h.root(false)
	require.Empty(t, s.Backfills)
	require.Len(t, s.Operations, 3)
	require.Equal(t, int64(3), s.Unfinished)
	require.Zero(t, s.Remaining)
	require.Equal(t, cursor, s.Cursor)
	require.True(t, s.Paused)
	require.False(t, s.Coordination.Armed)
	require.Zero(t, pureObligations(h))
	for i := 0; i < 3; i++ {
		nominal := base.Add(time.Duration(i) * time.Second)
		id := fmt.Sprintf("incarnation/backfill/1/%d/action/start", nominal.UnixNano())
		require.Contains(t, s.Operations, id)
		require.Equal(t, nominal.UnixNano(), s.Operations[id].Get(ctx).Nominal)
		request := allowRequest(h, id)
		require.Equal(t, "workflow-"+nominal.Format(time.RFC3339), request.WorkflowId)
		require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, request.WorkflowIdReusePolicy)
	}
}

func TestCoordinatorBackfillCapAndRequestRotation(t *testing.T) {
	t.Parallel()
	base := generationStart()
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, base, time.Second), now, now)
	coordinatorStep(h, true)
	for i := 0; i < 4; i++ {
		coordinatorBackfill(h, base, base, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	}
	s, ctx := h.root(true)
	before := proto.Clone(s.RootState)
	require.Error(t, s.AcceptBackfill(ctx.(chasm.MutableContext), &schedulepb.BackfillRequest{StartTime: timestamppb.New(base), EndTime: timestamppb.New(base)}))
	protorequire.ProtoEqual(t, before, s.RootState)
	h.reload()
	admitted := []uint64{}
	for step := 0; step < 30; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		previous := s.OperationId
		coordinatorStep(h, true)
		s, ctx = h.root(false)
		if s.OperationId != previous {
			admitted = append(admitted, s.Operations[s.OperationId].Get(ctx).BackfillId)
			allowAck(h, s.OperationId)
		}
	}
	require.Equal(t, []uint64{1, 2, 3, 4}, admitted)
	s, _ = h.root(false)
	require.Empty(t, s.Backfills)
	require.Equal(t, uint64(4), s.BackfillSequence)
}

func TestCoordinatorSustainedManualAutomaticBackfillCompetition(t *testing.T) {
	t.Parallel()
	base := generationStart()
	end := base.Add(4 * time.Second)
	h := coordinatorHarness(t, generationSpec(base, end, time.Second), base.Add(-time.Nanosecond), base.Add(10*time.Second))
	coordinatorBackfill(h, base, end, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	counts := map[string]int{}
	for step := 0; step < 80; step++ {
		s, _ := h.root(false)
		if len(s.Manuals) < 4 {
			coordinatorManual(h)
		}
		s, _ = h.root(false)
		require.True(t, s.Coordination.Armed)
		previous := s.OperationId
		coordinatorStep(h, true)
		s, ctx := h.root(false)
		if s.OperationId != previous {
			op := s.Operations[s.OperationId].Get(ctx)
			class := "automatic"
			if op.ManualId > 0 {
				class = "manual"
			}
			if op.BackfillId > 0 {
				class = "backfill"
			}
			counts[class]++
			allowAck(h, s.OperationId)
		}
		require.LessOrEqual(t, len(s.Operations), 4)
		require.LessOrEqual(t, len(s.Manuals), 4)
		require.LessOrEqual(t, len(s.Backfills), 4)
	}
	require.Equal(t, 5, counts["automatic"])
	require.Equal(t, 5, counts["backfill"])
	require.GreaterOrEqual(t, counts["manual"], 5)
	s, _ := h.root(false)
	require.Empty(t, s.Backfills)
}

func TestCoordinatorCapacityLeaseSurvivesNewIngress(t *testing.T) {
	t.Parallel()
	base := generationStart()
	end := base.Add(3 * time.Second)
	h := coordinatorHarness(t, generationSpec(base, end, time.Second), base.Add(-time.Nanosecond), end)
	for step := 0; step < 12; step++ {
		s, _ := h.root(false)
		if s.Unfinished == 4 {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ := h.root(false)
	require.Equal(t, int64(4), s.Unfinished)
	ids := allowIDs(h)
	coordinatorManual(h)
	coordinatorBackfill(h, base, base, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	for step := 0; step < 8; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.False(t, s.Coordination.Armed)
	owner, key := s.Coordination.OwnerClass, s.Coordination.OwnerKey
	require.NotEmpty(t, key)
	require.Equal(t, scheduler2pb.WORK_CLASS_MANUAL, owner)
	coordinatorManual(h)
	s, _ = h.root(false)
	require.Equal(t, key, s.Coordination.OwnerKey)
	allowAck(h, ids[0])
	s, _ = h.root(false)
	require.True(t, s.Coordination.Armed)
	previous := s.OperationId
	for step := 0; step < 8; step++ {
		coordinatorStep(h, true)
		s, _ = h.root(false)
		if s.OperationId != previous {
			break
		}
	}
	s, ctx := h.root(false)
	require.Equal(t, uint64(1), s.Operations[s.OperationId].Get(ctx).ManualId)
	require.Equal(t, int64(4), s.Unfinished)
}

func TestCoordinatorBackfillRevisionInvalidatesLeaseAndRebasesNominal(t *testing.T) {
	t.Parallel()
	base := generationStart()
	end := base.Add(4 * time.Second)
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, end, time.Second), now, now)
	coordinatorStep(h, true)
	coordinatorBackfill(h, base, end, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	coordinatorStep(h, true)
	coordinatorStep(h, true)
	coordinatorStep(h, true)
	s, _ := h.root(false)
	require.Equal(t, base.UnixNano(), s.Backfills[0].Continuation.DisposedThrough)
	require.Equal(t, base.Add(time.Second).UnixNano(), s.Backfills[0].Continuation.Pending.Nominal)
	oldLease := s.Coordination.OwnerKey
	require.NotEmpty(t, oldLease)
	first := h.request()
	replacement, _ := frozenRequest(t)
	replacement.WorkflowId = "updated"
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: generationSpec(base, end, 2*time.Second), Revision: 2, After: now, CatchupWindow: time.Hour, Request: replacement, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}))
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.NotEqual(t, oldLease, s.Coordination.OwnerKey)
	for step := 0; step < 16; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx = h.root(false)
	require.Empty(t, s.Backfills)
	require.Len(t, s.Operations, 3)
	for _, seconds := range []int{0, 2, 4} {
		nominal := base.Add(time.Duration(seconds) * time.Second)
		id := fmt.Sprintf("incarnation/backfill/1/%d/action/start", nominal.UnixNano())
		require.Contains(t, s.Operations, id)
		op := s.Operations[id].Get(ctx)
		require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, op.Policy)
		if seconds > 0 {
			require.Equal(t, "updated-"+nominal.Format(time.RFC3339), allowRequest(h, id).WorkflowId)
		}
	}
	protorequire.ProtoEqual(t, first, allowRequest(h, first.RequestId))
}

func TestCoordinatorBackfillOverridePreservesAcceptedWaiter(t *testing.T) {
	t.Parallel()
	base := generationStart()
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, base, time.Second), now, now)
	coordinatorStep(h, true)
	// First accepted manual command becomes the tracked blocker under SKIP.
	s, ctx := h.root(true)
	require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}))
	commitGeneration(h, true)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	blocker := s.OperationId
	coordinatorBackfill(h, base, base, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	coordinatorStep(h, true)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Len(t, s.Queue, 1)
	require.Equal(t, uint64(1), s.Queue[0].BackfillId)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, s.Queue[0].Policy)
	// Request completion is handoff completion, independent of its blocked workflow.
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Empty(t, s.Backfills)
	require.Equal(t, blocker, s.BlockingOperationId)
	require.Equal(t, int64(100), s.Remaining)
	sequentialComplete(h, blocker, nil)
	coordinatorStep(h, true)
	s, ctx = h.root(false)
	require.Empty(t, s.Queue)
	require.Equal(t, uint64(1), s.Operations[s.OperationId].Get(ctx).BackfillId)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, s.Operations[s.OperationId].Get(ctx).Policy)
}
