package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

// This is the concrete cap4/full-queue schedule that readiness-only round robin
// misses: a drain frees a queue vacancy while consuming the only free operation.
func TestCoordinatorFullQueueVacancySurvivesOperationContention(t *testing.T) {
	t.Parallel()
	base := generationStart()
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, base.Add(3*time.Second), time.Second), now, now)
	coordinatorStep(h, true) // Exhaust automatic enumeration; all fixture ingress is accepted normally.
	accept := func(policy enumspb.ScheduleOverlapPolicy) {
		s, ctx := h.root(true)
		require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: policy}))
		commitGeneration(h, true)
	}
	accept(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	coordinatorStep(h, true)
	s, _ := h.root(false)
	blocker := s.OperationId
	allows := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		accept(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
		coordinatorStep(h, true)
		s, _ = h.root(false)
		allows = append(allows, s.OperationId)
	}
	// Fill the queue through a finite tracked backfill. Its last transfer leaves
	// the cyclic vacancy turn after BACKFILL, so MANUAL wins the next contention.
	coordinatorBackfill(h, base, base.Add(3*time.Second), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	for step := 0; step < 12; step++ {
		s, _ = h.root(false)
		if len(s.Backfills) == 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Empty(t, s.Backfills)
	require.Len(t, s.Queue, 4)
	require.Equal(t, int64(4), s.Unfinished)
	require.Equal(t, blocker, s.BlockingOperationId)
	accept(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	coordinatorBackfill(h, base, base, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	coordinatorStep(h, true) // Scan competitor; neither transfer can pass a full queue.
	s, _ = h.root(false)
	require.NotNil(t, s.Backfills[0].Continuation.Pending)
	require.False(t, s.Coordination.Armed)
	sequentialComplete(h, blocker, nil)
	// Failed drain publication must not expose its vacancy or lease.
	before, physical := cloneNodes(h.durable), len(h.physical)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Len(t, s.Queue, 3)
	require.Equal(t, int64(4), s.Unfinished)
	require.NotEqual(t, blocker, s.BlockingOperationId)
	require.Equal(t, scheduler2pb.WORK_CLASS_MANUAL, s.Coordination.OwnerClass)
	lease := s.Coordination.OwnerKey
	require.NotEmpty(t, lease)
	require.False(t, s.Coordination.Armed, "ready tracked backfill cannot steal the reserved vacancy")
	// New tracked ingress must not refill the reserved vacancy either.
	accept(enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	s, _ = h.root(false)
	require.Equal(t, lease, s.Coordination.OwnerKey)
	require.Len(t, s.Queue, 3)
	require.False(t, s.Coordination.Armed)
	allowAck(h, allows[0])
	before, physical = cloneNodes(h.durable), len(h.physical)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable)
	require.Len(t, h.physical, physical)
	s, _ = h.root(false)
	require.Equal(t, lease, s.Coordination.OwnerKey)
	require.Len(t, s.Manuals, 2)
	coordinatorStep(h, true)
	s, ctx := h.root(false)
	require.Equal(t, uint64(5), s.Operations[s.OperationId].Get(ctx).ManualId)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, s.Operations[s.OperationId].Get(ctx).Policy)
	require.Equal(t, int64(4), s.Unfinished)
	require.Len(t, s.Queue, 3)
	// Releasing the lease lets the tracked competitor use the vacancy even while
	// operation capacity stays full. Completing its range does not require an RPC.
	for step := 0; step < 6; step++ {
		s, _ = h.root(false)
		if len(s.Backfills) == 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Empty(t, s.Backfills)
	require.Len(t, s.Queue, 4)
	require.Equal(t, uint64(2), s.Queue[3].BackfillId)
	require.Equal(t, int64(4), s.Unfinished)
	require.Len(t, s.Manuals, 1)
}
