package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
)

func idleCoordinatorHarness(t *testing.T) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	h.now = generationStart().UnixNano()
	configureGeneration(h, &schedulepb.ScheduleSpec{}, "seed", 1, generationStart().Add(-time.Second), time.Hour)
	fireGeneration(h, true)
	return h
}

func exhaustCoordinator(h *treeHarness) {
	h.t.Helper()
	s, _ := h.root(true)
	s.Coordination = &scheduler2pb.CoordinationState{Token: math.MaxUint64}
	h.commit(true)
}

func requireRejectedCoordinatorUnchanged(h *treeHarness, before map[string]*persistencespb.ChasmNode, physical int) {
	h.t.Helper()
	// Deliberately commit after rejection rather than discarding the tree. Only
	// CHASM's mutable-access version stamp may change, not DATA or task metadata.
	timers := commitGeneration(h, true)
	require.Empty(h.t, timers)
	require.Len(h.t, h.physical, physical)
	after := cloneNodes(h.durable)
	before = cloneNodes(before)
	require.Len(h.t, after, len(before))
	for path, node := range before {
		require.Contains(h.t, after, path)
		if node.Metadata != nil {
			node.Metadata.LastUpdateVersionedTransition = nil
		}
		if after[path].Metadata != nil {
			after[path].Metadata.LastUpdateVersionedTransition = nil
		}
		protorequire.ProtoEqual(h.t, node, after[path])
	}
}

func TestCoordinatorTokenRejectionBeforeManualMutation(t *testing.T) {
	t.Parallel()
	h := idleCoordinatorHarness(t)
	exhaustCoordinator(h)
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, ctx := h.root(true)
	err := s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{})
	require.ErrorContains(t, err, "coordinator token exhausted")
	requireRejectedCoordinatorUnchanged(h, before, physical)
}

func TestCoordinatorTokenRejectionBeforeCompletionMutation(t *testing.T) {
	t.Parallel()
	h := idleCoordinatorHarness(t)
	_, request := frozenRequest(t)
	h.apply(decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now, CatchupDeadline: h.now, Request: request}, true)
	s, _ := h.root(true)
	operationID := s.OperationId
	s.Queue = []*scheduler2pb.QueuedOccurrence{{ManualId: 9, Nominal: h.now, Actual: h.now, WorkflowId: "queued", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}}
	s.Coordination = &scheduler2pb.CoordinationState{Token: math.MaxUint64}
	h.commit(true)
	before, physical := cloneNodes(h.durable), len(h.physical)
	s, ctx := h.root(true)
	err := s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Complete, Incarnation: "incarnation", OperationID: operationID, ChainID: "chain"})
	require.ErrorContains(t, err, "coordinator token exhausted")
	requireRejectedCoordinatorUnchanged(h, before, physical)
}

func TestCoordinatorTokenRejectionBeforeGenerationMutation(t *testing.T) {
	t.Parallel()
	for _, configured := range []bool{false, true} {
		name := "initial"
		if configured {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			h.now = generationStart().UnixNano()
			revision := int64(1)
			if configured {
				configureGeneration(h, &schedulepb.ScheduleSpec{}, "old", 1, generationStart().Add(-time.Second), time.Hour)
				fireGeneration(h, true)
				revision = 2
			}
			exhaustCoordinator(h)
			before, physical := cloneNodes(h.durable), len(h.physical)
			s, ctx := h.root(true)
			request, _ := frozenRequest(t)
			err := s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: generationSpec(generationStart(), generationStart().Add(time.Hour), time.Second), Seed: "new", Revision: revision, After: generationStart().Add(-time.Second), CatchupWindow: time.Hour, Request: request})
			require.ErrorContains(t, err, "coordinator token exhausted")
			requireRejectedCoordinatorUnchanged(h, before, physical)
		})
	}
}

func TestExhaustedCoordinatorAllowsTransitionsWithoutNewTask(t *testing.T) {
	t.Parallel()
	t.Run("capacity_wait_acceptance", func(t *testing.T) {
		h := idleCoordinatorHarness(t)
		s, _ := h.root(true)
		s.Unfinished = 4
		for i := range 4 {
			s.Queue = append(s.Queue, &scheduler2pb.QueuedOccurrence{ManualId: uint64(i + 1), Nominal: h.now, Actual: h.now, WorkflowId: "queued", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL})
		}
		s.Coordination = &scheduler2pb.CoordinationState{Token: math.MaxUint64}
		h.commit(true)
		s, ctx := h.root(true)
		require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}))
		require.Empty(t, commitGeneration(h, true))
		s, _ = h.root(false)
		require.Len(t, s.Manuals, 1)
		require.False(t, s.Coordination.Armed)
	})
	t.Run("idle_update", func(t *testing.T) {
		h := idleCoordinatorHarness(t)
		exhaustCoordinator(h)
		s, ctx := h.root(true)
		require.NoError(t, s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: 2, Paused: true}))
		require.Empty(t, commitGeneration(h, true))
		s, _ = h.root(false)
		require.True(t, s.Paused)
	})
	t.Run("reuse_existing_wake", func(t *testing.T) {
		h := idleCoordinatorHarness(t)
		s, ctx := h.root(true)
		s.Coordination = &scheduler2pb.CoordinationState{Token: math.MaxUint64, Armed: true, At: h.now}
		ctx.(chasm.MutableContext).AddTask(s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, h.now)}, &scheduler2pb.WorkTask{Token: math.MaxUint64})
		commitGeneration(h, true)
		s, ctx = h.root(true)
		request, _ := frozenRequest(t)
		require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Seed: "replacement", Revision: 2, CatchupWindow: time.Hour, Request: request}))
		require.Empty(t, commitGeneration(h, true))
		s, _ = h.root(false)
		require.Equal(t, uint64(math.MaxUint64), s.Coordination.Token)
		require.True(t, s.Coordination.Armed)
	})
}
