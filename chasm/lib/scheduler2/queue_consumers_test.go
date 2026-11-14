package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestQueueConsumersRejectInvalidSummaryBeforePublication(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unknown format", "negative count", "mixed representations"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			s, ctx := h.root(true)
			s.IndexedQueue = newQueueState()
			switch kind {
			case "unknown format":
				s.IndexedQueue.FormatVersion = 2
			case "negative count":
				s.IndexedQueue.AutomaticOrdinary = -1
			case "mixed representations":
				s.Queue = []*pb.QueuedOccurrence{{WorkflowId: "mixed"}}
			default:
				t.Fatal("unknown case")
			}
			before := common.CloneProto(s.RootState)
			require.False(t, s.IsExecutionSkippable(ctx))
			require.Panics(t, func() { s.SearchAttributes(ctx) }, "invalid queue metadata must not emit a visibility count")
			planned, demand, wake, err := s.planWork(ctx)
			require.Error(t, err)
			require.Nil(t, planned)
			require.Nil(t, demand)
			require.True(t, wake.IsZero())
			require.Error(t, s.applyTransition(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Update, Incarnation: s.Incarnation}))
			protorequire.ProtoEqual(t, before, s.RootState)
			require.Empty(t, h.backend.TasksByCategory)
		})
	}
}

func TestIndexedQueueCoordinatorPublicationAndCleanup(t *testing.T) {
	t.Parallel()
	for _, arrivals := range []bool{false, true} {
		name := "without arrivals"
		if arrivals {
			name = "arrivals during cleanup"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testIndexedQueueCoordinatorCleanup(t, arrivals)
		})
	}
}

func testIndexedQueueCoordinatorCleanup(t *testing.T, arrivals bool) {
	t.Helper()
	h := newQueueHarness(t, nil)
	sequentialBudget(h, 10)
	base := generationStart()
	h.now = base.Add(3 * time.Second).UnixNano()
	request, _ := frozenRequest(t)
	configureBuffer(h, generationSpec(base, base.Add(3*time.Second), time.Second), 1, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, time.Minute, request)
	for step := 0; step < 16 && generationState(h).Armed; step++ {
		h.now = max(h.now, generationState(h).WakeAt)
		fireGeneration(h, true)
	}
	s, ctx := h.root(false)
	require.False(t, generationState(h).Armed)
	require.Empty(t, s.Queue)
	require.EqualValues(t, 3, s.queueLength())
	queued, err := s.queueProjection(ctx)
	require.NoError(t, err)
	require.Len(t, queued, 3)
	first := h.request()
	sequentialComplete(h, first.RequestId, &commonpb.Payload{Data: []byte("done")})
	before := cloneNodes(h.durable)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable, "rejected drain retains queue and issuance authority")
	coordinatorStep(h, true)
	s, ctx = h.root(false)
	require.EqualValues(t, 2, s.queueLength())
	require.NotNil(t, s.IndexedQueue.CompletedDrain)
	require.EqualValues(t, 3, s.IndexedQueue.CompletedDrain.Remaining)
	admitted, started := s.OperationId, s.Started
	want := append([]decision.Queued(nil), queued[1:]...)
	for remaining := int64(3); remaining > 0; remaining-- {
		if arrivals {
			h.now++
			dropped := s.BufferDropped
			_, wire := frozenRequest(t)
			h.apply(decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: h.now,
				Actual: h.now, CatchupDeadline: h.now, WorkflowID: "arriving", Request: wire}, true)
			s, ctx = h.root(false)
			require.Equal(t, remaining, s.IndexedQueue.CompletedDrain.Remaining, "arrivals cannot extend frozen source cleanup")
			current, err := s.queueProjection(ctx)
			require.NoError(t, err)
			require.Len(t, current, min(len(want)+1, 4), "finite harness retains its four-entry capacity")
			if len(current) > len(want) {
				want = append(want, current[len(current)-1])
				require.Equal(t, dropped, s.BufferDropped)
			} else {
				require.Equal(t, dropped+1, s.BufferDropped)
			}
		}
		before := cloneNodes(h.durable)
		coordinatorStep(h, false)
		require.Equal(t, before, h.durable, "rejected maintenance preserves source, active entries and wake authority")
		s, ctx = h.root(false)
		require.False(t, s.IsExecutionSkippable(ctx))
		require.True(t, s.Coordination.Armed, "cleanup debt retains a physical work wake")
		coordinatorStep(h, true)
		s, ctx = h.root(false)
		require.Equal(t, admitted, s.OperationId)
		require.Equal(t, started, s.Started, "maintenance does not admit another operation")
		if remaining > 1 {
			require.Equal(t, remaining-1, s.IndexedQueue.CompletedDrain.Remaining)
		}
	}
	require.Nil(t, s.IndexedQueue.CompletedDrain)
	projected, err := s.queueProjection(ctx)
	require.NoError(t, err)
	require.Equal(t, want, projected)
	require.Empty(t, s.Queue)
}

func TestPublicQueueExpiryWideNanoseconds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                       string
		now, actual, catchup, want int64
	}{
		{"overflowed age", math.MaxInt64, math.MinInt64, math.MaxInt64, 1},
		{"future after rollback", math.MinInt64, math.MaxInt64, 3, 0},
		{"inclusive cutoff", math.MaxInt64, 0, math.MaxInt64, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, publicExpiredWaiters([]decision.Queued{{Actual: tc.actual}}, nil, tc.now, tc.catchup))
		})
	}
}

func TestPublicCreationUsesIndexedQueue(t *testing.T) {
	t.Parallel()
	limit := 5
	h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	for range 5 {
		publicProjectionAdmission(h)
	}
	s, ctx := h.root(false)
	require.NotNil(t, s.IndexedQueue)
	require.Empty(t, s.Queue, "public queue must not dual-write the finite reference slice")
	require.EqualValues(t, 4, s.queueLength())
	projected, err := s.queueProjection(ctx)
	require.NoError(t, err)
	require.Len(t, projected, 4)
	require.EqualValues(t, 5, s.queueLength()+s.UnconfirmedStarts)
	h.reload()
	s, _ = h.root(false)
	require.NotNil(t, s.IndexedQueue)
	require.EqualValues(t, 4, s.queueLength())
	require.Empty(t, s.Queue)
}
