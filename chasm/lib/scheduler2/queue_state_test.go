package scheduler2

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func newQueueHarness(t *testing.T, records []decision.Queued) *treeHarness {
	t.Helper()
	h := newTreeHarness(t)
	s, ctx := h.root(true)
	s.IndexedQueue = newQueueState()
	s.QueueRecords = chasm.NewDataMap[*pb.QueuedOccurrence](ctx.(chasm.MutableContext))
	s.QueuePages = chasm.NewDataMap[*commonpb.Payload](ctx.(chasm.MutableContext))
	for _, q := range records {
		require.NoError(t, s.stageQueueAppend(ctx.(chasm.MutableContext), q))
	}
	h.commit(true)
	return h
}
func queueFixtureRecord(sequence int, at int64, policy enumspb.ScheduleOverlapPolicy, explicit bool) decision.Queued {
	q := decision.Queued{Nominal: int64(sequence), Actual: at, Policy: policy, WorkflowID: fmt.Sprintf("workflow%d", sequence)}
	if explicit {
		q.ManualID = uint64(sequence + 1)
	}
	return q
}
func queueHarnessState(t *testing.T, h *treeHarness) (*Scheduler, chasm.MutableContext, decision.State) {
	t.Helper()
	s, ctx := h.root(true)
	state, err := s.state(ctx)
	require.NoError(t, err)
	return s, ctx.(chasm.MutableContext), state
}
func TestQueueStateDrainReferenceAndMaintenance(t *testing.T) {
	t.Parallel()
	ordinary := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	cancel := enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
	terminate := enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	for _, name := range []string{"replacement", "nonprefix", "paused", "budget", "expired"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			records := []decision.Queued{queueFixtureRecord(1, 109, ordinary, false), queueFixtureRecord(2, 102, cancel, false), queueFixtureRecord(3, 110, ordinary, false), queueFixtureRecord(4, 103, cancel, true), queueFixtureRecord(5, 109, ordinary, true), queueFixtureRecord(6, 110, terminate, true), queueFixtureRecord(7, 110, ordinary, false)}
			if name == "nonprefix" {
				records = records[:3]
			}
			if name == "expired" {
				records = records[:2]
				records[0].Actual = 100
			}
			h := newQueueHarness(t, records)
			s, ctx, state := queueHarnessState(t, h)
			if name == "paused" {
				s.Paused = true
				state.Root.Paused = true
			}
			if name == "budget" {
				s.Remaining = 0
				state.Root.Remaining = 0
			}
			reference := state
			reference.IndexedQueue = decision.QueueSummary{}
			reference.Queue = records
			event := decision.Event{Kind: decision.Drain, Incarnation: state.Root.Incarnation, Now: 110, CatchupWindow: 3, Request: []byte("request")}
			expected, err := decision.Step(reference, event)
			require.NoError(t, err)
			plan, err := s.planQueueDrain(ctx, state, event.Now, event.CatchupWindow)
			require.NoError(t, err)
			require.NotNil(t, plan)
			require.NoError(t, s.stageQueueDrain(ctx, *plan, state))
			h.commit(false) // Rejected publication restores old projection and generation.
			s, ctx, state = queueHarnessState(t, h)
			actual, err := s.queueProjection(ctx)
			require.NoError(t, err)
			require.Equal(t, records, actual)
			if name == "paused" {
				s.Paused = true
				state.Root.Paused = true
			}
			if name == "budget" {
				s.Remaining = 0
				state.Root.Remaining = 0
			}
			plan, err = s.planQueueDrain(ctx, state, event.Now, event.CatchupWindow)
			require.NoError(t, err)
			require.NoError(t, s.stageQueueDrain(ctx, *plan, state))
			h.commit(true)
			s, ctx, _ = queueHarnessState(t, h)
			actual, err = s.queueProjection(ctx)
			require.NoError(t, err)
			require.Equal(t, expected.Queue, actual)
			projected := append([]decision.Queued(nil), expected.Queue...)
			remaining := int64(len(records))
			for remaining > 0 {
				s, ctx, _ = queueHarnessState(t, h)
				require.Equal(t, remaining, s.IndexedQueue.CompletedDrain.Remaining)
				// New arrivals and changed controls cannot rewrite the completed predicate.
				arrival := queueFixtureRecord(100+int(remaining), 0, ordinary, true)
				require.NoError(t, s.stageQueueAppend(ctx, arrival))
				projected = append(projected, arrival)
				s.Paused = !s.Paused
				progressed, err := s.maintainQueue(ctx)
				require.NoError(t, err)
				require.True(t, progressed)
				h.commit(false)
				s, ctx, _ = queueHarnessState(t, h)
				require.Equal(t, remaining, s.IndexedQueue.CompletedDrain.Remaining)
				require.NoError(t, s.stageQueueAppend(ctx, arrival))
				progressed, err = s.maintainQueue(ctx)
				require.NoError(t, err)
				require.True(t, progressed)
				h.commit(true)
				remaining--
				s, ctx, _ = queueHarnessState(t, h)
				actual, err = s.queueProjection(ctx)
				require.NoError(t, err)
				require.Equal(t, projected, actual)
			}
			require.Nil(t, s.IndexedQueue.CompletedDrain)
		})
	}
}

func TestQueueStateRejectsStaleAndExhausted(t *testing.T) {
	t.Parallel()
	q := queueFixtureRecord(1, 1, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, true)
	h := newQueueHarness(t, []decision.Queued{q})
	s, ctx, state := queueHarnessState(t, h)
	p, err := s.planQueueDrain(ctx, state, 2, 1)
	require.NoError(t, err)
	require.NoError(t, s.stageQueueAppend(ctx, queueFixtureRecord(2, 2, q.Policy, true)))
	require.ErrorContains(t, s.stageQueueDrain(ctx, *p, state), "fence")
	h.commit(false)
	s, ctx, _ = queueHarnessState(t, h)
	s.IndexedQueue.NextSequence = math.MaxUint64
	require.ErrorContains(t, s.stageQueueAppend(ctx, q), "exhausted")
	h.commit(false)
	s, ctx, state = queueHarnessState(t, h)
	state.QueueChange = decision.QueueChange{Appended: true, Append: queueFixtureRecord(3, 2, q.Policy, true)}
	state.IndexedQueue.Counts[3]++
	state.IndexedQueue.Version++
	p, err = s.planQueueDrain(ctx, state, 2, 1)
	require.NoError(t, err)
	require.True(t, p.Selected)
	before, err := s.queueProjection(ctx)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.ErrorContains(t, s.stageQueueDrain(ctx, *p, state), "fence")
}

func TestQueueStatePlanningControlsAndPreview(t *testing.T) {
	t.Parallel()
	ordinary := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	effect := enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	h := newQueueHarness(t, []decision.Queued{queueFixtureRecord(1, 1, ordinary, true)})
	s, ctx, state := queueHarnessState(t, h)
	// A prospective effect arrival becomes the exact selected record in the
	// transient index overlay, without touching the durable source record set.
	appendValue := queueFixtureRecord(2, 2, effect, true)
	state.QueueChange = decision.QueueChange{Appended: true, Append: appendValue}
	state.IndexedQueue.Counts[5]++
	state.IndexedQueue.Version++
	plan, err := s.planQueueDrain(ctx, state, 2, 1)
	require.NoError(t, err)
	require.Equal(t, appendValue, plan.Candidate)
	require.EqualValues(t, 2, plan.SelectedSequence)
	require.EqualValues(t, 1, plan.Superseded[3])
	projection, err := s.queueProjection(ctx)
	require.NoError(t, err)
	require.Len(t, projection, 1)
	require.EqualValues(t, 2, s.IndexedQueue.NextSequence)
	_, ok := s.QueueRecords.Get(ctx, queueRecordKey(2))
	require.False(t, ok)
	// A blocker prevents all dispositions. Capacity alone prevents selection,
	// but cannot prevent disposition when all automatic waiters are ineligible.
	state.QueueChange = decision.QueueChange{}
	state.IndexedQueue, err = s.queueSummary()
	require.NoError(t, err)
	state.Root.BlockingOperationID = "blocked"
	plan, err = s.planQueueDrain(ctx, state, 2, 1)
	require.NoError(t, err)
	require.Nil(t, plan)
	state.Root.BlockingOperationID = ""
	state.Root.Unfinished = 4
	plan, err = s.planQueueDrain(ctx, state, 2, 1)
	require.NoError(t, err)
	require.True(t, plan.Selected)
	require.ErrorContains(t, s.stageQueueDrain(ctx, *plan, state), "capacity")
	h = newQueueHarness(t, []decision.Queued{queueFixtureRecord(1, 1, ordinary, false)})
	s, ctx, state = queueHarnessState(t, h)
	state.Root.Unfinished = 4
	plan, err = s.planQueueDrain(ctx, state, 100, 1)
	require.NoError(t, err)
	require.False(t, plan.Selected)
	require.NoError(t, s.stageQueueDrain(ctx, *plan, state))
	require.EqualValues(t, 1, s.IndexedQueue.CompletedDrain.Missed)
}

func TestQueueStateCorruptionAndCleanupPreflight(t *testing.T) {
	t.Parallel()
	h := newQueueHarness(t, []decision.Queued{queueFixtureRecord(1, 1, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, true), queueFixtureRecord(2, 2, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, true)})
	s, ctx, state := queueHarnessState(t, h)
	p, err := s.planQueueDrain(ctx, state, 3, 3)
	require.NoError(t, err)
	require.NoError(t, s.stageQueueDrain(ctx, *p, state))
	h.commit(true)
	s, ctx, _ = queueHarnessState(t, h)
	before := common.CloneProto(s.IndexedQueue)
	s.QueueRecords.Delete(ctx, queueRecordKey(s.IndexedQueue.CompletedDrain.NextSequence))
	progressed, err := s.maintainQueue(ctx)
	require.ErrorContains(t, err, "record absent")
	require.False(t, progressed)
	require.True(t, proto.Equal(before, s.IndexedQueue))
	h.commit(false)
	s, _, _ = queueHarnessState(t, h)
	require.True(t, proto.Equal(before, s.IndexedQueue))
	s.Queue = append(s.Queue, encodeQueued(queueFixtureRecord(3, 3, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, true)))
	_, err = s.queueSummary()
	require.ErrorContains(t, err, "format")
	// A corrupt candidate is discarded by the lease; do not close/publish it.
	h.reload()
	s, ctx, _ = queueHarnessState(t, h)
	s.IndexedQueue.CompletedDrain.Threshold = nil
	_, err = s.maintainQueue(ctx)
	require.ErrorContains(t, err, "completed queue drain")
}

func TestQueueStatePlannerRejectsMalformedPreview(t *testing.T) {
	t.Parallel()
	q := queueFixtureRecord(1, 1, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, true)
	h := newQueueHarness(t, []decision.Queued{q})
	s, ctx, state := queueHarnessState(t, h)
	state.IndexedQueue.Counts[3] = -1
	_, err := s.planQueueDrain(ctx, state, 2, 1)
	require.ErrorContains(t, err, "count")
	state.IndexedQueue.Counts[3] = 1
	s.IndexedQueue = nil
	state.QueueChange = decision.QueueChange{Appended: true, Append: q}
	state.IndexedQueue.Counts[3]++
	state.IndexedQueue.Version++
	_, err = s.planQueueDrain(ctx, state, 2, 1)
	require.ErrorContains(t, err, "indexed storage")
}

func TestQueueStateSignedExtremeEligibility(t *testing.T) {
	t.Parallel()
	q := queueFixtureRecord(1, math.MinInt64, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, false)
	h := newQueueHarness(t, []decision.Queued{q})
	s, ctx, state := queueHarnessState(t, h)
	plan, err := s.planQueueDrain(ctx, state, 1e18, 3)
	require.NoError(t, err)
	require.False(t, plan.Selected)
	require.EqualValues(t, 1, plan.Ineligible[0])
	require.NoError(t, s.stageQueueDrain(ctx, *plan, state))
	h.commit(true)
	s, ctx, _ = queueHarnessState(t, h)
	require.EqualValues(t, 1, s.IndexedQueue.CompletedDrain.Missed)
	result, err := s.queueProjection(ctx)
	require.NoError(t, err)
	require.Empty(t, result)
	progressed, err := s.maintainQueue(ctx)
	require.NoError(t, err)
	require.True(t, progressed)
	h.commit(true)
	s, _, _ = queueHarnessState(t, h)
	require.Nil(t, s.IndexedQueue.CompletedDrain)
}

func TestQueueStateIndexedPlanReferenceTraces(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(17, 29))
	policies := []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}
	for trace := 0; trace < 48; trace++ {
		t.Run(fmt.Sprint(trace), func(t *testing.T) {
			// The deterministic generator belongs to this serial parent trace sequence.
			records := make([]decision.Queued, 1+rng.IntN(20))
			for i := range records {
				records[i] = queueFixtureRecord(i+1, int64(90+rng.IntN(25)), policies[rng.IntN(3)], rng.IntN(2) == 0)
			}
			h := newQueueHarness(t, records)
			s, ctx, state := queueHarnessState(t, h)
			state.Root.Paused = trace%5 == 0
			state.Root.Remaining = int64(trace % 3)
			reference := state
			reference.IndexedQueue = decision.QueueSummary{}
			reference.Queue = records
			event := decision.Event{Kind: decision.Drain, Incarnation: state.Root.Incarnation, Now: 110, CatchupWindow: 7, Request: []byte("request")}
			expected, err := decision.Step(reference, event)
			require.NoError(t, err)
			p, err := s.planQueueDrain(ctx, state, 110, 7)
			require.NoError(t, err)
			require.NotNil(t, p)
			indexedEvent := event
			indexedEvent.QueueDrain = p
			next, err := decision.Step(state, indexedEvent)
			require.NoError(t, err)
			require.Equal(t, expected.Operation, next.Operation)
			require.Equal(t, expected.Root, next.Root)
			require.NoError(t, s.stageQueueDrain(ctx, *p, state))
			h.commit(true)
			for {
				s, ctx, _ = queueHarnessState(t, h)
				actual, err := s.queueProjection(ctx)
				require.NoError(t, err)
				require.Equal(t, expected.Queue, actual)
				progressed, err := s.maintainQueue(ctx)
				require.NoError(t, err)
				if !progressed {
					break
				}
				h.commit(true)
			}
		})
	}
}
