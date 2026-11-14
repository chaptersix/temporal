package decision

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

func indexedReference(state State) State {
	state.IndexedQueue = QueueSummary{Enabled: true, Generation: 1, Version: 1}
	for _, q := range state.Queue {
		state.IndexedQueue.Counts[QueueClass(q)]++
	}
	state.Queue = nil
	return state
}

// The test planner deliberately scans the finite oracle. Runtime plans must use
// index queries; projecting this scan into production would violate the work bound.
func referenceDrainPlan(state State, event Event) QueueDrainPlan {
	indexed := indexedReference(state)
	plan := QueueDrainPlan{Fence: QueueDrainFence{Summary: indexed.IndexedQueue, Root: state.Root, Capacity: state.Capacity, Now: event.Now, Catchup: event.CatchupWindow}}
	selected := DrainIndex(state, event.Now, event.CatchupWindow)
	first := true
	for i, q := range state.Queue {
		class := QueueClass(q)
		if !queueEligible(state, q, event.Now, event.CatchupWindow) {
			plan.Ineligible[class]++
			continue
		}
		if first {
			plan.FirstSequence = uint64(i + 1)
			plan.FirstClass = class
		}
		if i < selected && (first || effectPolicy(q.Policy) != NoEffect) {
			plan.Superseded[class]++
		} else if i != selected {
			plan.Retained[class]++
		}
		first = false
	}
	if selected >= 0 {
		plan.Selected = true
		plan.SelectedSequence = uint64(selected + 1)
		plan.Candidate = state.Queue[selected]
	}
	return plan
}
func requireQueueProjection(t *testing.T, reference, indexed State) {
	t.Helper()
	summary := indexedReference(reference).IndexedQueue
	require.Equal(t, summary.Counts, indexed.IndexedQueue.Counts)
	require.Nil(t, indexed.Queue)
	indexed.IndexedQueue = QueueSummary{}
	indexed.QueueChange = QueueChange{}
	reference.Queue = nil
	require.Equal(t, reference, indexed)
}
func queueTestRecord(at int64, policy enumspb.ScheduleOverlapPolicy, explicit bool) Queued {
	q := Queued{Actual: at, Nominal: at, WorkflowID: "workflow", Policy: policy}
	if explicit {
		q.ManualID = uint64(at + 1)
	}
	return q
}
func TestIndexedQueueDrainMatchesFinite(t *testing.T) {
	t.Parallel()
	ordinary := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	cancel := enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
	terminate := enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	for _, name := range []string{"replacement", "nonprefix expiry", "paused", "budget exhausted", "no eligible", "operation unavailable", "no eligible and operation unavailable"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state := State{Root: Root{Incarnation: "incarnation", Revision: 1, Remaining: 3}, Capacity: Capacity{Public: true}}
			state.Queue = []Queued{queueTestRecord(109, ordinary, false), queueTestRecord(109, ordinary, true), queueTestRecord(109, cancel, false), queueTestRecord(109, cancel, true), queueTestRecord(109, terminate, false), queueTestRecord(109, terminate, true), queueTestRecord(110, ordinary, false)}
			switch name {
			case "replacement":
			case "nonprefix expiry":
				state.Queue[1] = queueTestRecord(102, ordinary, false)
			case "paused":
				state.Root.Paused = true
			case "budget exhausted":
				state.Root.Limited = true
				state.Root.Remaining = 0
			case "no eligible":
				state.Queue = []Queued{queueTestRecord(102, ordinary, false)}
			case "no eligible and operation unavailable":
				state.Queue = []Queued{queueTestRecord(102, ordinary, false)}
				state.Capacity.Public = false
				state.Root.Unfinished = 4
			case "operation unavailable":
				state.Capacity.Public = false
				state.Root.Unfinished = 4
			default:
				t.Fatal("unexpected case")
			}
			event := Event{Kind: Drain, Incarnation: "incarnation", Now: 110, CatchupWindow: 3, Request: []byte("request")}
			reference, err := Step(state, event)
			require.NoError(t, err)
			plan := referenceDrainPlan(state, event)
			event.QueueDrain = &plan
			indexed, err := Step(indexedReference(state), event)
			require.NoError(t, err)
			requireQueueProjection(t, reference, indexed)
			applied := name != "operation unavailable"
			require.Equal(t, applied, indexed.QueueChange.Drained)
			require.Equal(t, applied, indexed.IndexedQueue.Maintenance)
			if applied {
				require.EqualValues(t, 2, indexed.IndexedQueue.Generation)
				require.Equal(t, plan, indexed.QueueChange.Drain)
			}
		})
	}
}
func TestIndexedQueueAppendMatchesFinite(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
		for _, limit := range []int64{-1, 0, 1, 5} {
			for _, kind := range []Kind{Due, DispatchManual, DispatchBackfill} {
				state := State{Root: Root{Incarnation: "incarnation", Revision: 1, Policy: policy, BlockingOperationID: "active"}, Capacity: Capacity{Public: true, MaxBuffer: limit}, Queue: []Queued{queueTestRecord(99, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, false)}}
				event := Event{Kind: kind, Incarnation: "incarnation", Now: 110, Nominal: 100, Actual: 100, CatchupDeadline: 200, WorkflowID: "new", Policy: policy, BackfillID: 1}
				if kind == DispatchManual {
					state.Manuals = []ManualCommand{{ID: 1, Nominal: 100, Policy: policy}}
				}
				reference, err := Step(state, event)
				require.NoError(t, err)
				indexed, err := Step(indexedReference(state), event)
				require.NoError(t, err)
				requireQueueProjection(t, reference, indexed)
				appended := len(reference.Queue) > len(state.Queue)
				require.Equal(t, appended, indexed.QueueChange.Appended)
				if appended {
					require.Equal(t, reference.Queue[len(reference.Queue)-1], indexed.QueueChange.Append)
				}
				// Both a stale event and a normal update must drop the previous output.
				for _, incarnation := range []string{"stale", "incarnation"} {
					next, err := Step(indexed, Event{Kind: Update, Incarnation: incarnation, Revision: 2, Policy: policy})
					require.NoError(t, err)
					require.Equal(t, QueueChange{}, next.QueueChange)
				}
			}
		}
	}
}
func TestIndexedQueuePlanRejectsInconsistency(t *testing.T) {
	t.Parallel()
	state := State{Root: Root{Incarnation: "incarnation", Revision: 1}, Capacity: Capacity{Public: true}, Queue: []Queued{queueTestRecord(109, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, false)}}
	event := Event{Kind: Drain, Incarnation: "incarnation", Now: 110, CatchupWindow: 3, Request: []byte("request")}
	for _, mutate := range []func(*QueueDrainPlan){
		func(p *QueueDrainPlan) { p.Fence.Summary.Version++ },
		func(p *QueueDrainPlan) { p.Fence.Root.Paused = true },
		func(p *QueueDrainPlan) { p.Fence.Capacity.MaxBuffer++ },
		func(p *QueueDrainPlan) { p.Retained[0] = 1 },
		func(p *QueueDrainPlan) { p.Ineligible[0] = -1 },
		func(p *QueueDrainPlan) { p.FirstClass = 3 },
		func(p *QueueDrainPlan) { p.SelectedSequence = 2 },
		func(p *QueueDrainPlan) { p.Candidate.Actual = 1 },
	} {
		plan := referenceDrainPlan(state, event)
		mutate(&plan)
		event.QueueDrain = &plan
		_, err := Step(indexedReference(state), event)
		require.Error(t, err)
	}
	invalid := indexedReference(state)
	invalid.Queue = state.Queue
	_, err := Step(invalid, event)
	require.ErrorContains(t, err, "representation")
	invalid = indexedReference(state)
	invalid.IndexedQueue.Counts = [6]int64{math.MaxInt64, 1}
	_, err = Step(invalid, event)
	require.ErrorContains(t, err, "count")
}
func TestIndexedQueueMaintenanceDefersOnlyDrain(t *testing.T) {
	t.Parallel()
	state := indexedReference(State{Root: Root{Incarnation: "incarnation", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, BlockingOperationID: "active"}, Capacity: Capacity{Public: true}})
	state.IndexedQueue.Maintenance = true
	next, err := Step(state, Event{Kind: Due, Incarnation: "incarnation", Nominal: 100, Actual: 100, Now: 110, CatchupDeadline: 200, WorkflowID: "new"})
	require.NoError(t, err)
	require.True(t, next.QueueChange.Appended)
	next.Root.BlockingOperationID = ""
	drained, err := Step(next, Event{Kind: Drain, Incarnation: "incarnation", Now: 110, CatchupWindow: 3})
	require.NoError(t, err)
	require.Equal(t, QueueChange{}, drained.QueueChange)
	require.Equal(t, next.IndexedQueue, drained.IndexedQueue)
}

func TestIndexedQueueDrainGeneratedReference(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		state := State{Root: Root{Incarnation: "incarnation", Revision: 1, Paused: rapid.Bool().Draw(rt, "paused"), Limited: rapid.Bool().Draw(rt, "limited"), Remaining: int64(rapid.IntRange(0, 3).Draw(rt, "remaining"))}, Capacity: Capacity{Public: rapid.Bool().Draw(rt, "public")}}
		state.Root.Unfinished = int64(rapid.IntRange(0, 5).Draw(rt, "unfinished"))
		n := rapid.IntRange(1, 40).Draw(rt, "size")
		policies := []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER}
		for range n {
			state.Queue = append(state.Queue, queueTestRecord(int64(rapid.IntRange(90, 120).Draw(rt, "actual")), policies[rapid.IntRange(0, 2).Draw(rt, "policy")], rapid.Bool().Draw(rt, "explicit")))
		}
		event := Event{Kind: Drain, Incarnation: "incarnation", Now: 110, CatchupWindow: int64(rapid.IntRange(0, 20).Draw(rt, "catchup")), Request: []byte("request")}
		reference, err := Step(state, event)
		if err != nil {
			rt.Fatal(err)
		}
		plan := referenceDrainPlan(state, event)
		event.QueueDrain = &plan
		indexed, err := Step(indexedReference(state), event)
		if err != nil {
			rt.Fatal(err)
		}
		expectedCounts := indexedReference(reference).IndexedQueue.Counts
		if indexed.IndexedQueue.Counts != expectedCounts || indexed.Root != reference.Root || QueueEffect(indexed) != QueueEffect(reference) {
			rt.Fatalf("projection differs: reference=%+v indexed=%+v", reference, indexed)
		}
		if (indexed.Operation == nil) != (reference.Operation == nil) {
			rt.Fatal("operation presence differs")
		}
		if indexed.Operation != nil && (indexed.Operation.ID != reference.Operation.ID || indexed.Operation.Policy != reference.Operation.Policy) {
			rt.Fatal("operation identity differs")
		}
	})
}

func TestQueueEligibilitySignedDifferenceOverflow(t *testing.T) {
	t.Parallel()
	state := State{Root: Root{Incarnation: "incarnation", Revision: 1}, Capacity: Capacity{Public: true}, Queue: []Queued{queueTestRecord(math.MinInt64, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, false)}}
	// Both operands are representable; their positive elapsed difference is not.
	event := Event{Kind: Drain, Incarnation: "incarnation", Now: 1_000_000_000_000_000_000, CatchupWindow: 3, Request: []byte("request")}
	require.False(t, queueEligible(state, state.Queue[0], event.Now, event.CatchupWindow))
	reference, err := Step(state, event)
	require.NoError(t, err)
	require.Nil(t, reference.Operation)
	require.Empty(t, reference.Queue)
	plan := referenceDrainPlan(state, event)
	event.QueueDrain = &plan
	indexed, err := Step(indexedReference(state), event)
	require.NoError(t, err)
	requireQueueProjection(t, reference, indexed)
	require.True(t, indexed.QueueChange.Drained)
	require.EqualValues(t, 1, indexed.QueueChange.Drain.Ineligible[0])
}
