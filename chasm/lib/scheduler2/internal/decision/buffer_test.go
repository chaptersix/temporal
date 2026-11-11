package decision

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

func queuedInput(t *testing.T, policy enumspb.ScheduleOverlapPolicy, count int) State {
	t.Helper()
	state := admitted(t)
	state.Root.Policy = policy
	state.Root.Remaining = 9
	for index := 0; index < count; index++ {
		at := int64(20 + index)
		var err error
		state, err = Step(state, Event{Kind: Due, Incarnation: "inc", Now: at, Nominal: at, Actual: at, CatchupDeadline: at, WorkflowID: fmt.Sprintf("workflow-%d", at)})
		require.NoError(t, err)
	}
	return state
}
func TestBufferCapacityAndNoAdmissionDebit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		policy                   enumspb.ScheduleOverlapPolicy
		count, retained, dropped int
	}{
		{"one_first", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1, 1, 0}, {"one_excess", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 5, 1, 4},
		{"all_below", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 3, 3, 0}, {"all_exact", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 4, 4, 0}, {"all_overflow", enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 5, 4, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := queuedInput(t, tc.policy, tc.count)
			var expected []Queued
			for index := 0; index < tc.retained; index++ {
				at := int64(20 + index)
				expected = append(expected, Queued{Nominal: at, Actual: at, WorkflowID: fmt.Sprintf("workflow-%d", at), Policy: tc.policy})
			}
			require.Equal(t, expected, state.Queue)
			require.Equal(t, int64(tc.dropped), state.Root.BufferDropped)
			require.Equal(t, int64(9), state.Root.Remaining)
			require.Equal(t, int64(20+tc.count-1), state.Root.Cursor)
			require.Equal(t, int64(10), state.Operation.Nominal)
		})
	}
}
func TestBufferPolicySnapshotAndFIFOAfterUpdate(t *testing.T) {
	t.Parallel()
	state := queuedInput(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1)
	var err error
	state, err = Step(state, Event{Kind: Update, Incarnation: "inc", Revision: 1, Limited: true, Remaining: 9, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL})
	require.NoError(t, err)
	state, err = Step(state, Event{Kind: Due, Incarnation: "inc", Now: 30, Nominal: 30, Actual: 30, CatchupDeadline: 30, WorkflowID: "second"})
	require.NoError(t, err)
	state, err = Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "first-chain"})
	require.NoError(t, err)
	state, err = Step(state, Event{Kind: Update, Incarnation: "inc", Revision: 2, Limited: true, Remaining: 9, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP})
	require.NoError(t, err)
	before := slices.Clone(state.Queue)
	state, err = Step(state, Event{Kind: Due, Incarnation: "inc", Now: 40, Nominal: 40, Actual: 40, CatchupDeadline: 40, WorkflowID: "must-not-bypass", Request: []byte("ingress")})
	require.NoError(t, err)
	require.Equal(t, before, state.Queue)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, state.Queue[0].Policy)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, state.Queue[1].Policy)
	state, err = Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 50, CatchupWindow: 100, Request: []byte("current-template")})
	require.NoError(t, err)
	require.Equal(t, int64(20), state.Operation.Nominal)
	require.Equal(t, "workflow-20", state.Operation.WorkflowID)
	require.Equal(t, []byte("current-template"), state.Operation.Request)
	require.Equal(t, int64(40), state.Root.Cursor)
	require.Equal(t, int64(8), state.Root.Remaining)
	require.Equal(t, before[1:], state.Queue)
}
func TestBufferRollbackDoesNotUndisposeObservedDueTime(t *testing.T) {
	t.Parallel()
	state := queuedInput(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 1)
	state, err := Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: 25})
	require.NoError(t, err)
	state, err = Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 15, CatchupWindow: 0, Request: []byte("current")})
	require.NoError(t, err)
	require.Equal(t, int64(20), state.Operation.Nominal)
	require.Equal(t, int64(20), state.Root.Cursor)
	require.Empty(t, state.Queue)
	require.Equal(t, int64(8), state.Root.Remaining)
}
func TestBufferOccupiedDrainPreservesQueue(t *testing.T) {
	t.Parallel()
	for _, phase := range []Phase{Pending, Started, Ambiguous} {
		t.Run(fmt.Sprintf("phase_%d", phase), func(t *testing.T) {
			t.Parallel()
			state := queuedInput(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 4)
			state.Operation.Phase = phase
			before := detachedCompletionState(t, state)
			after, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 1000, CatchupWindow: 0})
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
func TestBufferProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		policy := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
		budget := int64(4)
		var revision, cursor, started, dropped, now, deadline int64
		paused, counted := false, false
		var queue []Queued
		var operation, workflowID, chain string
		var phase Phase
		var request []byte
		state := State{Root: Root{Incarnation: "inc", Limited: true, Remaining: budget, Policy: policy}}
		policies := []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}
		for index, command := range rapid.SliceOfN(rapid.SampledFrom([]string{"due", "complete", "ack", "expire", "drain", "update"}), 1, 128).Draw(rt, "commands") {
			now += int64(time.Second)
			event := Event{Incarnation: "inc", Now: now, OperationID: operation, ChainID: operation + "/chain"}
			active := operation != "" && phase != Completed
			admitNominal := int64(0)
			admitWorkflow := ""
			switch command {
			case "due":
				event.Kind = Due
				event.Nominal = now
				event.Actual = now
				event.CatchupDeadline = now
				event.WorkflowID = fmt.Sprintf("workflow-%d", index)
				event.Request = []byte(fmt.Sprintf("ingress-%d", index))
				cursor = now
				if !paused && budget > 0 {
					if active || len(queue) > 0 {
						switch policy {
						case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
							if len(queue) > 0 {
								dropped++
							} else {
								queue = append(queue, Queued{Nominal: now, Actual: now, WorkflowID: event.WorkflowID, Policy: policy})
							}
						case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
							if len(queue) == 4 {
								dropped++
							} else {
								queue = append(queue, Queued{Nominal: now, Actual: now, WorkflowID: event.WorkflowID, Policy: policy})
							}
						default:
						}
					} else {
						admitNominal = now
						admitWorkflow = event.WorkflowID
					}
				}
			case "complete", "ack":
				event.Kind = Complete
				if command == "ack" {
					event.Kind = Acknowledged
				}
				if operation != "" {
					if !counted {
						started++
						counted = true
						chain = operation + "/chain"
					}
					if command == "complete" {
						phase = Completed
					} else if phase != Completed {
						phase = Started
					}
				}
			case "expire":
				now += int64(60 * time.Second)
				event.Now = now
				event.Kind = Expire
				if phase == Pending && now >= deadline {
					phase = Ambiguous
				}
			case "update":
				revision++
				policy = rapid.SampledFrom(policies).Draw(rt, "policy")
				paused = rapid.Bool().Draw(rt, "paused")
				budget = int64(rapid.IntRange(0, 5).Draw(rt, "budget"))
				event.Kind = Update
				event.Revision = revision
				event.Policy = policy
				event.Paused = paused
				event.Limited = true
				event.Remaining = budget
			case "drain":
				event.Kind = Drain
				event.CatchupWindow = int64(rapid.IntRange(0, 90).Draw(rt, "catchup_seconds")) * int64(time.Second)
				event.Request = []byte(fmt.Sprintf("drain-template-%d", index))
				if !active && len(queue) > 0 {
					selected := -1
					if !paused && budget > 0 {
						for i, candidate := range queue {
							if now-candidate.Actual <= event.CatchupWindow {
								selected = i
								break
							}
						}
					}
					if selected < 0 {
						queue = nil
					} else {
						candidate := queue[selected]
						admitNominal = candidate.Nominal
						admitWorkflow = candidate.WorkflowID
						queue = slices.Clone(queue[selected+1:])
					}
				}
			default:
				rt.Fatalf("unknown command %q", command)
			}
			if admitNominal != 0 {
				operation = fmt.Sprintf("inc/automatic/%d/action/start", admitNominal)
				workflowID = admitWorkflow
				phase = Pending
				chain = ""
				counted = false
				budget--
				deadline = now + int64(60*time.Second)
				request = bytes.Clone(event.Request)
			}
			next, err := Step(state, event)
			if err != nil {
				rt.Fatal(err)
			}
			state = next
			expectedRoot := Root{Incarnation: "inc", Revision: revision, Cursor: cursor, Paused: paused, Limited: true, Remaining: budget, Started: started, Policy: policy, BufferDropped: dropped}
			if state.Root != expectedRoot || !slices.Equal(queue, state.Queue) {
				rt.Fatalf("buffer state after %s: root=%+v expected=%+v queue=%+v expected=%+v", command, state.Root, expectedRoot, state.Queue, queue)
			}
			if operation != "" {
				op := state.Operation
				if op == nil || op.ID != operation || op.WorkflowID != workflowID || op.Phase != phase || op.ChainID != chain || !bytes.Equal(op.Request, request) {
					rt.Fatalf("admitted operation changed after %s: %+v", command, op)
				}
			} else if state.Operation != nil {
				rt.Fatal("unexpected admission")
			}
			if len(event.Request) > 0 {
				event.Request[0] ^= 0xff
				if state.Operation != nil && !bytes.Equal(state.Operation.Request, request) {
					rt.Fatal("request snapshot aliases command")
				}
			}
		}
	})
}

func TestBufferExpiredPrefixAndRefillOrdering(t *testing.T) {
	t.Parallel()
	state := queuedInput(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 4)
	state, err := Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain"})
	require.NoError(t, err)
	drained, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 25, CatchupWindow: 3, Request: []byte("current")})
	require.NoError(t, err)
	require.Equal(t, int64(22), drained.Operation.Nominal)
	require.Equal(t, "workflow-22", drained.Operation.WorkflowID)
	require.Equal(t, state.Queue[3:], drained.Queue)
	require.Equal(t, int64(8), drained.Root.Remaining)
	for _, refillFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("refill_before_drain_%t", refillFirst), func(t *testing.T) {
			t.Parallel()
			input := queuedInput(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1)
			input, err := Step(input, Event{Kind: Update, Incarnation: "inc", Revision: 1, Limited: true, Remaining: 0, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE})
			require.NoError(t, err)
			retained, err := Step(input, Event{Kind: Drain, Incarnation: "inc", Now: 30, CatchupWindow: 100})
			require.NoError(t, err)
			require.Equal(t, input, retained)
			completed, err := Step(retained, Event{Kind: Complete, Incarnation: "inc", OperationID: retained.Operation.ID, ChainID: "chain"})
			require.NoError(t, err)
			if refillFirst {
				completed, err = Step(completed, Event{Kind: Update, Incarnation: "inc", Revision: 2, Limited: true, Remaining: 2, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE})
				require.NoError(t, err)
			}
			after, err := Step(completed, Event{Kind: Drain, Incarnation: "inc", Now: 30, CatchupWindow: 100, Request: []byte("current")})
			require.NoError(t, err)
			require.Empty(t, after.Queue)
			if refillFirst {
				require.Equal(t, int64(20), after.Operation.Nominal)
				require.Equal(t, int64(1), after.Root.Remaining)
			} else {
				require.Equal(t, int64(10), after.Operation.Nominal)
				require.Zero(t, after.Root.Remaining)
				refilled, err := Step(after, Event{Kind: Update, Incarnation: "inc", Revision: 2, Limited: true, Remaining: 2, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE})
				require.NoError(t, err)
				final, err := Step(refilled, Event{Kind: Drain, Incarnation: "inc", Now: 31, CatchupWindow: 100, Request: []byte("current")})
				require.NoError(t, err)
				require.Equal(t, refilled, final)
			}
		})
	}
}

func TestBufferQueueOwnership(t *testing.T) {
	t.Parallel()
	for _, kind := range []Kind{Update, Drain} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			initial := queuedInput(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 3)
			initial.Operation.Phase = Completed
			// Spare capacity also exposes implementations that append into caller storage.
			queue := make([]Queued, len(initial.Queue), 8)
			copy(queue, initial.Queue)
			initial.Queue = queue
			expectedInput := slices.Clone(initial.Queue)
			event := Event{Kind: kind, Incarnation: "inc", Now: 30, Revision: 1, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, Limited: true, Remaining: 9, CatchupWindow: 100, Request: []byte("current")}
			result, err := Step(initial, event)
			require.NoError(t, err)
			require.NotEmpty(t, result.Queue)
			expectedOutput := slices.Clone(result.Queue)
			result.Queue[0].WorkflowID = "output-mutated"
			require.Equal(t, expectedInput, initial.Queue)
			initial.Queue[len(initial.Queue)-1].WorkflowID = "input-mutated"
			require.Equal(t, expectedOutput[len(expectedOutput)-1], result.Queue[len(result.Queue)-1])
		})
	}
}
