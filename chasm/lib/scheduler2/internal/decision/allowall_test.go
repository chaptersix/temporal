package decision

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

// This oracle owns its map and derives phases, accounting and blocking identity
// independently. Step receives only the event-addressed record, as the adapter does.
func TestAllowAllKeyedProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		type expectedOp struct {
			id      string
			phase   Phase
			policy  enumspb.ScheduleOverlapPolicy
			counted bool
			request []byte
		}
		actual := State{Root: Root{Incarnation: "inc", Limited: true, Remaining: 256, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}}
		records := map[string]*Operation{}
		expected := map[string]*expectedOp{}
		var order []string
		latest, blocker := "", ""
		var unfinished, started, dropped, cursor, admissions int64
		policy := enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
		revision := int64(0)
		length := rapid.IntRange(5, 128).Draw(t, "length")
		for i := 0; i < length; i++ {
			now := int64(i+1) * RetryHorizon
			command := rapid.IntRange(0, 5).Draw(t, "command")
			event := Event{Incarnation: "inc", Now: now, ChainID: "chain"}
			selected := latest
			if len(order) > 0 && command != 0 && command != 5 {
				selected = order[rapid.IntRange(0, len(order)-1).Draw(t, "target")]
			}
			actual.Operation = records[selected]
			event.OperationID = selected
			switch command {
			case 0:
				event.Kind = Due
				event.Nominal = now
				event.Actual = now
				event.CatchupDeadline = now
				event.Request = []byte(fmt.Sprintf("immutable-%d", i))
				actual.Operation = records[latest]
				cursor = now
				if policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || blocker == "" {
					if unfinished == 4 {
						dropped++
					} else {
						id := fmt.Sprintf("inc/automatic/%d/action/start", now)
						expected[id] = &expectedOp{id: id, phase: Pending, policy: policy, request: bytes.Clone(event.Request)}
						order = append(order, id)
						latest = id
						unfinished++
						admissions++
						if policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
							blocker = id
						}
					}
				}
			case 1, 2:
				event.Kind = Acknowledged
				if command == 2 {
					event.Kind = Complete
				}
				if op := expected[selected]; op != nil && op.phase != Completed && op.phase != StartOnly {
					if !op.counted {
						started++
						op.counted = true
					}
					op.phase = Started
					if op.policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
						op.phase = StartOnly
					} else if command == 2 {
						op.phase = Completed
					}
					if op.phase == Completed || op.phase == StartOnly {
						unfinished--
						if blocker == selected {
							blocker = ""
						}
					}
				}
			case 3:
				event.Kind = Expire
				if op := expected[selected]; op != nil && op.phase == Pending {
					op.phase = Ambiguous
				}
			case 4:
				event.Kind = Acknowledged
				event.Incarnation = "foreign"
			case 5:
				event.Kind = Update
				revision++
				event.Revision = revision
				event.Limited = true
				event.Remaining = 256 - admissions
				policy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
				if rapid.Bool().Draw(t, "skip") {
					policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
				}
				event.Policy = policy
				actual.Operation = records[latest]
			default:
				t.Fatal("unreachable command")
			}
			before := actual
			next, err := Step(actual, event)
			require.NoError(t, err)
			if next.Operation != nil {
				records[next.Operation.ID] = next.Operation
			}
			actual = next
			require.Equal(t, unfinished, actual.Root.Unfinished)
			require.Equal(t, started, actual.Root.Started)
			require.Equal(t, blocker, actual.Root.BlockingOperationID)
			require.Equal(t, dropped, actual.Root.CapacityDropped)
			require.Equal(t, cursor, actual.Root.Cursor)
			require.Equal(t, 256-admissions, actual.Root.Remaining)
			for id, want := range expected {
				got := records[id]
				require.NotNil(t, got)
				require.Equal(t, want.phase, got.Phase)
				require.Equal(t, want.policy, got.Policy)
				require.Equal(t, want.request, got.Request)
			}
			if len(event.Request) > 0 {
				event.Request[0] = '!'
				require.NotEqual(t, event.Request, actual.Operation.Request)
			}
			if actual.Operation != nil && before.Operation != nil && actual.Operation.ID == before.Operation.ID {
				saved := bytes.Clone(before.Operation.Request)
				actual.Operation.Request[0] = '!'
				require.Equal(t, saved, before.Operation.Request)
				actual.Operation.Request[0] = saved[0]
			}
		}
	})
}

func TestAllowAllDrainCapacityAndDisposition(t *testing.T) {
	t.Parallel()
	state := State{Root: Root{Incarnation: "inc", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, Unfinished: 4, Remaining: 8, Limited: true}, Queue: []Queued{{Nominal: 10, Actual: 10, WorkflowID: "queued", Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}}}
	require.False(t, CanDrain(state, 20, 100))
	retained, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 20, CatchupWindow: 100})
	require.NoError(t, err)
	require.Equal(t, state, retained)
	require.True(t, CanDrain(state, 20, 0))
	disposed, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 20, CatchupWindow: 0})
	require.NoError(t, err)
	require.Empty(t, disposed.Queue)
	require.Equal(t, int64(8), disposed.Root.Remaining)
	state.Root.Unfinished = 3
	admitted, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 20, CatchupWindow: 100, Request: []byte("current")})
	require.NoError(t, err)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, admitted.Operation.Policy)
	require.Equal(t, admitted.Operation.ID, admitted.Root.BlockingOperationID)
	require.Equal(t, int64(4), admitted.Root.Unfinished)
}
