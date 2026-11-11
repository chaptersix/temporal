package decision

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestActivityTerminalObservation(t *testing.T) {
	t.Parallel()
	for _, phase := range []Phase{Completed, StartOnly} {
		t.Run(map[Phase]string{Completed: "completed", StartOnly: "start-only"}[phase], func(t *testing.T) {
			t.Parallel()
			state := State{Root: Root{Incarnation: "inc", ActivitySet: true, ActivityAt: 20, Started: 7, Remaining: 3, UnconfirmedStarts: 2}, Operation: &Operation{ID: "op", ChainID: "chain", Phase: phase}, Carryover: &Carryover{Result: []byte("result")}}
			event := Event{Kind: Acknowledged, Incarnation: "inc", OperationID: "op", ChainID: "chain", StartObserved: true, StartObservedAt: 40, Now: 100}
			got, err := Step(state, event)
			require.NoError(t, err)
			want := clone(state)
			want.Root.ActivityAt = 40
			want.Operation.StartObserved = true
			want.Operation.StartObservedAt = 40
			require.Equal(t, want, got)
			event.StartObservedAt = 200
			duplicate, err := Step(got, event)
			require.NoError(t, err)
			require.Equal(t, got, duplicate)
			for _, bad := range []Event{
				{Kind: Acknowledged, Incarnation: "foreign", OperationID: "op", ChainID: "chain", StartObserved: true, StartObservedAt: 99},
				{Kind: Acknowledged, Incarnation: "inc", OperationID: "gone", ChainID: "chain", StartObserved: true, StartObservedAt: 99},
				{Kind: Acknowledged, Incarnation: "inc", OperationID: "op", ChainID: "wrong", StartObserved: true, StartObservedAt: 99},
				{Kind: Acknowledged, Incarnation: "inc", OperationID: "op", ChainID: "chain", StartObservedAt: 99},
			} {
				ignored, err := Step(state, bad)
				require.NoError(t, err)
				require.Equal(t, state, ignored)
			}
		})
	}
}

func TestActivitySummaryAcrossKeyedResults(t *testing.T) {
	t.Parallel()
	state := admitted(t)
	require.Equal(t, int64(1), state.Root.UnconfirmedStarts)
	state.Root.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	state.Root.Remaining = 2
	next, err := Step(state, Event{Kind: Due, Incarnation: "inc", Now: 20, Nominal: 20, CatchupDeadline: 30, Request: []byte("second")})
	require.NoError(t, err)
	require.Equal(t, int64(2), next.Root.UnconfirmedStarts)
	// Address the older unresolved operation while the latest operation remains pending.
	next.Operation = state.Operation
	next, err = Step(next, Event{Kind: Expire, Incarnation: "inc", OperationID: state.Operation.ID, Now: state.Operation.Deadline})
	require.NoError(t, err)
	require.Equal(t, int64(2), next.Root.UnconfirmedStarts)
	next, err = Step(next, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: 90})
	require.NoError(t, err)
	require.Equal(t, int64(1), next.Root.UnconfirmedStarts)
	require.False(t, next.Root.ActivitySet, "completion is not start-clock evidence")
}

func TestActivityControlAndRollbackClock(t *testing.T) {
	t.Parallel()
	state := State{Root: Root{Incarnation: "inc", ActivitySet: true, ActivityAt: 50}}
	next, err := Step(state, Event{Kind: Update, Incarnation: "inc", Revision: 1, Now: 40})
	require.NoError(t, err)
	require.Equal(t, int64(50), next.Root.ActivityAt)
	next, err = Step(next, Event{Kind: AcceptManual, Incarnation: "inc", Nominal: 1, Now: 60})
	require.NoError(t, err)
	require.Equal(t, int64(60), next.Root.ActivityAt)
	stale, err := Step(next, Event{Kind: Update, Incarnation: "inc", Revision: 1, Now: 100})
	require.NoError(t, err)
	require.Equal(t, next, stale)
	// Zero is valid explicit evidence, distinct from absent observation.
	zero := admitted(t)
	zero, err = Step(zero, Event{Kind: Acknowledged, Incarnation: "inc", OperationID: zero.Operation.ID, ChainID: "zero", StartObserved: true, StartObservedAt: 0, Now: 100})
	require.NoError(t, err)
	require.True(t, zero.Root.ActivitySet)
	require.Zero(t, zero.Root.ActivityAt)
	require.True(t, zero.Operation.StartObserved)
	require.Zero(t, zero.Root.UnconfirmedStarts)
}

func TestActivityFirstObservationAfterLegacyAcknowledgment(t *testing.T) {
	t.Parallel()
	state := admitted(t)
	id := state.Operation.ID
	acknowledged, err := Step(state, Event{Kind: Acknowledged, Incarnation: "inc", OperationID: id, ChainID: "chain", Now: 80})
	require.NoError(t, err)
	require.False(t, acknowledged.Root.ActivitySet)
	require.Zero(t, acknowledged.Root.UnconfirmedStarts)
	observed, err := Step(acknowledged, Event{Kind: Acknowledged, Incarnation: "inc", OperationID: id, ChainID: "chain", Now: 100, StartObserved: true, StartObservedAt: 60})
	require.NoError(t, err)
	want := clone(acknowledged)
	want.Root.ActivitySet = true
	want.Root.ActivityAt = 60
	want.Operation.StartObserved = true
	want.Operation.StartObservedAt = 60
	require.Equal(t, want, observed)
	// Losing the retained action record does not lose the root's activity evidence.
	observed.Operation = nil
	afterEviction, err := Step(observed, Event{Kind: Acknowledged, Incarnation: "inc", OperationID: id, ChainID: "chain", StartObserved: true, StartObservedAt: 200})
	require.NoError(t, err)
	require.Equal(t, observed, afterEviction)
}
