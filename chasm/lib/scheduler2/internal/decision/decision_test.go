package decision

import (
	"bytes"
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

var sequenceLength = flag.Int("scheduler2.events", 32, "events per generated scheduler scenario")

func admitted(t *testing.T) State {
	t.Helper()
	state, err := Step(State{Root: Root{Incarnation: "inc", Limited: true, Remaining: 1}}, Event{
		Kind: Due, Incarnation: "inc", Now: 10, Nominal: 10, CatchupDeadline: 20, Request: []byte("frozen"),
	})
	require.NoError(t, err)
	return state
}

func TestAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		root     Root
		now      int64
		want     Root
		admitted bool
	}{
		{"eligible", Root{Limited: true, Remaining: 1}, 10, Root{Cursor: 10, Limited: true}, true},
		{"paused", Root{Paused: true, Limited: true, Remaining: 1}, 10, Root{Cursor: 10, Paused: true, Limited: true, Remaining: 1}, false},
		{"exhausted", Root{Limited: true}, 10, Root{Cursor: 10, Limited: true}, false},
		{"expired", Root{Limited: true, Remaining: 1}, 21, Root{Cursor: 10, Limited: true, Remaining: 1}, false},
		{"not due", Root{Limited: true, Remaining: 1}, 9, Root{Limited: true, Remaining: 1}, false},
		{"at catchup boundary", Root{Limited: true, Remaining: 1}, 20, Root{Cursor: 10, Limited: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.root.Incarnation, tc.want.Incarnation = "inc", "inc"
			got, err := Step(State{Root: tc.root}, Event{Kind: Due, Incarnation: "inc", Now: tc.now, Nominal: 10, CatchupDeadline: 20, Request: []byte("request")})
			require.NoError(t, err)
			want := State{Root: tc.want}
			if tc.admitted {
				want.Operation = &Operation{OccurrenceID: "automatic/10", ActionID: "inc/automatic/10/action", ID: "inc/automatic/10/action/start", Nominal: 10, Deadline: tc.now + RetryHorizon, RetryAt: tc.now, Phase: Pending, Request: []byte("request")}
			}
			require.Equal(t, want, got)
		})
	}
}

func TestLostAcknowledgmentAndEarlyCompletion(t *testing.T) {
	t.Parallel()
	state := admitted(t)
	original := clone(state)
	request, ok := Issue(state, 10)
	require.True(t, ok)
	// The remote accepts these exact bytes but the response is lost.
	state, err := Step(state, Event{Kind: Update, Incarnation: "inc", Revision: 1, Limited: true, Remaining: 5, Request: []byte("replacement")})
	require.NoError(t, err)
	state, err = Step(state, Event{Kind: RetryableFailure, Incarnation: "inc", OperationID: state.Operation.ID, Now: 11})
	require.NoError(t, err)
	retry, ok := Issue(state, 11+int64(time.Second))
	require.True(t, ok)
	require.Equal(t, request, retry)
	state, err = Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: 12})
	require.NoError(t, err)
	completed := clone(state)
	for _, kind := range []Kind{Acknowledged, RetryableFailure, Complete, Expire} {
		state, err = Step(state, Event{Kind: kind, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: state.Operation.Deadline})
		require.NoError(t, err)
		require.Equal(t, completed, state)
	}
	require.Equal(t, int64(1), state.Root.Started)
	require.Equal(t, int64(5), state.Root.Remaining)
	require.Equal(t, Pending, original.Operation.Phase)
	_, ok = Issue(state, 13)
	require.False(t, ok)
}

func TestExpiryAndLateEvidence(t *testing.T) {
	t.Parallel()
	for _, kind := range []Kind{Acknowledged, Complete} {
		t.Run(map[Kind]string{Acknowledged: "acknowledgment", Complete: "completion"}[kind], func(t *testing.T) {
			t.Parallel()
			state := admitted(t)
			deadline := state.Operation.Deadline
			_, ok := Issue(state, deadline)
			require.False(t, ok)
			var err error
			state, err = Step(state, Event{Kind: Expire, Incarnation: "inc", OperationID: state.Operation.ID, Now: deadline})
			require.NoError(t, err)
			require.Equal(t, Ambiguous, state.Operation.Phase)
			_, ok = Issue(state, 10) // Rollback cannot resurrect expired issuance.
			require.False(t, ok)
			_, ok = Wake(state)
			require.False(t, ok)
			state, err = Step(state, Event{Kind: kind, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: deadline + 1})
			require.NoError(t, err)
			require.Equal(t, int64(1), state.Root.Started)
			require.Equal(t, int64(0), state.Root.Remaining)
			want := Started
			if kind == Complete {
				want = Completed
			}
			require.Equal(t, want, state.Operation.Phase)
		})
	}
}

func TestIdentityAndImmutableOwnership(t *testing.T) {
	t.Parallel()
	state := admitted(t)
	for _, event := range []Event{
		{Kind: Due, Incarnation: "old", Now: 20, Nominal: 20},
		{Kind: Update, Incarnation: "old", Revision: 9, Remaining: 99},
		{Kind: Complete, Incarnation: "old", OperationID: state.Operation.ID, ChainID: "chain"},
		{Kind: Complete, Incarnation: "inc", OperationID: "unknown", ChainID: "chain"},
		{Kind: Due, Incarnation: "inc", Now: 10, Nominal: 10},
	} {
		got, err := Step(state, event)
		require.NoError(t, err)
		require.Equal(t, state, got)
		got.Operation.Request[0] = 'X'
		require.Equal(t, []byte("frozen"), state.Operation.Request)
	}
	state, err := Step(state, Event{Kind: Acknowledged, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain"})
	require.NoError(t, err)
	got, err := Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "other"})
	require.ErrorContains(t, err, "conflicting workflow chain")
	require.Equal(t, state, got)
}

func TestConcurrentIssueOwnership(t *testing.T) {
	t.Parallel()
	state := admitted(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 32 {
				request, _ := Issue(state, 10)
				request[0] = 'X'
			}
		})
	}
	wg.Wait()
	require.Equal(t, []byte("frozen"), state.Operation.Request)
}

func TestEventSequences(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		state := State{Root: Root{Incarnation: "inc", Limited: true, Remaining: 1}}
		payload := rapid.SliceOfN(rapid.Byte(), 1, 32).Draw(t, "request")
		var err error
		state, err = Step(state, Event{Kind: Due, Incarnation: "inc", Now: 10, Nominal: 10, CatchupDeadline: 20, Request: payload})
		if err != nil {
			t.Fatal(err)
		}
		frozen := bytes.Clone(payload)
		payload[0] ^= 255
		accepted, completed, expired := false, false, false
		revision := int64(0)
		for n := 0; n < *sequenceLength; n++ {
			kind := rapid.SampledFrom([]Kind{Acknowledged, Complete, RetryableFailure, Expire, Update, Due}).Draw(t, "kind")
			now := rapid.SampledFrom([]int64{0, 10, 11, 10 + RetryHorizon - 1, 10 + RetryHorizon, 10 + RetryHorizon + 1}).Draw(t, "now")
			stale := rapid.Bool().Draw(t, "stale")
			event := Event{Kind: kind, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: now, Nominal: 10, Revision: revision + 1, Limited: true, Remaining: 0, Request: []byte("changed")}
			if stale {
				event.Incarnation = "old"
			}
			before := clone(state)
			state, err = Step(state, event)
			if err != nil {
				t.Fatal(err)
			}
			if !stale {
				switch kind {
				case Acknowledged:
					accepted = true
				case Complete:
					accepted = true
					completed = true
				case Expire, RetryableFailure:
					if !accepted && now >= 10+RetryHorizon {
						expired = true
					}
				case Update:
					revision++
				default:
					// A duplicate occurrence leaves the reference state unchanged.
				}
			}
			if !bytes.Equal(state.Operation.Request, frozen) || state.Operation.ID != "inc/automatic/10/action/start" {
				t.Fatalf("identity or frozen input changed: %#v", state)
			}
			if !bytes.Equal(before.Operation.Request, frozen) {
				t.Fatal("input state mutated")
			}
			wantCount := int64(0)
			if accepted {
				wantCount = 1
			}
			if state.Root.Started != wantCount || state.Root.Remaining != 0 || state.Root.Cursor != 10 {
				t.Fatalf("accounting or cursor changed: %#v", state.Root)
			}
			wantPhase := Pending
			if expired {
				wantPhase = Ambiguous
			}
			if accepted {
				wantPhase = Started
			}
			if completed {
				wantPhase = Completed
			}
			if state.Operation.Phase != wantPhase {
				t.Fatalf("phase %v, expected %v", state.Operation.Phase, wantPhase)
			}
			request, issue := Issue(state, now)
			if issue && (wantPhase != Pending || now >= 10+RetryHorizon || !bytes.Equal(request, frozen)) {
				t.Fatal("invalid issuance")
			}
			if wantPhase == Pending {
				if _, ok := Wake(state); !ok {
					t.Fatal("lost wakeup")
				}
			}
		}
		state, err = Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: state.Operation.ID, ChainID: "chain", Now: 10 + RetryHorizon + 1})
		if err != nil {
			t.Fatal(err)
		}
		if state.Operation.Phase != Completed || state.Root.Started != 1 {
			t.Fatal("recovery completion did not settle operation exactly once")
		}
	})
}
