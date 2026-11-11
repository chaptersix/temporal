package decision

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

func TestEffectsIndependentProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		state, err := Step(State{Root: Root{Incarnation: "inc", Limited: true, Remaining: 10}}, Event{Kind: Due, Incarnation: "inc", Nominal: 1, Now: 1, CatchupDeadline: 1, Request: []byte("start")})
		require.NoError(t, err)
		id := "inc/automatic/1/action/start"
		var queue []enumspb.ScheduleOverlapPolicy
		expected := map[EffectKind]Effect{}
		var order []EffectKind
		now, revision, cursor, drops := int64(1), int64(0), int64(1), int64(0)
		known := false
		length := rapid.IntRange(1, 128).Draw(t, "length")
		for i := 0; i < length; i++ {
			now += int64(rapid.IntRange(1, 20).Draw(t, "elapsed")) * RetryBackoff
			command := rapid.IntRange(0, 7).Draw(t, "command")
			event := Event{Incarnation: "inc", Now: now, OperationID: id, ChainID: "chain"}
			switch command {
			case 0, 1, 2:
				policy := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
				if command == 0 {
					policy = enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
				}
				if command == 1 {
					policy = enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
				}
				revision++
				state, err = Step(state, Event{Kind: Update, Incarnation: "inc", Now: now, Revision: revision, Limited: true, Remaining: 9, Policy: policy})
				require.NoError(t, err)
				event.Kind = Due
				event.Nominal = now
				event.Actual = now
				event.CatchupDeadline = now
				event.WorkflowID = fmt.Sprintf("queued-%d", i)
				cursor = now
				if len(queue) == 4 {
					drops++
				} else {
					queue = append(queue, policy)
				}
			case 3:
				event.Kind = Acknowledged
				known = true
			case 4, 5, 6, 7:
				kinds := []Kind{EffectSucceeded, EffectNotFound, EffectFailed, EffectExpired}
				event.Kind = kinds[command-4]
				kind := Cancel
				if rapid.Bool().Draw(t, "terminate_result") {
					kind = Terminate
				}
				suffix := "/cancel"
				if kind == Terminate {
					suffix = "/terminate"
				}
				event.EffectID = id + suffix
				if effect, ok := expected[kind]; ok && (effect.Phase == EffectPending || effect.Phase == EffectAmbiguous) {
					switch command {
					case 4:
						effect.Phase = EffectAccepted
					case 5:
						effect.Phase = EffectAbsent
					case 6:
						effect = effectsOracleFailure(effect, now)
					case 7:
						if now >= effect.Deadline {
							effect.Phase = EffectAmbiguous
						}
					default:
						t.Fatal("unreachable result")
					}
					expected[kind] = effect
				}
			default:
				t.Fatal("unreachable command")
			}
			// Demand derives only from accepted queue identities, independently of policy updates.
			desired := NoEffect
			for _, policy := range queue {
				if policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
					desired = Terminate
				} else if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER && desired == NoEffect {
					desired = Cancel
				}
			}
			if desired == Terminate {
				if effect, ok := expected[Cancel]; ok && effect.Phase != EffectAccepted && effect.Phase != EffectAbsent {
					effect.Phase = EffectSuperseded
					expected[Cancel] = effect
				}
			}
			if desired != NoEffect {
				if _, ok := expected[desired]; !ok {
					suffix := "/cancel"
					if desired == Terminate {
						suffix = "/terminate"
					}
					expected[desired] = Effect{ID: id + suffix, TargetID: id, Kind: desired, Phase: AwaitingTarget}
					order = append(order, desired)
				}
			}
			for kind, effect := range expected {
				if known && effect.Phase == AwaitingTarget {
					effect.Phase = EffectPending
					effect.ChainID = "chain"
					effect.RetryAt = now
					effect.Deadline = now + RetryHorizon
					expected[kind] = effect
				}
			}
			before := state
			state, err = Step(state, event)
			require.NoError(t, err)
			require.Len(t, state.Effects, len(expected))
			for i, kind := range order {
				require.Equal(t, expected[kind], state.Effects[i])
			}
			require.Equal(t, id, state.Root.BlockingOperationID)
			require.Equal(t, int64(1), state.Root.Unfinished)
			require.Equal(t, int64(9), state.Root.Remaining)
			require.Equal(t, cursor, state.Root.Cursor)
			require.Equal(t, drops, state.Root.BufferDropped)
			require.Len(t, state.Queue, len(queue))
			for i, policy := range queue {
				require.Equal(t, policy, state.Queue[i].Policy)
			}
			if len(before.Effects) > 0 && len(state.Effects) > 0 {
				saved := before.Effects[0]
				state.Effects[0].ChainID = "mutated"
				require.Equal(t, saved, before.Effects[0])
				state.Effects[0] = expected[order[0]]
			}
		}
		state, err = Step(state, Event{Kind: Complete, Incarnation: "inc", OperationID: id, ChainID: "chain", Now: now})
		require.NoError(t, err)
		require.Empty(t, state.Effects)
		require.Empty(t, state.Root.BlockingOperationID)
	})
}

func TestEffectsFreeSelectionAndOverflow(t *testing.T) {
	t.Parallel()
	for _, policies := range [][]enumspb.ScheduleOverlapPolicy{
		{enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE},
		{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER},
	} {
		state := State{Root: Root{Incarnation: "inc", Limited: true, Remaining: 5}}
		for i, p := range policies {
			state.Queue = append(state.Queue, Queued{Nominal: int64(i + 1), Actual: int64(i + 1), WorkflowID: fmt.Sprint(i + 1), Policy: p})
		}
		selected := 2
		retained := []int64{2, 4}
		superseded := int64(1)
		if policies[0] == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL {
			selected = 3
			retained = []int64{2}
			superseded = 2
		}
		next, err := Step(state, Event{Kind: Drain, Incarnation: "inc", Now: 4, CatchupWindow: 10, Request: []byte("current")})
		require.NoError(t, err)
		require.Equal(t, int64(selected+1), next.Operation.Nominal)
		require.Equal(t, superseded, next.Root.OverlapSuperseded)
		require.Len(t, next.Queue, len(retained))
		for i, n := range retained {
			require.Equal(t, n, next.Queue[i].Nominal)
		}
		require.Equal(t, int64(4), next.Root.Remaining)
	}
}

// Keep the independent arithmetic oracle separate from command generation.
func effectsOracleFailure(effect Effect, now int64) Effect {
	if effect.Phase != EffectPending {
		return effect
	}
	if now >= effect.Deadline {
		effect.Phase = EffectAmbiguous
		return effect
	}
	at := now
	if effect.RetryAt > at {
		at = effect.RetryAt
	}
	if at > effect.Deadline-RetryBackoff {
		at = effect.Deadline - RetryBackoff
	}
	effect.RetryAt = at + RetryBackoff
	return effect
}
