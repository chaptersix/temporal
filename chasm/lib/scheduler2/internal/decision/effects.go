package decision

import (
	"math"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

type EffectKind int

const (
	NoEffect EffectKind = iota
	Cancel
	Terminate
)

type EffectPhase int

const (
	AwaitingTarget EffectPhase = iota + 1
	EffectPending
	EffectAccepted
	EffectAbsent
	EffectAmbiguous
	EffectSuperseded
)

// Effect holds bounded intent metadata separately from the frozen RPC request.
type Effect struct {
	ID       string
	TargetID string
	Kind     EffectKind
	Phase    EffectPhase
	ChainID  string
	Deadline int64
	RetryAt  int64
}

func effectPolicy(policy enumspb.ScheduleOverlapPolicy) EffectKind {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
		return Cancel
	case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return Terminate
	default:
		return NoEffect
	}
}

func reconcileEffects(next State) State {
	if next.Root.BlockingOperationID == "" {
		next.Effects = nil
		return next
	}
	if len(next.Effects) > 0 && next.Effects[0].TargetID != next.Root.BlockingOperationID {
		next.Effects = nil
	}
	desired := QueueEffect(next)
	if desired == NoEffect {
		return next
	}
	found := false
	for i := range next.Effects {
		effect := &next.Effects[i]
		found = found || effect.Kind == desired
		if desired == Terminate && effect.Kind == Cancel && effect.Phase != EffectAccepted && effect.Phase != EffectAbsent {
			effect.Phase = EffectSuperseded
		}
	}
	if !found {
		suffix := "/cancel"
		if desired == Terminate {
			suffix = "/terminate"
		}
		next.Effects = append(next.Effects, Effect{ID: next.Root.BlockingOperationID + suffix, TargetID: next.Root.BlockingOperationID, Kind: desired, Phase: AwaitingTarget})
	}
	return next
}

// BindEffects starts the retry horizon only after the immutable target chain is known.
func BindEffects(state State, target *Operation, now int64) (State, error) {
	if target == nil || target.ID != state.Root.BlockingOperationID || target.ChainID == "" {
		return state, nil
	}
	next := clone(state)
	for i := range next.Effects {
		effect := &next.Effects[i]
		if effect.Phase != AwaitingTarget {
			continue
		}
		if now > math.MaxInt64-RetryHorizon {
			return State{}, serviceerror.NewInvalidArgument("effect binding time exceeds supported range")
		}
		effect.ChainID = target.ChainID
		effect.Phase = EffectPending
		effect.RetryAt = now
		effect.Deadline = now + RetryHorizon
	}
	return next, nil
}

// EffectWake returns the current deadline without preparing RPC payloads.
func EffectWake(effect Effect) (int64, bool) {
	if effect.Phase != EffectPending {
		return 0, false
	}
	return min(effect.RetryAt, effect.Deadline), true
}

func effectResult(next State, event Event) State {
	for i := range next.Effects {
		effect := &next.Effects[i]
		if effect.ID != event.EffectID || (effect.Phase != EffectPending && effect.Phase != EffectAmbiguous) {
			continue
		}
		switch event.Kind {
		case EffectSucceeded:
			effect.Phase = EffectAccepted
		case EffectNotFound:
			effect.Phase = EffectAbsent
		case EffectFailed:
			if effect.Phase == EffectPending {
				if event.Now >= effect.Deadline {
					effect.Phase = EffectAmbiguous
				} else {
					effect.RetryAt = min(max(event.Now, effect.RetryAt), effect.Deadline-RetryBackoff) + RetryBackoff
				}
			}
		case EffectExpired:
			if event.Now >= effect.Deadline {
				effect.Phase = EffectAmbiguous
			}
		default:
		}
	}
	return next
}
