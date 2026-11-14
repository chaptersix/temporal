package scheduler2

import (
	"go.temporal.io/api/serviceerror"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

// validateEffects establishes the fixed two-intent bound before decoding metadata.
func (s *Scheduler) validateEffects() error {
	if len(s.Effects) > 2 || len(s.EffectRequests) > 2 {
		return serviceerror.NewInternal("overlap effect bound exceeded")
	}
	if s.fixedEffectStorage() && len(s.EffectRequests) != 0 || !s.fixedEffectStorage() && (s.CancelRequest.IsSet() || s.TerminateRequest.IsSet()) {
		return serviceerror.NewInternal("mixed overlap request storage")
	}
	var seen uint8
	matched := 0
	for _, effect := range s.Effects {
		if effect == nil {
			return serviceerror.NewInternal("overlap effect metadata is missing")
		}
		if err := validateEffectIdentity(s.BlockingOperationId, effect.Id, effect.TargetId, effect.Kind, effect.Phase, &seen); err != nil {
			return err
		}
		present := s.effectRequestPresent(effect.Id, decision.EffectKind(effect.Kind))
		if present {
			matched++
		} else if effect.Phase == pb.EFFECT_PHASE_PENDING {
			return serviceerror.NewInternal("pending overlap request is missing")
		}
	}
	if matched != s.effectRequestCount() {
		return serviceerror.NewInternal("unowned overlap request")
	}
	return nil
}

func validateDecisionEffects(state decision.State) error {
	if len(state.Effects) > 2 {
		return serviceerror.NewInternal("overlap effect bound exceeded")
	}
	var seen uint8
	for _, effect := range state.Effects {
		if err := validateEffectIdentity(state.Root.BlockingOperationID, effect.ID, effect.TargetID, pb.EffectKind(effect.Kind), pb.EffectPhase(effect.Phase), &seen); err != nil {
			return err
		}
	}
	return nil
}

func validateEffectIdentity(blocking, id, target string, kind pb.EffectKind, phase pb.EffectPhase, seen *uint8) error {
	var suffix string
	switch kind {
	case pb.EFFECT_KIND_CANCEL:
		suffix = "/cancel"
	case pb.EFFECT_KIND_TERMINATE:
		suffix = "/terminate"
	default:
		return serviceerror.NewInternal("invalid overlap effect kind")
	}
	bit := uint8(1 << uint(kind))
	if *seen&bit != 0 || blocking == "" || target != blocking || id != target+suffix || phase < pb.EFFECT_PHASE_AWAITING_TARGET || phase > pb.EFFECT_PHASE_SUPERSEDED {
		return serviceerror.NewInternal("invalid overlap effect identity")
	}
	*seen |= bit
	return nil
}
