package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestEffectMetadataBoundRejectsBeforeUse(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"too many", "nil", "duplicate", "kind", "phase", "target", "identity", "request count", "missing request", "orphan request"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			s, ctx := h.root(true)
			s.BlockingOperationId = "target"
			s.Effects = []*pb.EffectState{{Id: "target/cancel", TargetId: "target", Kind: pb.EFFECT_KIND_CANCEL, Phase: pb.EFFECT_PHASE_PENDING}}
			switch name {
			case "too many":
				s.Effects = append(s.Effects, s.Effects[0], s.Effects[0])
			case "nil":
				s.Effects[0] = nil
			case "duplicate":
				s.Effects = append(s.Effects, common.CloneProto(s.Effects[0]))
			case "kind":
				s.Effects[0].Kind = pb.EFFECT_KIND_UNSPECIFIED
			case "phase":
				s.Effects[0].Phase = pb.EFFECT_PHASE_UNSPECIFIED
			case "target":
				s.Effects[0].TargetId = "other"
			case "identity":
				s.Effects[0].Id = "target/other"
			case "missing request":
			case "orphan request":
				s.EffectRequests["orphan"] = chasm.NewDataField(ctx.(chasm.MutableContext), &pb.Payload{})
			case "request count":
				for _, id := range []string{"a", "b", "c"} {
					s.EffectRequests[id] = chasm.NewDataField(ctx.(chasm.MutableContext), &pb.Payload{})
				}
			default:
				t.Fatal("unexpected fixture")
			}
			before := common.CloneProto(s.RootState)
			require.Error(t, s.validateEffects())
			_, err := s.stateFor(ctx, "target")
			require.Error(t, err)
			valid, err := (*EffectHandler)(nil).Validate(ctx, s, chasm.TaskInvocation{}, &pb.EffectTask{EffectId: "target/cancel"})
			require.Error(t, err)
			require.False(t, valid)
			request, err := s.effectRequest(ctx, "target/cancel")
			require.Error(t, err)
			require.Nil(t, request)
			protorequire.ProtoEqual(t, before, s.RootState)
			if name == "nil" {
				require.Nil(t, s.Effects[0])
			}
			require.Empty(t, h.backend.TasksByCategory)
		})
	}
}

func TestEffectMetadataBoundAcceptsBothKinds(t *testing.T) {
	t.Parallel()
	s := New("incarnation", 1)
	s.BlockingOperationId = "target"
	s.Effects = []*pb.EffectState{
		{Id: "target/terminate", TargetId: "target", Kind: pb.EFFECT_KIND_TERMINATE, Phase: pb.EFFECT_PHASE_AWAITING_TARGET},
		{Id: "target/cancel", TargetId: "target", Kind: pb.EFFECT_KIND_CANCEL, Phase: pb.EFFECT_PHASE_SUPERSEDED},
	}
	require.NoError(t, s.validateEffects())
	state := decision.State{Root: decision.Root{BlockingOperationID: s.BlockingOperationId}}
	for _, effect := range s.Effects {
		state.Effects = append(state.Effects, decodeEffect(effect))
	}
	require.NoError(t, validateDecisionEffects(state))
	state.Effects = append(state.Effects, state.Effects[0])
	require.Error(t, validateDecisionEffects(state))
}
