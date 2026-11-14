package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
)

func TestEffectMembershipIndependentBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, message string }{
		{"valid", ""},
		{"duplicate", "invalid overlap effect identity"},
		{"orphan", "unowned overlap request"},
		{"missing", "pending overlap request is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			s, ctx := h.root(true)
			s.BlockingOperationId = "target"
			s.Effects = []*pb.EffectState{{Id: "target/cancel", TargetId: "target", Kind: pb.EFFECT_KIND_CANCEL, Phase: pb.EFFECT_PHASE_PENDING}}
			s.EffectRequests["target/cancel"] = chasm.NewDataField(ctx.(chasm.MutableContext), &pb.Payload{})
			switch tc.name {
			case "duplicate":
				s.Effects = append(s.Effects, common.CloneProto(s.Effects[0]))
			case "orphan":
				s.EffectRequests["orphan"] = chasm.NewDataField(ctx.(chasm.MutableContext), &pb.Payload{})
			case "missing":
				delete(s.EffectRequests, "target/cancel")
			default:
			}
			err := s.validateEffects()
			if tc.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.message)
				_, err = s.stateFor(ctx, "")
				require.ErrorContains(t, err, tc.message)
			}
		})
	}
}
