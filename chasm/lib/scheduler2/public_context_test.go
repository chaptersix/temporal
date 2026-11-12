package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/contextutil"
)

func TestContextMetadataUsesActionSkeleton(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(false)
	s.PublicActionRef = "unavailable-payload-owner"
	require.Equal(t, map[string]string{contextutil.MetadataKeyWorkflowType: "workflow"}, s.ContextMetadata(ctx))
	s.Closed = true
	require.Equal(t, map[string]string{contextutil.MetadataKeyWorkflowType: "workflow"}, s.ContextMetadata(ctx))
}

func TestContextMetadataWithoutPublicAction(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, ctx := h.root(true)
	require.Nil(t, s.ContextMetadata(ctx))
	s.PublicSchedule = chasm.NewDataField(ctx.(chasm.MutableContext), &schedulepb.Schedule{})
	require.Nil(t, s.ContextMetadata(ctx))
}
