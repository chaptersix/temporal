package scheduler2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedVisibilityWaitsForCoherentProjection(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, ctx := h.root(false)
	require.True(t, s.VisibilityReady(ctx))
	owner := [32]byte{1}
	stamp := stageListTest(t, h, owner, listTestStatic(t, "visible", 2), 2, nil)
	s, ctx = h.root(false)
	require.False(t, s.VisibilityReady(ctx))
	finishListTest(t, h, owner, stamp, true)
	h.durable["ListStaticA"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx = h.root(false)
	require.True(t, s.VisibilityReady(ctx))
	require.NotContains(t, h.node.MaterializedPathsForTest(), "ListStaticA")
}

func TestPreparedVisibilityRejectsCorruptControl(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	stageListTest(t, h, [32]byte{1}, listTestStatic(t, "visible", 2), 2, nil)
	s, ctx := h.root(true)
	s.ListControl.Get(ctx).PendingSlot = 3
	require.Panics(t, func() { s.VisibilityReady(ctx) })
}
