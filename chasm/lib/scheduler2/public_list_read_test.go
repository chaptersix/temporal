package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func selectedListReadTree(t *testing.T) *treeHarness {
	t.Helper()
	// newTreeHarness registers NewNilLibrary: detached read/export cannot depend
	// on live builders, config getters or timezone/source services.
	h := newTreeHarness(t)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	s.PublicList = chasm.NewDataField(ctx, &schedulepb.ScheduleListInfo{})
	s.PublicSchedule = chasm.NewDataField(ctx, &schedulepb.Schedule{})
	h.commit(true)
	owner := [32]byte{1}
	notes := "selected notes"
	stamp := stageListTest(t, h, owner, listTestStatic(t, "selected", 2), 2, &notes)
	s, raw = h.root(false)
	require.Nil(t, s.Memo(raw), "pending initial window is not a typed-nil message")
	finishListTest(t, h, owner, stamp, true)
	for _, path := range []string{"PublicList", "PublicSchedule"} {
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	return h
}
func TestSelectedListDetachedMemoAvoidsLegacyPartitions(t *testing.T) {
	t.Parallel()
	h := selectedListReadTree(t)
	s, ctx := h.root(false)
	memo := s.Memo(ctx).(*schedulepb.ScheduleListInfo)
	require.Equal(t, "selected", memo.WorkflowType.Name)
	require.Equal(t, "selected notes", memo.Notes)
	require.Len(t, memo.Spec.Interval, 2)
	require.Equal(t, time.Unix(100, 0).UTC(), memo.FutureActionTimes[0].AsTime())
	original := common.CloneProto(memo)
	memo.WorkflowType.Name = "caller mutation"
	memo.Spec.Interval = nil
	memo.FutureActionTimes = nil
	require.True(t, proto.Equal(original, s.Memo(ctx)))
	for _, path := range []string{"PublicList", "PublicSchedule"} {
		require.NotContains(t, h.node.MaterializedPathsForTest(), path)
	}
}
func TestSelectedListFrameworkCloseAvoidsStaticDecode(t *testing.T) {
	t.Parallel()
	h := selectedListReadTree(t)
	for _, path := range []string{"ListStaticA", "ListNotes"} {
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	s, raw := h.root(true)
	ctx, err := s.reserveMemoVersion(raw.(chasm.MutableContext))
	require.NoError(t, err)
	s.Paused = true
	s.publishMemoVersion(ctx)
	require.Contains(t, s.SearchAttributes(ctx), publicNextActionTime.Value(time.Unix(100, 0).UTC()))
	require.Contains(t, s.SearchAttributes(ctx), chasm.SearchAttributeTemporalSchedulePaused.Value(true))
	node := h.node
	h.commit(true)
	for _, path := range []string{"PublicList", "PublicSchedule", "ListStaticA", "ListNotes"} {
		require.NotContains(t, node.MaterializedPathsForTest(), path)
	}
	require.Contains(t, node.MaterializedPathsForTest(), "ListControl")
}
func TestSelectedListInvalidControlDoesNotFallBack(t *testing.T) {
	t.Parallel()
	h := selectedListReadTree(t)
	s, raw := h.root(true)
	control := common.CloneProto(s.ListControl.Get(raw))
	control.SlotAOwner = []byte{1}
	s.ListControl = chasm.NewDataField(raw.(chasm.MutableContext), control)
	require.Panics(t, func() { s.SearchAttributes(raw) })
	require.Panics(t, func() { s.Memo(raw) })
	for _, path := range []string{"PublicList", "PublicSchedule"} {
		require.NotContains(t, h.node.MaterializedPathsForTest(), path)
	}
}
