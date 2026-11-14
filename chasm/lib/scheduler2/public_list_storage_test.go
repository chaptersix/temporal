package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func listTestStatic(t *testing.T, name string, limit int64) *schedulepb.ScheduleListInfo {
	t.Helper()
	schedule := &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}, {Interval: durationpb.New(2 * time.Hour)}}, TimezoneData: []byte("opaque source")}, Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowType: &commonpb.WorkflowType{Name: name}}}}}
	staged, err := stagePublicListStatic(schedule, limit)
	require.NoError(t, err)
	require.Equal(t, []byte("opaque source"), schedule.Spec.TimezoneData)
	require.Nil(t, staged.Spec.TimezoneData)
	return staged
}
func stageListTest(t *testing.T, h *treeHarness, owner [32]byte, static *schedulepb.ScheduleListInfo, limit int64, notes *string) uint64 {
	t.Helper()
	s, raw := h.root(true)
	ctx, err := s.reserveMemoVersion(raw.(chasm.MutableContext))
	require.NoError(t, err)
	plan, err := s.prepareListRefresh(ctx, owner, static, limit, nil, notes)
	require.NoError(t, err)
	s.publishListRefresh(ctx, plan)
	h.commit(true)
	return plan.control.PendingStamp
}
func finishListTest(t *testing.T, h *treeHarness, owner [32]byte, stamp uint64, publish bool) {
	t.Helper()
	s, raw := h.root(true)
	ctx, err := s.reserveMemoVersion(raw.(chasm.MutableContext))
	require.NoError(t, err)
	plan, ok, err := s.prepareListWindow(ctx, owner, stamp, []*timestamppb.Timestamp{timestamppb.New(time.Unix(100, 0))})
	require.NoError(t, err)
	require.True(t, ok)
	s.publishListWindow(ctx, plan)
	h.commit(publish)
}
func TestListPartitionCoherentReplacementAndReload(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	owner1, owner2 := [32]byte{1}, [32]byte{2}
	notes := "first"
	stamp1 := stageListTest(t, h, owner1, listTestStatic(t, "first", 2), 2, &notes)
	s, ctx := h.root(false)
	memo, err := s.listMemo(ctx)
	require.NoError(t, err)
	require.Nil(t, memo)
	finishListTest(t, h, owner1, stamp1, true)
	notes = "second"
	stamp2 := stageListTest(t, h, owner2, listTestStatic(t, "second", 1), 1, &notes)
	s, ctx = h.root(false)
	memo, err = s.listMemo(ctx)
	require.NoError(t, err)
	require.Equal(t, "first", memo.WorkflowType.Name)
	require.Len(t, memo.Spec.Interval, 2)
	require.Equal(t, "second", memo.Notes)
	require.Len(t, memo.FutureActionTimes, 1)
	plan, ok, err := s.prepareListWindow(ctx, owner1, stamp1, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, plan.control)
	finishListTest(t, h, owner2, stamp2, false)
	s, ctx = h.root(false)
	memo, err = s.listMemo(ctx)
	require.NoError(t, err)
	require.Equal(t, "first", memo.WorkflowType.Name)
	finishListTest(t, h, owner2, stamp2, true)
	s, ctx = h.root(false)
	memo, err = s.listMemo(ctx)
	require.NoError(t, err)
	require.Equal(t, "second", memo.WorkflowType.Name)
	require.Len(t, memo.Spec.Interval, 1)
	require.False(t, s.ListStaticA.IsSet())
	require.True(t, s.ListStaticB.IsSet())
}
func TestListPartitionNoStaticReadAndLiveLimitRestage(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	owner := [32]byte{1}
	stamp := stageListTest(t, h, owner, listTestStatic(t, "first", 2), 2, nil)
	finishListTest(t, h, owner, stamp, true)
	h.durable["ListStaticA"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx := h.root(false)
	at, err := s.listFirstFuture(ctx)
	require.NoError(t, err)
	require.Equal(t, time.Unix(100, 0).UTC(), at.AsTime())
	require.NotContains(t, h.node.MaterializedPathsForTest(), "ListStaticA")
	_, err = s.prepareListRefresh(ctx, owner, nil, 1, nil, nil)
	require.ErrorContains(t, err, "restaging required")
	plan, err := s.prepareListRefresh(ctx, owner, nil, 2, nil, nil)
	require.NoError(t, err)
	require.False(t, plan.memoChanged)
	require.NotContains(t, h.node.MaterializedPathsForTest(), "ListStaticA")
	replacement := listTestStatic(t, "first", 1)
	plan, err = s.prepareListRefresh(ctx, owner, replacement, 1, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(2), plan.staticSlot)
}
func TestListPartitionStampAndOwnerRejection(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	owner := [32]byte{1}
	stamp := stageListTest(t, h, owner, listTestStatic(t, "first", 2), 2, nil)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	c := common.CloneProto(s.ListControl.Get(ctx))
	c.PendingStamp = math.MaxUint64
	s.ListControl = chasm.NewDataField(ctx, c)
	_, err := s.prepareListRefresh(ctx, owner, listTestStatic(t, "next", 2), 2, nil, nil)
	require.ErrorContains(t, err, "stamp exhausted")
	c.PendingStamp = stamp
	c.SlotAOwner = []byte{1}
	s.ListControl = chasm.NewDataField(ctx, c)
	_, err = s.listFirstFuture(ctx)
	require.ErrorContains(t, err, "slot ownership")
	c.SlotAOwner = owner[:]
	c.FutureActionTimes = make([]*timestamppb.Timestamp, 6)
	s.ListControl = chasm.NewDataField(ctx, c)
	_, err = s.listFirstFuture(ctx)
	require.ErrorContains(t, err, "invalid list control")
	s.ListControl = chasm.NewDataField(ctx, &scheduler2pb.PublicListControl{})
	_, err = s.listFirstFuture(ctx)
	require.ErrorContains(t, err, "slot ownership")
}

func TestListPartitionDynamicPreservesPendingJob(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	owner := [32]byte{1}
	stamp := stageListTest(t, h, owner, listTestStatic(t, "first", 2), 2, nil)
	s, raw := h.root(true)
	ctx, err := s.reserveMemoVersion(raw.(chasm.MutableContext))
	require.NoError(t, err)
	notes := "captured"
	plan, err := s.prepareListDynamic(ctx, []*schedulepb.ScheduleActionResult{{StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}}}, &notes)
	require.NoError(t, err)
	notes = "mutated caller"
	require.Equal(t, stamp, plan.control.PendingStamp)
	require.Equal(t, owner[:], plan.control.SlotAOwner)
	s.publishListRefresh(ctx, plan)
	h.commit(true)
	finishListTest(t, h, owner, stamp, true)
	s, raw = h.root(false)
	memo, err := s.listMemo(raw)
	require.NoError(t, err)
	require.Equal(t, "captured", memo.Notes)
	require.Equal(t, "run", memo.RecentActions[0].StartWorkflowResult.RunId)
}
