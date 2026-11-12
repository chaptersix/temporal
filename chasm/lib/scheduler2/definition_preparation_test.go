package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

func preparationCalendar(count int) *schedulepb.StructuredCalendarSpec {
	cal := &schedulepb.StructuredCalendarSpec{Minute: []*schedulepb.Range{{Start: 0}}, Hour: []*schedulepb.Range{{Start: 0}}, DayOfMonth: []*schedulepb.Range{{Start: 1}}, Month: []*schedulepb.Range{{Start: 1}}, DayOfWeek: []*schedulepb.Range{{Start: 0, End: 6, Step: 1}}, Year: []*schedulepb.Range{{Start: 2000, End: 2100, Step: 100}}}
	for range count {
		cal.Second = append(cal.Second, &schedulepb.Range{Start: 0})
	}
	return cal
}
func newPreparationHarness(t *testing.T, stage *StagedCalendarSource) *treeHarness {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	h := &treeHarness{t: t, registry: registry, now: int64(time.Second), transition: 1}
	h.resetBackend()
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	p, err := NewCalendarPreparation(chasm.NewMutableContext(t.Context(), h.node), stage)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(p))
	h.commit(true)
	return h
}
func preparationRoot(t *testing.T, h *treeHarness, mutable bool) (*CalendarPreparation, chasm.Context) {
	t.Helper()
	ctx := chasm.NewContext(t.Context(), h.node)
	if mutable {
		ctx = chasm.NewMutableContext(t.Context(), h.node)
	}
	value, err := h.node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	return value.(*CalendarPreparation), ctx
}
func installPreparation(t *testing.T, h *treeHarness, stage *StagedCalendarSource) {
	t.Helper()
	for {
		p, ctx := preparationRoot(t, h, true)
		done, err := p.InstallNextPages(ctx.(chasm.MutableContext), stage, 3)
		require.NoError(t, err)
		h.commit(true)
		if done {
			return
		}
	}
}

func TestCalendarPreparationDurablePhases(t *testing.T) {
	t.Parallel()
	source := preparationCalendar(1025)
	source.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	stage, err := StageCalendarSource(source, 1)
	require.NoError(t, err)
	source.Second[0].Start = 59
	h := newPreparationHarness(t, stage)
	p, ctx := preparationRoot(t, h, true)
	before := common.CloneProto(p.CalendarPreparationState)
	_, err = p.Advance(ctx.(chasm.MutableContext), 64)
	require.ErrorContains(t, err, "not installed")
	require.True(t, proto.Equal(before, p.CalendarPreparationState))
	h.commit(false)
	// Reconstruct caller-side staging from a detached source after real tree reload.
	p, ctx = preparationRoot(t, h, false)
	snapshot, revision := p.SourceSnapshot(ctx)
	require.Zero(t, snapshot.Second[0].Start)
	require.Equal(t, []byte{0xa0, 0x06, 0x01}, []byte(snapshot.ProtoReflect().GetUnknown()))
	rebuilt, err := StageCalendarSource(snapshot, revision)
	require.NoError(t, err)
	require.Equal(t, stage.sourceID, rebuilt.sourceID)
	snapshot.Second[0].Start = 58
	installPreparation(t, h, rebuilt)
	// Corrupt the source blob: an ordinary preparation quantum must read pages only.
	sourceNode := h.durable["Source"]
	require.NotNil(t, sourceNode)
	sourceNode.Data.Data = []byte{0xff}
	h.reload()
	calls := 0
	for {
		counts := h.node.BeginWorkInspectionForTest()
		p, ctx = preparationRoot(t, h, true)
		stats, err := p.Advance(ctx.(chasm.MutableContext), 64)
		require.NoError(t, err)
		require.LessOrEqual(t, stats.Steps, 64)
		require.LessOrEqual(t, counts.Snapshot()["data_decode_calls"], int64(stats.Pages))
		require.Zero(t, counts.Snapshot()["map_hydration_keys"])
		masks, done, err := p.PreparedMasks()
		require.NoError(t, err)
		h.commit(true) // Every quantum serializes and reloads fresh objects.
		calls++
		if done {
			require.Equal(t, occurrence.CalendarMasks{Fields: [6]uint64{1, 1, 1, 2, 2, 127}, Years: [2]uint64{1, 1 << 36}}, masks)
			break
		}
		require.Less(t, calls, 100)
	}
	require.Greater(t, calls, 10)
	durable := &persistencespb.WorkflowMutableState{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}, ChasmNodes: cloneNodes(h.durable)}
	nilRegistry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, nilRegistry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, nilRegistry.Register(NewNilLibrary()))
	detached, _, err := chasm.NewDetachedExecution[*CalendarPreparation](t.Context(), durable, nilRegistry)
	require.NoError(t, err)
	_, done, err := detached.PreparedMasks()
	require.NoError(t, err)
	require.True(t, done)
}

func TestCalendarPreparationFailedCommitAndReplacement(t *testing.T) {
	t.Parallel()
	stage, err := StageCalendarSource(preparationCalendar(33), 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, stage)
	durable := cloneNodes(h.durable)
	p, ctx := preparationRoot(t, h, true)
	_, err = p.InstallNextPages(ctx.(chasm.MutableContext), stage, 1)
	require.NoError(t, err)
	h.commit(false)
	require.Equal(t, durable, h.durable)
	p, ctx = preparationRoot(t, h, false)
	require.Zero(t, p.InstalledPages)
	_, ok := p.Pages.Get(ctx, calendarPageKey(stage.sourceID[:], 0, 0))
	require.False(t, ok)
	installPreparation(t, h, stage)
	p, ctx = preparationRoot(t, h, true)
	_, err = p.Advance(ctx.(chasm.MutableContext), 1)
	require.NoError(t, err)
	h.commit(false)
	p, _ = preparationRoot(t, h, false)
	require.Nil(t, p.Cursor)
	p, ctx = preparationRoot(t, h, true)
	_, err = p.Advance(ctx.(chasm.MutableContext), 1)
	require.NoError(t, err)
	h.commit(true)
	replacement := preparationCalendar(2)
	replacement.Second[0].Start = 1
	next, err := StageCalendarSource(replacement, 2)
	require.NoError(t, err)
	p, ctx = preparationRoot(t, h, true)
	oldState := common.CloneProto(p.CalendarPreparationState)
	require.NoError(t, p.ReplaceSource(ctx.(chasm.MutableContext), next))
	h.commit(false)
	p, _ = preparationRoot(t, h, false)
	require.True(t, proto.Equal(oldState, p.CalendarPreparationState))
	p, ctx = preparationRoot(t, h, true)
	require.NoError(t, p.ReplaceSource(ctx.(chasm.MutableContext), next))
	h.commit(true)
	p, ctx = preparationRoot(t, h, true)
	require.Nil(t, p.Cursor)
	oldPage, ok := p.Pages.Get(ctx, calendarPageKey(stage.sourceID[:], 0, 0))
	require.True(t, ok)
	require.Equal(t, stage.sourceID[:], oldPage.SourceId, "superseded pages remain retained, not scanned for cleanup")
	before := common.CloneProto(p.CalendarPreparationState)
	_, err = p.InstallNextPages(ctx.(chasm.MutableContext), stage, 1)
	require.Error(t, err)
	require.True(t, proto.Equal(before, p.CalendarPreparationState))
	_, err = p.Advance(ctx.(chasm.MutableContext), 1)
	require.ErrorContains(t, err, "not installed")
	h.commit(false)
	installPreparation(t, h, next)
	for {
		p, ctx = preparationRoot(t, h, true)
		_, err = p.Advance(ctx.(chasm.MutableContext), 64)
		require.NoError(t, err)
		masks, done, err := p.PreparedMasks()
		require.NoError(t, err)
		h.commit(true)
		if done {
			require.EqualValues(t, 3, masks.Fields[0])
			break
		}
	}
}

func TestCalendarPreparationStageValidationAndAliases(t *testing.T) {
	t.Parallel()
	stage, err := StageCalendarSource(preparationCalendar(2), 1)
	require.NoError(t, err)
	h := newPreparationHarness(t, stage)
	p, ctx := preparationRoot(t, h, true)
	for _, bad := range []*StagedCalendarSource{nil, {}} {
		_, err = NewCalendarPreparation(ctx.(chasm.MutableContext), bad)
		require.Error(t, err)
		require.Error(t, p.ReplaceSource(ctx.(chasm.MutableContext), bad))
	}
	for _, budget := range []int{0, 17} {
		_, err = p.InstallNextPages(ctx.(chasm.MutableContext), stage, budget)
		require.Error(t, err)
	}
	_, err = p.InstallNextPages(ctx.(chasm.MutableContext), stage, 1)
	require.NoError(t, err)
	first := common.CloneProto(stage.pages[0])
	stage.pages[0].Ranges[0].Start = 59
	stored, ok := p.Pages.Get(ctx, calendarPageKey(first.SourceId, first.Field, first.Page))
	require.True(t, ok)
	require.True(t, proto.Equal(first, stored), "installation clones caller staging pages")
	stored.Ranges[0].Start = 58
	h.commit(true)
	p, ctx = preparationRoot(t, h, false)
	stored, ok = p.Pages.Get(ctx, calendarPageKey(first.SourceId, first.Field, first.Page))
	require.True(t, ok)
	require.True(t, proto.Equal(first, stored))
}

func TestCalendarPreparationAddressedCorruption(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"missing-page", "wrong-identity", "invalid-range", "invalid-cursor"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			stage, err := StageCalendarSource(preparationCalendar(2), 1)
			require.NoError(t, err)
			h := newPreparationHarness(t, stage)
			installPreparation(t, h, stage)
			p, ctx := preparationRoot(t, h, true)
			key := calendarPageKey(stage.sourceID[:], 0, 0)
			switch kind {
			case "missing-page":
				p.Pages.Delete(ctx.(chasm.MutableContext), key)
			case "wrong-identity":
				page := common.CloneProto(stage.pages[0])
				page.SourceId[0]++
				p.Pages.Put(ctx.(chasm.MutableContext), key, page)
			case "invalid-range":
				page := common.CloneProto(stage.pages[0])
				page.Ranges[0].Start = -1
				p.Pages.Put(ctx.(chasm.MutableContext), key, page)
			case "invalid-cursor":
				p.Cursor = &scheduler2pb.CalendarPreparationCursor{Field: 256}
			default:
				t.Fatal("unknown fixture")
			}
			h.commit(true)
			p, ctx = preparationRoot(t, h, true)
			before := common.CloneProto(p.CalendarPreparationState)
			_, err = p.Advance(ctx.(chasm.MutableContext), 64)
			require.Error(t, err)
			require.True(t, proto.Equal(before, p.CalendarPreparationState))
		})
	}
}
