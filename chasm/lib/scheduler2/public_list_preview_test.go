package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func listPreviewDefinition(t *testing.T, spec *schedulepb.ScheduleSpec, seed string) (*treeHarness, *StagedDefinitionSource) {
	t.Helper()
	stage, err := StageDefinitionSource(spec, seed, 1)
	require.NoError(t, err)
	h := newDefinitionHarness(t, stage)
	installDefinition(t, h, stage)
	resolver := timezonesource.NewResolver(timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: clock.NewEventTimeSource().Update(time.Unix(1704067200, 0))}))
	snapshot, err := resolver.Resolve("UTC", nil)
	require.NoError(t, err)
	zone, err := StageDefinitionTimezone(stage, snapshot, 1)
	require.NoError(t, err)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.BindTimezone(ctx.(chasm.MutableContext), zone))
	child := p.Timezone.Get(ctx)
	done, err := child.InstallNextPages(ctx.(chasm.MutableContext), zone, 16)
	require.NoError(t, err)
	require.True(t, done)
	h.commit(true)
	h.durable["Source"].Data.Data = []byte{0xff}
	h.reload()
	return h, stage
}
func runListPreview(t *testing.T, h *treeHarness, stage *StagedDefinitionSource, job *scheduler2pb.PublicListPreview, limits func() (int, int), budget int) *scheduler2pb.PublicListPreview {
	t.Helper()
	for n := 0; n < 20000; n++ {
		before := common.CloneProto(job)
		p, ctx := definitionRoot(t, h, false)
		step, err := advanceListPreview(ctx, p, job, stage.owner, job.Stamp, limits, budget)
		require.NoError(t, err)
		require.False(t, step.stale)
		require.True(t, proto.Equal(before, job))
		require.LessOrEqual(t, step.stats.Steps, budget)
		require.NotContains(t, h.node.MaterializedPathsForTest(), "Source")
		wire, err := proto.Marshal(step.job)
		require.NoError(t, err)
		job = &scheduler2pb.PublicListPreview{}
		require.NoError(t, proto.Unmarshal(wire, job))
		h.reload()
		if job.Done {
			return job
		}
	}
	t.Fatal("preview fixture did not complete")
	return nil
}
func TestListPreviewReferenceAndSampling(t *testing.T) {
	t.Parallel()
	seed := "seed crossing several fixed hash blocks................................"
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(7 * time.Second)}}, Jitter: durationpb.New(4 * time.Second)}
	for _, budget := range []int{1, 7, 64} {
		h, stage := listPreviewDefinition(t, spec, seed)
		after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		job, err := newListPreview(stage.owner, 1, after, false, 0)
		require.NoError(t, err)
		calls := 0
		job = runListPreview(t, h, stage, job, func() (int, int) { calls++; return 2, 1000 }, budget)
		require.Equal(t, 5, calls)
		require.Len(t, job.FutureActionTimes, 5)
		canonical, err := stage.CanonicalSpec()
		require.NoError(t, err)
		compiled, err := legacy.NewSpecBuilder(func() int { return 2 }, func() int { return 1000 }).NewCompiledSpec(canonical)
		require.NoError(t, err)
		for _, got := range job.FutureActionTimes {
			want, err := compiled.GetNextTime(seed, after)
			require.NoError(t, err)
			require.Equal(t, want.Next, got.AsTime())
			after = want.Next
		}
	}
}
func TestListPreviewComputeLimitPublishesOnlyCompletedPrefix(t *testing.T) {
	t.Parallel()
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 2}}, Minute: []*schedulepb.Range{{End: 59}}, Hour: []*schedulepb.Range{{End: 23}}, DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}}, Month: []*schedulepb.Range{{Start: 1, End: 12}}, DayOfWeek: []*schedulepb.Range{{End: 6}}}}}
	h, stage := listPreviewDefinition(t, spec, "")
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	job, err := newListPreview(stage.owner, 2, after, false, 0)
	require.NoError(t, err)
	calls := 0
	job = runListPreview(t, h, stage, job, func() (int, int) {
		calls++
		if calls == 1 {
			return 2, 100
		}
		return 2, 1
	}, 1)
	require.Equal(t, 2, calls)
	require.Len(t, job.FutureActionTimes, 1)
	require.Equal(t, after.Add(time.Second), job.FutureActionTimes[0].AsTime())
	require.Empty(t, job.Work)
}
func TestListPreviewStaleFaultAndZoneReset(t *testing.T) {
	t.Parallel()
	h, stage := listPreviewDefinition(t, &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}, "")
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	job, err := newListPreview(stage.owner, 3, after, true, 2)
	require.NoError(t, err)
	calls := 0
	limits := func() (int, int) { calls++; return 2, 100 }
	p, ctx := definitionRoot(t, h, false)
	first, err := advanceListPreview(ctx, p, job, stage.owner, 3, limits, 64)
	require.NoError(t, err)
	require.Len(t, first.job.FutureActionTimes, 1)
	first.job.ZoneFingerprint[0] ^= 1
	reset, err := advanceListPreview(ctx, p, first.job, stage.owner, 3, limits, 1)
	require.NoError(t, err)
	require.True(t, reset.reset)
	require.Empty(t, reset.job.FutureActionTimes)
	require.Equal(t, after, reset.job.After.AsTime())
	require.Equal(t, 2, calls)
	p, ctx = definitionRoot(t, h, true)
	p.Pages.Delete(ctx.(chasm.MutableContext), definitionPageKey(stage.owner[:], 0))
	h.commit(true)
	p, ctx = definitionRoot(t, h, false)
	stale, err := advanceListPreview(ctx, p, reset.job, stage.owner, 4, limits, 64)
	require.NoError(t, err)
	require.True(t, stale.stale)
	before := common.CloneProto(reset.job)
	_, err = advanceListPreview(ctx, p, reset.job, stage.owner, 3, limits, 64)
	require.ErrorContains(t, err, "definition page")
	require.True(t, proto.Equal(before, reset.job))
	require.Equal(t, 2, calls, "continuations must not resample policy")
}
func TestListPreviewZeroTargetNeedsNoPreparedPages(t *testing.T) {
	t.Parallel()
	stage := stagedInterval(t, 1, "seed", 1)
	h := newDefinitionHarness(t, stage)
	job, err := newListPreview(stage.owner, 1, time.Unix(100, 0), true, -1)
	require.NoError(t, err)
	p, ctx := definitionRoot(t, h, false)
	step, err := advanceListPreview(ctx, p, job, stage.owner, 1, nil, 1)
	require.NoError(t, err)
	require.True(t, step.job.Done)
	require.Empty(t, step.job.FutureActionTimes)
}
