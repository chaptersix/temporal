package scheduler2

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newDefinitionHarness(t *testing.T, stage *StagedDefinitionSource) *treeHarness {
	t.Helper()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	h := &treeHarness{t: t, registry: registry, now: int64(time.Second), transition: 1}
	h.resetBackend()
	h.node = chasm.NewEmptyTree(registry, h.backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	p, err := NewDefinitionPreparation(chasm.NewMutableContext(t.Context(), h.node), stage)
	require.NoError(t, err)
	require.NoError(t, h.node.SetRootComponent(p))
	h.commit(true)
	return h
}
func definitionRoot(t *testing.T, h *treeHarness, mutable bool) (*DefinitionPreparation, chasm.Context) {
	t.Helper()
	ctx := chasm.NewContext(t.Context(), h.node)
	if mutable {
		ctx = chasm.NewMutableContext(t.Context(), h.node)
	}
	value, err := h.node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	return value.(*DefinitionPreparation), ctx
}
func installDefinition(t *testing.T, h *treeHarness, stage *StagedDefinitionSource) {
	t.Helper()
	for {
		p, ctx := definitionRoot(t, h, true)
		done, err := p.InstallNextPages(ctx.(chasm.MutableContext), stage, 16)
		require.NoError(t, err)
		h.commit(true)
		if done {
			return
		}
	}
}
func stagedInterval(t *testing.T, count int, seed string, revision int64) *StagedDefinitionSource {
	t.Helper()
	spec := &schedulepb.ScheduleSpec{Jitter: durationpb.New(time.Minute)}
	for range count {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Minute)})
	}
	stage, err := StageDefinitionSource(spec, seed, revision)
	require.NoError(t, err)
	return stage
}

func TestDefinitionPreparationReloadAndSourceIsolation(t *testing.T) {
	t.Parallel()
	stage := stagedInterval(t, 257, string(bytes.Repeat([]byte{0xff, 0, 1}, 100)), 1)
	h := newDefinitionHarness(t, stage)
	p, ctx := definitionRoot(t, h, false)
	snapshot, revision, identity, err := p.SourceSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, stage.header.identity[:], identity)
	rebuilt, err := StageDefinitionSnapshot(snapshot, revision)
	require.NoError(t, err)
	require.Equal(t, stage.owner, rebuilt.owner)
	snapshot.Seed[0] = 0
	_, _, err = p.Prepared(ctx, DefinitionEvaluationPolicy{})
	require.ErrorContains(t, err, "not installed")
	installDefinition(t, h, rebuilt)
	// Installed evaluation must not decode the original source blob.
	h.durable["Source"].Data.Data = []byte{0xff}
	h.reload()
	p, ctx = definitionRoot(t, h, true)
	header, reader, err := p.Prepared(ctx, DefinitionEvaluationPolicy{})
	require.NoError(t, err)
	expectedHeader, expectedReader, err := stage.Prepared(DefinitionEvaluationPolicy{})
	require.NoError(t, err)
	require.Equal(t, expectedHeader, header)
	for _, index := range []uint64{0, 15, 16, 256} {
		want, err := expectedReader.Term(false, index)
		require.NoError(t, err)
		got, err := reader.Term(false, index)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	for _, offset := range []uint64{0, 1, 23, 24, 25, 299, 300} {
		want, err := expectedReader.Seed(offset)
		require.NoError(t, err)
		got, err := reader.Seed(offset)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	h.commit(true)
	require.Panics(t, func() { _, _ = reader.Term(false, 0) })
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	durable := &persistencespb.WorkflowMutableState{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}, ChasmNodes: cloneNodes(h.durable)}
	detached, readCtx, err := chasm.NewDetachedExecution[*DefinitionPreparation](t.Context(), durable, registry)
	require.NoError(t, err)
	_, detachedReader, err := detached.Prepared(readCtx, DefinitionEvaluationPolicy{})
	require.NoError(t, err)
	got, err := detachedReader.Term(false, 256)
	require.NoError(t, err)
	require.Equal(t, int64(60), got.Interval)

}

func TestDefinitionPreparationInstallFaultAndCleanup(t *testing.T) {
	t.Parallel()
	stage := stagedInterval(t, 257, "seed", 1)
	h := newDefinitionHarness(t, stage)
	for ordinal := 0; ordinal < 16; ordinal++ {
		p, ctx := definitionRoot(t, h, true)
		before := common.CloneProto(p.DefinitionPreparationState)
		good := stage.pages[ordinal]
		stage.pages[ordinal] = []byte{0xff}
		_, err := p.InstallNextPages(ctx.(chasm.MutableContext), stage, 16)
		require.Error(t, err)
		require.True(t, proto.Equal(before, p.DefinitionPreparationState))
		require.Zero(t, countDefinitionPages(ctx, p, stage))
		stage.pages[ordinal] = good
		h.commit(false)
	}
	installDefinition(t, h, stage)
	replacement := stagedInterval(t, 1, "", 2)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.ReplaceSource(ctx.(chasm.MutableContext), replacement))
	h.commit(true)
	installDefinition(t, h, replacement)
	p, ctx = definitionRoot(t, h, true)
	oldPage, ok := p.Pages.Get(ctx, definitionPageKey(stage.owner[:], 15))
	require.True(t, ok)
	bad := common.CloneProto(oldPage)
	bad.Ordinal++
	p.Pages.Put(ctx.(chasm.MutableContext), definitionPageKey(stage.owner[:], 15), bad)
	h.commit(true)
	p, ctx = definitionRoot(t, h, true)
	before := common.CloneProto(p.DefinitionPreparationState)
	count := countDefinitionPages(ctx, p, stage, replacement)
	_, err := p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.Error(t, err)
	require.Equal(t, count, countDefinitionPages(ctx, p, stage, replacement))
	require.True(t, proto.Equal(before, p.DefinitionPreparationState))
	h.commit(false)
	p, ctx = definitionRoot(t, h, true)
	p.Pages.Put(ctx.(chasm.MutableContext), definitionPageKey(stage.owner[:], 15), oldPage)
	h.commit(true)
	p, ctx = definitionRoot(t, h, true)
	done, err := p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.NoError(t, err)
	require.False(t, done)
	h.commit(true)
	p, ctx = definitionRoot(t, h, true)
	done, err = p.CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, len(replacement.pages), countDefinitionPages(ctx, p, stage, replacement))
	h.commit(true)
}

func TestDefinitionStagingIdentityAndCodec(t *testing.T) {
	t.Parallel()
	spec := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{preparationCalendar(3)}, Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{preparationCalendar(1)}}
	before := common.CloneProto(spec)
	stage, err := StageDefinitionSource(spec, "seed", 7)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, spec))
	reference, err := occurrence.NewPublic(spec, "seed", 7)
	require.NoError(t, err)
	referenceState, err := reference.Begin(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, referenceState.DefinitionID, stage.Identity())
	h, err := decodeDefinitionHeader(stage.header.encode())
	require.NoError(t, err)
	require.Equal(t, stage.header, h)
	for i, page := range stage.pages {
		require.NoError(t, validateDefinitionPage(page, h, stage.owner, uint64(i)))
		require.LessOrEqual(t, len(page), definitionTermPageBytes)
		record := &scheduler2pb.DefinitionPreparationPage{OwnerId: stage.owner[:], Ordinal: ^uint64(0), Encoded: page}
		require.LessOrEqual(t, proto.Size(record), 1618)
	}
	header, reader, err := stage.Prepared(DefinitionEvaluationPolicy{})
	require.NoError(t, err)
	z, err := occurrence.NewResolvedLocation(occurrence.PreparedUTC())
	require.NoError(t, err)
	var state occurrence.PreparedSpecState
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		state, _, err = occurrence.AdvancePreparedSpec(state, header, z, after, reader, 64)
		require.NoError(t, err)
		if state.Result().Done {
			return
		}
	}
	t.Fatal("staged definition did not produce a result")
}

func countDefinitionPages(ctx chasm.Context, p *DefinitionPreparation, stages ...*StagedDefinitionSource) int {
	count := 0
	for _, stage := range stages {
		for ordinal := range stage.pages {
			if _, ok := p.Pages.Get(ctx, definitionPageKey(stage.owner[:], uint64(ordinal))); ok {
				count++
			}
		}
	}
	return count
}

func TestDefinitionPreparationTimezoneBinding(t *testing.T) {
	t.Parallel()
	stage := stagedInterval(t, 1, "seed", 1)
	h := newDefinitionHarness(t, stage)
	resolver := timezonesource.NewResolver(timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: clock.NewEventTimeSource().Update(time.Unix(1704067200, 0))}))
	snapshot, err := resolver.Resolve("UTC", nil)
	require.NoError(t, err)
	zone, err := StageDefinitionTimezone(stage, snapshot, 1)
	require.NoError(t, err)
	p, ctx := definitionRoot(t, h, true)
	require.NoError(t, p.BindTimezone(ctx.(chasm.MutableContext), zone))
	child := p.Timezone.Get(ctx)
	_, err = child.InstallNextPages(ctx.(chasm.MutableContext), zone, 16)
	require.NoError(t, err)
	h.commit(true)
	p, ctx = definitionRoot(t, h, false)
	prepared, err := p.PreparedTimezone(ctx)
	require.NoError(t, err)
	require.Equal(t, snapshot.Prepared().Fingerprint(), prepared.Fingerprint())
	source, generation, owner := p.Timezone.Get(ctx).SourceSnapshot(ctx)
	require.Equal(t, stage.owner[:], owner)
	rebuilt, err := StageDefinitionTimezoneSnapshot(stage, source, generation)
	require.NoError(t, err)
	require.Equal(t, zone.ownerID, rebuilt.ownerID)
	next := stagedInterval(t, 1, "seed", 2)
	p, ctx = definitionRoot(t, h, true)
	require.NoError(t, p.ReplaceSource(ctx.(chasm.MutableContext), next))
	h.commit(true)
	p, ctx = definitionRoot(t, h, true)
	_, err = p.PreparedTimezone(ctx)
	require.ErrorContains(t, err, "binding")
	require.ErrorContains(t, p.BindTimezone(ctx.(chasm.MutableContext), zone), "binding")
	replacement, err := StageDefinitionTimezone(next, snapshot, 2)
	require.NoError(t, err)
	require.NoError(t, p.BindTimezone(ctx.(chasm.MutableContext), replacement))
	child = p.Timezone.Get(ctx)
	_, err = child.InstallNextPages(ctx.(chasm.MutableContext), replacement, 16)
	require.NoError(t, err)
	h.commit(true)
	p, ctx = definitionRoot(t, h, true)
	_, err = p.PreparedTimezone(ctx)
	require.NoError(t, err)
	done, err := p.Timezone.Get(ctx).CleanupSuperseded(ctx.(chasm.MutableContext), 16)
	require.NoError(t, err)
	require.True(t, done)
	h.commit(true)
}

func TestDefinitionStagingEmptyAndTimestampBoundary(t *testing.T) {
	t.Parallel()
	for _, spec := range []*schedulepb.ScheduleSpec{{}, {StartTime: timestamppb.New(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)), EndTime: timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC))}} {
		stage, err := StageDefinitionSource(spec, "", 1)
		require.NoError(t, err)
		require.Empty(t, stage.pages)
		decoded, err := decodeDefinitionHeader(stage.header.encode())
		require.NoError(t, err)
		require.Equal(t, stage.header, decoded)
		h := newDefinitionHarness(t, stage)
		p, ctx := definitionRoot(t, h, false)
		header, _, err := p.Prepared(ctx, DefinitionEvaluationPolicy{})
		require.NoError(t, err)
		require.Equal(t, spec.StartTime != nil, header.HasStart)
		require.Equal(t, spec.EndTime != nil, header.HasEnd)
	}
}

func TestDefinitionCodecRejectsCorruption(t *testing.T) {
	t.Parallel()
	stage, err := StageDefinitionSource(&schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{preparationCalendar(1)}}, string(bytes.Repeat([]byte("a"), 25)), 1)
	require.NoError(t, err)
	for ordinal, page := range stage.pages {
		bad := bytes.Clone(page)
		bad[1]++
		require.Error(t, validateDefinitionPage(bad, stage.header, stage.owner, uint64(ordinal)))
		require.Error(t, validateDefinitionPage(append(bytes.Clone(page), 0), stage.header, stage.owner, uint64(ordinal)))
	}
	bad := bytes.Clone(stage.pages[0])
	bad[3] ^= 1
	require.Error(t, validateDefinitionPage(bad, stage.header, stage.owner, 0))
	for _, offset := range []int{0, 57, 70} {
		bad := stage.header.encode()
		bad[offset] = 2
		_, err := decodeDefinitionHeader(bad)
		require.Error(t, err)
	}
}

func TestDefinitionRecordStorageBounds(t *testing.T) {
	t.Parallel()
	spec := &schedulepb.ScheduleSpec{}
	for range 16 {
		spec.StructuredCalendar = append(spec.StructuredCalendar, preparationCalendar(1))
	}
	stage, err := StageDefinitionSource(spec, string(bytes.Repeat([]byte("s"), 24)), 1)
	require.NoError(t, err)
	require.Len(t, stage.pages[0], 1570)
	require.Len(t, stage.pages[1], 26)
	page := &scheduler2pb.DefinitionPreparationPage{OwnerId: stage.owner[:], Ordinal: ^uint64(0), Encoded: stage.pages[0]}
	require.Equal(t, 1618, proto.Size(page))
	state := stage.state()
	state.Revision = 1<<63 - 1
	state.TotalPages = ^uint64(0)
	state.InstalledPages = ^uint64(0)
	state.CleanupHead = ^uint64(0)
	state.CleanupTail = ^uint64(0)
	state.CleanupCount = ^uint64(0)
	require.LessOrEqual(t, proto.Size(state), 229)
	old := &scheduler2pb.DefinitionSupersededSource{OwnerId: stage.owner[:], Header: stage.header.encode(), InstalledPages: ^uint64(0), Cursor: ^uint64(0)}
	require.Equal(t, 149, proto.Size(old))
}
