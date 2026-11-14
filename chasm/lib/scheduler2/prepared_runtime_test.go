//go:build test_dep

package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func preparedRuntimeTree(t *testing.T) *treeHarness {
	t.Helper()
	h, _ := newBoundedRuntimeTree(t, 0, false)
	s, read := h.root(false)
	definition, err := s.publicDefinition(read)
	require.NoError(t, err)
	definition.State.Paused = false
	definition.Spec = &schedulepb.ScheduleSpec{}
	for range 257 {
		definition.Spec.Interval = append(definition.Spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Second)})
	}
	s, raw := h.root(true)
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	require.NoError(t, s.UpdatePublic(ctx, &workflowservice.UpdateScheduleRequest{Namespace: "namespace", ScheduleId: "schedule", Schedule: definition}, schedulerconfig.DefaultTweakables))
	h.commit(true)
	s, read = h.root(false)
	g := s.Generator.Get(read)
	original := common.CloneProto(g.Definition.Get(read))
	spec := &schedulepb.ScheduleSpec{}
	require.NoError(t, proto.Unmarshal(original.Spec, spec))
	// All source compilation and resolution happen outside the mutation callback.
	stage, err := StageDefinitionSource(spec, original.Seed, g.Continuation.Revision)
	require.NoError(t, err)
	resolver := timezonesource.NewResolver(timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: clock.NewEventTimeSource().Update(time.Unix(0, h.now))}))
	snapshot, err := resolver.Resolve("UTC", nil)
	require.NoError(t, err)
	zone, err := StageDefinitionTimezone(stage, snapshot, 1)
	require.NoError(t, err)
	s, raw = h.root(true)
	ctx = raw.(chasm.MutableContext)
	g = s.Generator.Get(ctx)
	p, err := NewDefinitionPreparation(ctx, stage)
	require.NoError(t, err)
	g.PreparedDefinition = chasm.NewComponentField(ctx, p)
	require.NoError(t, p.BindTimezone(ctx, zone))
	done, err := p.Timezone.Get(ctx).InstallNextPages(ctx, zone, 16)
	require.NoError(t, err)
	require.True(t, done)
	h.commit(true)
	for {
		s, raw = h.root(true)
		p = s.Generator.Get(raw).PreparedDefinition.Get(raw)
		done, err = p.InstallNextPages(raw.(chasm.MutableContext), stage, 16)
		require.NoError(t, err)
		h.commit(true)
		if done {
			break
		}
	}
	for _, path := range []string{"Generator$Definition", "Generator$PreparedDefinition$Source", "Generator$PreparedDefinition$Timezone$Source"} {
		require.NotNil(t, h.durable[path], path)
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	return h
}

func TestPreparedAutomaticScanAndTransferAvoidSourceReads(t *testing.T) {
	t.Parallel()
	h := preparedRuntimeTree(t)
	h.now += int64(5 * time.Second)
	s, read := h.root(false)
	initial := common.CloneProto(s.Generator.Get(read).Continuation)
	sawPartial := false
	for quantum := 0; quantum < 40; quantum++ {
		counts := h.node.BeginWorkInspectionForTest()
		s, raw := h.root(true)
		ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
		require.NoError(t, s.scanAutomatic(ctx))
		g := s.Generator.Get(ctx)
		partial := g.PreparedWork.IsSet()
		if partial {
			sawPartial = true
			require.True(t, proto.Equal(initial, g.Continuation), "partial calendar work cannot advance occurrence ownership")
		}
		require.NoError(t, s.reconcileWork(ctx))
		h.commit(true)
		observed := counts.Snapshot()
		require.Zero(t, observed["tree_visits"])
		require.Zero(t, observed["map_key_copies"])

		if !partial {
			break
		}
		require.Less(t, quantum, 39)
	}
	require.True(t, sawPartial)
	s, raw := h.root(true)
	g := s.Generator.Get(raw)
	require.NotNil(t, g.Continuation.Pending)
	require.False(t, g.PreparedWork.IsSet())
	before := g.Continuation.Pending.Nominal
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	require.NoError(t, s.transferAutomatic(ctx))
	require.EqualValues(t, 1, s.OperationStore.OperationCount)
	require.Nil(t, g.Continuation.Pending)
	require.Equal(t, before, g.Continuation.DisposedThrough)
	for _, path := range []string{"Generator/Definition", "Generator/PreparedDefinition/Source", "Generator/PreparedDefinition/Timezone/Source"} {
		require.NotContains(t, h.node.MaterializedPathsForTest(), path)
	}
	require.Contains(t, h.node.MaterializedPathsForTest(), "Generator/Request")
	h.commit(true)
}

func TestPreparedAutomaticPhysicalWorkAndClose(t *testing.T) {
	t.Parallel()
	h := preparedRuntimeTree(t)
	h.now += int64(5 * time.Second)
	sawPartial := false
	for quantum := 0; quantum < 40; quantum++ {
		counts := h.node.BeginWorkInspectionForTest()
		executed, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, executed)
		commitGeneration(h, true)
		require.Zero(t, counts.Snapshot()["tree_visits"])
		s, read := h.root(false)
		g := s.Generator.Get(read)
		sawPartial = sawPartial || g.PreparedWork.IsSet()
		if s.OperationStore.OperationCount > 0 {
			require.True(t, sawPartial)
			require.EqualValues(t, 1, s.OperationStore.OperationCount)
			return
		}
		h.now += int64(time.Millisecond)
	}
	t.Fatal("prepared physical work did not admit candidate")
}

func TestPreparedBackfillScanTransferAndWorkCleanup(t *testing.T) {
	t.Parallel()
	h := preparedRuntimeTree(t)
	s, raw := h.root(true)
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	s.Generator.Get(ctx).Armed = false
	require.NoError(t, s.AcceptBackfill(ctx, &schedulepb.BackfillRequest{StartTime: timestamppb.New(time.Unix(0, h.now).Add(-2 * time.Second)), EndTime: timestamppb.New(time.Unix(0, h.now).Add(-time.Second)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
	h.commit(true)
	partials := 0
	for quantum := 0; quantum < 60; quantum++ {
		s, raw = h.root(true)
		ctx = chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
		if len(s.Backfills) == 0 {
			require.EqualValues(t, 2, s.OperationStore.OperationCount)
			_, exists := s.BackfillWork.Get(ctx, "1")
			require.False(t, exists)
			require.Positive(t, partials)
			return
		}
		b := s.selectedBackfill()
		if b.Continuation != nil && b.Continuation.Pending != nil {
			require.NoError(t, s.transferBackfill(ctx))
		} else {
			require.NoError(t, s.scanBackfill(ctx))
			if _, exists := s.BackfillWork.Get(ctx, "1"); exists {
				partials++
			}
		}
		for _, path := range []string{"Generator/Definition", "Generator/PreparedDefinition/Source", "Generator/PreparedDefinition/Timezone/Source"} {
			require.NotContains(t, h.node.MaterializedPathsForTest(), path)
		}
		h.commit(true)
	}
	t.Fatal("prepared backfill did not finish")
}
