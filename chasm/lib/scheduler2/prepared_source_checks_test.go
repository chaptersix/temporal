//go:build test_dep

package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

func TestPreparedSourceAutomaticPhysicalGrant(t *testing.T) {
	t.Parallel()
	e := newSourceTaskHarness(t, 257, "UTC")
	e.finish(t)
	h := e.h
	s, raw := h.root(true)
	ctx := chasm.ContextWithValue(raw.(chasm.MutableContext), workQuantaKey{}, 0)
	s.Paused = false
	g := s.Generator.Get(ctx)
	g.Armed = true
	g.WakeAt = h.now
	require.NoError(t, s.reconcileWork(ctx))
	h.commit(true)
	h.now += int64(3 * time.Second)
	sawCheck, sawPartial := false, false
	for quantum := 0; quantum < 100; quantum++ {
		s, read := h.root(false)
		g = s.Generator.Get(read)
		request := s.SourceRequest.Get(read)
		if request.Pending {
			if request.CheckKind == sourceCheckAutomatic {
				sawCheck = true
			}
			require.NoError(t, e.execute(e.task(t)))
			continue
		}
		if g.PreparedWork.IsSet() {
			sawPartial = true
			require.Equal(t, g.Continuation.Revision, g.PreparedSourceRevision)
		}
		if s.OperationStore.OperationCount > 0 {
			require.True(t, sawCheck)
			require.True(t, sawPartial)
			return
		}
		if s.Coordination.Armed && s.Coordination.At > h.now {
			h.now = s.Coordination.At
		}
		executed, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, executed, "quantum=%d root=%v coordinator=%v generator=%v request=%v", quantum, s.RootState, s.Coordination, g.GeneratorState, request)
		commitGeneration(h, true)
		h.now += int64(time.Millisecond)
	}
	t.Fatal("physical source check and work failed to admit an occurrence")
}

func TestPreparedSourcePreviewFenceAndBackfillGrant(t *testing.T) {
	t.Parallel()
	e := newSourceTaskHarness(t, 1, "UTC")
	e.finish(t)
	h := e.h
	s, read := h.root(false)
	p := s.Generator.Get(read).PreparedDefinition.Get(read)
	owner := [32]byte(p.OwnerId)
	stamp := stageListTest(t, h, owner, listTestStatic(t, "source", 1), 1, nil)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	job, err := newListPreview(owner, stamp, time.Unix(0, h.now), false, 0)
	require.NoError(t, err)
	s.ListPreview = chasm.NewDataField(ctx, job)
	ready, err := s.ensurePreviewSourceReady(ctx, job)
	require.NoError(t, err)
	require.False(t, ready)
	oldToken := s.SourceRequest.Get(ctx).Token
	ready, err = s.ensureAutomaticSourceReady(ctx, s.Generator.Get(ctx))
	require.NoError(t, err)
	require.False(t, ready)
	require.Equal(t, oldToken, s.SourceRequest.Get(ctx).Token, "another job must not replace outstanding source owner")
	// Replace the logical preview while its old check is outstanding.
	replacement := common.CloneProto(job)
	replacement.Stamp++
	s.ListPreview = chasm.NewDataField(ctx, replacement)
	h.commit(true)
	require.NoError(t, e.execute(e.task(t)))
	s, read = h.root(false)
	if current, ok := s.ListPreview.TryGet(read); ok {
		require.False(t, current.PreparedSourceReady)
	}
	require.Zero(t, s.Generator.Get(read).PreparedSourceRevision)
	// No automatic execution: a paused root can independently grant a backfill.
	s, raw = h.root(true)
	ctx = raw.(chasm.MutableContext)
	s.ListPreview = chasm.Field[*pb.PublicListPreview]{}
	s.Backfills = []*pb.BackfillState{{Id: 7}}
	ready, err = s.ensureBackfillSourceReady(ctx, s.Generator.Get(ctx), s.Backfills[0])
	require.NoError(t, err)
	require.False(t, ready)
	grant, err := s.prepareSourceGrant(ctx, s.SourceRequest.Get(ctx), s.Generator.Get(ctx).PreparedDefinition.Get(ctx))
	require.NoError(t, err)
	s.publishSourceGrant(ctx, grant)
	require.Equal(t, s.Generator.Get(ctx).Continuation.Revision, s.Backfills[0].PreparedSourceRevision)
	require.Zero(t, s.Generator.Get(ctx).PreparedSourceRevision)
}

func TestPreparedStagedPublicCanonicalFrontendParity(t *testing.T) {
	t.Parallel()
	service, _, _, _ := preparationServiceFixture(t)
	request := stagedPublicTestRequest()
	request.Schedule.Spec = &schedulepb.ScheduleSpec{CronString: []string{"0 12 * * MON"}}
	original := common.CloneProto(request)
	reference, err := legacy.NewSpecBuilder(func() int { return 100 }, func() int { return 1000 }).NewCompiledSpec(request.Schedule.Spec)
	require.NoError(t, err)
	stage, err := service.stagePublicCreate(request, "namespace", 5)
	require.NoError(t, err)
	require.True(t, proto.Equal(reference.CanonicalForm(), stage.request.Schedule.Spec))
	require.True(t, proto.Equal(original, request))
	require.Empty(t, stage.request.Schedule.Spec.CronString)
	require.NotEmpty(t, stage.request.Schedule.Spec.StructuredCalendar)
}

func TestPreparedSourceGrantConsumedAfterOneOutput(t *testing.T) {
	t.Parallel()
	e := newSourceTaskHarness(t, 257, "UTC")
	e.finish(t)
	h := e.h
	s, raw := h.root(true)
	ready, err := s.ensureAutomaticSourceReady(raw.(chasm.MutableContext), s.Generator.Get(raw))
	require.NoError(t, err)
	require.False(t, ready)
	h.commit(true)
	require.NoError(t, e.execute(e.task(t)))
	h.now += int64(3 * time.Second)
	partial := false
	for quantum := 0; quantum < 100; quantum++ {
		s, raw = h.root(true)
		g := s.Generator.Get(raw)
		require.Equal(t, g.Continuation.Revision, g.PreparedSourceRevision)
		require.NoError(t, s.scanPreparedAutomatic(raw.(chasm.MutableContext), g))
		if g.PreparedWork.IsSet() {
			partial = true
			require.Equal(t, g.Continuation.Revision, g.PreparedSourceRevision)
			h.commit(true)
			continue
		}
		require.True(t, partial)
		require.Zero(t, g.PreparedSourceRevision)
		require.NotNil(t, g.Continuation.Pending)
		h.commit(true)
		return
	}
	t.Fatal("grant did not produce one bounded output")
}
