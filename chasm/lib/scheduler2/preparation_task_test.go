//go:build test_dep

package scheduler2

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type sourceTaskHarness struct {
	h                *treeHarness
	service          *PreparationService
	ctx              context.Context
	key              chasm.ExecutionKey
	reads, updates   int
	discard, lostAck bool
	seen             map[string]bool
}

func newSourceTaskHarness(t *testing.T, terms int, zoneName string, custom ...[]byte) *sourceTaskHarness {
	t.Helper()
	h, _ := newBoundedRuntimeTree(t, 0, false)
	wall := clock.NewEventTimeSource().Update(time.Unix(0, h.now))
	service, err := NewPreparationService(timezonesource.NewEnvironment(timezonesource.SourceConfig{Clock: wall}), wall)
	require.NoError(t, err)
	library := newBoundedLibrary(NewStartHandler(nil), nil)
	library.preparation = service
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(library))
	h.registry = registry
	h.reload()
	s, read := h.root(false)
	g := s.Generator.Get(read)
	revision := g.Continuation.Revision
	spec := &schedulepb.ScheduleSpec{TimezoneName: zoneName}
	if len(custom) > 0 {
		spec.TimezoneData = custom[0]
	}
	for range terms {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Second)})
	}
	stage, err := StageDefinitionSource(spec, "immutable seed", revision)
	require.NoError(t, err)
	service.stages.Put(preparationCacheKey('d', stage.owner), stage)
	header, _, err := stage.Prepared(DefinitionEvaluationPolicy{ReturnExcluded: true})
	require.NoError(t, err)
	cursor, err := occurrence.NewPreparedCursor(stage.header.identity, revision, header)
	require.NoError(t, err)
	state, err := cursor.Begin(time.Unix(0, h.now))
	require.NoError(t, err)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	g = s.Generator.Get(ctx)
	p, err := NewDefinitionPreparation(ctx, stage)
	require.NoError(t, err)
	g.PreparedDefinition = chasm.NewComponentField(ctx, p)
	g.Continuation = encodeContinuation(state)
	require.NoError(t, s.beginSourcePreparation(ctx))
	h.commit(true)
	e := &sourceTaskHarness{h: h, service: service, key: chasm.ExecutionKey{NamespaceID: "namespace", BusinessID: "schedule", RunID: "incarnation"}, seen: map[string]bool{}}
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, read func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
		e.reads++
		cc := chasm.NewContext(ctx, h.node)
		component, err := h.node.Component(cc, ref)
		if err != nil {
			return err
		}
		err = read(cc, component)
		for _, path := range h.node.MaterializedPathsForTest() {
			e.seen[path] = true
		}
		return err
	})
	engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
		e.updates++
		cc := chasm.NewMutableContext(ctx, h.node)
		component, err := h.node.Component(cc, ref)
		if err != nil {
			return nil, err
		}
		if err = update(cc, component); err != nil {
			h.reload()
			return nil, err
		}
		for _, path := range h.node.MaterializedPathsForTest() {
			e.seen[path] = true
		}
		h.commit(!e.discard)
		if e.discard || e.lostAck {
			return nil, errors.New("injected source response publication/ack loss")
		}
		return nil, nil
	})
	e.ctx = chasm.NewEngineContext(t.Context(), engine)
	return e
}
func (e *sourceTaskHarness) task(t *testing.T) *tasks.ChasmTask {
	t.Helper()
	kind, ok := e.h.registry.TaskIDFor(&pb.SourcePreparationTask{})
	require.True(t, ok)
	for i := len(e.h.physical) - 1; i >= 0; i-- {
		if e.h.physical[i].Info.TypeId == kind {
			return e.h.physical[i]
		}
	}
	t.Fatal("source physical task missing")
	return nil
}
func (e *sourceTaskHarness) execute(task *tasks.ChasmTask) error {
	return e.h.node.ExecuteSideEffectTask(e.ctx, e.key, task, func(chasm.NodeBackend, chasm.Context, chasm.Component) error { return nil })
}
func (e *sourceTaskHarness) request(t *testing.T) *pb.SourcePreparationRequest {
	t.Helper()
	s, ctx := e.h.root(false)
	return common.CloneProto(s.SourceRequest.Get(ctx))
}
func (e *sourceTaskHarness) finish(t *testing.T) {
	t.Helper()
	for n := 0; n < 100; n++ {
		if !e.request(t).Pending {
			return
		}
		require.NoError(t, e.execute(e.task(t)))
	}
	t.Fatal("source preparation did not finish")
}

func TestSourcePreparationPhysicalBoundedInstallAndSameFingerprint(t *testing.T) {
	t.Parallel()
	e := newSourceTaskHarness(t, 1000, "")
	e.h.durable["Generator$PreparedDefinition$Source"].Data.Data = []byte{0xff}
	e.h.reload()
	definitionBatches := 0
	for n := 0; n < 100; n++ {
		r := e.request(t)
		if !r.Pending {
			break
		}
		s, ctx := e.h.root(false)
		before := s.Generator.Get(ctx).PreparedDefinition.Get(ctx).InstalledPages
		require.NoError(t, e.execute(e.task(t)))
		s, ctx = e.h.root(false)
		after := s.Generator.Get(ctx).PreparedDefinition.Get(ctx).InstalledPages
		require.LessOrEqual(t, after-before, uint64(16))
		if r.Phase == sourceDefinition {
			definitionBatches++
		}
	}
	require.False(t, e.request(t).Pending)
	require.GreaterOrEqual(t, definitionBatches, 4)
	require.False(t, e.seen["Generator/PreparedDefinition/Source"])
	s, ctx := e.h.root(false)
	zone := common.CloneProto(s.Generator.Get(ctx).PreparedDefinition.Get(ctx).Timezone.Get(ctx).TimezonePreparationState)
	s, raw := e.h.root(true)
	require.NoError(t, s.beginSourcePreparation(raw.(chasm.MutableContext)))
	e.h.commit(true)
	e.finish(t)
	s, ctx = e.h.root(false)
	next := s.Generator.Get(ctx).PreparedDefinition.Get(ctx).Timezone.Get(ctx).TimezonePreparationState
	require.True(t, proto.Equal(zone, next), "same fingerprint retains exact active generation/resolution metadata")
}

func TestSourcePreparationCacheMissAndResponseLoss(t *testing.T) {
	t.Parallel()
	for _, discard := range []bool{false, true} {
		t.Run(map[bool]string{false: "lost ack", true: "discarded publication"}[discard], func(t *testing.T) {
			e := newSourceTaskHarness(t, 300, "")
			r := e.request(t)
			e.service.stages.Delete(preparationCacheKey('d', [32]byte(r.DefinitionOwnerId)))
			original := cloneNodes(e.h.durable)
			task := e.task(t)
			e.discard = discard
			e.lostAck = !discard
			require.Error(t, e.execute(task))
			require.True(t, e.seen["Generator/PreparedDefinition/Source"])
			if discard {
				require.Equal(t, original, e.h.durable)
			} else {
				require.Greater(t, e.request(t).Token, r.Token)
			}
			e.discard = false
			e.lostAck = false
			if discard {
				require.NoError(t, e.execute(task))
			} else {
				before := cloneNodes(e.h.durable)
				_ = e.execute(task)
				require.Equal(t, before, e.h.durable)
			}
			e.finish(t)
			require.False(t, e.request(t).Pending)
		})
	}
}

func TestSourcePreparationStaleOwnerAndResolveError(t *testing.T) {
	t.Parallel()
	e := newSourceTaskHarness(t, 1, "definitely-missing-zone-for-source-task")
	require.NoError(t, e.execute(e.task(t)))
	require.Equal(t, sourceResolve, e.request(t).Phase)
	before := cloneNodes(e.h.durable)
	r := e.request(t)
	require.Error(t, e.execute(e.task(t)))
	require.Equal(t, before, e.h.durable)
	require.Equal(t, r, e.request(t))
	handler := &SourcePreparationHandler{service: e.service}
	ref := chasm.NewComponentRef[*Scheduler](e.key)
	for _, task := range []*pb.SourcePreparationTask{{Token: r.Token + 1, DefinitionOwnerId: r.DefinitionOwnerId}, {Token: r.Token, DefinitionOwnerId: make([]byte, 32)}} {
		updates := e.updates
		require.NoError(t, handler.Execute(e.ctx, ref, chasm.TaskAttributes{}, task))
		require.Equal(t, updates, e.updates)
		require.Equal(t, before, e.h.durable)
	}
}

func TestSourcePreparationTimezoneCacheRecovery(t *testing.T) {
	t.Parallel()
	for _, afterBatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "before source persisted", true: "after source persisted"}[afterBatch], func(t *testing.T) {
			e := newSourceTaskHarness(t, 1, "custom", largeTimezoneFixture())
			for n := 0; n < 10 && e.request(t).Phase != sourceTimezone; n++ {
				require.NoError(t, e.execute(e.task(t)))
			}
			r := e.request(t)
			require.Equal(t, sourceTimezone, r.Phase)
			if afterBatch {
				require.NoError(t, e.execute(e.task(t)))
				require.True(t, e.request(t).Pending)
			}
			e.service.stages.Delete(preparationCacheKey('t', [32]byte(r.CandidateSourceId)))
			e.finish(t)
			s, ctx := e.h.root(false)
			zone := s.Generator.Get(ctx).PreparedDefinition.Get(ctx).Timezone.Get(ctx)
			require.Nil(t, zone.Pending)
			require.Equal(t, r.CandidateSourceId, zone.SourceId)
			prepared, err := zone.PreparedTimezone(ctx)
			require.NoError(t, err)
			require.Positive(t, prepared.PageCount())
		})
	}
}
func TestSourcePreparationCompletionCounterRejection(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"coordinator", "idle", "source"} {
		t.Run(mode, func(t *testing.T) {
			e := newSourceTaskHarness(t, 1, "")
			for n := 0; n < 10 && e.request(t).Phase != sourceTimezone; n++ {
				require.NoError(t, e.execute(e.task(t)))
			}
			r := e.request(t)
			require.Equal(t, sourceTimezone, r.Phase)
			candidate, ok := e.service.Timezone([32]byte(r.CandidateSourceId))
			require.True(t, ok)
			batch, err := candidate.InstallBatch(0, 0, 16)
			require.NoError(t, err)
			response := sourceResponse{task: &pb.SourcePreparationTask{Token: r.Token, DefinitionOwnerId: r.DefinitionOwnerId}, timezone: batch}
			require.True(t, sourceResponseCompletes(r, response), "fixture must use a valid final install batch")
			s, raw := e.h.root(true)
			ctx := raw.(chasm.MutableContext)
			switch mode {
			case "idle":
				s.Paused = false
				s.Limited = true
				s.Remaining = 0
				s.ActivitySet = true
				s.ActivityAt = e.h.now
				s.IdleDuration = int64(time.Second)
				s.IdleToken = math.MaxUint64
				s.IdleArmed = false
				s.Generator.Get(ctx).Armed = false
			case "coordinator":
				s.Coordination.Token = math.MaxUint64
				s.Coordination.Armed = false
				s.Generator.Get(ctx).Armed = true
				s.Generator.Get(ctx).WakeAt = e.h.now
			case "source":
				r.Token = math.MaxUint64
				s.SourceRequest = chasm.NewDataField(ctx, r)
				response.task.Token = r.Token
			default:
				t.Fatalf("unknown failure mode %q", mode)
			}
			e.h.commit(true)
			before, physical := cloneNodes(e.h.durable), len(e.h.physical)
			s, raw = e.h.root(true)
			_, err = s.mergeSourcePreparation(raw.(chasm.MutableContext), response)
			require.ErrorContains(t, err, "token exhausted")
			requireRejectedCoordinatorUnchanged(e.h, before, physical)
		})
	}
}
