package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func localUpdateConfig(t *testing.T, revision int64) UpdateConfig {
	t.Helper()
	request, _ := frozenRequest(t)
	request.WorkflowId = "replacement"
	return UpdateConfig{Generation: GenerationConfig{Spec: &schedulepb.ScheduleSpec{}, Request: request, Revision: revision, Seed: "replacement", CatchupWindow: time.Hour, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, Paused: true, Limited: true, Remaining: 7, PauseOnFailure: true}
}

func TestUpdateConfigurationPublishesWholeReplacement(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	h.now += int64(time.Second)
	config := localUpdateConfig(t, 2)
	before := cloneNodes(h.durable)
	s, ctx := h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, false)
	require.Equal(t, before, h.durable)
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.Equal(t, int64(2), s.Revision)
	require.True(t, s.Paused)
	require.True(t, s.Limited)
	require.Equal(t, int64(7), s.Remaining)
	require.True(t, s.PauseOnFailure)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, s.Policy)
	require.Equal(t, h.now, s.ActivityAt)
	g := s.Generator.Get(ctx)
	require.Equal(t, int64(2), g.Continuation.Revision)
	require.Equal(t, h.now, g.Continuation.DisposedThrough)
	require.True(t, s.Coordination.Armed)
	definition := g.Definition.Get(ctx)
	expected, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.Generation.Request)
	require.NoError(t, err)
	require.Equal(t, expected, definition.Request)
	require.Equal(t, "replacement", definition.Seed)
}

func TestUpdateConfigurationRejectsBeforeWrites(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"stale revision", "negative remaining", "missing request", "work token", "generation token", "idle token", "closed", "unconfigured"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := idleHarness(t, time.Minute)
			config := localUpdateConfig(t, 2)
			s, ctx := h.root(true)
			switch name {
			case "stale revision":
				config.Generation.Revision = 1
			case "negative remaining":
				config.Remaining = -1
				config.Limited = false
			case "missing request":
				config.Generation.Request = nil
			case "work token":
				s.Coordination = &scheduler2pb.CoordinationState{Token: math.MaxUint64}
			case "generation token":
				s.Generator.Get(ctx).Token = math.MaxUint64
			case "idle token":
				s.Paused = true
				s.IdleArmed = false
				s.IdleToken = math.MaxUint64
				config.Paused = false
				config.Remaining = 0
			case "closed":
				require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
			case "unconfigured":
				s.HasGenerator = false
			default:
				t.Fatal("unknown rejection case")
			}
			commitGeneration(h, true)
			before, physical := cloneNodes(h.durable), len(h.physical)
			s, ctx = h.root(true)
			require.Error(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
			requireRejectedCoordinatorUnchanged(h, before, physical)
		})
	}
}

func TestUpdateConfigurationPreservesAdmittedEffectsAndWaiters(t *testing.T) {
	t.Parallel()
	h, trackedID, pendingID, effectID := lifecycleFixture(t)
	s, ctx := h.root(false)
	revision := s.Revision + 1
	tracked := append([]byte(nil), s.Requests[trackedID].Get(ctx).Data...)
	pending := append([]byte(nil), s.Requests[pendingID].Get(ctx).Data...)
	effect := effectSnapshot(h, effectID)
	queue := make([]*scheduler2pb.QueuedOccurrence, len(s.Queue))
	for i, entry := range s.Queue {
		queue[i] = proto.Clone(entry).(*scheduler2pb.QueuedOccurrence)
	}
	config := localUpdateConfig(t, revision)
	config.Generation.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	config.Remaining = 0
	s, ctx = h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.Equal(t, tracked, s.Requests[trackedID].Get(ctx).Data)
	require.Equal(t, pending, s.Requests[pendingID].Get(ctx).Data)
	protorequire.DeepEqual(t, effect, effectSnapshot(h, effectID))
	protorequire.ProtoSliceEqual(t, queue, s.Queue)
	require.Equal(t, trackedID, s.BlockingOperationId)
	require.Zero(t, s.Remaining)
	require.True(t, s.Paused)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, s.Policy)
}

func TestUpdateFinalAutomaticIntentHoldsIdle(t *testing.T) {
	t.Parallel()
	base := generationStart()
	h := coordinatorHarness(t, generationSpec(base, base, time.Second), base.Add(-time.Nanosecond), base)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), time.Minute))
	commitGeneration(h, true)
	for step := 0; step < 4; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx = h.root(false)
	require.Len(t, s.Operations, 1)
	require.Equal(t, scheduler2pb.ENUMERATION_STATUS_EXHAUSTED, s.Generator.Get(ctx).Continuation.Terminal)
	require.False(t, s.Coordination.Armed)
	require.False(t, s.IdleArmed)
	require.Equal(t, int64(1), s.UnconfirmedStarts)
	id := s.OperationId
	frozen := append([]byte(nil), s.Requests[id].Get(ctx).Data...)
	h.now = base.Add(2 * time.Minute).UnixNano()
	h.apply(decision.Event{Kind: decision.Expire, Incarnation: "incarnation", OperationID: id}, true)
	s, ctx = h.root(false)
	require.False(t, s.Closed)
	require.False(t, s.IdleArmed)
	require.Equal(t, scheduler2pb.Phase(decision.Ambiguous), s.Operations[id].Get(ctx).Phase)
	require.Equal(t, frozen, s.Requests[id].Get(ctx).Data)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "last-chain", StartObserved: true, StartObservedAt: h.now}, true)
	s, _ = h.root(false)
	require.True(t, s.IdleArmed)
	require.Equal(t, h.now+int64(time.Minute), s.IdleAt)
	h.now = s.IdleAt
	count, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.True(t, s.Closed)
}
