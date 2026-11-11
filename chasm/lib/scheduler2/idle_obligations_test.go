package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func TestIdleObligationsManualQueueAndUpdate(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	s, _ := h.root(false)
	require.Len(t, s.Manuals, 1)
	require.False(t, s.IdleArmed)
	h.now += int64(time.Second)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	first := s.OperationId
	require.Empty(t, s.Manuals)
	require.Equal(t, int64(1), s.UnconfirmedStarts)
	require.False(t, s.IdleArmed)
	allowAck(h, first)
	s, _ = h.root(false)
	require.True(t, s.IdleArmed, "confirmed tracked workflow alone is not an idle hold")
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, true)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Empty(t, s.Manuals)
	require.Len(t, s.Queue, 1)
	require.False(t, s.IdleArmed)
	// A future-policy update must retain the frozen buffered occurrence.
	h.now += int64(time.Second)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	s, _ = h.root(false)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, s.Queue[0].Policy)
	require.False(t, s.IdleArmed)
	sequentialComplete(h, first, nil)
	before := cloneNodes(h.durable)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Empty(t, s.Queue)
	require.NotEqual(t, first, s.OperationId)
	require.False(t, s.IdleArmed, "drain replaces queue hold with an unconfirmed start")
	allowAck(h, s.OperationId)
	s, _ = h.root(false)
	require.True(t, s.IdleArmed)
	require.Equal(t, s.ActivityAt+int64(time.Minute), s.IdleAt)
	require.False(t, s.Closed)
}

func TestIdleObligationsBackfillDefinitionReplacement(t *testing.T) {
	t.Parallel()
	base := generationStart()
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, base, time.Second), now, now)
	coordinatorStep(h, true)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), time.Minute))
	commitGeneration(h, true)
	coordinatorBackfill(h, base, base, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Len(t, s.Backfills, 1)
	require.NotNil(t, s.Backfills[0].Continuation.Pending)
	require.Equal(t, scheduler2pb.WORK_CLASS_BACKFILL, s.Coordination.OwnerClass)
	require.NotEmpty(t, s.Coordination.OwnerKey)
	require.False(t, s.IdleArmed)
	// A new empty definition invalidates the leased unadmitted candidate. It must
	// finish the accepted range through the coordinator, not silently delete it.
	h.now += int64(time.Second)
	template, _ := frozenRequest(t)
	configureBuffer(h, &schedulepb.ScheduleSpec{}, 2, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, time.Hour, template)
	s, _ = h.root(false)
	require.Len(t, s.Backfills, 1)
	require.False(t, s.IdleArmed)
	require.Empty(t, s.Coordination.OwnerKey)
	before := cloneNodes(h.durable)
	coordinatorStep(h, false)
	require.Equal(t, before, h.durable)
	for step := 0; step < 3; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Empty(t, s.Backfills)
	require.Empty(t, s.Operations)
	require.True(t, s.IdleArmed)
	require.Equal(t, h.now+int64(time.Minute), s.IdleAt)
}

func TestIdleObligationsEffectCompletionAndDrain(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	coordinatorStep(h, true)
	s, _ := h.root(false)
	first := s.OperationId
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}, true)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Len(t, s.Queue, 1)
	require.Len(t, s.Effects, 1)
	effectID := s.Effects[0].Id
	assertEffectsPhase(h, 0, decision.AwaitingTarget)
	require.False(t, s.IdleArmed)
	allowAck(h, first)
	assertEffectsPhase(h, 0, decision.EffectPending)
	snapshot := effectSnapshot(h, effectID)
	require.NotNil(t, snapshot)
	h.now += int64(time.Second)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	require.Equal(t, snapshot, effectSnapshot(h, effectID), "control update must preserve bound effect input")
	s, _ = h.root(false)
	h.now = s.Effects[0].Deadline
	effectsResult(h, effectID, decision.EffectExpired, true)
	assertEffectsPhase(h, 0, decision.EffectAmbiguous)
	s, _ = h.root(false)
	require.False(t, s.IdleArmed)
	effectsResult(h, effectID, decision.EffectNotFound, true)
	assertEffectsPhase(h, 0, decision.EffectAbsent)
	s, _ = h.root(false)
	require.Equal(t, first, s.BlockingOperationId)
	require.False(t, s.IdleArmed, "effect settlement does not dispose the accepted waiter")
	sequentialComplete(h, first, nil)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Empty(t, s.Queue)
	require.False(t, s.IdleArmed)
	require.NotEqual(t, first, s.OperationId)
	allowAck(h, s.OperationId)
	s, _ = h.root(false)
	require.True(t, s.IdleArmed)
	for _, effect := range s.Effects {
		require.NotEqual(t, scheduler2pb.EffectPhase(decision.AwaitingTarget), effect.Phase)
		require.NotEqual(t, scheduler2pb.EffectPhase(decision.EffectPending), effect.Phase)
		require.NotEqual(t, scheduler2pb.EffectPhase(decision.EffectAmbiguous), effect.Phase)
	}
}

func TestIdleObligationsOpenSelectivePayloadLoading(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	s, ctx := h.root(true)
	s.Diagnostics = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.Payload{Data: []byte("diagnostics")})
	s.Requests["unrelated"] = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.Payload{Data: []byte("request")})
	s.Carryover = chasm.NewDataField(ctx.(chasm.MutableContext), &scheduler2pb.CarryoverState{Result: []byte("result")})
	s.CarryoverSet = true
	commitGeneration(h, true)
	poisoned := 0
	for _, node := range h.durable {
		if node.Data != nil && node.Metadata.GetDataAttributes() != nil {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.Equal(t, 4, poisoned, "definition, request, carryover and diagnostics are all present")
	h.reload()
	s, ctx = h.root(false)
	require.False(t, s.Closed)
	attrs, task := storedIdleTask(h)
	valid, err := (&IdleHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	require.NoError(t, err)
	require.True(t, valid)
	h.now = attrs.ScheduledTime.UnixNano()
	s, ctx = h.root(true)
	require.NoError(t, (&IdleHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, task))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.True(t, s.Closed)
	require.Panics(t, func() { s.Diagnostics.Get(ctx) }, "poison must fail when explicitly loaded")
}
