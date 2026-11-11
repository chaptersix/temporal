package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// lifecycleFixture has an unresolved tracked operation with a bound cancel
// effect, a newer pending ALLOW_ALL start and a runnable manual coordinator task.
func lifecycleFixture(t *testing.T) (harness *treeHarness, trackedID, pendingStartID, effectID string) {
	t.Helper()
	h, first := bufferedHarness(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 2)
	allowAck(h, first.RequestId)
	allowUpdate(h, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	allowDue(h, true)
	s, _ := h.root(false)
	pendingID := s.OperationId
	coordinatorManual(h)
	s, _ = h.root(false)
	require.True(t, s.Coordination.Armed)
	require.Contains(t, s.PendingStarts, pendingID)
	require.Len(t, s.Effects, 1)
	require.Equal(t, decision.EffectPending, decision.EffectPhase(s.Effects[0].Phase))
	return h, first.RequestId, pendingID, s.Effects[0].Id
}

func TestLifecycleClosePublicationAndTaskFences(t *testing.T) {
	t.Parallel()
	h, _, pendingID, effectID := lifecycleFixture(t)
	s, _ := h.root(false)
	startAttrs := chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.PendingStarts[pendingID])}}
	effectAttrs := chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.Effects[0].RetryAt)}}
	workAttrs := chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.Coordination.At)}}
	work := &scheduler2pb.WorkTask{Token: s.Coordination.Token}
	physical := append([]*tasks.ChasmTask(nil), h.physical...)
	snapshot := cloneNodes(h.durable)
	s, ctx := h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	commitGeneration(h, false)
	require.Equal(t, snapshot, h.durable)
	s, ctx = h.root(false)
	require.False(t, s.Closed)
	require.Equal(t, chasm.LifecycleStateRunning, s.LifecycleState(ctx))
	valid, err := (&WorkHandler{}).Validate(ctx, s, workAttrs, work)
	require.NoError(t, err)
	require.True(t, valid)
	require.NotNil(t, allowRequest(h, pendingID))
	require.NotNil(t, effectSnapshot(h, effectID))
	s, ctx = h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	timers := commitGeneration(h, true)
	require.Empty(t, timers)
	require.Zero(t, pureObligations(h))
	require.Zero(t, logicalCount(h.durable))
	s, ctx = h.root(false)
	require.True(t, s.Closed)
	require.Equal(t, chasm.LifecycleStateCompleted, s.LifecycleState(ctx))
	require.False(t, s.Coordination.Armed)
	require.False(t, s.ManualArmed)
	require.False(t, s.DrainArmed)
	valid, err = (&WorkHandler{}).Validate(ctx, s, workAttrs, work)
	require.NoError(t, err)
	require.False(t, valid)
	valid, err = (&StartHandler{}).Validate(ctx, s, startAttrs, &scheduler2pb.StartTask{OperationId: pendingID})
	require.NoError(t, err)
	require.False(t, valid)
	valid, err = (&EffectHandler{}).Validate(ctx, s, effectAttrs, &scheduler2pb.EffectTask{EffectId: effectID})
	require.NoError(t, err)
	require.False(t, valid)
	for _, task := range physical {
		_, valid, err := h.node.ValidateSideEffectTask(t.Context(), task)
		require.NoError(t, err)
		require.False(t, valid)
	}
	closed := proto.Clone(s.RootState)
	s, ctx = h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	require.NoError(t, (&WorkHandler{}).Execute(ctx.(chasm.MutableContext), s, workAttrs.TaskAttributes, work))
	commitGeneration(h, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, closed, s.RootState)
}

func TestLifecycleClosedResultsAndPoisonedPayloadsAreNoOps(t *testing.T) {
	t.Parallel()
	h, trackedID, pendingID, effectID := lifecycleFixture(t)
	s, ctx := h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	commitGeneration(h, true)
	// Root is the only component control value needed by the terminal fence.
	// Every non-root serialized DATA payload is poisoned, including operations,
	// requests, generator definition, effect request and diagnostics.
	poisoned := 0
	for path, node := range h.durable {
		if path != "" && node.Data != nil && node.Metadata.GetDataAttributes() != nil {
			node.Data.Data = []byte{0xff}
			poisoned++
		}
	}
	require.GreaterOrEqual(t, poisoned, 6)
	h.reload()
	s, ctx = h.root(false)
	closed := proto.Clone(s.RootState)
	request, err := s.request(ctx, pendingID)
	require.NoError(t, err)
	require.Nil(t, request)
	effect, err := s.effectRequest(ctx, effectID)
	require.NoError(t, err)
	require.Nil(t, effect)
	events := []decision.Event{
		{Kind: decision.Acknowledged, OperationID: pendingID, ChainID: "late-chain"},
		{Kind: decision.Callback, OperationID: trackedID, ChainID: "wrong-chain", Outcome: decision.Failure, Failure: []byte{0xff}},
		{Kind: decision.Complete, OperationID: trackedID, ChainID: "late-chain"},
		{Kind: decision.RetryableFailure, OperationID: pendingID},
		{Kind: decision.Expire, OperationID: pendingID},
		{Kind: decision.EffectSucceeded, EffectID: effectID},
		{Kind: decision.EffectNotFound, EffectID: effectID},
		{Kind: decision.EffectFailed, EffectID: effectID},
		{Kind: decision.EffectExpired, EffectID: effectID},
	}
	for _, event := range events {
		event.Incarnation = "incarnation"
		h.apply(event, true)
		s, _ = h.root(false)
		protorequire.ProtoEqual(t, closed, s.RootState)
	}
	s, ctx = h.root(false)
	require.Panics(t, func() { s.Requests[pendingID].Get(ctx) })
}

func TestLifecycleClosedControlsRejectAndTerminateIsIdempotent(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	s, ctx := h.root(true)
	_, err := s.Terminate(ctx.(chasm.MutableContext), chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.True(t, s.Closed)
	closed := proto.Clone(s.RootState)
	controls := []struct {
		name string
		call func(*Scheduler, chasm.MutableContext) error
	}{
		{"manual", func(s *Scheduler, ctx chasm.MutableContext) error { return s.AcceptManual(ctx, nil) }},
		{"backfill", func(s *Scheduler, ctx chasm.MutableContext) error { return s.AcceptBackfill(ctx, nil) }},
		{"definition", func(s *Scheduler, ctx chasm.MutableContext) error {
			return s.ConfigureGeneration(ctx, GenerationConfig{Spec: &schedulepb.ScheduleSpec{}})
		}},
		{"update", func(s *Scheduler, ctx chasm.MutableContext) error {
			return s.Apply(ctx, decision.Event{Kind: decision.Update, Incarnation: s.Incarnation, Revision: s.Revision + 1})
		}},
	}
	for _, control := range controls {
		t.Run(control.name, func(t *testing.T) {
			s, ctx := h.root(true)
			err := control.call(s, ctx.(chasm.MutableContext))
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
			require.Empty(t, commitGeneration(h, true))
			s, _ = h.root(false)
			protorequire.ProtoEqual(t, closed, s.RootState)
		})
	}
	s, ctx = h.root(true)
	_, err = s.Terminate(ctx.(chasm.MutableContext), chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	commitGeneration(h, true)
	s, ctx = h.root(false)
	protorequire.ProtoEqual(t, closed, s.RootState)
	require.Equal(t, chasm.LifecycleStateCompleted, s.LifecycleState(ctx))
}

// Previously committed request IDs win before closed-component control checks.
// This exercises the framework DATA-node dedup layout through the option-aware
// mock engine; its bounded retention and History-storage limits still apply.
func TestLifecycleClosedDedupKeepsPriorSuccess(t *testing.T) {
	t.Parallel()
	e := newManualCommandEngine(t)
	require.NoError(t, Trigger(e.ctx, e.ref, "manual-before-close", &schedulepb.TriggerImmediatelyRequest{}))
	base := generationStart()
	require.NoError(t, Backfill(e.ctx, e.ref, "backfill-before-close", &schedulepb.BackfillRequest{StartTime: timestamppb.New(base), EndTime: timestamppb.New(base)}))
	s, ctx := e.h.root(true)
	require.NoError(t, s.Close(ctx.(chasm.MutableContext)))
	commitGeneration(e.h, true)
	snapshot := cloneNodes(e.h.durable)
	require.NoError(t, Trigger(e.ctx, e.ref, "manual-before-close", nil))
	require.NoError(t, Backfill(e.ctx, e.ref, "backfill-before-close", nil))
	require.Equal(t, snapshot, e.h.durable)
	for _, call := range []struct {
		id  string
		run func() error
	}{
		{"manual-after-close", func() error { return Trigger(e.ctx, e.ref, "manual-after-close", nil) }},
		{"backfill-after-close", func() error { return Backfill(e.ctx, e.ref, "backfill-after-close", nil) }},
	} {
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, call.run(), &notFound)
		used, err := e.h.node.HasRequestID(call.id, e.maxAge)
		require.NoError(t, err)
		require.False(t, used)
		require.Equal(t, snapshot, e.h.durable)
	}
}
