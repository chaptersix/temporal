package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func idleHarness(t *testing.T, duration time.Duration) *treeHarness {
	t.Helper()
	h := manualHarness(t)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), duration))
	commitGeneration(h, true)
	return h
}
func storedIdleTask(h *treeHarness) (chasm.TaskAttributes, *scheduler2pb.IdleTask) {
	h.t.Helper()
	s, _ := h.root(false)
	var attrs chasm.TaskAttributes
	var task *scheduler2pb.IdleTask
	require.NoError(h.t, h.node.EachPureTask(time.Unix(0, s.IdleAt), func(_ chasm.NodePureTask, a chasm.TaskAttributes, value any) (bool, error) {
		if idle, ok := value.(*scheduler2pb.IdleTask); ok {
			require.Nil(h.t, task, "duplicate persisted idle obligation")
			attrs = a
			task = proto.Clone(idle).(*scheduler2pb.IdleTask)
		}
		return false, nil
	}))
	require.NotNil(h.t, task)
	return attrs, task
}

func TestIdleExactDeadlineRollbackAndFailedClosePublication(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	s, _ := h.root(false)
	require.True(t, s.ActivitySet)
	require.Equal(t, h.now, s.ActivityAt)
	require.True(t, s.IdleArmed)
	require.Equal(t, h.now+int64(time.Minute), s.IdleAt)
	require.Equal(t, 1, pureObligations(h))
	attrs, task := storedIdleTask(h)
	snapshot := cloneNodes(h.durable)
	h.now = s.IdleAt - 1
	s, ctx := h.root(true)
	require.ErrorContains(t, (&IdleHandler{}).Execute(ctx.(chasm.MutableContext), s, attrs, task), "not due")
	h.reload()
	require.Equal(t, snapshot, h.durable)
	h.now = attrs.ScheduledTime.UnixNano()
	count, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	commitGeneration(h, false)
	require.Equal(t, snapshot, h.durable)
	s, _ = h.root(false)
	require.False(t, s.Closed)
	require.True(t, s.IdleArmed)
	count, err = deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.True(t, s.Closed)
	require.False(t, s.IdleArmed)
	require.Equal(t, chasm.LifecycleStateCompleted, s.LifecycleState(ctx))
	require.Zero(t, pureObligations(h))
	valid, err := (&IdleHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestIdleRearmIsAtomicAndDoesNotDuplicate(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	oldAttrs, oldTask := storedIdleTask(h)
	s, _ := h.root(false)
	before := proto.Clone(s.RootState)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), 2*time.Minute))
	commitGeneration(h, false)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, before, s.RootState)
	require.Equal(t, 1, pureObligations(h))
	s, ctx = h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), 2*time.Minute))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.Equal(t, s.ActivityAt+int64(2*time.Minute), s.IdleAt)
	require.Greater(t, s.IdleToken, oldTask.Token)
	valid, err := (&IdleHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: oldAttrs}, oldTask)
	require.NoError(t, err)
	require.False(t, valid)
	token, deadline := s.IdleToken, s.IdleAt
	for i := 0; i < 3; i++ {
		s, ctx = h.root(true)
		require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), 2*time.Minute))
		commitGeneration(h, true)
	}
	s, _ = h.root(false)
	require.Equal(t, token, s.IdleToken)
	require.Equal(t, deadline, s.IdleAt)
	require.Equal(t, 1, pureObligations(h))
	closedBefore := proto.Clone(s.RootState)
	s, ctx = h.root(true)
	require.NoError(t, (&IdleHandler{}).Execute(ctx.(chasm.MutableContext), s, oldAttrs, oldTask))
	commitGeneration(h, true)
	s, _ = h.root(false)
	protorequire.ProtoEqual(t, closedBefore, s.RootState)
	s, ctx = h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), 0))
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.Zero(t, s.IdleDuration)
	require.False(t, s.IdleArmed)
	require.Zero(t, pureObligations(h))
}

func TestIdleUpdateAdvancesAnchorAndInvalidatesOldDeadline(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	attrs, task := storedIdleTask(h)
	h.now += int64(10 * time.Second)
	s, _ := h.root(false)
	revision := s.Revision + 1
	event := decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: revision, Limited: true, Remaining: 1}
	before := cloneNodes(h.durable)
	h.apply(event, false)
	require.Equal(t, before, h.durable)
	h.apply(event, true)
	s, ctx := h.root(false)
	require.Equal(t, h.now, s.ActivityAt)
	require.Equal(t, h.now+int64(time.Minute), s.IdleAt)
	valid, err := (&IdleHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	require.NoError(t, err)
	require.False(t, valid)
	token := s.IdleToken
	h.now++
	h.apply(event, true) // same revision is not a new activity event
	s, _ = h.root(false)
	require.Equal(t, token, s.IdleToken)
	require.Equal(t, h.now-1, s.ActivityAt)
}

func TestIdleAmbiguousWaitAndObservedStartedEligibility(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
	coordinatorStep(h, true)
	s, ctx := h.root(false)
	id := s.OperationId
	deadline := s.Operations[id].Get(ctx).Deadline
	require.Equal(t, int64(1), s.UnconfirmedStarts)
	require.False(t, s.IdleArmed)
	h.now = deadline
	h.apply(decision.Event{Kind: decision.Expire, Incarnation: "incarnation", OperationID: id}, true)
	s, _ = h.root(false)
	require.False(t, s.IdleArmed)
	require.False(t, s.Coordination.Armed)
	require.Equal(t, int64(1), s.UnconfirmedStarts)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "chain", StartObserved: true, StartObservedAt: h.now}, true)
	s, ctx = h.root(false)
	require.Equal(t, decision.Started, decision.Phase(s.Operations[id].Get(ctx).Phase))
	require.Equal(t, int64(1), s.Unfinished)
	require.Zero(t, s.UnconfirmedStarts)
	require.True(t, s.IdleArmed)
	require.Equal(t, h.now, s.ActivityAt)
	require.Equal(t, h.now+int64(time.Minute), s.IdleAt)
}

// These fixtures isolate the eligibility projection, including conditions that
// cannot be inferred from an unarmed coordinator. They do not claim every
// isolated metadata combination is a complete production event history.
func TestIdleEligibilityMetadataProjection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		edit     func(*Scheduler, chasm.MutableContext)
		eligible bool
	}{
		{"paused", func(s *Scheduler, _ chasm.MutableContext) { s.Paused = true }, false},
		{"pending manual", func(s *Scheduler, _ chasm.MutableContext) { s.Manuals = []*scheduler2pb.ManualCommand{{Id: 1}} }, false},
		{"pending backfill", func(s *Scheduler, _ chasm.MutableContext) { s.Backfills = []*scheduler2pb.BackfillState{{Id: 1}} }, false},
		{"queued", func(s *Scheduler, _ chasm.MutableContext) { s.Queue = []*scheduler2pb.QueuedOccurrence{{Nominal: 1}} }, false},
		{"unconfirmed", func(s *Scheduler, _ chasm.MutableContext) { s.UnconfirmedStarts = 1 }, false},
		{"awaiting effect", func(s *Scheduler, _ chasm.MutableContext) {
			s.Effects = []*scheduler2pb.EffectState{{Phase: scheduler2pb.EffectPhase(decision.AwaitingTarget)}}
		}, false},
		{"pending effect", func(s *Scheduler, _ chasm.MutableContext) {
			s.Effects = []*scheduler2pb.EffectState{{Phase: scheduler2pb.EffectPhase(decision.EffectPending)}}
		}, false},
		{"ambiguous effect", func(s *Scheduler, _ chasm.MutableContext) {
			s.Effects = []*scheduler2pb.EffectState{{Phase: scheduler2pb.EffectPhase(decision.EffectAmbiguous)}}
		}, false},
		{"accepted effect", func(s *Scheduler, _ chasm.MutableContext) {
			s.Effects = []*scheduler2pb.EffectState{{Phase: scheduler2pb.EffectPhase(decision.EffectAccepted)}}
		}, true},
		{"active scan unarmed", func(s *Scheduler, c chasm.MutableContext) {
			g := s.Generator.Get(c)
			g.Armed = false
			g.Continuation.Terminal = scheduler2pb.ENUMERATION_STATUS_ACTIVE
		}, false},
		{"yield unarmed", func(s *Scheduler, c chasm.MutableContext) {
			g := s.Generator.Get(c)
			g.Armed = false
			g.LastStatus = scheduler2pb.ENUMERATION_STATUS_YIELD
			g.Continuation.Terminal = scheduler2pb.ENUMERATION_STATUS_ACTIVE
		}, false},
		{"budget exhausted active", func(s *Scheduler, c chasm.MutableContext) {
			s.Remaining = 0
			g := s.Generator.Get(c)
			g.Armed = false
			g.Continuation.Terminal = scheduler2pb.ENUMERATION_STATUS_ACTIVE
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			s, ctx := h.root(true)
			tc.edit(s, ctx.(chasm.MutableContext))
			commitGeneration(h, true)
			s, ctx = h.root(true)
			require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), time.Minute))
			commitGeneration(h, true)
			s, _ = h.root(false)
			require.Equal(t, tc.eligible, s.IdleArmed)
			require.False(t, s.Closed)
		})
	}
}

func TestIdleConfigurationBoundariesRejectBeforeWrites(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		duration      time.Duration
		token, anchor bool
	}{
		{"negative", -time.Nanosecond, false, false}, {"over maximum", 365*24*time.Hour + time.Nanosecond, false, false},
		{"token exhausted", time.Minute, true, false}, {"deadline overflow", time.Nanosecond, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := manualHarness(t)
			s, _ := h.root(true)
			if tc.token {
				s.IdleToken = math.MaxUint64
			}
			if tc.anchor {
				s.ActivitySet = true
				s.ActivityAt = math.MaxInt64
			}
			commitGeneration(h, true)
			s, ctx := h.root(true)
			before := proto.Clone(s.RootState)
			require.Error(t, s.ConfigureIdle(ctx.(chasm.MutableContext), tc.duration))
			require.Empty(t, commitGeneration(h, true))
			s, _ = h.root(false)
			protorequire.ProtoEqual(t, before, s.RootState)
			require.Zero(t, pureObligations(h))
		})
	}
	h := idleHarness(t, 365*24*time.Hour)
	s, _ := h.root(false)
	require.True(t, s.IdleArmed)
}
