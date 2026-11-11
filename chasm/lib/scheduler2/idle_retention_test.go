package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIdleAnchorSurvivesRetentionAndClockRollback(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	base := h.now
	oldAttrs, oldTask := storedIdleTask(h)
	high := base + int64(time.Hour)
	var first string
	for i := 0; i < 5; i++ {
		// Later admissions run after a clock rollback; their nominal identities
		// retain deterministic eviction order independently of observation time.
		h.now = base + int64(i)*int64(time.Second)
		acceptManual(h, &schedulepb.TriggerImmediatelyRequest{
			ScheduledTime: timestamppb.New(time.Unix(0, h.now)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}, true)
		coordinatorStep(h, true)
		s, _ := h.root(false)
		id := s.OperationId
		observedNano := h.now
		if i == 0 {
			first, observedNano = id, high
			h.now = high
		}
		h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: id + "/chain", StartObserved: true, StartObservedAt: observedNano}, true)
		s, _ = h.root(false)
		require.Equal(t, high, s.ActivityAt)
		require.True(t, s.IdleArmed)
		require.Equal(t, high+int64(time.Minute), s.IdleAt)
		require.Equal(t, 1, pureObligations(h))
	}
	s, _ := h.root(false)
	require.Len(t, s.Operations, 4)
	require.NotContains(t, s.Operations, first)
	token := s.IdleToken
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: first, ChainID: first + "/chain", StartObserved: true, StartObservedAt: high + int64(time.Hour)}, true)
	s, ctx := h.root(true)
	require.Equal(t, high, s.ActivityAt)
	require.Equal(t, token, s.IdleToken)
	require.NoError(t, (&IdleHandler{}).Execute(ctx.(chasm.MutableContext), s, oldAttrs, oldTask))
	commitGeneration(h, true)
	h.now = high + int64(time.Minute) + 1
	count, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.True(t, s.Closed)
	require.Equal(t, high, s.ActivityAt)
	require.Zero(t, pureObligations(h))
}

func TestIdlePauseResumeReplacesExpiredObligation(t *testing.T) {
	t.Parallel()
	h := idleHarness(t, time.Minute)
	oldAttrs, oldTask := storedIdleTask(h)
	h.now += int64(time.Second)
	s, _ := h.root(false)
	revision := s.Revision + 1
	h.apply(decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: revision, Paused: true}, true)
	s, _ = h.root(false)
	require.True(t, s.Paused)
	require.False(t, s.IdleArmed)
	require.Zero(t, pureObligations(h))
	h.now = oldAttrs.ScheduledTime.UnixNano() + 1
	s, ctx := h.root(true)
	require.NoError(t, (&IdleHandler{}).Execute(ctx.(chasm.MutableContext), s, oldAttrs, oldTask))
	commitGeneration(h, true)
	before := cloneNodes(h.durable)
	resume := decision.Event{Kind: decision.Update, Incarnation: "incarnation", Revision: revision + 1}
	h.apply(resume, false)
	require.Equal(t, before, h.durable)
	h.apply(resume, true)
	s, ctx = h.root(false)
	require.False(t, s.Paused)
	require.False(t, s.Closed)
	require.True(t, s.IdleArmed)
	require.Greater(t, s.IdleToken, oldTask.Token)
	require.Equal(t, h.now+int64(time.Minute), s.IdleAt)
	require.Equal(t, 1, pureObligations(h))
	valid, err := (&IdleHandler{}).Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: oldAttrs}, oldTask)
	require.NoError(t, err)
	require.False(t, valid)
	h.now = s.IdleAt
	count, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.True(t, s.Closed)
}

func TestIdleHistoricalBackfillUsesStartObservation(t *testing.T) {
	t.Parallel()
	base := generationStart()
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, generationSpec(base, base, time.Second), now, now)
	coordinatorStep(h, true)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), time.Minute))
	commitGeneration(h, true)
	coordinatorBackfill(h, base, base, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	for step := 0; step < 4; step++ {
		s, _ = h.root(false)
		if !s.Coordination.Armed {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx = h.root(false)
	require.Empty(t, s.Backfills)
	require.Len(t, s.Operations, 1)
	id := s.OperationId
	op := s.Operations[id].Get(ctx)
	require.Equal(t, base.UnixNano(), op.Nominal)
	require.Equal(t, uint64(1), op.BackfillId)
	require.Equal(t, now.UnixNano(), s.ActivityAt)
	require.False(t, s.IdleArmed)
	observedNano := now.Add(5 * time.Second).UnixNano()
	h.now = observedNano
	event := decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "historical-chain", StartObserved: true, StartObservedAt: observedNano}
	before := cloneNodes(h.durable)
	h.apply(event, false)
	require.Equal(t, before, h.durable)
	h.apply(event, true)
	s, _ = h.root(false)
	require.Equal(t, observedNano, s.ActivityAt)
	require.Equal(t, observedNano+int64(time.Minute), s.IdleAt)
	require.True(t, s.IdleArmed)
	require.Zero(t, s.UnconfirmedStarts)
	h.now = s.IdleAt
	count, err := deliverGeneration(h, time.Unix(0, h.now))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	commitGeneration(h, true)
	s, _ = h.root(false)
	require.True(t, s.Closed)
}
