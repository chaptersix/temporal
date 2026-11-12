package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublicTimestampRangeContinuationRoundTrip(t *testing.T) {
	t.Parallel()
	for _, year := range []int{1, 100, 1677, 1999, 2100, 2500, 9999} {
		at := time.Date(year, 1, 2, 0, 0, 1, 0, time.UTC)
		t.Run(at.Format("2006"), func(t *testing.T) {
			t.Parallel()
			enumerator, err := occurrence.NewPublic(&schedulepb.ScheduleSpec{StartTime: timestamppb.New(at), Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}, "", 1)
			require.NoError(t, err)
			state, err := enumerator.BeginRange(at, at.Add(2*time.Second))
			require.NoError(t, err)
			var observed []time.Time
			for range 4 {
				wire, err := proto.Marshal(encodeContinuation(state))
				require.NoError(t, err)
				stored := &scheduler2pb.OccurrenceContinuation{}
				require.NoError(t, proto.Unmarshal(wire, stored))
				restored := decodeContinuation(stored)
				require.Equal(t, state, restored)
				result, err := enumerator.PeekRange(restored, at.Add(2*time.Second), 8)
				require.NoError(t, err)
				if result.Status == occurrence.Exhausted || result.Status == occurrence.Impossible {
					break
				}
				require.Equal(t, occurrence.Active, result.Status)
				observed = append(observed, result.State.Pending.Actual)
				state, err = enumerator.DisposeRange(result.State)
				require.NoError(t, err)
			}
			if year > 2100 {
				require.Empty(t, observed)
			} else {
				require.Equal(t, []time.Time{at, at.Add(time.Second), at.Add(2 * time.Second)}, observed)
			}
		})
	}
}

func TestPublicWideManualQueuedReload(t *testing.T) {
	t.Parallel()
	firstTime := time.Date(2500, 1, 2, 3, 4, 5, 123456789, time.UTC)
	secondTime := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	h := executingPublicTree(t, &schedulepb.SchedulePatch{Pause: "manual only", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(firstTime), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}})
	for range 8 {
		s, _ := h.root(false)
		if s.OperationId != "" {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ := h.root(false)
	firstID := s.OperationId
	require.NotEmpty(t, firstID)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: firstID, ChainID: "first", PublicRunID: "first-run", StartObserved: true, StartObservedAt: h.now}, true)
	s, ctx := h.root(true)
	require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(secondTime), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}))
	commitGeneration(h, true)
	for range 8 {
		s, _ = h.root(false)
		if len(s.Queue) > 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Len(t, s.Queue, 1)
	require.Equal(t, secondTime, decodeTime(s.Queue[0].Nominal, s.Queue[0].NominalTime))
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: firstID, ChainID: "first", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
	for range 8 {
		s, _ = h.root(false)
		if len(s.Queue) == 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, ctx = h.root(false)
	require.Empty(t, s.Queue)
	require.NotEqual(t, firstID, s.OperationId)
	operation := s.Operations[s.OperationId].Get(ctx)
	require.Equal(t, secondTime, decodeTime(operation.Nominal, operation.NominalTime))
	request := h.request()
	scheduled := chasm.SearchAttributeTemporalScheduledStartTime.Value(secondTime)
	require.Equal(t, scheduled.Value.MustEncode().Data, request.SearchAttributes.IndexedFields[scheduled.Field].Data)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: s.OperationId, ChainID: "second", PublicRunID: "second-run", StartObserved: true, StartObservedAt: h.now + 1}, true)
	s, ctx = h.root(false)
	recent := s.publicRecent(ctx, 10)
	require.Len(t, recent, 2)
	require.Equal(t, firstTime, recent[0].ScheduleTime.AsTime())
	require.Equal(t, secondTime, recent[1].ScheduleTime.AsTime())
}

func TestPublicEmptyFutureBackfill(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, &schedulepb.SchedulePatch{Pause: "no automatic actions"})
	s, ctx := h.root(true)
	require.NoError(t, s.AcceptBackfill(ctx.(chasm.MutableContext), &schedulepb.BackfillRequest{StartTime: timestamppb.New(time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)), EndTime: timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)), OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}))
	commitGeneration(h, true)
	for range 8 {
		s, _ = h.root(false)
		if len(s.Backfills) == 0 {
			break
		}
		coordinatorStep(h, true)
	}
	s, _ = h.root(false)
	require.Empty(t, s.Backfills)
	require.Zero(t, s.Started)
	require.Empty(t, s.Operations)
}

func TestPublicTimestampRepresentationExtremes(t *testing.T) {
	t.Parallel()
	for _, at := range []time.Time{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)} {
		nano, wide := encodeTime(at)
		require.Zero(t, nano)
		require.NotNil(t, wide)
		require.Equal(t, at, decodeTime(nano, wide))
		require.Equal(t, at, decodeDecisionTime(wide).Time(nano))
	}
}
