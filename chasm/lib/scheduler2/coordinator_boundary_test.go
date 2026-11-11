package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCoordinatorBackfillJitterBeyondEndCompletesWithoutAdmission(t *testing.T) {
	t.Parallel()
	nominal := time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)
	spec := generationSpec(nominal, nominal, 90*time.Minute)
	spec.Jitter = durationpb.New(24 * time.Hour)
	now := nominal.Add(24 * time.Hour)
	h := coordinatorHarness(t, spec, now, now)
	coordinatorStep(h, true)
	coordinatorBackfill(h, nominal, nominal, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	coordinatorStep(h, false)
	s, _ := h.root(false)
	require.Len(t, s.Backfills, 1)
	require.Nil(t, s.Backfills[0].Continuation)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Empty(t, s.Backfills, "nominal equals end but deterministic jittered actual is after end")
	require.Empty(t, s.Operations)
	require.Empty(t, s.Queue)
	require.False(t, s.Coordination.Armed)
	require.Zero(t, pureObligations(h))
}

func TestCoordinatorBackfillExclusionYieldRotatesRequests(t *testing.T) {
	t.Parallel()
	base := generationStart()
	spec := generationSpec(base, base.Add(time.Minute), time.Second)
	ranges := func(a, b int32) []*schedulepb.Range { return []*schedulepb.Range{{Start: a, End: b, Step: 1}} }
	spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{{Second: ranges(0, 59), Minute: ranges(0, 59), Hour: ranges(0, 23), DayOfMonth: ranges(1, 31), Month: ranges(1, 12), DayOfWeek: ranges(0, 6)}}
	now := base.Add(time.Hour)
	h := coordinatorHarness(t, spec, now, now)
	coordinatorStep(h, true)
	for i := 0; i < 2; i++ {
		coordinatorBackfill(h, base, base.Add(time.Minute), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
	}
	coordinatorStep(h, true)
	s, _ := h.root(false)
	require.Equal(t, uint64(1), s.LastBackfill)
	require.Zero(t, s.BackfillFocus)
	require.NotNil(t, s.Backfills[0].Continuation)
	require.Nil(t, s.Backfills[1].Continuation)
	first := common.CloneProto(s.Backfills[0].Continuation)
	coordinatorStep(h, true)
	s, _ = h.root(false)
	require.Equal(t, uint64(2), s.LastBackfill)
	protorequire.ProtoEqual(t, first, s.Backfills[0].Continuation)
	require.NotNil(t, s.Backfills[1].Continuation)
	require.Empty(t, s.Operations)
	require.Empty(t, s.Coordination.OwnerKey, "bounded exclusion scans do not acquire queue leases")
	// Rejected definition publication must retain both historical continuations.
	before := cloneNodes(h.durable)
	request, _ := frozenRequest(t)
	s, ctx := h.root(true)
	require.NoError(t, s.ConfigureGeneration(ctx.(chasm.MutableContext), GenerationConfig{Spec: generationSpec(base, base.Add(time.Minute), time.Second), Revision: 2, After: now, CatchupWindow: time.Hour, Request: request}))
	commitGeneration(h, false)
	require.Equal(t, before, h.durable)
}

func TestCoordinatorBackfillOverlapDispositions(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			base := generationStart()
			now := base.Add(time.Hour)
			h := coordinatorHarness(t, generationSpec(base, base.Add(time.Second), time.Second), now, now)
			coordinatorStep(h, true)
			s, ctx := h.root(true)
			require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}))
			commitGeneration(h, true)
			coordinatorStep(h, true)
			s, _ = h.root(false)
			blocker, cursorNano := s.BlockingOperationId, s.Cursor
			coordinatorBackfill(h, base, base.Add(time.Second), policy)
			for step := 0; step < 6; step++ {
				s, _ = h.root(false)
				if len(s.Backfills) == 0 {
					break
				}
				coordinatorStep(h, true)
			}
			s, ctx = h.root(false)
			require.Empty(t, s.Backfills)
			require.Equal(t, blocker, s.BlockingOperationId)
			require.Equal(t, cursorNano, s.Cursor)
			require.Equal(t, int64(100), s.Remaining)
			switch policy {
			case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
				require.Empty(t, s.Queue)
				require.Len(t, s.Operations, 1)
			case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
				require.Len(t, s.Queue, 1)
				require.Equal(t, base.UnixNano(), s.Queue[0].Nominal)
			case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
				require.Empty(t, s.Queue)
				require.Len(t, s.Operations, 3)
				require.Equal(t, policy, s.Operations[s.OperationId].Get(ctx).Policy)
			default:
				require.Len(t, s.Queue, 2)
				require.Equal(t, base.UnixNano(), s.Queue[0].Nominal)
				require.Equal(t, base.Add(time.Second).UnixNano(), s.Queue[1].Nominal)
			}
			for _, q := range s.Queue {
				require.Equal(t, uint64(1), q.BackfillId)
				require.Equal(t, policy, q.Policy)
			}
		})
	}
}
