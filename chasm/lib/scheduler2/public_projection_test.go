package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func publicProjectionAdmission(h *treeHarness) string {
	h.t.Helper()
	h.now += 2
	_, wire := frozenRequest(h.t)
	h.apply(decision.Event{Kind: decision.Due, Incarnation: "incarnation", Nominal: h.now - 1, Actual: h.now, CatchupDeadline: h.now, WorkflowID: "public-action", Request: wire}, true)
	s, _ := h.root(false)
	return s.OperationId
}

func TestPublicProjectionLifecycleAndLateAcknowledgment(t *testing.T) {
	t.Parallel()
	for _, callbackFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("callbackFirst=%v", callbackFirst), func(t *testing.T) {
			t.Parallel()
			h := executingPublicTree(t, nil)
			id := publicProjectionAdmission(h)
			s, ctx := h.root(false)
			description, err := s.DescribePublic(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 1, description.Info.BufferSize)
			require.Empty(t, description.Info.RunningWorkflows)
			require.Empty(t, description.Info.RecentActions)
			ack := decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: id, ChainID: "canonical-chain", PublicRunID: "actual-start-run", StartObserved: true, StartObservedAt: h.now + 5}
			callback := decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "canonical-chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}
			if callbackFirst {
				h.apply(callback, true)
				s, ctx = h.root(false)
				description, err = s.DescribePublic(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, description.Info.ActionCount)
				require.Empty(t, description.Info.RecentActions, "completion chain is not a start response run ID")
			}
			h.apply(ack, true)
			s, ctx = h.root(false)
			description, err = s.DescribePublic(ctx)
			require.NoError(t, err)
			status := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			if callbackFirst {
				status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			} else {
				protorequire.ProtoEqual(t, &commonpb.WorkflowExecution{WorkflowId: "public-action", RunId: "actual-start-run"}, description.Info.RunningWorkflows[0])
			}
			expected := &schedulepb.ScheduleActionResult{ScheduleTime: timestamppb.New(time.Unix(0, h.now).UTC()), ActualTime: timestamppb.New(time.Unix(0, h.now+5).UTC()), StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "public-action", RunId: "actual-start-run"}, StartWorkflowStatus: status}
			require.Len(t, description.Info.RecentActions, 1)
			protorequire.ProtoEqual(t, expected, description.Info.RecentActions[0])
			require.Zero(t, description.Info.BufferSize)
			h.apply(callback, true)
			h.apply(ack, true)
			s, ctx = h.root(false)
			description, err = s.DescribePublic(ctx)
			require.NoError(t, err)
			require.Empty(t, description.Info.RunningWorkflows)
			require.EqualValues(t, 1, description.Info.ActionCount)
			expected.StartWorkflowStatus = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			protorequire.ProtoEqual(t, expected, description.Info.RecentActions[0])
			description.Info.RecentActions[0].StartWorkflowResult.RunId = "caller mutation"
			again, err := s.DescribePublic(ctx)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, expected, again.Info.RecentActions[0])
		})
	}
}

func TestPublicProjectionHistorySurvivesOperationEviction(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, _ := h.root(true)
	s.Policy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	h.commit(true)
	for i := range 14 {
		h.now += int64(time.Second)
		id := publicProjectionAdmission(h)
		s, _ = h.root(false)
		h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: id, ChainID: fmt.Sprintf("chain%d", i), PublicRunID: fmt.Sprintf("run%d", i), StartObserved: true, StartObservedAt: h.now}, true)
	}
	s, ctx := h.root(false)
	require.Len(t, s.Operations, 4)
	description, err := s.DescribePublic(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 14, description.Info.ActionCount)
	require.Empty(t, description.Info.RunningWorkflows)
	require.Len(t, description.Info.RecentActions, 10)
	require.Equal(t, "run4", description.Info.RecentActions[0].StartWorkflowResult.RunId)
	require.Equal(t, "run13", description.Info.RecentActions[9].StartWorkflowResult.RunId)
	list := s.Memo(ctx).(*schedulepb.ScheduleListInfo)
	require.Len(t, list.RecentActions, 5)
	require.Equal(t, "run9", list.RecentActions[0].StartWorkflowResult.RunId)
	require.LessOrEqual(t, len(s.PublicRuntime.Get(ctx).Actions), 14)
	// Visibility can read the compact list without loading action definitions.
	h.durable["PublicSchedule"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx = h.root(false)
	require.Len(t, s.Memo(ctx).(*schedulepb.ScheduleListInfo).RecentActions, 5)
	require.NotContains(t, h.node.MaterializedPathsForTest(), "PublicSchedule")
}

func TestPublicProjectionFutureReadOnlyAndRemaining(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(false)
	before := cloneNodes(h.durable)
	description, err := s.DescribePublic(ctx)
	require.NoError(t, err)
	require.Len(t, description.Info.FutureActionTimes, 10)
	for i, at := range description.Info.FutureActionTimes {
		require.Equal(t, generationStart().Add(time.Duration(i+1)*time.Minute), at.AsTime())
	}
	require.Len(t, s.Memo(ctx).(*schedulepb.ScheduleListInfo).FutureActionTimes, 5)
	require.Equal(t, before, h.durable)
	s, ctx = h.root(true)
	s.Limited, s.Remaining = true, 2
	s.publishPublicList(ctx.(chasm.MutableContext), s.PublicSchedule.Get(ctx))
	h.commit(true)
	s, ctx = h.root(false)
	description, err = s.DescribePublic(ctx)
	require.NoError(t, err)
	require.Len(t, description.Info.FutureActionTimes, 2)
	require.Len(t, s.Memo(ctx).(*schedulepb.ScheduleListInfo).FutureActionTimes, 2)
}

func TestPublicProjectionDispositionCounters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                         string
		policy                       enumspb.ScheduleOverlapPolicy
		queue                        []decision.Queued
		missed                       bool
		wantSkip, wantMiss, wantDrop int64
	}{
		{name: "skip", policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, wantSkip: 1},
		{name: "buffer_one_policy", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, queue: []decision.Queued{{Nominal: 1, Actual: 1}}, wantSkip: 1},
		{name: "buffer_capacity", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, queue: make([]decision.Queued, 4), wantDrop: 1},
		{name: "catchup_before_overlap", policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, missed: true, wantMiss: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := decision.State{Root: decision.Root{Incarnation: "inc", Policy: tc.policy, BlockingOperationID: "running", Cursor: 0}, Queue: tc.queue}
			event := decision.Event{Kind: decision.Due, Incarnation: "inc", Nominal: 10, Actual: 10, Now: 10, CatchupDeadline: 10, WorkflowID: "wf"}
			if tc.missed {
				event.CatchupDeadline = 9
			}
			next, err := decision.Step(before, event)
			require.NoError(t, err)
			skip, miss, drop := publicDispositionCounts(before, next, event)
			require.Equal(t, []int64{tc.wantSkip, tc.wantMiss, tc.wantDrop}, []int64{skip, miss, drop})
		})
	}
	before := decision.State{Root: decision.Root{Incarnation: "inc"}, Queue: []decision.Queued{{Nominal: 1, Actual: 1}, {Nominal: 2, Actual: 2, ManualID: 1}}}
	next := before
	next.Queue = before.Queue[1:]
	skip, miss, drop := publicDispositionCounts(before, next, decision.Event{Kind: decision.Drain, Now: 20, CatchupWindow: 5})
	require.Equal(t, []int64{0, 1, 0}, []int64{skip, miss, drop})
}

func TestPublicProjectionUsesLiveSpecComputeLimits(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	limit := 1
	builder := legacy.NewSpecBuilder(func() int { return 0 }, func() int { return limit })
	s, ctx := h.root(false)
	ctx = chasm.ContextWithValue(ctx, publicSpecBuilderKey{}, builder)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0, End: 5}}, Minute: []*schedulepb.Range{{Start: 0, End: 59}}, Hour: []*schedulepb.Range{{Start: 0, End: 23}}, DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}}, Month: []*schedulepb.Range{{Start: 1, End: 12}}, DayOfWeek: []*schedulepb.Range{{Start: 0, End: 6}}}}}
	require.Empty(t, s.publicFuture(ctx, spec, 10), "configured iteration limit must govern Describe preview")
	list := &schedulepb.ScheduleListInfo{}
	s.publicListProjection(ctx, list, spec)
	require.Empty(t, list.FutureActionTimes, "the same limit must govern persisted List preview")
	limit = 100
	require.Len(t, s.publicFuture(ctx, spec, 10), 10, "dynamic limit updates must not be frozen at registration")
	s.publicListProjection(ctx, list, spec)
	require.Len(t, list.FutureActionTimes, 5)
}

func TestPublicProjectionTruncatesOnlyListSpec(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(true)
	schedule, err := s.publicDefinition(ctx)
	require.NoError(t, err)
	for i := range 12 {
		schedule.Spec.Interval = append(schedule.Spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Duration(i+2) * time.Minute)})
		schedule.Spec.StructuredCalendar = append(schedule.Spec.StructuredCalendar, &schedulepb.StructuredCalendarSpec{Minute: []*schedulepb.Range{{Start: int32(i)}}})
		schedule.Spec.ExcludeStructuredCalendar = append(schedule.Spec.ExcludeStructuredCalendar, &schedulepb.StructuredCalendarSpec{Second: []*schedulepb.Range{{Start: int32(i)}}})
	}
	// Publication clones/truncates only compact list data, leaving the complete
	// definition available to Describe and to future-time compilation.
	original := common.CloneProto(schedule.Spec)
	require.NoError(t, s.storePublicDefinition(ctx.(chasm.MutableContext), schedule))
	ctx = chasm.ContextWithValue(ctx, publicListSpecLimitKey{}, func() int { return 3 })
	s.publishPublicList(ctx.(chasm.MutableContext), schedule)
	list := s.PublicList.Get(ctx)
	require.Len(t, list.Spec.Interval, 3)
	require.Len(t, list.Spec.StructuredCalendar, 3)
	require.Len(t, list.Spec.ExcludeStructuredCalendar, 3)
	protorequire.ProtoEqual(t, original, schedule.Spec)
	stored, err := s.DescribePublic(ctx)
	require.NoError(t, err)
	require.Len(t, stored.Schedule.Spec.Interval, 13, "Describe retains the stored complete definition, independent of list publication")
}
