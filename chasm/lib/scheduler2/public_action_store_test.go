package scheduler2

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func indexedProjectionHarness(t *testing.T) *treeHarness {
	t.Helper()
	h := executingPublicTree(t, nil)
	s, raw := h.root(true)
	s.PublicRuntime = chasm.NewDataField(raw.(chasm.MutableContext), &pb.PublicRuntime{})
	s.ProjectionStore = &pb.ProjectionStoreControl{FormatVersion: projectionStoreVersion, NextActionSequence: 1}
	h.commit(true)
	return h
}

func TestPublicActionRecentOrdering(t *testing.T) {
	t.Parallel()
	runtime := &pb.PublicRuntime{}
	// Acknowledgment delivery is reverse admission order; actual-time ties still
	// retain admission order, independently of callback arrival.
	for i := 15; i >= 1; i-- {
		record := &pb.PublicActionRecord{Sequence: uint64(i), Action: &pb.PublicAction{OperationId: fmt.Sprint(i), Result: &schedulepb.ScheduleActionResult{ActualTime: timestamppb.New(time.Unix(int64(i%3), 0)), StartWorkflowResult: &commonpb.WorkflowExecution{RunId: fmt.Sprint(i)}}}}
		updateRecentActions(runtime, record)
	}
	var got []uint64
	for _, a := range runtime.RecentActions {
		got = append(got, a.Sequence)
	}
	require.Equal(t, []uint64{1, 4, 7, 10, 13, 2, 5, 8, 11, 14}, got)
	require.NoError(t, validateRecentActions(runtime.RecentActions, 16))
	// A later observation older than the retained window does not evict a member.
	before := common.CloneProto(runtime)
	updateRecentActions(runtime, &pb.PublicActionRecord{Sequence: 16, Action: &pb.PublicAction{OperationId: "old", Result: &schedulepb.ScheduleActionResult{ActualTime: timestamppb.New(time.Unix(-1, 0)), StartWorkflowResult: &commonpb.WorkflowExecution{RunId: "old"}}}})
	protorequire.ProtoEqual(t, before, runtime)
}

func TestPublicActionRunningProjectionOutputScope(t *testing.T) {
	t.Parallel()
	h := indexedProjectionHarness(t)
	var expected []*commonpb.WorkflowExecution
	// Exercise the storage seam with 25 started records; this fixture tests the
	// output traversal, not reachability of this state under one overlap policy.
	for i := 1; i <= 25; i++ {
		s, raw := h.root(true)
		ctx := raw.(chasm.MutableContext)
		ctx, memoErr := s.reserveMemoVersion(ctx)
		require.NoError(t, memoErr)
		id := fmt.Sprintf("operation-%02d", i)
		execution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: "run"}
		expected = append(expected, execution)
		p := publicTransitionPlan{runtime: common.CloneProto(s.PublicRuntime.Get(ctx)), projection: newProjectionMutation(s.ProjectionStore)}
		p.projection.control.NextActionSequence++
		p.projection.actions[id] = &pb.PublicActionRecord{Sequence: uint64(i), OperationOwned: true, Action: &pb.PublicAction{OperationId: id, Result: &schedulepb.ScheduleActionResult{ActualTime: timestamppb.New(time.Unix(int64(i), 0)), StartWorkflowResult: execution}}}
		records, err := s.prepareOperationRecords(ctx, []operationRecordChange{{id: id, operation: &pb.OperationState{Id: id, Phase: pb.PHASE_STARTED}}})
		require.NoError(t, err)
		p, err = s.finalizePublicTransition(ctx, p, records)
		require.NoError(t, err)
		s.publishOperationRecords(ctx, records)
		s.publishPublicTransition(ctx, p)
		h.commit(true)
	}
	s, ctx := h.root(false)
	running, err := s.runningProjection(ctx)
	require.NoError(t, err)
	protorequire.ProtoSliceEqual(t, expected, running)
	require.EqualValues(t, 25, s.ProjectionStore.RunningCount)
	for _, path := range h.node.MaterializedPathsForTest() {
		require.NotContains(t, path, "ActionRecords#")
		require.NotContains(t, path, "Operations#")
		require.NotContains(t, path, "Requests#")
	}
	// Point corruption rejects the query instead of truncating output.
	h.durable["RunningRecords#000000000000000d"].Data.Data = []byte{0xff}
	h.reload()
	s, ctx = h.root(false)
	require.Panics(t, func() { _, _ = s.runningProjection(ctx) }) // DATA decode boundary
}

func TestPublicActionPreflightDoesNotMutateObservedRecord(t *testing.T) {
	t.Parallel()
	h := indexedProjectionHarness(t)
	id := publicProjectionAdmission(h)
	s, raw := h.root(true)
	ctx := raw.(chasm.MutableContext)
	old, ok := s.ActionRecords.Get(ctx, id)
	require.True(t, ok)
	snapshot := common.CloneProto(old)
	event := decision.Event{Kind: decision.Acknowledged, OperationID: id, Incarnation: s.Incarnation, ChainID: "chain", PublicRunID: "run", StartObserved: true, StartObservedAt: h.now}
	before, err := s.stateFor(ctx, id)
	require.NoError(t, err)
	next, err := decision.Step(before, event)
	require.NoError(t, err)
	plan, err := s.preparePublicTransition(ctx, before, next, event, false)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, snapshot, old)
	require.Equal(t, "run", plan.projection.actions[id].Action.Result.StartWorkflowResult.RunId)
	// An exhausted admission sequence rejects before publication.
	s.ProjectionStore.NextActionSequence = math.MaxUint64
	_, err = s.preparePublicTransition(ctx, before, next, event, true)
	require.Error(t, err)
	protorequire.ProtoEqual(t, snapshot, old)
}

func TestPublicActionInvalidSummaryRejected(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*Scheduler, chasm.MutableContext)
	}{
		{"format", func(s *Scheduler, _ chasm.MutableContext) { s.ProjectionStore.FormatVersion = 99 }},
		{"count", func(s *Scheduler, _ chasm.MutableContext) { s.ProjectionStore.RunningCount = 1 }},
		{"mixed", func(s *Scheduler, ctx chasm.MutableContext) {
			s.PublicRuntime = chasm.NewDataField(ctx, &pb.PublicRuntime{Actions: []*pb.PublicAction{{OperationId: "old"}}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := indexedProjectionHarness(t)
			s, raw := h.root(true)
			ctx := raw.(chasm.MutableContext)
			ctx, memoErr := s.reserveMemoVersion(ctx)
			require.NoError(t, memoErr)
			test.mutate(s, ctx)
			_, err := s.DescribePublic(ctx)
			require.Error(t, err)
			require.Error(t, s.checkPublicMutation(ctx))
			require.Panics(t, func() { s.SearchAttributes(ctx) })
		})
	}
}
