package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublicCommandCapacityCombinedPreflightAndRollback(t *testing.T) {
	t.Parallel()
	request := &schedulepb.BackfillRequest{StartTime: timestamppb.New(generationStart()), EndTime: timestamppb.New(generationStart().Add(time.Second))}
	patch := &schedulepb.SchedulePatch{}
	for range 99 {
		patch.BackfillRequest = append(patch.BackfillRequest, common.CloneProto(request))
	}
	h := executingPublicTree(t, patch)
	s, ctx := h.root(true)
	before := proto.Clone(s.RootState)
	err := s.PatchPublic(ctx.(chasm.MutableContext), &schedulepb.SchedulePatch{Pause: "must not apply", TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}, BackfillRequest: []*schedulepb.BackfillRequest{request}})
	var capacity *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "too many concurrent backfillers", err.Error())
	require.True(t, proto.Equal(before, s.RootState), "whole patch must reject before pause or ingress publication")
	h.reload()
	s, ctx = h.root(true)
	durable := cloneNodes(h.durable)
	require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}))
	h.commit(false)
	require.Equal(t, durable, h.durable)
	s, ctx = h.root(true)
	require.Len(t, s.Backfills, 99)
	require.Empty(t, s.Manuals)
	require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}))
	h.commit(true)
	s, ctx = h.root(true)
	require.Len(t, s.Backfills, 99)
	require.Len(t, s.Manuals, 1)
	before = proto.Clone(s.RootState)
	require.ErrorAs(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}), &capacity)
	require.ErrorAs(t, s.AcceptBackfill(ctx.(chasm.MutableContext), request), &capacity)
	require.True(t, proto.Equal(before, s.RootState))
}

func TestPublicManualIngressRetainsHundredAcrossReload(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(true)
	for range 100 {
		require.NoError(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}))
	}
	h.commit(true)
	s, ctx = h.root(true)
	require.Len(t, s.Manuals, 100)
	var capacity *serviceerror.FailedPrecondition
	require.ErrorAs(t, s.AcceptManual(ctx.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{}), &capacity)
	require.Zero(t, s.queueLength())
	require.Zero(t, s.Unfinished)
}
