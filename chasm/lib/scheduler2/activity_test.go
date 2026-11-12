package scheduler2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestActivityExecutionFactory(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 42)
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{HandleExecutionKey: func() chasm.ExecutionKey { return chasm.ExecutionKey{RunID: "allocated"} }, HandleNow: func(chasm.Component) time.Time { return now }}}
	s, err := NewExecution(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, "allocated", s.Incarnation)
	require.True(t, s.ActivitySet)
	require.Equal(t, int64(42), s.ActivityAt)
	now = time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = NewExecution(ctx, 3)
	require.Error(t, err)
	ctx.HandleExecutionKey = func() chasm.ExecutionKey { return chasm.ExecutionKey{} }
	_, err = NewExecution(ctx, 3)
	require.Error(t, err)
}

func TestActivityStartObservationUsesSnapshotOffset(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	h.admit()
	s, cc := h.root(false)
	id := s.OperationId
	wall := time.Unix(0, 100)
	skipped := time.Duration(17)
	handler := NewStartHandler(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		require.True(t, request.GetTimeSkippingConfig().GetEnabled())
		require.Equal(t, 17*time.Nanosecond, request.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration())
		// Distinguish issuance offset, returned wall observation and merge execution time.
		skipped = 99
		wall = time.Unix(0, 200)
		return "chain", nil
	})
	handler.wallNow = func() time.Time { return wall }
	mock := chasm.NewMockEngine(gomock.NewController(t))
	mock.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ chasm.ComponentRef, read func(chasm.Context, chasm.Component) error, _ ...chasm.TransitionOption) error {
		snapshotContext := &commonpb.TimeSkippingStatePropagation{InitialSkippedDuration: durationpb.New(skipped)}
		// The real tree context owns DATA loading; wrap only clock propagation.
		return read(&activityContext{Context: cc, config: &commonpb.TimeSkippingConfig{Enabled: true}, propagation: snapshotContext}, s)
	})
	mock.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, ref chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
		require.Equal(t, "incarnation", ref.RunID)
		h.now = 999
		root, mutable := h.root(true)
		if err := update(mutable.(chasm.MutableContext), root); err != nil {
			return nil, err
		}
		h.commit(true)
		return nil, nil
	})
	require.NoError(t, handler.Execute(chasm.NewEngineContext(t.Context(), mock), chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{RunID: "incarnation"}), chasm.TaskAttributes{}, &scheduler2pb.StartTask{OperationId: id, WakeTime: s.PendingStarts[id]}))
	s, cc = h.root(false)
	require.Equal(t, int64(217), s.ActivityAt)
	require.Equal(t, int64(217), s.Operations[id].Get(cc).StartObservedAt)
	require.Zero(t, s.UnconfirmedStarts)
}

type activityContext struct {
	chasm.Context
	propagation *commonpb.TimeSkippingStatePropagation
	config      *commonpb.TimeSkippingConfig
}

func (c *activityContext) GetTimeSkippingPropagateState() (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	return c.config, c.propagation
}

func TestActivityTimestampRange(t *testing.T) {
	t.Parallel()
	for _, at := range []time.Time{time.Unix(0, 0), time.Unix(0, 1<<63-1), time.Unix(0, -1<<63)} {
		got, err := activityNano(at)
		require.NoError(t, err)
		require.Equal(t, at.UnixNano(), got)
	}
	for _, at := range []time.Time{time.Unix(0, 1<<63-1).Add(time.Nanosecond), time.Unix(0, -1<<63).Add(-time.Nanosecond)} {
		_, err := activityNano(at)
		require.Error(t, err)
	}
}

func TestActivityObservationOverflowRetainsUnconfirmedIntent(t *testing.T) {
	t.Parallel()
	var requests []*workflowservice.StartWorkflowExecutionRequest
	start := func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		requests = append(requests, request)
		return "accepted-chain", nil
	}
	e := newTaskEngine(t, start)
	root, cc := e.h.root(false)
	id := root.OperationId
	before := common.CloneProto(root.RootState)
	beforeOp := common.CloneProto(root.Operations[id].Get(cc))
	handler := NewStartHandler(start)
	handler.wallNow = func() time.Time { return time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC) }
	ref := chasm.NewComponentRef[*Scheduler](e.key)
	task := &scheduler2pb.StartTask{OperationId: id, WakeTime: root.PendingStarts[id]}
	require.Error(t, handler.Execute(e.ctx, ref, chasm.TaskAttributes{}, task))
	root, cc = e.h.root(false)
	protorequire.ProtoEqual(t, before, root.RootState)
	protorequire.ProtoEqual(t, beforeOp, root.Operations[id].Get(cc))
	require.Equal(t, int64(1), root.UnconfirmedStarts)
	// Retry preserves the exact request ID and payload after remote acceptance.
	handler.wallNow = func() time.Time { return time.Unix(0, 500) }
	require.NoError(t, handler.Execute(e.ctx, ref, chasm.TaskAttributes{}, task))
	require.Len(t, requests, 2)
	protorequire.ProtoEqual(t, requests[0], requests[1])
	root, cc = e.h.root(false)
	require.Zero(t, root.UnconfirmedStarts)
	require.Equal(t, int64(500), root.Operations[id].Get(cc).StartObservedAt)
}

func TestActivityStartSnapshotPropagatesExecutionClock(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		config      *commonpb.TimeSkippingConfig
		propagation *commonpb.TimeSkippingStatePropagation
	}{
		{name: "disabled"},
		{name: "enabled", config: &commonpb.TimeSkippingConfig{Enabled: true}, propagation: &commonpb.TimeSkippingStatePropagation{InitialSkippedDuration: durationpb.New(17 * time.Second)}},
		{name: "offset without config", propagation: &commonpb.TimeSkippingStatePropagation{InitialSkippedDuration: durationpb.New(23 * time.Second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTreeHarness(t)
			h.admit()
			s, cc := h.root(false)
			original, err := s.request(cc, s.OperationId)
			require.NoError(t, err)
			snapshot, err := s.startSnapshot(&activityContext{Context: cc, config: tc.config, propagation: tc.propagation}, s.OperationId)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.config, snapshot.request.TimeSkippingConfig)
			protorequire.ProtoEqual(t, tc.propagation, snapshot.request.TimeSkippingStatePropagation)
			require.Equal(t, tc.propagation.GetInitialSkippedDuration().AsDuration(), snapshot.skipped)
			// Issuance must not mutate the stored admitted request, even if the RPC
			// client mutates its request or execution clock state changes afterwards.
			if snapshot.request.TimeSkippingConfig != nil {
				snapshot.request.TimeSkippingConfig.Enabled = false
				require.True(t, tc.config.Enabled)
			}
			if snapshot.request.TimeSkippingStatePropagation != nil {
				snapshot.request.TimeSkippingStatePropagation.InitialSkippedDuration = durationpb.New(time.Hour)
				require.NotEqual(t, time.Hour, tc.propagation.GetInitialSkippedDuration().AsDuration())
			}
			restored, err := s.request(cc, s.OperationId)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, original, restored)
		})
	}
}
