package scheduler2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

func TestStartPhysicalWakePreservesLogicalFence(t *testing.T) {
	e := newTaskEngine(t, func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error) {
		return "chain", nil
	})
	root, cc := e.h.root(false)
	physical := e.h.physical[0]
	payload := &scheduler2pb.StartTask{}
	require.NoError(t, proto.Unmarshal(physical.Info.Data.Data, payload))
	wake := root.PendingStarts[root.OperationId]
	require.Equal(t, wake, payload.WakeTime)
	logical := time.Unix(0, wake)
	// task_key_generator rounds upward, and the shard cursor may clamp it later.
	for _, delivered := range []time.Time{logical.Add(common.ScheduledTaskMinPrecision).Truncate(common.ScheduledTaskMinPrecision), logical.Add(time.Hour)} {
		physical.SetVisibilityTime(delivered)
		inTree, valid, err := e.h.node.ValidateSideEffectTask(e.ctx, physical)
		require.NoError(t, err)
		require.True(t, inTree)
		require.True(t, valid)
	}
	// A matching physical timestamp cannot authorize a stale logical wake.
	attrs := chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: logical}}
	handler := &StartHandler{}
	for _, staleWake := range []int64{wake - 1, wake + 1} {
		stale := proto.Clone(payload).(*scheduler2pb.StartTask)
		stale.WakeTime = staleWake
		valid, err := handler.Validate(cc, root, attrs, stale)
		require.NoError(t, err)
		require.False(t, valid)
	}
}

func TestEffectPhysicalWakePreservesLogicalFence(t *testing.T) {
	handler := NewEffectHandler(func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error { return nil }, nil)
	e, _, physical := newEffectsEngine(t, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, handler)
	root, cc := e.h.root(false)
	payload := &scheduler2pb.EffectTask{}
	require.NoError(t, proto.Unmarshal(physical.Info.Data.Data, payload))
	wake := root.Effects[0].RetryAt
	require.Equal(t, wake, payload.WakeTime)
	logical := time.Unix(0, wake)
	for _, delivered := range []time.Time{logical.Add(common.ScheduledTaskMinPrecision).Truncate(common.ScheduledTaskMinPrecision), logical.Add(time.Hour)} {
		physical.SetVisibilityTime(delivered)
		inTree, valid, err := e.h.node.ValidateSideEffectTask(e.ctx, physical)
		require.NoError(t, err)
		require.True(t, inTree)
		require.True(t, valid)
	}
	attrs := chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: logical}}
	for _, staleWake := range []int64{wake - 1, wake + 1} {
		stale := proto.Clone(payload).(*scheduler2pb.EffectTask)
		stale.WakeTime = staleWake
		valid, err := handler.Validate(cc, root, attrs, stale)
		require.NoError(t, err)
		require.False(t, valid)
	}
}
