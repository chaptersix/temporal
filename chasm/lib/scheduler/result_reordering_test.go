package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecuteResultsFollowIdentityAcrossBufferMovement(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := timestamppb.New(env.TimeSource.Now())
	first := &schedulespb.BufferedStart{RequestId: "first", WorkflowId: "first-workflow", Attempt: 1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}
	second := &schedulespb.BufferedStart{RequestId: "second", WorkflowId: "second-workflow", Attempt: 1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}
	invoker.BufferedStarts = []*schedulespb.BufferedStart{second, first}
	result := []*schedulespb.BufferedStart{
		{RequestId: "first", RunId: "first-run", StartTime: now},
		{RequestId: "second", RunId: "second-run", StartTime: now},
	}
	started, duplicates, _ := invoker.RecordExecuteResult(ctx, result, nil)
	require.Equal(t, 2, started)
	require.Zero(t, duplicates)
	require.Equal(t, "second-run", invoker.BufferedStarts[0].GetRunId())
	require.Equal(t, "first-run", invoker.BufferedStarts[1].GetRunId())
	started, duplicates, _ = invoker.RecordExecuteResult(ctx, result, nil)
	require.Zero(t, started)
	require.Equal(t, 2, duplicates)
}
