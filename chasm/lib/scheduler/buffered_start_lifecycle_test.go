package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBufferedStartLifecycleTransitionsEngine(t *testing.T) {
	t.Run("unprocessed to ready", func(t *testing.T) {
		engine, engineCtx, rootRef, logger := newTaskValidityEngine(t, defaultSchedule())
		handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
			Config:         defaultConfig(),
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		})
		now := time.Now()

		var invoker *scheduler.Invoker
		_, _, err := chasm.UpdateComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				invoker = s.Invoker.Get(ctx)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					NominalTime:   timestamppb.New(now),
					ActualTime:    timestamppb.New(now),
					RequestId:     "ready",
					WorkflowId:    "ready-workflow",
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}}
				ctx.AddTask(invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)

		dropped, err := chasmtest.ExecutePureTask(
			context.Background(), engine, invoker, handler,
			chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
		require.NoError(t, err)
		require.False(t, dropped)

		_, err = chasm.ReadComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
				start := s.Invoker.Get(ctx).GetBufferedStarts()[0]
				require.Equal(t, int64(1), start.GetAttempt())
				require.Empty(t, start.GetRunId())
				require.Nil(t, start.GetBackoffTime())
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
	})

	t.Run("ready to retrying", func(t *testing.T) {
		engine, engineCtx, rootRef, handler, frontendClient := newInvokerExecuteEngineEnv(t)
		now := time.Now()

		var invoker *scheduler.Invoker
		_, _, err := chasm.UpdateComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				invoker = s.Invoker.Get(ctx)
				invoker.LastProcessedTime = timestamppb.New(now)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					NominalTime:   timestamppb.New(now),
					ActualTime:    timestamppb.New(now),
					DesiredTime:   timestamppb.New(now),
					RequestId:     "retrying",
					WorkflowId:    "retrying-workflow",
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					Attempt:       1,
				}}
				ctx.AddTask(invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)

		frontendClient.EXPECT().
			StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("retrying")).
			Return(nil, serviceerror.NewDeadlineExceeded("retry"))

		dropped, err := chasmtest.ExecuteSideEffectTask(
			context.Background(), engine, invoker, handler,
			chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
		require.NoError(t, err)
		require.False(t, dropped)

		_, err = chasm.ReadComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
				start := s.Invoker.Get(ctx).GetBufferedStarts()[0]
				require.Equal(t, int64(2), start.GetAttempt())
				require.Empty(t, start.GetRunId())
				require.True(t, start.GetBackoffTime().AsTime().After(now))
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
	})
}
