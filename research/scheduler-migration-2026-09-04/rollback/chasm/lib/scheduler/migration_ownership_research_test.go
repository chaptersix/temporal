package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	historyapi "go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type rollbackCloseFailureEngine struct {
	chasm.Engine
	failures int
}

func (e *rollbackCloseFailureEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	update func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) ([]byte, error) {
	if e.failures > 0 {
		e.failures--
		return nil, serviceerror.NewUnavailable("injected source close failure")
	}
	return e.Engine.UpdateComponent(ctx, ref, update, opts...)
}

func newRollbackResearchEnv(t *testing.T) (*schedulerTestEngine, *scheduler.SchedulerMigrateToWorkflowTaskHandler, *historyservicemock.MockHistoryServiceClient) {
	t.Helper()
	ts := clock.NewEventTimeSource().Update(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		_, err := s.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
			NamespaceId: namespaceID,
			ScheduleId:  scheduleID,
			RequestId:   "rollback-operation-1",
		})
		return err
	}))
	client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	handler := scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
		Config:           defaultConfig(),
		MetricsHandler:   metrics.NoopMetricsHandler,
		BaseLogger:       env.logger,
		HistoryClient:    client,
		SaMapperProvider: searchattribute.NewTestMapperProvider(nil),
	})
	return env, handler, client
}

func TestRollbackResearchForeignDestinationMustNotCloseSource(t *testing.T) {
	env, handler, client := newRollbackResearchEnv(t)
	client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewWorkflowExecutionAlreadyStarted("unrelated scheduler owns ID", "foreign-request", "foreign-run"))
	err := handler.Execute(env.engineCtx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{})
	var closed bool
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		closed = s.Closed
		return nil
	}))
	require.Error(t, err, "unrelated destination must fail ownership verification; source closed=%v", closed)
	require.False(t, closed)
}

func TestRollbackResearchCloseRetryKeepsRequestOwnership(t *testing.T) {
	env, handler, client := newRollbackResearchEnv(t)
	engine := &rollbackCloseFailureEngine{Engine: env.engine, failures: 1}
	ctx := chasm.NewEngineContext(context.Background(), engine)
	var requestIDs []string
	client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
			requestIDs = append(requestIDs, req.StartRequest.RequestId)
			if len(requestIDs) == 1 {
				return &historyservice.StartWorkflowExecutionResponse{RunId: "owned-run"}, nil
			}
			return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("running destination", requestIDs[0], "owned-run")
		}).Times(2)
	err := handler.Execute(ctx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{})
	require.ErrorContains(t, err, "injected source close failure")
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed)
		require.NotNil(t, s.WorkflowMigration)
		return nil
	}))
	require.NoError(t, handler.Execute(ctx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{}))
	require.Equal(t, requestIDs[0], requestIDs[1], "retry must reuse the durable migration operation's destination start identity")
}

func TestRollbackResearchNativeSuccessfulHandoff(t *testing.T) {
	env, handler, client := newRollbackResearchEnv(t)
	client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
			var args schedulespb.StartScheduleArgs
			require.NoError(t, sdk.PreferProtoDataConverter.FromPayloads(req.StartRequest.Input, &args))
			require.False(t, args.Schedule.State.Paused)
			require.Empty(t, args.Schedule.State.Notes)
			return &historyservice.StartWorkflowExecutionResponse{RunId: "owned-run"}, nil
		})
	require.NoError(t, handler.Execute(env.engineCtx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{}))
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.True(t, s.Closed)
		require.Nil(t, s.WorkflowMigration)
		return nil
	}))
}

func TestRollbackResearchCloseRetryMustNotRestartCompletedDestination(t *testing.T) {
	for _, status := range []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			env, handler, client := newRollbackResearchEnv(t)
			engine := &rollbackCloseFailureEngine{Engine: env.engine, failures: 1}
			ctx := chasm.NewEngineContext(context.Background(), engine)
			shard := historyi.NewMockShardContext(gomock.NewController(t))
			shard.EXPECT().GetConfig().Return(&configs.Config{
				WorkflowIdReuseMinimalInterval: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(0),
			}).AnyTimes()
			shard.EXPECT().GetTimeSource().Return(env.timeSource).AnyTimes()
			ns := namespacepkg.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: namespace}, nil, "cluster")
			var originalRequestID string
			chainsStarted := 0
			client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
					if originalRequestID == "" {
						originalRequestID = req.StartRequest.RequestId
						chainsStarted++
						return &historyservice.StartWorkflowExecutionResponse{RunId: "owned-run"}, nil
					}
					// StartWorkflow's request-ID dedup runs before its reuse-policy resolver.
					if req.StartRequest.RequestId == originalRequestID {
						return &historyservice.StartWorkflowExecutionResponse{RunId: "owned-run"}, nil
					}
					_, err := historyapi.ResolveDuplicateWorkflowID(
						shard,
						definition.NewWorkflowKey(namespaceID, req.StartRequest.WorkflowId, "owned-run"),
						ns,
						"restarted-run",
						enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
						status,
						map[string]*persistencespb.RequestIDInfo{originalRequestID: {EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}},
						"owned-run",
						req.StartRequest.WorkflowIdReusePolicy,
						req.StartRequest.WorkflowIdConflictPolicy,
						env.timeSource.Now(),
						nil,
						false,
					)
					if err != nil {
						return nil, err
					}
					chainsStarted++
					return &historyservice.StartWorkflowExecutionResponse{RunId: "restarted-run"}, nil
				}).Times(2)
			require.ErrorContains(t, handler.Execute(ctx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{}), "injected source close failure")
			require.NoError(t, handler.Execute(ctx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{}))
			require.Equal(t, 1, chainsStarted, "closing the destination between retries must not resurrect the exported snapshot into a new workflow chain")
		})
	}
}

func TestRollbackResearchNativeStartFailureRetainsSource(t *testing.T) {
	env, handler, client := newRollbackResearchEnv(t)
	client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("injected destination start failure"))
	err := handler.Execute(env.engineCtx, env.rootRef, chasm.TaskAttributes{}, &schedulerpb.SchedulerMigrateToWorkflowTask{})
	require.ErrorContains(t, err, "injected destination start failure")
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed)
		require.NotNil(t, s.WorkflowMigration)
		return nil
	}))
}

func TestRollbackResearchNativeCHASMSentinelExpires(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	logger := log.NewNoopLogger()
	engine, ctx := newTestEngineContext(t, logger, withEngineTimeSource(ts))
	handler := scheduler.NewTestHandler(logger)
	_, err := handler.CreateSentinel(ctx, &schedulerpb.CreateSentinelRequest{
		Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
	})
	require.NoError(t, err)
	req := &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				Namespace: namespace, NamespaceId: namespaceID, ScheduleId: scheduleID,
				Schedule: defaultSchedule(), Info: &schedulepb.ScheduleInfo{},
			},
			GeneratorState: &schedulerpb.GeneratorState{},
			InvokerState:   &schedulerpb.InvokerState{},
		},
	}
	_, err = handler.TestCreateFromMigrationState(ctx, req)
	require.ErrorIs(t, err, scheduler.ErrSentinelBlocked)
	ref := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})
	ts.Advance(scheduler.SentinelIdleTime)
	fired, err := engine.FirePureTasks(ref, ts.Now())
	require.NoError(t, err)
	require.Positive(t, fired)
	_, err = handler.TestCreateFromMigrationState(ctx, req)
	require.NoError(t, err, "a CHASM sentinel's actual idle task releases the ID for migration")
}
