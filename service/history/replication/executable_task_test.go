package replication

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type executableTaskTestDeps struct {
	controller              *gomock.Controller
	clusterMetadata         *cluster.MockMetadata
	clientBean              *client.MockBean
	shardController         *shard.MockController
	namespaceCache          *namespace.MockRegistry
	remoteHistoryFetcher    *eventhandler.MockHistoryPaginatedFetcher
	metricsHandler          metrics.Handler
	logger                  log.Logger
	sourceCluster           string
	sourceShardKey          ClusterShardKey
	eagerNamespaceRefresher *MockEagerNamespaceRefresher
	config                  *configs.Config
	namespaceId             string
	workflowId              string
	runId                   string
	taskId                  int64
	mockExecutionManager    *persistence.MockExecutionManager
	serializer              serialization.Serializer
	resendHandler           *eventhandler.MockResendHandler
	toolBox                 ProcessToolBox
	task                    *ExecutableTaskImpl
}

func setupExecutableTaskTest(t *testing.T) *executableTaskTestDeps {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)
	remoteHistoryFetcher := eventhandler.NewMockHistoryPaginatedFetcher(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	config := tests.NewDynamicConfig()
	serializer := serialization.NewSerializer()
	resendHandler := eventhandler.NewMockResendHandler(controller)

	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	creationTime := time.Unix(0, rand.Int63())
	receivedTime := creationTime.Add(time.Duration(rand.Int63()))
	sourceCluster := cluster.TestAlternativeClusterName
	sourceShardKey := ClusterShardKey{
		ClusterID: int32(cluster.TestAlternativeClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	taskId := rand.Int63()
	toolBox := ProcessToolBox{
		Config:                  config,
		ClusterMetadata:         clusterMetadata,
		ClientBean:              clientBean,
		ResendHandler:           resendHandler,
		ShardController:         shardController,
		NamespaceCache:          namespaceCache,
		MetricsHandler:          metricsHandler,
		Logger:                  logger,
		EagerNamespaceRefresher: eagerNamespaceRefresher,
		DLQWriter:               NewExecutionManagerDLQWriter(mockExecutionManager),
		EventSerializer:         serializer,
		RemoteHistoryFetcher:    remoteHistoryFetcher,
	}

	task := NewExecutableTask(
		toolBox,
		taskId,
		"metrics-tag",
		creationTime,
		receivedTime,
		sourceCluster,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: namespaceId,
				WorkflowId:  workflowId,
				RunId:       runId,
				TaskId:      taskId,
				TaskEquivalents: []*persistencespb.ReplicationTaskInfo{
					{NamespaceId: namespaceId, WorkflowId: workflowId, RunId: runId},
					{NamespaceId: namespaceId, WorkflowId: workflowId, RunId: runId},
				},
			},
		},
	)

	return &executableTaskTestDeps{
		controller:              controller,
		clusterMetadata:         clusterMetadata,
		clientBean:              clientBean,
		shardController:         shardController,
		namespaceCache:          namespaceCache,
		remoteHistoryFetcher:    remoteHistoryFetcher,
		metricsHandler:          metricsHandler,
		logger:                  logger,
		sourceCluster:           sourceCluster,
		sourceShardKey:          sourceShardKey,
		eagerNamespaceRefresher: eagerNamespaceRefresher,
		config:                  config,
		namespaceId:             namespaceId,
		workflowId:              workflowId,
		runId:                   runId,
		taskId:                  taskId,
		mockExecutionManager:    mockExecutionManager,
		serializer:              serializer,
		resendHandler:           resendHandler,
		toolBox:                 toolBox,
		task:                    task,
	}
}

func TestTaskID(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, deps.task.taskID, deps.task.TaskID())
}

func TestCreationTime(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, deps.task.taskCreationTime, deps.task.TaskCreationTime())
}

func TestAckStateAttempt(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, ctasks.TaskStatePending, deps.task.State())
	require.False(t, deps.task.TerminalState())

	deps.task.Ack()
	require.Equal(t, ctasks.TaskStateAcked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	deps.task.Nack(nil)
	require.Equal(t, ctasks.TaskStateAcked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Abort()
	require.Equal(t, ctasks.TaskStateAcked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Cancel()
	require.Equal(t, ctasks.TaskStateAcked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Reschedule()
	require.Equal(t, ctasks.TaskStateAcked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	require.True(t, deps.task.TerminalState())
}

func TestNackStateAttempt(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, ctasks.TaskStatePending, deps.task.State())
	require.False(t, deps.task.TerminalState())

	deps.task.Nack(nil)
	require.Equal(t, ctasks.TaskStateNacked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	deps.task.Ack()
	require.Equal(t, ctasks.TaskStateNacked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Abort()
	require.Equal(t, ctasks.TaskStateNacked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Cancel()
	require.Equal(t, ctasks.TaskStateNacked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Reschedule()
	require.Equal(t, ctasks.TaskStateNacked, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	require.True(t, deps.task.TerminalState())
}

func TestAbortStateAttempt(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, ctasks.TaskStatePending, deps.task.State())
	require.False(t, deps.task.TerminalState())

	deps.task.Abort()
	require.Equal(t, ctasks.TaskStateAborted, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	deps.task.Ack()
	require.Equal(t, ctasks.TaskStateAborted, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Nack(nil)
	require.Equal(t, ctasks.TaskStateAborted, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Cancel()
	require.Equal(t, ctasks.TaskStateAborted, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Reschedule()
	require.Equal(t, ctasks.TaskStateAborted, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	require.True(t, deps.task.TerminalState())
}

func TestCancelStateAttempt(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, ctasks.TaskStatePending, deps.task.State())
	require.False(t, deps.task.TerminalState())

	deps.task.Cancel()
	require.Equal(t, ctasks.TaskStateCancelled, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	deps.task.Ack()
	require.Equal(t, ctasks.TaskStateCancelled, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Nack(nil)
	require.Equal(t, ctasks.TaskStateCancelled, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Abort()
	require.Equal(t, ctasks.TaskStateCancelled, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())
	deps.task.Reschedule()
	require.Equal(t, ctasks.TaskStateCancelled, deps.task.State())
	require.Equal(t, 1, deps.task.Attempt())

	require.True(t, deps.task.TerminalState())
}

func TestRescheduleStateAttempt(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	require.Equal(t, ctasks.TaskStatePending, deps.task.State())
	require.False(t, deps.task.TerminalState())

	deps.task.Reschedule()
	require.Equal(t, ctasks.TaskStatePending, deps.task.State())
	require.Equal(t, 2, deps.task.Attempt())

	require.False(t, deps.task.TerminalState())
}

func TestIsRetryableError(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	err := errors.New("OwO")
	require.True(t, deps.task.IsRetryableError(err))

	err = serviceerror.NewInternal("OwO")
	require.True(t, deps.task.IsRetryableError(err))

	err = serviceerror.NewUnavailable("OwO")
	require.True(t, deps.task.IsRetryableError(err))

	err = serviceerror.NewInvalidArgument("OwO")
	require.False(t, deps.task.IsRetryableError(err))
}

func TestResend_Success(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	deps.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(nil)

	doContinue, err := deps.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	require.NoError(t, err)
	require.True(t, doContinue)
}

func TestResend_NotFound(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	deps.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(serviceerror.NewNotFound(""))
	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: resendErr.NamespaceId,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: resendErr.WorkflowId,
			RunId:      resendErr.RunId,
		},
		ClosedWorkflowOnly: false,
	}).Return(&historyservice.DeleteWorkflowExecutionResponse{}, nil)

	doContinue, err := deps.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	require.NoError(t, err)
	require.False(t, doContinue)
}

func TestResend_ResendError_Success(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	gomock.InOrder(
		deps.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(anotherResendErr),
		deps.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(anotherResendErr.NamespaceId),
			anotherResendErr.WorkflowId,
			anotherResendErr.RunId,
			anotherResendErr.StartEventId,
			anotherResendErr.StartEventVersion,
			anotherResendErr.EndEventId,
			anotherResendErr.EndEventVersion,
		).Return(nil),
		deps.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(nil),
	)

	doContinue, err := deps.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	require.NoError(t, err)
	require.True(t, doContinue)
}

func TestResend_ResendError_Error(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	gomock.InOrder(
		deps.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(anotherResendErr),
		deps.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(anotherResendErr.NamespaceId),
			anotherResendErr.WorkflowId,
			anotherResendErr.RunId,
			anotherResendErr.StartEventId,
			anotherResendErr.StartEventVersion,
			anotherResendErr.EndEventId,
			anotherResendErr.EndEventVersion,
		).Return(&serviceerrors.RetryReplication{}),
	)

	doContinue, err := deps.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	require.Error(t, err)
	require.False(t, doContinue)
}

func TestResend_SecondResendError_SameWorkflowRun(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             resendErr.RunId,
		StartEventId:      resendErr.StartEventId,
		StartEventVersion: resendErr.StartEventVersion,
		EndEventId:        resendErr.EndEventId,
		EndEventVersion:   resendErr.EndEventVersion,
	}

	deps.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(anotherResendErr)

	doContinue, err := deps.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	var dataLossErr *serviceerror.DataLoss
	require.ErrorAs(t, err, &dataLossErr)
	require.False(t, doContinue)
}

func TestResend_Error(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	deps.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(serviceerror.NewUnavailable(""))

	doContinue, err := deps.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	require.Error(t, err)
	require.False(t, doContinue)
}

func TestResend_TransitionHistoryDisabled(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(deps.controller)
	deps.clientBean.EXPECT().GetRemoteAdminClient(deps.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()

	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories:    syncStateErr.VersionHistories,
			TargetClusterId:     int32(deps.clusterMetadata.GetAllClusterInfo()[deps.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(nil, consts.ErrTransitionHistoryDisabled).Times(1)

	mockRemoteAdminClient.EXPECT().AddTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.AddTasksRequest, opts ...grpc.CallOption) (*adminservice.AddTasksResponse, error) {
			require.Equal(t, deps.sourceShardKey.ShardID, request.ShardId)
			require.Len(t, request.Tasks, len(deps.task.replicationTask.GetRawTaskInfo().TaskEquivalents))
			for _, task := range request.Tasks {
				require.Equal(t, tasks.CategoryIDReplication, int(task.CategoryId))
			}
			return nil, nil
		},
	)

	doContinue, err := deps.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	require.Nil(t, err)
	require.False(t, doContinue)
}

func TestSyncState_SourceMutableStateHasUnFlushedBufferEvents(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(deps.controller)
	deps.clientBean.EXPECT().GetRemoteAdminClient(deps.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()

	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories:    syncStateErr.VersionHistories,
			TargetClusterId:     int32(deps.clusterMetadata.GetAllClusterInfo()[deps.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(nil, serviceerror.NewWorkflowNotReady("workflow not ready")).Times(1)

	doContinue, err := deps.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	require.Nil(t, err)
	require.False(t, doContinue)
}

func TestBackFillEvents_Success(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	workflowKey := definition.NewWorkflowKey(
		deps.namespaceId,
		deps.workflowId,
		deps.runId,
	)

	startEventId := int64(20)
	startEventVersion := int64(10)
	endEventId := int64(21)
	endEventVersion := int64(12)
	newRunId := uuid.NewString()
	remoteCluster := "remote cluster"
	eventBatchOriginal1 := []*historypb.HistoryEvent{
		{EventId: 20, Version: 10},
	}
	eventBatchOriginal2 := []*historypb.HistoryEvent{
		{EventId: 21, Version: 12},
	}
	blogOriginal1, err := deps.serializer.SerializeEvents(eventBatchOriginal1)
	require.NoError(t, err)

	blogOriginal2, err := deps.serializer.SerializeEvents(eventBatchOriginal2)
	require.NoError(t, err)
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 20, Version: 10},
			{EventId: 28, Version: 12},
		},
	}
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return []*eventhandler.HistoryBatch{
			{RawEventBatch: blogOriginal1, VersionHistory: versionHistory},
			{RawEventBatch: blogOriginal2, VersionHistory: versionHistory},
		}, nil, nil
	})
	eventBatchNewRun := []*historypb.HistoryEvent{
		{EventId: 1, Version: 12},
		{EventId: 2, Version: 12},
	}
	blobNewRun, err := deps.serializer.SerializeEvents(eventBatchNewRun)
	require.NoError(t, err)
	fetcherNewRun := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return []*eventhandler.HistoryBatch{
			{RawEventBatch: blobNewRun, VersionHistory: &historyspb.VersionHistory{
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 4, Version: 12},
				},
			}},
		}, nil, nil
	})
	deps.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		gomock.Any(),
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		newRunId,
		int64(1),
		endEventVersion,
		int64(1),
		endEventVersion,
	).Return(fetcherNewRun)
	deps.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		gomock.Any(),
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
	).Return(fetcher)
	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().BackfillHistoryEvents(
		gomock.Any(), protomock.Eq(&historyi.BackfillHistoryEventsRequest{
			WorkflowKey:         workflowKey,
			SourceClusterName:   deps.sourceCluster,
			VersionedHistory:    deps.task.replicationTask.VersionedTransition,
			VersionHistoryItems: versionHistory.Items,
			Events:              [][]*historypb.HistoryEvent{eventBatchOriginal1},
		})).Return(nil)
	engine.EXPECT().BackfillHistoryEvents(
		gomock.Any(), protomock.Eq(&historyi.BackfillHistoryEventsRequest{
			WorkflowKey:         workflowKey,
			SourceClusterName:   deps.sourceCluster,
			VersionedHistory:    deps.task.replicationTask.VersionedTransition,
			VersionHistoryItems: versionHistory.Items,
			Events:              [][]*historypb.HistoryEvent{eventBatchOriginal2},
			NewRunID:            newRunId,
			NewEvents:           eventBatchNewRun,
		})).Return(nil)
	task := NewExecutableTask(
		deps.toolBox,
		deps.taskId,
		"metrics-tag",
		time.Now(),
		time.Now(),
		deps.sourceCluster,
		deps.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: deps.namespaceId,
				WorkflowId:  deps.workflowId,
				RunId:       deps.runId,
				TaskId:      deps.taskId,
				TaskEquivalents: []*persistencespb.ReplicationTaskInfo{
					{NamespaceId: deps.namespaceId, WorkflowId: deps.workflowId, RunId: deps.runId},
					{NamespaceId: deps.namespaceId, WorkflowId: deps.workflowId, RunId: deps.runId},
				},
			},
		},
	)
	err = task.BackFillEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
		newRunId,
	)
	require.NoError(t, err)
}

func TestGetNamespaceInfo_Process(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	})
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	name, toProcess, err := deps.task.GetNamespaceInfo(context.Background(), namespaceID)
	require.NoError(t, err)
	require.Equal(t, namespaceName, name)
	require.True(t, toProcess)
}

func TestGetNamespaceInfo_Skip(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
			},
		},
	})
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	name, toProcess, err := deps.task.GetNamespaceInfo(context.Background(), namespaceID)
	require.NoError(t, err)
	require.Equal(t, namespaceName, name)
	require.False(t, toProcess)
}

func TestGetNamespaceInfo_Deleted(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    namespaceID,
			Name:  namespaceName,
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	})
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	name, toProcess, err := deps.task.GetNamespaceInfo(context.Background(), namespaceID)
	require.NoError(t, err)
	require.Equal(t, namespaceName, name)
	require.False(t, toProcess)
}

func TestGetNamespaceInfo_Error(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, errors.New("OwO")).AnyTimes()

	_, _, err := deps.task.GetNamespaceInfo(context.Background(), namespaceID)
	require.Error(t, err)
}

func TestGetNamespaceInfo_NotFoundOnCurrentCluster_SyncFromRemoteSuccess(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	namespaceEntry := namespace.FromPersistentState(
		&persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespaceID,
				Name: namespaceName,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
		})

	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).Times(1)
	deps.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntry, nil)

	name, toProcess, err := deps.task.GetNamespaceInfo(context.Background(), namespaceID)
	require.NoError(t, err)
	require.Equal(t, namespaceName, name)
	require.True(t, toProcess)
}

func TestGetNamespaceInfo_NamespaceFailoverNotSync_SyncFromRemoteSuccess(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	now := time.Now()
	task := NewExecutableTask(
		ProcessToolBox{
			Config:                  deps.config,
			ClusterMetadata:         deps.clusterMetadata,
			ClientBean:              deps.clientBean,
			ShardController:         deps.shardController,
			NamespaceCache:          deps.namespaceCache,
			ResendHandler:           deps.resendHandler,
			MetricsHandler:          deps.metricsHandler,
			Logger:                  deps.logger,
			EagerNamespaceRefresher: deps.eagerNamespaceRefresher,
			DLQWriter:               NoopDLQWriter{},
		},
		rand.Int63(),
		"metrics-tag",
		now,
		now,
		deps.sourceCluster,
		deps.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType:            enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 80},
		},
	)
	namespaceEntryOld := namespace.FromPersistentState(
		&persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespaceID,
				Name: namespaceName,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverVersion: 10,
		})
	namespaceEntryNew := namespace.FromPersistentState(
		&persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   namespaceID,
				Name: namespaceName,
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverVersion: 100,
		})

	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryOld, nil).Times(1)
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryNew, nil).Times(1)
	deps.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntryNew, nil)

	name, toProcess, err := task.GetNamespaceInfo(context.Background(), namespaceID)
	require.NoError(t, err)
	require.Equal(t, namespaceName, name)
	require.True(t, toProcess)
}

func TestGetNamespaceInfo_NamespaceFailoverBehind_StillBehandAfterSyncFromRemote(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	now := time.Now()
	task := NewExecutableTask(
		ProcessToolBox{
			Config:                  deps.config,
			ClusterMetadata:         deps.clusterMetadata,
			ClientBean:              deps.clientBean,
			ShardController:         deps.shardController,
			NamespaceCache:          deps.namespaceCache,
			ResendHandler:           deps.resendHandler,
			MetricsHandler:          deps.metricsHandler,
			Logger:                  deps.logger,
			EagerNamespaceRefresher: deps.eagerNamespaceRefresher,
			DLQWriter:               NoopDLQWriter{},
		},
		rand.Int63(),
		"metrics-tag",
		now,
		now,
		deps.sourceCluster,
		deps.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType:            enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 80},
		},
	)
	namespaceEntryOld := namespace.FromPersistentState(&persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		FailoverVersion: 10,
	})

	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryOld, nil).Times(1)
	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryOld, nil).Times(1)
	deps.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntryOld, nil)

	name, toProcess, err := task.GetNamespaceInfo(context.Background(), namespaceID)
	require.Empty(t, name)
	require.Error(t, err)
	require.False(t, toProcess)
}

func TestGetNamespaceInfo_NotFoundOnCurrentCluster_SyncFromRemoteFailed(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	namespaceID := uuid.NewString()

	deps.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).AnyTimes()
	deps.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		nil, errors.New("some error"))

	_, toProcess, err := deps.task.GetNamespaceInfo(context.Background(), namespaceID)
	require.Nil(t, err)
	require.False(t, toProcess)
}

func TestMarkPoisonPill(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	shardID := rand.Int31()
	shardContext := historyi.NewMockShardContext(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(deps.namespaceId),
		deps.workflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	deps.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: deps.task.sourceClusterName,
		TaskInfo:          deps.task.replicationTask.RawTaskInfo,
	}).Return(nil)

	err := deps.task.MarkPoisonPill()
	require.NoError(t, err)
}

func TestMarkPoisonPill_MaxAttemptsReached(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	deps.task.markPoisonPillAttempts = MarkPoisonPillMaxAttempts - 1
	shardID := rand.Int31()
	shardContext := historyi.NewMockShardContext(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(deps.namespaceId),
		deps.workflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	deps.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: deps.task.sourceClusterName,
		TaskInfo:          deps.task.replicationTask.RawTaskInfo,
	}).Return(serviceerror.NewInternal("failed"))

	err := deps.task.MarkPoisonPill()
	require.Error(t, err)
	err = deps.task.MarkPoisonPill()
	require.NoError(t, err)
}

func TestSyncState(t *testing.T) {
	deps := setupExecutableTaskTest(t)
	defer deps.controller.Finish()

	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					Checksum: &persistencespb.Checksum{
						Value: []byte("test-checksum"),
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(deps.controller)
	deps.clientBean.EXPECT().GetRemoteAdminClient(deps.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories:    syncStateErr.VersionHistories,
			TargetClusterId:     int32(deps.clusterMetadata.GetAllClusterInfo()[deps.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(&adminservice.SyncWorkflowStateResponse{
		VersionedTransitionArtifact: versionedTransitionArtifact,
	}, nil).Times(1)

	shardContext := historyi.NewMockShardContext(deps.controller)
	engine := historyi.NewMockEngine(deps.controller)
	deps.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), versionedTransitionArtifact, deps.sourceCluster).Return(nil)

	doContinue, err := deps.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	require.NoError(t, err)
	require.True(t, doContinue)
}
