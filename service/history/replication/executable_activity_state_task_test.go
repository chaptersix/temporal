package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupExecutableActivityStateTask(t *testing.T) (*gomock.Controller, *cluster.MockMetadata, *client.MockBean, *shard.MockController, *namespace.MockRegistry, metrics.Handler, log.Logger, *MockExecutableTask, *MockEagerNamespaceRefresher, *persistence.MockExecutionManager, *configs.Config, *replicationspb.SyncActivityTaskAttributes, string, ClusterShardKey, int64, *ExecutableActivityStateTask) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	executableTask := NewMockExecutableTask(controller)
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)
	config := tests.NewDynamicConfig()
	replicationTask := &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        uuid.NewString(),
		WorkflowId:         uuid.NewString(),
		RunId:              uuid.NewString(),
		Version:            rand.Int63(),
		ScheduledEventId:   rand.Int63(),
		ScheduledTime:      timestamppb.New(time.Unix(0, rand.Int63())),
		StartedEventId:     rand.Int63(),
		StartedTime:        timestamppb.New(time.Unix(0, rand.Int63())),
		LastHeartbeatTime:  timestamppb.New(time.Unix(0, rand.Int63())),
		Details:            &commonpb.Payloads{},
		Attempt:            rand.Int31(),
		LastFailure:        &failurepb.Failure{},
		LastWorkerIdentity: uuid.NewString(),
		BaseExecutionInfo:  &workflowspb.BaseExecutionInfo{},
		VersionHistory:     &historyspb.VersionHistory{},
	}
	sourceClusterName := cluster.TestCurrentClusterName
	sourceShardKey := ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	taskID := rand.Int63()
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	task := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: clusterMetadata,
			ClientBean:      clientBean,
			ShardController: shardController,
			NamespaceCache:  namespaceCache,
			MetricsHandler:  metricsHandler,
			Logger:          logger,
			DLQWriter:       NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:          config,
		},
		taskID,
		time.Unix(0, rand.Int63()),
		replicationTask,
		sourceClusterName,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	task.ExecutableTask = executableTask
	executableTask.EXPECT().TaskID().Return(taskID).AnyTimes()
	executableTask.EXPECT().SourceClusterName().Return(sourceClusterName).AnyTimes()
	executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()

	return controller, clusterMetadata, clientBean, shardController, namespaceCache, metricsHandler, logger, executableTask, eagerNamespaceRefresher, mockExecutionManager, config, replicationTask, sourceClusterName, sourceShardKey, taskID, task
}

func generateReplicationAttribute(
	namespaceId string,
	workflowId string,
	runId string,
) *replicationspb.SyncActivityTaskAttributes {
	return &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        namespaceId,
		WorkflowId:         workflowId,
		RunId:              runId,
		Version:            rand.Int63(),
		ScheduledEventId:   rand.Int63(),
		ScheduledTime:      timestamppb.New(time.Unix(0, rand.Int63())),
		StartedEventId:     rand.Int63(),
		StartedTime:        timestamppb.New(time.Unix(0, rand.Int63())),
		LastHeartbeatTime:  timestamppb.New(time.Unix(0, rand.Int63())),
		Details:            &commonpb.Payloads{},
		Attempt:            rand.Int31(),
		LastFailure:        &failurepb.Failure{},
		LastWorkerIdentity: uuid.NewString(),
		BaseExecutionInfo:  &workflowspb.BaseExecutionInfo{},
		VersionHistory:     &historyspb.VersionHistory{},
	}
}

func assertAttributeEqual(
	t *testing.T,
	expected *replicationspb.SyncActivityTaskAttributes,
	actual *historyservice.ActivitySyncInfo,
) {
	require.Equal(t, expected.Version, actual.Version)
	require.Equal(t, expected.ScheduledEventId, actual.ScheduledEventId)
	require.Equal(t, expected.ScheduledTime, actual.ScheduledTime)
	require.Equal(t, expected.StartedEventId, actual.StartedEventId)
	require.Equal(t, expected.StartedTime, actual.StartedTime)
	require.Equal(t, expected.LastHeartbeatTime, actual.LastHeartbeatTime)
	require.Equal(t, expected.Details, actual.Details)
	require.Equal(t, expected.Attempt, actual.Attempt)
	require.Equal(t, expected.LastFailure, actual.LastFailure)
	require.Equal(t, expected.LastWorkerIdentity, actual.LastWorkerIdentity)
	require.Equal(t, expected.VersionHistory, actual.VersionHistory)
}

func TestExecutableActivityStateTask_Execute_Process(t *testing.T) {
	controller, _, _, shardController, _, _, _, executableTask, _, _, _, replicationTask, _, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := historyi.NewMockShardContext(controller)
	engine := historyi.NewMockEngine(controller)
	shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(task.NamespaceID),
		task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        replicationTask.NamespaceId,
		WorkflowId:         replicationTask.WorkflowId,
		RunId:              replicationTask.RunId,
		Version:            replicationTask.Version,
		ScheduledEventId:   replicationTask.ScheduledEventId,
		ScheduledTime:      replicationTask.ScheduledTime,
		StartedEventId:     replicationTask.StartedEventId,
		StartVersion:       replicationTask.StartVersion,
		StartedTime:        replicationTask.StartedTime,
		LastHeartbeatTime:  replicationTask.LastHeartbeatTime,
		Details:            replicationTask.Details,
		Attempt:            replicationTask.Attempt,
		LastFailure:        replicationTask.LastFailure,
		LastWorkerIdentity: replicationTask.LastWorkerIdentity,
		BaseExecutionInfo:  replicationTask.BaseExecutionInfo,
		VersionHistory:     replicationTask.GetVersionHistory(),
	}).Return(nil)

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableActivityStateTask_Execute_Skip_TerminalState(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(true)

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableActivityStateTask_Execute_Skip_Namespace(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableActivityStateTask_Execute_Err(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	err := errors.New("OwO")
	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	require.Equal(t, err, task.Execute())
}

func TestExecutableActivityStateTask_HandleErr_Resend_Success(t *testing.T) {
	controller, _, _, shardController, _, _, _, executableTask, _, _, _, replicationTask, sourceClusterName, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := historyi.NewMockShardContext(controller)
	engine := historyi.NewMockEngine(controller)
	shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(task.NamespaceID),
		task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        replicationTask.NamespaceId,
		WorkflowId:         replicationTask.WorkflowId,
		RunId:              replicationTask.RunId,
		Version:            replicationTask.Version,
		ScheduledEventId:   replicationTask.ScheduledEventId,
		ScheduledTime:      replicationTask.ScheduledTime,
		StartedEventId:     replicationTask.StartedEventId,
		StartVersion:       replicationTask.StartVersion,
		StartedTime:        replicationTask.StartedTime,
		LastHeartbeatTime:  replicationTask.LastHeartbeatTime,
		Details:            replicationTask.Details,
		Attempt:            replicationTask.Attempt,
		LastFailure:        replicationTask.LastFailure,
		LastWorkerIdentity: replicationTask.LastWorkerIdentity,
		BaseExecutionInfo:  replicationTask.BaseExecutionInfo,
		VersionHistory:     replicationTask.GetVersionHistory(),
	}).Return(nil)

	err := serviceerrors.NewRetryReplication(
		"",
		task.NamespaceID,
		task.WorkflowID,
		task.RunID,
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
	)
	executableTask.EXPECT().Resend(gomock.Any(), sourceClusterName, err, ResendAttempt).Return(true, nil)

	require.NoError(t, task.HandleErr(err))
}

func TestExecutableActivityStateTask_HandleErr_Resend_Error(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, sourceClusterName, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	err := serviceerrors.NewRetryReplication(
		"",
		task.NamespaceID,
		task.WorkflowID,
		task.RunID,
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
	)
	executableTask.EXPECT().Resend(gomock.Any(), sourceClusterName, err, ResendAttempt).Return(false, errors.New("OwO"))

	require.Equal(t, err, task.HandleErr(err))
}

func TestExecutableActivityStateTask_HandleErr_Other(t *testing.T) {
	controller, _, _, _, _, _, _, _, _, _, _, _, _, _, _, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	err := errors.New("OwO")
	require.Equal(t, err, task.HandleErr(err))

	err = serviceerror.NewNotFound("")
	require.Nil(t, task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	require.Equal(t, err, task.HandleErr(err))
}

func TestExecutableActivityStateTask_MarkPoisonPill(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, replicationTaskAttr, _, _, taskID, task := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: replicationTaskAttr,
		},
		RawTaskInfo: nil,
	}
	executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	executableTask.EXPECT().MarkPoisonPill().Times(1)

	err := task.MarkPoisonPill()
	require.NoError(t, err)

	require.Equal(t, &persistencespb.ReplicationTaskInfo{
		NamespaceId:      task.NamespaceID,
		WorkflowId:       task.WorkflowID,
		RunId:            task.RunID,
		TaskId:           task.ExecutableTask.TaskID(),
		TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		ScheduledEventId: task.req.ScheduledEventId,
		Version:          task.req.Version,
	}, replicationTask.RawTaskInfo)
}

func TestExecutableActivityStateTask_BatchedTask_ShouldBatchTogether_AndExecute(t *testing.T) {
	controller, clusterMetadata, clientBean, shardController, namespaceCache, metricsHandler, logger, executableTask, _, mockExecutionManager, config, _, sourceClusterName, sourceShardKey, _, _ := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	replicationAttribute1 := generateReplicationAttribute(namespaceId, workflowId, runId)
	config.EnableReplicationTaskBatching = func() bool {
		return true
	}
	task1 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: clusterMetadata,
			ClientBean:      clientBean,
			ShardController: shardController,
			NamespaceCache:  namespaceCache,
			MetricsHandler:  metricsHandler,
			Logger:          logger,
			DLQWriter:       NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:          config,
		},
		1,
		time.Unix(0, rand.Int63()),
		replicationAttribute1,
		sourceClusterName,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	task1.ExecutableTask = executableTask

	replicationAttribute2 := generateReplicationAttribute(namespaceId, workflowId, runId)
	task2 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: clusterMetadata,
			ClientBean:      clientBean,
			ShardController: shardController,
			NamespaceCache:  namespaceCache,
			MetricsHandler:  metricsHandler,
			Logger:          logger,
			DLQWriter:       NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:          config,
		},
		2,
		time.Unix(0, rand.Int63()),
		replicationAttribute2,
		sourceClusterName,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	task2.ExecutableTask = executableTask

	batchResult, batched := task1.BatchWith(task2)
	require.True(t, batched)
	activityTask, _ := batchResult.(*ExecutableActivityStateTask)
	require.Equal(t, 2, len(activityTask.activityInfos))
	assertAttributeEqual(t, replicationAttribute1, activityTask.activityInfos[0])
	assertAttributeEqual(t, replicationAttribute2, activityTask.activityInfos[1])

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), namespaceId).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := historyi.NewMockShardContext(controller)
	engine := historyi.NewMockEngine(controller)
	shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()

	engine.EXPECT().SyncActivities(gomock.Any(), &historyservice.SyncActivitiesRequest{
		NamespaceId:    namespaceId,
		WorkflowId:     workflowId,
		RunId:          runId,
		ActivitiesInfo: activityTask.activityInfos,
	})
	err := batchResult.Execute()
	require.Nil(t, err)
}

func TestExecutableActivityStateTask_BatchWith_InvalidBatchTask_ShouldNotBatch(t *testing.T) {
	controller, clusterMetadata, clientBean, shardController, namespaceCache, metricsHandler, logger, _, _, mockExecutionManager, config, _, sourceClusterName, sourceShardKey, _, _ := setupExecutableActivityStateTask(t)
	defer controller.Finish()

	namespaceId := uuid.NewString()
	runId := uuid.NewString()
	replicationAttribute1 := generateReplicationAttribute(namespaceId, "wf_1", runId)
	task1 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: clusterMetadata,
			ClientBean:      clientBean,
			ShardController: shardController,
			NamespaceCache:  namespaceCache,
			MetricsHandler:  metricsHandler,
			Logger:          logger,
			DLQWriter:       NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:          config,
		},
		1,
		time.Unix(0, rand.Int63()),
		replicationAttribute1,
		sourceClusterName,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)

	replicationAttribute2 := generateReplicationAttribute(namespaceId, "wf_2", runId)
	task2 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: clusterMetadata,
			ClientBean:      clientBean,
			ShardController: shardController,
			NamespaceCache:  namespaceCache,
			MetricsHandler:  metricsHandler,
			Logger:          logger,
			DLQWriter:       NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:          config,
		},
		2,
		time.Unix(0, rand.Int63()),
		replicationAttribute2,
		sourceClusterName,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	batchResult, batched := task1.BatchWith(task2)
	require.False(t, batched)
	require.Nil(t, batchResult)
}

