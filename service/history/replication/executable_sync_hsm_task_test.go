package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
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

func setupExecutableSyncHSMTask(t *testing.T) (*gomock.Controller, *cluster.MockMetadata, *client.MockBean, *shard.MockController, *namespace.MockRegistry, metrics.Handler, log.Logger, *MockExecutableTask, *MockEagerNamespaceRefresher, *persistence.MockExecutionManager, *configs.Config, *replicationspb.SyncHSMAttributes, string, ClusterShardKey, int64, *ExecutableSyncHSMTask) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	executableTask := NewMockExecutableTask(controller)
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)

	replicationTask := &replicationspb.SyncHSMAttributes{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		VersionHistory: &historyspb.VersionHistory{
			BranchToken: []byte("branch token 2"),
			Items: []*historyspb.VersionHistoryItem{
				{EventId: 5, Version: 10},
				{EventId: 10, Version: 20},
			},
		},
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				"test": {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"machine1": {
							Data: []byte("machine1 data"),
						},
						"machine2": {
							Data: []byte("machine1 data"),
						},
					},
				},
			},
		},
	}
	sourceClusterName := cluster.TestCurrentClusterName
	sourceShardKey := ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	config := tests.NewDynamicConfig()

	taskID := rand.Int63()
	taskCreationTime := time.Unix(0, rand.Int63())
	task := NewExecutableSyncHSMTask(
		ProcessToolBox{
			ClusterMetadata:         clusterMetadata,
			ClientBean:              clientBean,
			ShardController:         shardController,
			NamespaceCache:          namespaceCache,
			MetricsHandler:          metricsHandler,
			Logger:                  logger,
			EagerNamespaceRefresher: eagerNamespaceRefresher,
			DLQWriter:               NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:                  config,
		},
		taskID,
		taskCreationTime,
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
	executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
	executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()

	return controller, clusterMetadata, clientBean, shardController, namespaceCache, metricsHandler, logger, executableTask, eagerNamespaceRefresher, mockExecutionManager, config, replicationTask, sourceClusterName, sourceShardKey, taskID, task
}

func TestExecutableSyncHSMTask_Execute_Process(t *testing.T) {
	controller, _, _, shardController, _, _, _, executableTask, _, _, _, replicationTaskAttr, _, _, _, task := setupExecutableSyncHSMTask(t)
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
	engine.EXPECT().SyncHSM(gomock.Any(), &historyi.SyncHSMRequest{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: task.NamespaceID,
			WorkflowID:  task.WorkflowID,
			RunID:       task.RunID,
		},
		StateMachineNode:    replicationTaskAttr.GetStateMachineNode(),
		EventVersionHistory: replicationTaskAttr.GetVersionHistory(),
	}).Return(nil)

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableSyncHSMTask_Execute_Skip_TerminalState(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, task := setupExecutableSyncHSMTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(true)

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableSyncHSMTask_Execute_Skip_Namespace(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, task := setupExecutableSyncHSMTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableSyncHSMTask_Execute_Err(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, task := setupExecutableSyncHSMTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	require.Equal(t, err, task.Execute())
}

func TestExecutableSyncHSMTask_HandleErr_Resend_Success(t *testing.T) {
	controller, _, _, shardController, _, _, _, executableTask, _, _, _, _, sourceClusterName, _, _, task := setupExecutableSyncHSMTask(t)
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
	engine.EXPECT().SyncHSM(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, task.HandleErr(err))
}

func TestExecutableSyncHSMTask_HandleErr_Resend_Error(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, sourceClusterName, _, _, task := setupExecutableSyncHSMTask(t)
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

func TestExecutableSyncHSMTask_MarkPoisonPill(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, replicationTaskAttr, _, _, taskID, task := setupExecutableSyncHSMTask(t)
	defer controller.Finish()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{
			SyncHsmAttributes: replicationTaskAttr,
		},
		RawTaskInfo: nil,
	}
	executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	executableTask.EXPECT().MarkPoisonPill().Times(1)

	err := task.MarkPoisonPill()
	require.NoError(t, err)

	require.Equal(t, &persistencespb.ReplicationTaskInfo{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.ExecutableTask.TaskID(),
		TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
		VisibilityTime: timestamppb.New(task.TaskCreationTime()),
	}, replicationTask.RawTaskInfo)
}
