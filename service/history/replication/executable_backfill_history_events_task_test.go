package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
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
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func setupExecutableBackfillHistoryEventsTask(t *testing.T) (*gomock.Controller, *cluster.MockMetadata, *client.MockBean, *shard.MockController, *namespace.MockRegistry, metrics.Handler, log.Logger, *MockExecutableTask, *MockEagerNamespaceRefresher, serialization.Serializer, *persistence.MockExecutionManager, *configs.Config, *replicationspb.ReplicationTask, string, ClusterShardKey, int64, *ExecutableBackfillHistoryEventsTask, []*historypb.HistoryEvent, [][]*historypb.HistoryEvent, []*commonpb.DataBlob, []*historypb.HistoryEvent, string, int64, int64, int64) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	executableTask := NewMockExecutableTask(controller)
	eventSerializer := serialization.NewSerializer()
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)

	firstEventID := int64(10)
	nextEventID := int64(21)
	version := rand.Int63()
	eventsBlob, _ := eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: firstEventID,
		Version: version,
	}})
	events, _ := eventSerializer.DeserializeEvents(eventsBlob)
	eventsBatches := [][]*historypb.HistoryEvent{events}
	newEventsBlob, _ := eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: 1,
		Version: version,
	}})
	newRunEvents, _ := eventSerializer.DeserializeEvents(newEventsBlob)
	newRunID := uuid.NewString()

	eventsBlobs := []*commonpb.DataBlob{eventsBlob}
	taskID := rand.Int63()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
				RunId:       uuid.NewString(),
				EventVersionHistory: []*historyspb.VersionHistoryItem{{
					EventId: nextEventID - 1,
					Version: version,
				}},
				EventBatches: eventsBlobs,
				NewRunInfo: &replicationspb.NewRunInfo{
					RunId:      newRunID,
					EventBatch: newEventsBlob,
				},
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	sourceClusterName := cluster.TestCurrentClusterName
	sourceShardKey := ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	config := tests.NewDynamicConfig()

	taskCreationTime := time.Unix(0, rand.Int63())
	task := NewExecutableBackfillHistoryEventsTask(
		ProcessToolBox{
			ClusterMetadata:         clusterMetadata,
			ClientBean:              clientBean,
			ShardController:         shardController,
			NamespaceCache:          namespaceCache,
			MetricsHandler:          metricsHandler,
			Logger:                  logger,
			EventSerializer:         eventSerializer,
			EagerNamespaceRefresher: eagerNamespaceRefresher,
			DLQWriter:               NewExecutionManagerDLQWriter(mockExecutionManager),
			Config:                  config,
		},
		taskID,
		taskCreationTime,
		sourceClusterName,
		sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = executableTask
	executableTask.EXPECT().TaskID().Return(taskID).AnyTimes()
	executableTask.EXPECT().SourceClusterName().Return(sourceClusterName).AnyTimes()
	executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
	executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()

	return controller, clusterMetadata, clientBean, shardController, namespaceCache, metricsHandler, logger, executableTask, eagerNamespaceRefresher, eventSerializer, mockExecutionManager, config, replicationTask, sourceClusterName, sourceShardKey, taskID, task, events, eventsBatches, eventsBlobs, newRunEvents, newRunID, firstEventID, nextEventID, version
}

func TestExecutableBackfillHistoryEventsTask_Execute_Process(t *testing.T) {
	controller, _, _, shardController, _, _, _, executableTask, _, _, _, _, replicationTask, sourceClusterName, _, _, task, _, eventsBatches, _, newRunEvents, newRunID, _, _, _ := setupExecutableBackfillHistoryEventsTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask)
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

	engine.EXPECT().BackfillHistoryEvents(gomock.Any(), &historyi.BackfillHistoryEventsRequest{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: task.NamespaceID,
			WorkflowID:  task.WorkflowID,
			RunID:       task.RunID,
		},
		SourceClusterName:   sourceClusterName,
		VersionedHistory:    replicationTask.VersionedTransition,
		VersionHistoryItems: replicationTask.GetBackfillHistoryTaskAttributes().EventVersionHistory,
		Events:              eventsBatches,
		NewEvents:           newRunEvents,
		NewRunID:            newRunID,
	}).Return(nil)

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableBackfillHistoryEventsTask_Execute_Skip_TerminalState(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _, _ := setupExecutableBackfillHistoryEventsTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(true)

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableBackfillHistoryEventsTask_Execute_Skip_Namespace(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _, _ := setupExecutableBackfillHistoryEventsTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := task.Execute()
	require.NoError(t, err)
}

func TestExecutableBackfillHistoryEventsTask_Execute_Err(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _, _ := setupExecutableBackfillHistoryEventsTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	require.Equal(t, err, task.Execute())
}

func TestExecutableBackfillHistoryEventsTask_HandleErr_Resend_Success(t *testing.T) {
	controller, _, _, shardController, _, _, _, executableTask, _, _, _, _, replicationTask, sourceClusterName, _, _, task, _, _, _, _, _, firstEventID, nextEventID, version := setupExecutableBackfillHistoryEventsTask(t)
	defer controller.Finish()

	executableTask.EXPECT().TerminalState().Return(false)
	executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask)
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
		firstEventID,
		version,
		nextEventID-1,
		version,
	)
	executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		sourceClusterName,
		definition.NewWorkflowKey(task.NamespaceID, task.WorkflowID, task.RunID),
		firstEventID+1,
		version,
		nextEventID-2,
		version,
		"").Return(nil)
	engine.EXPECT().BackfillHistoryEvents(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, task.HandleErr(err))
}

func TestExecutableBackfillHistoryEventsTask_HandleErr_Resend_Error(t *testing.T) {
	controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, sourceClusterName, _, _, task, _, _, _, _, _, firstEventID, nextEventID, version := setupExecutableBackfillHistoryEventsTask(t)
	defer controller.Finish()

	executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	err := serviceerrors.NewRetryReplication(
		"",
		task.NamespaceID,
		task.WorkflowID,
		task.RunID,
		firstEventID,
		version,
		nextEventID-1,
		version,
	)
	backFillErr := errors.New("OwO")
	executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		sourceClusterName,
		definition.NewWorkflowKey(task.NamespaceID, task.WorkflowID, task.RunID),
		firstEventID+1,
		version,
		nextEventID-2,
		version,
		"").Return(backFillErr)
	actualErr := task.HandleErr(err)

	require.Equal(t, backFillErr, actualErr)
}
