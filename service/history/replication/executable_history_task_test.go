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
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func setupExecutableHistoryTask(t *testing.T, replicationMultipleBatches bool) (*gomock.Controller, *cluster.MockMetadata, *client.MockBean, *shard.MockController, *namespace.MockRegistry, metrics.Handler, log.Logger, *MockExecutableTask, *MockEagerNamespaceRefresher, serialization.Serializer, *persistence.MockExecutionManager, *eventhandler.MockHistoryEventsHandler, *replicationspb.HistoryTaskAttributes, string, ClusterShardKey, int64, *ExecutableHistoryTask, []*historypb.HistoryEvent, [][]*historypb.HistoryEvent, *commonpb.DataBlob, []*commonpb.DataBlob, []*historypb.HistoryEvent, string, ProcessToolBox) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)
	namespaceCache := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NoopMetricsHandler
	logger := log.NewNoopLogger()
	executableTask := NewMockExecutableTask(controller)
	eagerNamespaceRefresher := NewMockEagerNamespaceRefresher(controller)
	eventSerializer := serialization.NewSerializer()
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	mockEventHandler := eventhandler.NewMockHistoryEventsHandler(controller)
	taskID := rand.Int63()
	processToolBox := ProcessToolBox{
		ClusterMetadata:         clusterMetadata,
		ClientBean:              clientBean,
		ShardController:         shardController,
		NamespaceCache:          namespaceCache,
		MetricsHandler:          metricsHandler,
		Logger:                  logger,
		EagerNamespaceRefresher: eagerNamespaceRefresher,
		EventSerializer:         eventSerializer,
		DLQWriter:               NewExecutionManagerDLQWriter(mockExecutionManager),
		Config:                  tests.NewDynamicConfig(),
		HistoryEventsHandler:    mockEventHandler,
	}
	processToolBox.Config.ReplicationMultipleBatches = dynamicconfig.GetBoolPropertyFn(replicationMultipleBatches)

	firstEventID := rand.Int63()
	nextEventID := firstEventID + 1
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

	var eventsBlobs []*commonpb.DataBlob
	var eventsBlobSingle *commonpb.DataBlob
	if processToolBox.Config.ReplicationMultipleBatches() {
		eventsBlobs = []*commonpb.DataBlob{eventsBlob}
	} else {
		eventsBlobSingle = eventsBlob
	}

	replicationTask := &replicationspb.HistoryTaskAttributes{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		BaseExecutionInfo: &workflowspb.BaseExecutionInfo{},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{
			EventId: nextEventID - 1,
			Version: version,
		}},
		Events:        eventsBlobSingle,
		NewRunEvents:  newEventsBlob,
		NewRunId:      newRunID,
		EventsBatches: eventsBlobs,
	}
	sourceClusterName := cluster.TestCurrentClusterName
	sourceShardKey := ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}

	task := NewExecutableHistoryTask(
		processToolBox,
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

	return controller, clusterMetadata, clientBean, shardController, namespaceCache, metricsHandler, logger, executableTask, eagerNamespaceRefresher, eventSerializer, mockExecutionManager, mockEventHandler, replicationTask, sourceClusterName, sourceShardKey, taskID, task, events, eventsBatches, eventsBlobSingle, eventsBlobs, newRunEvents, newRunID, processToolBox
}

func TestExecutableHistoryTaskSuite(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testExecutableHistoryTask(t, tc.replicationMultipleBatches)
		})
	}
}

func testExecutableHistoryTask(t *testing.T, replicationMultipleBatches bool) {
	t.Run("Execute_Process", func(t *testing.T) {
		controller, _, _, shardController, _, _, _, executableTask, _, _, _, mockEventHandler, _, sourceClusterName, _, _, task, _, eventsBatches, _, _, newRunEvents, newRunID, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
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
		mockEventHandler.EXPECT().HandleHistoryEvents(
		gomock.Any(),
			sourceClusterName,
			definition.NewWorkflowKey(task.NamespaceID, task.WorkflowID, task.RunID),
			task.baseExecutionInfo,
			task.versionHistoryItems,
			eventsBatches,
			newRunEvents,
			newRunID,
	).Return(nil).Times(1)

		err := task.Execute()
		require.NoError(t, err)
	})

	t.Run("Execute_Skip_TerminalState", func(t *testing.T) {
		controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		executableTask.EXPECT().TerminalState().Return(true)

		err := task.Execute()
		require.NoError(t, err)
	})

	t.Run("Execute_Skip_Namespace", func(t *testing.T) {
		controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		executableTask.EXPECT().TerminalState().Return(false)
		executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

		err := task.Execute()
		require.NoError(t, err)
	})

	t.Run("Execute_Err", func(t *testing.T) {
		controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
		executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

		require.Equal(t, err, task.Execute())
	})

	t.Run("HandleErr_Resend_Success", func(t *testing.T) {
		controller, _, _, shardController, _, _, _, executableTask, _, _, _, mockEventHandler, _, sourceClusterName, _, _, task, _, eventsBatches, _, _, newRunEvents, newRunID, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
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
		mockEventHandler.EXPECT().HandleHistoryEvents(
		gomock.Any(),
			sourceClusterName,
			definition.NewWorkflowKey(task.NamespaceID, task.WorkflowID, task.RunID),
			task.baseExecutionInfo,
			task.versionHistoryItems,
			eventsBatches,
			newRunEvents,
			newRunID,
	).Return(nil)

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
	})

	t.Run("HandleErr_Resend_Error", func(t *testing.T) {
		controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, sourceClusterName, _, _, task, _, _, _, _, _, _, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
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
	})

	t.Run("HandleErr_Other", func(t *testing.T) {
		controller, _, _, _, _, _, _, executableTask, _, _, _, _, _, _, _, _, task, _, _, _, _, _, _, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

	err := errors.New("OwO")
		require.Equal(t, err, task.HandleErr(err))

	err = serviceerror.NewNotFound("")
		require.Nil(t, task.HandleErr(err))

	err = consts.ErrDuplicate
		executableTask.EXPECT().MarkTaskDuplicated().Times(1)
		require.Nil(t, task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
		require.Equal(t, err, task.HandleErr(err))
	})

	t.Run("MarkPoisonPill", func(t *testing.T) {
		controller, _, _, shardController, _, _, _, executableTask, _, _, _, _, replicationTaskAttr, _, _, taskID, task, _, _, _, _, _, _, _ := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

	shardID := rand.Int31()
		shardContext := historyi.NewMockShardContext(controller)
		shardController.EXPECT().GetShardByNamespaceWorkflow(
			namespace.ID(task.NamespaceID),
			task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
			SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: replicationTaskAttr,
		},
		RawTaskInfo: nil,
	}
		executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
		executableTask.EXPECT().MarkPoisonPill().Times(1)

		err := task.MarkPoisonPill()
		require.NoError(t, err)
	})

	t.Run("BatchWith_Success", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	})

	t.Run("BatchWith_EventNotConsecutive_BatchFailed", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		currentTask, incomingTask := generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	currentTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 101,
				Version: 3,
			},
			{
				EventId: 102,
				Version: 3,
			},
			{
				EventId: 103,
				Version: 3,
			},
		},
	}
	incomingTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 105,
				Version: 3,
			},
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
		require.False(t, success)
	})

	t.Run("BatchWith_EventVersionNotMatch_BatchFailed", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		currentTask, incomingTask := generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	currentTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 101,
				Version: 3,
			},
			{
				EventId: 102,
				Version: 3,
			},
			{
				EventId: 103,
				Version: 3,
			},
		},
	}
	incomingTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 104,
				Version: 4,
			},
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
		require.False(t, success)
	})

	t.Run("BatchWith_VersionHistoryDoesNotMatch_BatchFailed", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		currentTask, incomingTask := generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	currentTask.versionHistoryItems = []*historyspb.VersionHistoryItem{
		{
			EventId: 108,
			Version: 3,
		},
	}
	incomingTask.versionHistoryItems = []*historyspb.VersionHistoryItem{
		{
			EventId: 108,
			Version: 4,
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
		require.False(t, success)
	})

	t.Run("BatchWith_WorkflowKeyDoesNotMatch_BatchFailed", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		currentTask, incomingTask := generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	currentTask.WorkflowID = "1"
	incomingTask.WorkflowID = "2"
	_, success := currentTask.BatchWith(incomingTask)
		require.False(t, success)
	})

	t.Run("BatchWith_CurrentTaskHasNewRunEvents_BatchFailed", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		currentTask, incomingTask := generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	currentTask.eventsDesResponse.newRunEvents = []*historypb.HistoryEvent{
		{
			EventId: 104,
			Version: 3,
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
		require.False(t, success)
	})

	t.Run("BatchWith_IncomingTaskHasNewRunEvents_BatchSuccess", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, eventSerializer, _, _, _, sourceClusterName, sourceShardKey, _, _, _, _, _, _, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		currentTask, incomingTask := generateTwoBatchableTasks(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox)
	incomingTask.newRunID = uuid.NewString()
	incomingTask.eventsDesResponse.newRunEvents = []*historypb.HistoryEvent{
		{
			EventId: 104,
			Version: 3,
		},
	}
	batchedTask, success := currentTask.BatchWith(incomingTask)
		require.True(t, success)
		require.Equal(t, incomingTask.newRunID, batchedTask.(*ExecutableHistoryTask).newRunID)
	})

	t.Run("NewExecutableHistoryTask", func(t *testing.T) {
		controller, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, task, _, _, eventsBlobSingle, eventsBlobs, _, _, processToolBox := setupExecutableHistoryTask(t, replicationMultipleBatches)
		defer controller.Finish()

		if processToolBox.Config.ReplicationMultipleBatches() {
			require.Equal(t, eventsBlobs, task.eventsBlobs)
	} else {
			require.Equal(t, 1, len(task.eventsBlobs))
			require.Equal(t, eventsBlobSingle, task.eventsBlobs[0])
	}
	})
}

func generateTwoBatchableTasks(t *testing.T, controller *gomock.Controller, eventSerializer serialization.Serializer, sourceClusterName string, sourceShardKey ClusterShardKey, processToolBox ProcessToolBox) (*ExecutableHistoryTask, *ExecutableHistoryTask) {
	currentEvent := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 101,
				Version: 3,
			},
			{
				EventId: 102,
				Version: 3,
			},
			{
				EventId: 103,
				Version: 3,
			},
		},
	}
	incomingEvent := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 104,
				Version: 3,
			},
			{
				EventId: 105,
				Version: 3,
			},
			{
				EventId: 106,
				Version: 3,
			},
		},
	}
	currentVersionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: 102,
			Version: 3,
		},
	}
	incomingVersionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: 108,
			Version: 3,
		},
	}
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	workflowKeyCurrent := definition.NewWorkflowKey(namespaceId, workflowId, runId)
	workflowKeyIncoming := definition.NewWorkflowKey(namespaceId, workflowId, runId)
	sourceTaskId := int64(111)
	incomingTaskId := int64(120)
	currentTask := buildExecutableHistoryTask(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox, currentEvent, nil, "", sourceTaskId, currentVersionHistoryItems, workflowKeyCurrent)
	incomingTask := buildExecutableHistoryTask(t, controller, eventSerializer, sourceClusterName, sourceShardKey, processToolBox, incomingEvent, nil, "", incomingTaskId, incomingVersionHistoryItems, workflowKeyIncoming)

	resultTask, batched := currentTask.BatchWith(incomingTask)

	// following assert are used for testing happy case, do not delete
	require.True(t, batched)

	resultHistoryTask, _ := resultTask.(*ExecutableHistoryTask)
	require.NotNil(t, resultHistoryTask)

	require.Equal(t, sourceTaskId, resultHistoryTask.TaskID())
	require.Equal(t, incomingVersionHistoryItems, resultHistoryTask.versionHistoryItems)
	expectedBatchedEvents := append(currentEvent, incomingEvent...)

	require.Equal(t, len(resultHistoryTask.eventsDesResponse.events), len(expectedBatchedEvents))
	for i := range expectedBatchedEvents {
		protorequire.ProtoSliceEqual(t, expectedBatchedEvents[i], resultHistoryTask.eventsDesResponse.events[i])
	}
	require.Nil(t, resultHistoryTask.eventsDesResponse.newRunEvents)
	return currentTask, incomingTask
}

func buildExecutableHistoryTask(
	t *testing.T,
	controller *gomock.Controller,
	eventSerializer serialization.Serializer,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	processToolBox ProcessToolBox,
	events [][]*historypb.HistoryEvent,
	newRunEvents []*historypb.HistoryEvent,
	newRunID string,
	taskId int64,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	workflowKey definition.WorkflowKey,
) *ExecutableHistoryTask {
	eventsBlob, _ := eventSerializer.SerializeEvents(events[0])
	newRunEventsBlob, _ := eventSerializer.SerializeEvents(newRunEvents)
	replicationTaskAttribute := &replicationspb.HistoryTaskAttributes{
		WorkflowId:          workflowKey.WorkflowID,
		NamespaceId:         workflowKey.NamespaceID,
		RunId:               workflowKey.RunID,
		BaseExecutionInfo:   &workflowspb.BaseExecutionInfo{},
		VersionHistoryItems: versionHistoryItems,
		Events:              eventsBlob,
		NewRunEvents:        newRunEventsBlob,
		NewRunId:            newRunID,
	}
	executableTask := NewMockExecutableTask(controller)
	executableTask.EXPECT().TaskID().Return(taskId).AnyTimes()
	executableTask.EXPECT().SourceClusterName().Return(sourceClusterName).AnyTimes()
	executableHistoryTask := NewExecutableHistoryTask(
		processToolBox,
		taskId,
		time.Unix(0, rand.Int63()),
		replicationTaskAttribute,
		sourceClusterName,
		sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	executableHistoryTask.ExecutableTask = executableTask
	return executableHistoryTask
}
