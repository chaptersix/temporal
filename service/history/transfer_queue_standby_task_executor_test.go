package history

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

type transferQueueStandbyTaskExecutorTestEnv struct {
	controller                          *gomock.Controller
	mockShard                           *shard.ContextTest
	mockNamespaceCache                  *namespace.MockRegistry
	mockClusterMetadata                 *cluster.MockMetadata
	workflowCache                       wcache.Cache
	logger                              log.Logger
	namespaceID                         namespace.ID
	namespace                           namespace.Name
	targetNamespaceID                   namespace.ID
	targetNamespace                     namespace.Name
	version                             int64
	now                                 time.Time
	timeSource                          *clock.EventTimeSource
	transferQueueStandbyTaskExecutor    queues.Executor
	mockExecutionMgr                    *persistence.MockExecutionManager
	mockHistoryClient                   *historyservice.MockHistoryServiceClient
	mockDeleteManager                   *MockDeleteManager
	standbyCluster                      string
	standbyClusterParticipatesInVersion bool
}

func setupTransferQueueStandbyTaskExecutorTest(t *testing.T, standbyClusterParticipatesInVersion bool) *transferQueueStandbyTaskExecutorTestEnv {
	env := &transferQueueStandbyTaskExecutorTestEnv{}

	env.namespaceID = tests.NamespaceID
	env.namespace = tests.Namespace
	env.targetNamespaceID = tests.TargetNamespaceID
	env.targetNamespace = tests.TargetNamespace
	env.version = tests.GlobalNamespaceEntry.FailoverVersion()
	env.now = time.Now().UTC()
	env.timeSource = clock.NewEventTimeSource().Update(env.now)
	env.standbyClusterParticipatesInVersion = standbyClusterParticipatesInVersion
	env.standbyCluster = cluster.TestAlternativeClusterName

	env.controller = gomock.NewController(t)

	config := tests.NewDynamicConfig()
	env.mockShard = shard.NewTestContext(
		env.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)
	env.mockShard.Resource.TimeSource = env.timeSource

	env.mockNamespaceCache = env.mockShard.Resource.NamespaceCache
	env.mockExecutionMgr = env.mockShard.Resource.ExecutionMgr
	env.mockClusterMetadata = env.mockShard.Resource.ClusterMetadata
	env.mockHistoryClient = env.mockShard.Resource.HistoryClient
	env.mockDeleteManager = NewMockDeleteManager(env.controller)

	if env.standbyClusterParticipatesInVersion {
		env.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, env.version).Return(env.standbyCluster).AnyTimes()
	} else {
		env.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, env.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	}
	env.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(env.standbyCluster).AnyTimes()

	env.workflowCache = wcache.NewHostLevelCache(env.mockShard.GetConfig(), env.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	env.logger = env.mockShard.GetLogger()

	env.transferQueueStandbyTaskExecutor = newTransferQueueStandbyTaskExecutor(
		env.mockShard,
		env.workflowCache,
		env.mockDeleteManager,
		nil,
		env.logger,
		metrics.NoopMetricsHandler,
		cluster.TestAlternativeClusterName,
		config,
	)

	return env
}

func (env *transferQueueStandbyTaskExecutorTestEnv) teardown() {
	env.controller.Finish()
	env.mockShard.StopForTest()
}

func TestProcessActivityTask_Pending(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = workflowTaskStartedEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, workflowTaskCompletedEvent.GetEventId(), activityID, activityType, taskQueueName, []byte{}, 1*time.Second, 1*time.Second, 1*time.Second, 0*time.Second)

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
	require.Equal(t, ai, mutableState.GetActivityByActivityID(activityID))
}

func TestProcessActivityTask_Success(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	activityID := "activity-1"
	activityType := "some random activity type"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = workflowTaskStartedEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, workflowTaskCompletedEvent.GetEventId(), activityID, activityType, taskQueueName, []byte{}, 1*time.Second, 1*time.Second, 1*time.Second, 0*time.Second)
	activityStartedEvent := addActivityTaskStartedEvent(mutableState, activityScheduledEvent.GetEventId(), uuid.New())
	addActivityTaskCompletedEvent(mutableState, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), nil, "some random identity")
	mutableState.FlushBufferedEvents()

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    activityScheduledEvent.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, activityStartedEvent.GetEventId(), activityStartedEvent.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessWorkflowTask_Pending(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
	require.Equal(t, wt, mutableState.GetWorkflowTaskByID(wt.ScheduledEventID))
}

func TestProcessWorkflowTask_Success(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCancelExecution_Pending(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), env.targetNamespace, env.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TargetNamespaceID:   env.targetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TargetChildWorkflow: true,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCancelExecution_Success(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), env.targetNamespace, env.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)
	addCancelRequestedEvent(mutableState, event.GetEventId(), env.targetNamespace, env.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	mutableState.FlushBufferedEvents()

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TargetNamespaceID:   env.targetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TargetChildWorkflow: true,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Pending(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := "some random signal control"
	signalHeader := &commonpb.Header{}
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		env.targetNamespace, env.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true, signalName, signalInput,
		signalControl, signalHeader)

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TargetNamespaceID:   env.targetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TargetChildWorkflow: true,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Success(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalControl := "some random signal control"
	signalHeader := &commonpb.Header{}
	event, _ = addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		env.targetNamespace, env.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true, signalName, signalInput,
		signalControl, signalHeader)
	addSignaledEvent(mutableState, event.GetEventId(), env.targetNamespace, env.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), "")
	mutableState.FlushBufferedEvents()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TargetNamespaceID:   env.targetNamespaceID.String(),
		TargetWorkflowID:    targetExecution.GetWorkflowId(),
		TargetRunID:         targetExecution.GetRunId(),
		TargetChildWorkflow: true,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessStartChildExecution_Pending(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		env.targetNamespace,
		env.targetNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TargetNamespaceID:   env.targetNamespaceID.String(),
		TargetWorkflowID:    childWorkflowID,
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessStartChildExecution_Success(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, true)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random child workflow ID",
		RunId:      uuid.New(),
	}
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event, _ = addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		env.targetNamespace,
		env.targetNamespaceID,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)
	addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType, nil)
	mutableState.FlushBufferedEvents()

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
		TargetNamespaceID:   env.targetNamespaceID.String(),
		TargetWorkflowID:    childExecution.GetWorkflowId(),
		InitiatedEventID:    event.GetEventId(),
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestVerifyVersions_InActive(t *testing.T) {
	env := setupTransferQueueStandbyTaskExecutorTest(t, false)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(env.mockShard, env.mockShard.GetEventsCache(), env.logger, env.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: env.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             env.version,
		VisibilityTimestamp: time.Now().UTC().Add(-1 * env.mockShard.GetConfig().StandbyTaskMissingEventsResendDelay(tasks.CategoryTransfer) * 2),
		TaskID:              taskID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(transferTask.NamespaceID)).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	env.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	env.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	resp := env.transferQueueStandbyTaskExecutor.Execute(context.Background(), newTaskExecutableForTransferStandby(env, transferTask))
	require.ErrorContains(t, resp.ExecutionErr, "task version mismatch")
}

func newTaskExecutableForTransferStandby(
	env *transferQueueStandbyTaskExecutorTestEnv,
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		env.transferQueueStandbyTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		env.timeSource,
		env.mockNamespaceCache,
		env.mockClusterMetadata,
		nil,
		queues.GetTaskTypeTagValue,
		nil,
		metrics.NoopMetricsHandler,
		nil,
	)
}
