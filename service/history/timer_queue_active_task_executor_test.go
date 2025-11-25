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
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	pm "go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/dummy"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type timerQueueActiveTaskExecutorTestDeps struct {
	controller              *gomock.Controller
	mockShard               *shard.ContextTest
	mockTxProcessor         *queues.MockQueue
	mockTimerProcessor      *queues.MockQueue
	mockVisibilityProcessor *queues.MockQueue
	mockArchivalProcessor   *queues.MockQueue
	mockNamespaceCache      *namespace.MockRegistry
	mockMatchingClient      *matchingservicemock.MockMatchingServiceClient
	mockClusterMetadata     *cluster.MockMetadata
	mockChasmEngine         *chasm.MockEngine

	mockHistoryEngine *historyEngineImpl
	mockDeleteManager *deletemanager.MockDeleteManager
	mockExecutionMgr  *persistence.MockExecutionManager

	config                       *configs.Config
	workflowCache                wcache.Cache
	logger                       log.Logger
	namespaceID                  namespace.ID
	namespaceEntry               *namespace.Namespace
	version                      int64
	now                          time.Time
	timeSource                   *clock.EventTimeSource
	timerQueueActiveTaskExecutor *timerQueueActiveTaskExecutor
}





func setupTimerQueueActiveTaskExecutorTest(t *testing.T) *timerQueueActiveTaskExecutorTestDeps {
	d := &timerQueueActiveTaskExecutorTestDeps{}

	d.namespaceID = tests.NamespaceID
	d.namespaceEntry = tests.GlobalNamespaceEntry
	d.version = d.namespaceEntry.FailoverVersion()
	d.now = time.Now().UTC()
	d.timeSource = clock.NewEventTimeSource().Update(d.now)

	d.controller = gomock.NewController(t)
	d.mockTxProcessor = queues.NewMockQueue(d.controller)
	d.mockTimerProcessor = queues.NewMockQueue(d.controller)
	d.mockVisibilityProcessor = queues.NewMockQueue(d.controller)
	d.mockArchivalProcessor = queues.NewMockQueue(d.controller)
	d.mockChasmEngine = chasm.NewMockEngine(d.controller)
	d.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	d.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	d.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	d.mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	d.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	d.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	d.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	d.mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	d.config = tests.NewDynamicConfig()
	d.mockShard = shard.NewTestContextWithTimeSource(
		d.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		d.config,
		d.timeSource,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	d.mockShard.SetStateMachineRegistry(reg)

	d.mockShard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		d.mockShard.GetExecutionManager(),
		d.mockShard.GetConfig(),
		d.mockShard.GetMetricsHandler(),
		d.mockShard.GetLogger(),
		false,
	))

	d.mockNamespaceCache = d.mockShard.Resource.NamespaceCache
	d.mockMatchingClient = d.mockShard.Resource.MatchingClient
	d.mockExecutionMgr = d.mockShard.Resource.ExecutionMgr
	d.mockClusterMetadata = d.mockShard.Resource.ClusterMetadata
	// ack manager will use the namespace information
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	d.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	d.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(d.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()
	d.workflowCache = wcache.NewHostLevelCache(d.mockShard.GetConfig(), d.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	d.logger = d.mockShard.GetLogger()

	d.mockDeleteManager = deletemanager.NewMockDeleteManager(d.controller)
	h := &historyEngineImpl{
		currentClusterName: d.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       d.mockShard,
		clusterMetadata:    d.mockClusterMetadata,
		executionManager:   d.mockExecutionMgr,
		logger:             d.logger,
		tokenSerializer:    tasktoken.NewSerializer(),
		metricsHandler:     d.mockShard.GetMetricsHandler(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			d.mockTxProcessor.Category():         d.mockTxProcessor,
			d.mockTimerProcessor.Category():      d.mockTimerProcessor,
			d.mockVisibilityProcessor.Category(): d.mockVisibilityProcessor,
			d.mockArchivalProcessor.Category():   d.mockArchivalProcessor,
		},
	}
	d.mockShard.SetEngineForTesting(h)
	d.mockHistoryEngine = h

	d.timerQueueActiveTaskExecutor = newTimerQueueActiveTaskExecutor(
		d.mockShard,
		d.workflowCache,
		d.mockDeleteManager,
		d.logger,
		metrics.NoopMetricsHandler,
		d.config,
		d.mockShard.Resource.GetMatchingClient(),
		d.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)
	return d
}



func TestProcessUserTimerTimeout_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
		d.mockShard,
		d.mockShard.GetEventsCache(),
		d.logger,
		d.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.UserTimerTask{
		WorkflowKey:         workflowKey,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())

	for _, currentTime := range []time.Time{
		d.now.Add(-timerTimeout),
		d.now.Add(2 * timerTimeout),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		d.timeSource.Update(currentTime)
		resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
		require.NoError(t, resp.ExecutionErr)

		_, ok := d.getMutableStateFromCache(workflowKey).GetUserTimerInfo(timerID)
		require.False(t, ok)

		d.clearMutableStateFromCache(workflowKey)
	}
}

func TestProcessUserTimerTimeout_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
		d.mockShard,
		d.mockShard.GetEventsCache(),
		d.logger,
		d.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	event = addTimerFiredEvent(mutableState, timerID)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, errNoTimerFired)
}

func TestProcessUserTimerTimeout_WfClosed(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
		d.mockShard,
		d.mockShard.GetEventsCache(),
		d.logger,
		d.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.UserTimerTask).VisibilityTimestamp,
		EventID:             event.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	persistenceMutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrWorkflowCompleted)
}

func TestProcessUserTimerTimeout_NoTimerAndWfClosed(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
		d.mockShard,
		d.mockShard.GetEventsCache(),
		d.logger,
		d.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: time.Now(),
		EventID:             event.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	persistenceMutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, errNoTimerFired)
}

func TestProcessActivityTimeout_NoRetryPolicy_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(200 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
	)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey:         workflowKey,
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())

	for _, currentTime := range []time.Time{
		d.now.Add(-timerTimeout),
		d.now.Add(2 * timerTimeout),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		d.timeSource.Update(currentTime)
		resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
		require.NoError(t, resp.ExecutionErr)

		_, ok := d.getMutableStateFromCache(workflowKey).GetActivityInfo(scheduledEvent.GetEventId())
		require.False(t, ok)

		d.clearMutableStateFromCache(workflowKey)
	}
}

func TestProcessActivityTimeout_NoRetryPolicy_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(200 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessActivityTimeout_RetryPolicy_Retry(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		999*time.Second,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	require.Nil(t, startedEvent)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey:         workflowKey,
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	activityInfo, ok := d.getMutableStateFromCache(workflowKey).GetActivityInfo(scheduledEvent.GetEventId())
	require.True(t, ok)
	require.Equal(t, scheduledEvent.GetEventId(), activityInfo.ScheduledEventId)
	require.Equal(t, common.EmptyEventID, activityInfo.StartedEventId)
	// only a schedule to start timer will be created, apart from the retry timer
	require.Equal(t, int32(workflow.TimerTaskStatusCreatedScheduleToStart), activityInfo.TimerTaskStatus)
}

func TestProcessActivityTimeout_RetryPolicy_RetryTimeout(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	require.Nil(t, startedEvent)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			ctx context.Context,
			request *persistence.UpdateWorkflowExecutionRequest,
		) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.Len(t, request.UpdateWorkflowEvents, 1)
			// activityStarted (since activity has retry policy), activityTimedOut, workflowTaskScheduled
			require.Len(t, request.UpdateWorkflowEvents[0].Events, 3)

			timeoutEvent := request.UpdateWorkflowEvents[0].Events[1]
			require.Equal(t, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, timeoutEvent.GetEventType())
			timeoutAttributes := timeoutEvent.GetActivityTaskTimedOutEventAttributes()
			require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutAttributes.Failure.GetTimeoutFailureInfo().GetTimeoutType())
			require.Contains(t, timeoutAttributes.Failure.Message, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String())
			require.Equal(t, enumspb.RETRY_STATE_TIMEOUT, timeoutAttributes.RetryState)
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessActivityTimeout_RetryPolicy_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey:         workflowKey,
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	_, ok := d.getMutableStateFromCache(workflowKey).GetActivityInfo(scheduledEvent.GetEventId())
	require.False(t, ok)
}

func TestProcessActivityTimeout_RetryPolicy_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	require.Nil(t, startedEvent)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             wt.ScheduledEventID,
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), common.TransientEventID, nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessActivityTimeout_Heartbeat_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	heartbeatTimerTimeout := time.Second
	scheduledEvent, _ := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		heartbeatTimerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	require.Nil(t, startedEvent)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]
	require.Equal(t, enumspb.TIMEOUT_TYPE_HEARTBEAT, task.(*tasks.ActivityTimeoutTask).TimeoutType)

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: time.Time{},
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestWorkflowTaskTimeout_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey:         workflowKey,
		ScheduleAttempt:     1,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: d.now,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	workflowTask := d.getMutableStateFromCache(workflowKey).GetPendingWorkflowTask()
	require.NotNil(t, workflowTask)
	require.True(t, workflowTask.ScheduledEventID != common.EmptyEventID)
	require.Equal(t, common.EmptyEventID, workflowTask.StartedEventID)
	require.Equal(t, int32(2), workflowTask.Attempt)
}

func TestWorkflowTaskTimeout_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: d.now,
		EventID:             wt.ScheduledEventID - 1,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestWorkflowTaskTimeout_StampMismatch(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startedEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())

	// Create timer task with original stamp
	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: d.now,
		EventID:             wt.ScheduledEventID,
		Stamp:               wt.Stamp,
	}

	// Modify the workflow task stamp in mutable state to create mismatch
	mutableState.GetExecutionInfo().WorkflowTaskStamp = wt.Stamp + 1

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrStaleReference)
}

func TestWorkflowBackoffTimer_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey:         workflowKey,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTimestamp: d.now,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	workflowTask := d.getMutableStateFromCache(workflowKey).GetPendingWorkflowTask()
	require.NotNil(t, workflowTask)
	require.True(t, workflowTask.ScheduledEventID != common.EmptyEventID)
	require.Equal(t, common.EmptyEventID, workflowTask.StartedEventID)
	require.Equal(t, int32(1), workflowTask.Attempt)
}

func TestWorkflowBackoffTimer_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
		VisibilityTimestamp: d.now,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, errNoTimerFired)
}

func TestActivityRetryTimer_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskQueueName,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	activityInfo.Attempt = 1

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		pm.Eq(&matchingservice.AddActivityTaskRequest{
			NamespaceId: d.namespaceID.String(),
			Execution:   execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: activityInfo.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduledEventId:       activityInfo.ScheduledEventId,
			ScheduleToStartTimeout: activityInfo.ScheduleToStartTimeout,
			Clock:                  vclock.NewVectorClock(d.mockClusterMetadata.GetClusterID(), d.mockShard.GetShardID(), timerTask.TaskID),
			VersionDirective:       worker_versioning.MakeUseAssignmentRulesDirective(),
		}),
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestActivityRetryTimer_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	identity := "identity"
	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := addActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		taskqueue,
		nil,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		timerTimeout,
		&commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			BackoffCoefficient:     1.2,
			MaximumInterval:        durationpb.New(5 * time.Second),
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"（╯' - ')╯ ┻━┻ "},
		},
	)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	require.Nil(t, startedEvent)

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrActivityTaskNotFound)
}

func TestWorkflowRunTimeout_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	expirationTime := 10 * time.Second

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(d.now.Add(expirationTime)),
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now.Add(expirationTime),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())

	for _, currrentTime := range []time.Time{
		d.now.Add(expirationTime - 1*time.Second),
		d.now.Add(expirationTime + 1*time.Second),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		d.timeSource.Update(currrentTime)
		resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
		require.NoError(t, resp.ExecutionErr)

		running := d.getMutableStateFromCache(workflowKey).IsWorkflowExecutionRunning()
		require.False(t, running)

		d.clearMutableStateFromCache(workflowKey)
	}
}

func TestWorkflowRunTimeout_Retry(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	executionRunTimeout := time.Duration(10 * time.Second)
	workflowRunTimeout := time.Duration(200 * time.Second)

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(workflowRunTimeout),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(d.now.Add(executionRunTimeout)),
		},
	)
	require.NoError(t, err)
	// need to override the workflow retry policy
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.HasRetryPolicy = true
	executionInfo.WorkflowExecutionExpirationTime = timestamp.TimeNowPtrUtcAddSeconds(1000)
	executionInfo.RetryMaximumAttempts = 10
	executionInfo.RetryInitialInterval = timestamp.DurationFromSeconds(1)
	executionInfo.RetryMaximumInterval = timestamp.DurationFromSeconds(1)
	executionInfo.RetryBackoffCoefficient = 1

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// move
	d.timeSource.Advance(workflowRunTimeout + 1*time.Second)
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	state, status := d.getMutableStateFromCache(workflowKey).GetWorkflowStateStatus()
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	d.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func TestWorkflowRunTimeout_Cron(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	expirationTime := time.Duration(10 * time.Second)

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(d.now.Add(expirationTime)),
		},
	)
	require.NoError(t, err)
	mutableState.GetExecutionState().StartTime = timestamppb.New(d.now)
	mutableState.GetExecutionInfo().CronSchedule = "* * * * *"

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// advance timer past run expiration time
	d.timeSource.Advance(expirationTime + 1*time.Second)
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	state, status := d.getMutableStateFromCache(workflowKey).GetWorkflowStateStatus()
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	d.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func TestWorkflowRunTimeout_WorkflowExpired(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(d.now.Add(-1 * time.Second)),
		},
	)
	require.NoError(t, err)
	mutableState.GetExecutionState().StartTime = timestamppb.New(d.now)
	mutableState.GetExecutionInfo().CronSchedule = "* * * * *"

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey:         workflowKey,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)

	state, status := d.getMutableStateFromCache(workflowKey).GetWorkflowStateStatus()
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
	d.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)
}

func TestWorkflowExecutionTimeout_Fire(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	firstRunID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowKey := definition.NewWorkflowKey(
		d.namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	var mutableState historyi.MutableState
	mutableState = workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEventWithOptions(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(d.now.Add(10 * time.Second)),
		},
		nil,
		uuid.New(),
		firstRunID,
	)
	require.NoError(t, err)

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         d.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          firstRunID,
		VisibilityTimestamp: d.now.Add(10 * time.Second),
		TaskID:              d.mustGenerateTaskID(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())

	for _, currentTime := range []time.Time{
		d.now.Add(5 * time.Second),
		d.now.Add(15 * time.Second),
	} {
		getWorkflowExecutionResponse := &persistence.GetWorkflowExecutionResponse{State: common.CloneProto(persistenceMutableState)}
		persistenceExecutionState := getWorkflowExecutionResponse.State.ExecutionState
		d.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
			ShardID:     d.mockShard.GetShardID(),
			NamespaceID: d.namespaceID.String(),
			WorkflowID:  execution.GetWorkflowId(),
		}).Return(&persistence.GetCurrentExecutionResponse{
			StartRequestID: persistenceExecutionState.CreateRequestId,
			RunID:          persistenceExecutionState.RunId,
			State:          persistenceExecutionState.State,
			Status:         persistenceExecutionState.Status,
		}, nil).Times(1)
		d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(getWorkflowExecutionResponse, nil)
		d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		d.timeSource.Update(currentTime)

		resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
		require.NoError(t, resp.ExecutionErr)

		mutableState = d.getMutableStateFromCache(workflowKey)
		require.False(t, mutableState.IsWorkflowExecutionRunning())
		state, status := mutableState.GetWorkflowStateStatus()
		require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, state)
		d.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)

		d.clearMutableStateFromCache(workflowKey)
	}
}

func TestWorkflowExecutionTimeout_Noop(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowRunTimeout:  durationpb.New(200 * time.Second),
				WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			},
			WorkflowExecutionExpirationTime: timestamppb.New(d.now.Add(10 * time.Second)),
		},
	)
	require.NoError(t, err)

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         d.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          uuid.New(), // does not match the firsrt runID of the execution
		VisibilityTimestamp: d.now,
		TaskID:              d.mustGenerateTaskID(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	persistenceExecutionState := persistenceMutableState.ExecutionState
	d.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     d.mockShard.GetShardID(),
		NamespaceID: d.namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: persistenceExecutionState.CreateRequestId,
		RunID:          persistenceExecutionState.RunId,
		State:          persistenceExecutionState.State,
		Status:         persistenceExecutionState.Status,
	}, nil).Times(1)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestExecuteChasmPureTimerTask_ZombieWorkflow(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: uuid.New(),
		RunId:      uuid.New(),
	}

	// Start the workflow with an event, and then push it into zombie state.
	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId:  d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
	)
	require.NoError(t, err)
	mutableState.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE

	// Add a valid timer task.
	timerTask := &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: d.now,
		TaskID:              d.mustGenerateTaskID(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// Execution should fail due to zombie.
	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrWorkflowZombie)
}

func TestExecuteStateMachineTimerTask_ZombieWorkflow(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId:  d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		},
	)
	require.NoError(t, err)
	mutableState.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE

	timerTask := &tasks.StateMachineTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: d.now,
		TaskID:              d.mustGenerateTaskID(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrWorkflowZombie)
}

func TestExecuteChasmSideEffectTimerTask_ExecutesTask(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree.
	chasmTree := historyi.NewMockChasmTree(d.controller)
	chasmTree.EXPECT().ExecuteSideEffectTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Times(1).Return(nil)

	// Mock mutable state.
	ms := historyi.NewMockMutableState(d.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().ChasmTree().Return(chasmTree).AnyTimes()

	// Add a valid timer task.
	timerTask := &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: d.now,
		TaskID:              d.mustGenerateTaskID(),
		Info: &persistencespb.ChasmTaskInfo{
			ComponentInitialVersionedTransition:    &persistencespb.VersionedTransition{},
			ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{},
			Path:                                   []string{},
			TypeId:                                 1234,
			Data: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	//nolint:revive // unchecked-type-assertion
	timerQueueActiveTaskExecutor := newTimerQueueActiveTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
		d.config,
		d.mockShard.Resource.GetMatchingClient(),
		d.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)

	// Execution should succeed.
	resp := timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.Nil(t, resp.ExecutionErr)
}

func TestExecuteChasmPureTimerTask_ExecutesAllPureTimers(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree and execute interface.
	mockEach := &chasm.MockNodePureTask{}
	chasmTree := historyi.NewMockChasmTree(d.controller)
	chasmTree.EXPECT().EachPureTask(gomock.Any(), gomock.Any()).
		Times(1).Do(
		func(_ time.Time, callback func(executor chasm.NodePureTask, taskAttributes chasm.TaskAttributes, task any) (bool, error)) error {
			_, err := callback(mockEach, chasm.TaskAttributes{}, nil)
			return err
		})

	// Mock mutable state.
	ms := historyi.NewMockMutableState(d.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().ChasmTree().Return(chasmTree).AnyTimes()

	// Add a valid timer task.
	timerTask := &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: d.now,
		TaskID:              d.mustGenerateTaskID(),
	}

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any())

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	//nolint:revive // unchecked-type-assertion
	timerQueueActiveTaskExecutor := newTimerQueueActiveTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
		d.config,
		d.mockShard.Resource.GetMatchingClient(),
		d.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)

	// Execution should succeed.
	resp := timerQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.Nil(t, resp.ExecutionErr)
}

func TestExecuteStateMachineTimerTask_ExecutesAllAvailableTimers(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	numInvocations := 0

	reg := d.mockShard.StateMachineRegistry()
	require.NoError(t, dummy.RegisterStateMachine(reg))
	require.NoError(t, dummy.RegisterTaskSerializers(reg))
	require.NoError(t, dummy.RegisterExecutor(
		reg,
		dummy.TaskExecutorOptions{
			ImmediateExecutor: nil,
			TimerExecutor: func(env hsm.Environment, node *hsm.Node, task dummy.TimerTask) error {
				numInvocations++
				return nil
			},
		},
	))

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := historyi.NewMockMutableState(d.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	root, err := hsm.NewRoot(
		reg,
		workflow.StateMachineType,
		ms,
		make(map[string]*persistencespb.StateMachineMap),
		ms,
	)
	require.NoError(t, err)
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	require.NoError(t, err)

	// Track some tasks.

	// Invalid reference, should be dropped.
	invalidTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
		},
		Type: dummy.TaskTypeTimer,
	}
	validTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: dummy.StateMachineType, Id: "dummy"},
			},
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
		},
		Type: dummy.TaskTypeTimer,
	}

	// Past deadline, should get executed.
	workflow.TrackStateMachineTimer(ms, d.now.Add(-time.Hour), invalidTask)
	workflow.TrackStateMachineTimer(ms, d.now.Add(-time.Hour), validTask)
	workflow.TrackStateMachineTimer(ms, d.now.Add(-time.Minute), validTask)
	// Future deadline, new task should be scheduled.
	futureDeadline := d.now.Add(time.Hour)
	workflow.TrackStateMachineTimer(ms, futureDeadline, validTask)

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any())

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetype, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey:         tests.WorkflowKey,
		VisibilityTimestamp: d.now,
		Version:             2,
	}

	// change now to a value earilier than task's visibility timestamp to test the case where system wall clock go backwards.
	d.timeSource.Update(d.now.Add(-30 * time.Minute))

	//nolint:revive // unchecked-type-assertion
	timerQueueActiveTaskExecutor := newTimerQueueActiveTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockShard.GetLogger(),
		metrics.NoopMetricsHandler,
		d.config,
		d.mockShard.Resource.GetMatchingClient(),
		d.mockChasmEngine,
	).(*timerQueueActiveTaskExecutor)

	err = timerQueueActiveTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, 2, numInvocations) // two valid tasks within the deadline.
	require.Equal(t, 1, len(info.StateMachineTimers))
	require.Equal(t, futureDeadline, info.StateMachineTimers[0].Deadline.AsTime())
}

func (d *timerQueueActiveTaskExecutorTestDeps) createPersistenceMutableState(
	ms historyi.MutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
	if err != nil {
		panic(err)
	}
	err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEventID, lastEventVersion,
	))
	if err != nil {
		panic(err)
	}
	return workflow.TestCloneToProto(ms)
}

func (d *timerQueueActiveTaskExecutorTestDeps) getMutableStateFromCache(
	workflowKey definition.WorkflowKey,
) historyi.MutableState {
	key := wcache.Key{
		WorkflowKey: workflowKey,
		ShardUUID:   d.mockShard.GetOwner(),
	}
	return wcache.GetMutableState(d.workflowCache, key)
}

func (d *timerQueueActiveTaskExecutorTestDeps) clearMutableStateFromCache(
	workflowKey definition.WorkflowKey,
) {
	wcache.ClearMutableState(d.workflowCache, wcache.Key{
		WorkflowKey: workflowKey,
		ShardUUID:   d.mockShard.GetOwner(),
	})
}

func (d *timerQueueActiveTaskExecutorTestDeps) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		d.timerQueueActiveTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		d.mockShard.GetTimeSource(),
		d.mockNamespaceCache,
		d.mockClusterMetadata,
		d.mockShard.ChasmRegistry(),
		queues.GetTaskTypeTagValue,
		nil,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
	)
}

func (d *timerQueueActiveTaskExecutorTestDeps) mustGenerateTaskID() int64 {
	taskID, err := d.mockShard.GenerateTaskID()
	if err != nil {
		panic(err)
	}
	return taskID
}

func TestProcessSingleActivityTimeoutTask(t *testing.T) {
	d := setupTimerQueueActiveTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	ms := historyi.NewMockMutableState(d.controller)

	testCases := []struct {
		name                         string
		ai                           *persistencespb.ActivityInfo
		timerSequenceID              workflow.TimerSequenceID
		expectRetryActivity          bool
		retryState                   enumspb.RetryState
		retryError                   error
		expectAddTimedTask           bool
		expectedUpdateMutableState   bool
		expectedScheduleWorkflowTask bool
	}{
		{
			name: "Retry Policy Not Set",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 1,
				Stamp:   1,
			},
			expectRetryActivity:          true,
			retryState:                   enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
			retryError:                   nil,
			expectAddTimedTask:           true,
			expectedUpdateMutableState:   true,
			expectedScheduleWorkflowTask: true,
		},
		{
			name: "Retry State Timeout",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 1,
				Stamp:   1,
			},
			expectRetryActivity:          true,
			retryState:                   enumspb.RETRY_STATE_TIMEOUT,
			retryError:                   nil,
			expectAddTimedTask:           true,
			expectedUpdateMutableState:   true,
			expectedScheduleWorkflowTask: true,
		},
		{
			name: "Retry State In Progress",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 1,
			},
			expectRetryActivity:          true,
			retryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
			retryError:                   nil,
			expectAddTimedTask:           false,
			expectedUpdateMutableState:   true,
			expectedScheduleWorkflowTask: false,
		},
		{
			name: "Attempt dont match",
			timerSequenceID: workflow.TimerSequenceID{
				Attempt: 1,
			},
			ai: &persistencespb.ActivityInfo{
				Attempt: 2,
			},
			expectRetryActivity:          false,
			expectAddTimedTask:           false,
			expectedUpdateMutableState:   false,
			expectedScheduleWorkflowTask: false,
		},
	}
	info := &persistencespb.WorkflowExecutionInfo{}

	for _, tc := range testCases {
		t.Run(tc.name, func() {
			if tc.expectRetryActivity {
				ms.EXPECT().RetryActivity(gomock.Any(), gomock.Any()).Return(tc.retryState, tc.retryError)
				ms.EXPECT().GetWorkflowType().Return(&commonpb.WorkflowType{Name: "test-workflow-type"}).AnyTimes()
			}

			if tc.expectAddTimedTask {
				ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
				ms.EXPECT().AddActivityTaskTimedOutEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
			}
			ms.EXPECT().GetEffectiveVersioningBehavior().Return(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED).AnyTimes()
			ms.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry).AnyTimes()

			result, err := d.timerQueueActiveTaskExecutor.processSingleActivityTimeoutTask(
				ms, tc.timerSequenceID, tc.ai)

			require.NoError(t, err)
			require.Equal(t, tc.expectedScheduleWorkflowTask, result.shouldScheduleWorkflowTask, "scheduleWorkflowTask")
			require.Equal(t, tc.expectedUpdateMutableState, result.shouldUpdateMutableState, "updateMutableState")
		})
	}
}

