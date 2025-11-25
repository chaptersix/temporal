package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/protorequire"
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

// TODO: remove all SetCurrentTime usage in this test suite
// after clusterName & getCurrentTime() method are deprecated
// from timerQueueStandbyTaskExecutor

type timerQueueStandbyTaskExecutorTestDeps struct {
	controller          *gomock.Controller
	mockExecutionMgr    *persistence.MockExecutionManager
	mockShard           *shard.ContextTest
	mockTxProcessor     *queues.MockQueue
	mockTimerProcessor  *queues.MockQueue
	mockNamespaceCache  *namespace.MockRegistry
	mockClusterMetadata *cluster.MockMetadata
	mockAdminClient     *adminservicemock.MockAdminServiceClient
	mockDeleteManager   *deletemanager.MockDeleteManager
	mockMatchingClient  *matchingservicemock.MockMatchingServiceClient
	mockChasmEngine     *chasm.MockEngine

	config               *configs.Config
	workflowCache        wcache.Cache
	logger               log.Logger
	namespaceID          namespace.ID
	namespaceEntry       *namespace.Namespace
	version              int64
	clusterName          string
	now                  time.Time
	timeSource           *clock.EventTimeSource
	fetchHistoryDuration time.Duration
	discardDuration      time.Duration
	clientBean           *client.MockBean

	timerQueueStandbyTaskExecutor *timerQueueStandbyTaskExecutor
}





func setupTimerQueueStandbyTaskExecutorTest(t *testing.T) *timerQueueStandbyTaskExecutorTestDeps {
	d := &timerQueueStandbyTaskExecutorTestDeps{}

	d.config = tests.NewDynamicConfig()
	d.config.EnableWorkflowTaskStampIncrementOnFailure = func() bool { return true }
	d.namespaceEntry = tests.GlobalStandbyNamespaceEntry
	d.namespaceID = d.namespaceEntry.ID()
	d.version = d.namespaceEntry.FailoverVersion()
	d.clusterName = cluster.TestAlternativeClusterName
	d.now = time.Now().UTC()
	d.timeSource = clock.NewEventTimeSource().Update(d.now)
	d.fetchHistoryDuration = time.Minute * 12
	d.discardDuration = time.Minute * 30

	d.controller = gomock.NewController(t)
	d.mockTxProcessor = queues.NewMockQueue(d.controller)
	d.mockTimerProcessor = queues.NewMockQueue(d.controller)
	d.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	d.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	d.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	d.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	d.clientBean = client.NewMockBean(d.controller)
	d.mockChasmEngine = chasm.NewMockEngine(d.controller)

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

	// ack manager will use the namespace information
	d.mockNamespaceCache = d.mockShard.Resource.NamespaceCache
	d.mockExecutionMgr = d.mockShard.Resource.ExecutionMgr
	d.mockClusterMetadata = d.mockShard.Resource.ClusterMetadata
	d.mockAdminClient = d.mockShard.Resource.RemoteAdminClient
	d.mockMatchingClient = d.mockShard.Resource.MatchingClient
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(d.namespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(d.namespaceEntry.Name(), nil).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	d.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	d.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(d.clusterName).AnyTimes()
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
		eventNotifier:      events.NewNotifier(d.timeSource, metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			d.mockTxProcessor.Category():    d.mockTxProcessor,
			d.mockTimerProcessor.Category(): d.mockTimerProcessor,
		},
	}
	d.mockShard.SetEngineForTesting(h)

	d.timerQueueStandbyTaskExecutor = newTimerQueueStandbyTaskExecutor(
		d.mockShard,
		d.workflowCache,
		d.mockDeleteManager,
		d.mockMatchingClient,
		d.mockChasmEngine,
		d.logger,
		metrics.NoopMetricsHandler,
		d.clusterName,
		d.config,
		d.clientBean,
	).(*timerQueueStandbyTaskExecutor)
	return d
}



func TestProcessUserTimerTimeout_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(-time.Second))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(-time.Second))
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(2*timerTimeout))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.fetchHistoryDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.fetchHistoryDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.discardDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.discardDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func TestProcessUserTimerTimeout_Success(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard,
		d.mockShard.GetEventsCache(),
		d.logger,
		d.version,
		execution.GetWorkflowId(),
		execution.GetRunId())
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

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessUserTimerTimeout_Multiple(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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

	timerID1 := "timer-1"
	timerTimeout1 := 2 * time.Second
	event, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID1, timerTimeout1)

	timerID2 := "timer-2"
	timerTimeout2 := 50 * time.Second
	_, _ = addTimerStartedEvent(mutableState, event.GetEventId(), timerID2, timerTimeout2)

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

	event = addTimerFiredEvent(mutableState, timerID1)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessActivityTimeout_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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

	taskqueue := "taskqueue"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)

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
		EventID:             event.EventId,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(-time.Second))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(-time.Second))
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(2 * timerTimeout))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(2*timerTimeout))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.fetchHistoryDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.fetchHistoryDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.discardDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.discardDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func TestProcessActivityTimeout_Success(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
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
		EventID:             event.GetEventId(),
	}

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessActivityTimeout_Heartbeat_Noop(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, heartbeatTimerTimeout)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
		VisibilityTimestamp: time.Unix(946684800, 0).Add(-100 * time.Second), // see pendingActivityTimerHeartbeats from mutable state
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessActivityTimeout_Multiple_CanUpdate(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	activityID1 := "activity 1"
	activityType1 := "activity type 1"
	timerTimeout1 := 2 * time.Second
	scheduledEvent1, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID1, activityType1, taskqueue, nil,
		timerTimeout1, timerTimeout1, timerTimeout1, timerTimeout1)
	startedEvent1 := addActivityTaskStartedEvent(mutableState, scheduledEvent1.GetEventId(), identity)

	activityID2 := "activity 2"
	activityType2 := "activity type 2"
	timerTimeout2 := 20 * time.Second
	scheduledEvent2, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID2, activityType2, taskqueue, nil,
		timerTimeout2, timerTimeout2, timerTimeout2, 10*time.Second)
	addActivityTaskStartedEvent(mutableState, scheduledEvent2.GetEventId(), identity)
	activityInfo2 := mutableState.GetPendingActivityInfos()[scheduledEvent2.GetEventId()]
	activityInfo2.TimerTaskStatus |= workflow.TimerTaskStatusCreatedHeartbeat
	activityInfo2.LastHeartbeatUpdateTime = timestamppb.New(time.Now().UTC())

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)

	timerTask := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		VisibilityTimestamp: activityInfo2.LastHeartbeatUpdateTime.AsTime().Add(-5 * time.Second),
		EventID:             scheduledEvent2.GetEventId(),
	}

	completeEvent1 := addActivityTaskCompletedEvent(mutableState, scheduledEvent1.GetEventId(), startedEvent1.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completeEvent1.GetEventId(), completeEvent1.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.Equal(t, 1, len(input.UpdateWorkflowMutation.Tasks[tasks.CategoryTimer]))
			require.Equal(t, 1, len(input.UpdateWorkflowMutation.UpsertActivityInfos))
			mutableState.GetExecutionInfo().LastUpdateTime = input.UpdateWorkflowMutation.ExecutionInfo.LastUpdateTime
			input.RangeID = 0
			input.UpdateWorkflowMutation.ExecutionInfo.LastRunningClock = 0
			input.UpdateWorkflowMutation.ExecutionInfo.LastFirstEventTxnId = 0
			input.UpdateWorkflowMutation.ExecutionInfo.StateTransitionCount = 0
			mutableState.GetExecutionInfo().LastRunningClock = 0
			mutableState.GetExecutionInfo().LastFirstEventTxnId = 0
			mutableState.GetExecutionInfo().StateTransitionCount = 0
			mutableState.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = input.UpdateWorkflowMutation.ExecutionInfo.WorkflowTaskOriginalScheduledTime
			mutableState.GetExecutionInfo().ExecutionStats = &persistencespb.ExecutionStats{}

			require.True(t, temporalproto.DeepEqual(&persistence.UpdateWorkflowExecutionRequest{
				ShardID: d.mockShard.GetShardID(),
				UpdateWorkflowMutation: persistence.WorkflowMutation{
					ExecutionInfo:             mutableState.GetExecutionInfo(),
					ExecutionState:            mutableState.GetExecutionState(),
					NextEventID:               mutableState.GetNextEventID(),
					Tasks:                     input.UpdateWorkflowMutation.Tasks,
					Condition:                 mutableState.GetNextEventID(),
					UpsertActivityInfos:       input.UpdateWorkflowMutation.UpsertActivityInfos,
					DeleteActivityInfos:       map[int64]struct{}{},
					UpsertTimerInfos:          map[string]*persistencespb.TimerInfo{},
					DeleteTimerInfos:          map[string]struct{}{},
					UpsertChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{},
					DeleteChildExecutionInfos: map[int64]struct{}{},
					UpsertRequestCancelInfos:  map[int64]*persistencespb.RequestCancelInfo{},
					DeleteRequestCancelInfos:  map[int64]struct{}{},
					UpsertSignalInfos:         map[int64]*persistencespb.SignalInfo{},
					DeleteSignalInfos:         map[int64]struct{}{},
					UpsertSignalRequestedIDs:  map[string]struct{}{},
					DeleteSignalRequestedIDs:  map[string]struct{}{},
					NewBufferedEvents:         nil,
					ClearBufferedEvents:       false,
				},
				UpdateWorkflowEvents: []*persistence.WorkflowEvents{},
			}, input))
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessWorkflowTaskTimeout_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_ = mutableState.UpdateCurrentVersion(d.version, false)
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
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.fetchHistoryDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.discardDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func TestProcessWorkflowTaskTimeout_ScheduleToStartTimer(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	workflowTaskScheduledEventID := int64(16384)

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		ScheduleAttempt:     1,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		VisibilityTimestamp: d.now,
		EventID:             workflowTaskScheduledEventID,
	}

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, nil, resp.ExecutionErr)
}

func TestProcessWorkflowTaskTimeout_Success(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessWorkflowTaskTimeout_AttemptMismatch(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	// This test verifies that when a workflow task fails and is rescheduled with a new attempt,
	// the old timer task (with old attempt and old stamp) correctly returns stale reference error.
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

	// We must manually update the version history here.
	// The logic for scheduling transient workflow task below will use the version history to determine
	// if there's failover and if workflow task attempt needs to be reset.
	vh, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().VersionHistories)
	require.NoError(t, err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(vh, versionhistory.NewVersionHistoryItem(
		event.GetEventId(), event.GetVersion(),
	))
	require.NoError(t, err)

	event, err = mutableState.AddWorkflowTaskFailedEvent(
		wt,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		nil,
		"",
		"",
		"",
		0,
	)
	require.NoError(t, err)

	wt, err = mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_TRANSIENT)
	require.NoError(t, err)
	require.Equal(t, int32(2), wt.Attempt)

	timerTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		// Timer task has old attempt (1) and old stamp (default 0).
		// Current workflow task has new attempt (2) and new stamp (incremented).
		// This should return stale reference error due to stamp mismatch.
		ScheduleAttempt:     1,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		VisibilityTimestamp: d.now,
		EventID:             wt.ScheduledEventID,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId()+1, event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	// After workflow task fails and is rescheduled, the stamp is incremented.
	// The old timer task with old stamp should now return stale reference error.
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrStaleReference)
}

func TestProcessWorkflowTaskTimeout_StampMismatch(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
		EventID:             wt.ScheduledEventID,
		Stamp:               wt.Stamp,
	}

	// Modify the workflow task stamp in mutable state to create mismatch
	mutableState.GetExecutionInfo().WorkflowTaskStamp = wt.Stamp + 1

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockShard.SetCurrentTime(d.clusterName, d.now)

	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrStaleReference)
}

func TestProcessWorkflowBackoffTimer_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
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

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.mockShard.SetCurrentTime(d.clusterName, time.Now().UTC().Add(d.fetchHistoryDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.mockShard.SetCurrentTime(d.clusterName, time.Now().UTC().Add(d.discardDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func TestProcessWorkflowBackoffTimer_Success(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_CRON,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, wt.ScheduledEventID, wt.Version)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessWorkflowRunTimeout_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	workflowRunTimeout := 200 * time.Second

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
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	startEvent := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = startEvent.GetEventId()
	completionEvent := addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	startTime := mutableState.GetExecutionState().GetStartTime().AsTime()
	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: startTime.Add(workflowRunTimeout),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(-time.Second))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(-time.Second))
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now)
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(2*workflowRunTimeout))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.fetchHistoryDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.fetchHistoryDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.discardDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.discardDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func TestProcessWorkflowRunTimeout_Success(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowRunTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessWorkflowExecutionTimeout_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	firstRunID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	workflowRunTimeout := 200 * time.Second

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	startedEvent, err := mutableState.AddWorkflowExecutionStartedEventWithOptions(
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
		},
		nil,
		uuid.New(),
		firstRunID,
	)
	require.NoError(t, err)

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         d.namespaceID.String(),
		WorkflowID:          execution.GetWorkflowId(),
		FirstRunID:          firstRunID,
		VisibilityTimestamp: d.now.Add(workflowRunTimeout),
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
	}, nil).Times(4)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.timeSource.Update(d.now.Add(-time.Second))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(-time.Second))
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(2 * workflowRunTimeout))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(2*workflowRunTimeout))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.fetchHistoryDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.fetchHistoryDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	d.timeSource.Update(d.now.Add(d.discardDuration))
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.discardDuration))
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskDiscarded, resp.ExecutionErr)
}

func TestProcessWorkflowExecutionTimeout_Success(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
		},
	)
	require.NoError(t, err)

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

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

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessRetryTimeout(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	startEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
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
	persistenceMutableState := d.createPersistenceMutableState(mutableState, startEvent.GetEventId(), startEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).AnyTimes()
	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             1,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: d.now,
		EventID:             int64(16384),
	}

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessActivityRetryTimer_Noop(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).AnyTimes()
	d.mockShard.SetCurrentTime(d.clusterName, d.now)

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)

	timerTask = &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             d.version - 1,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskVersionMismatch, resp.ExecutionErr)

	timerTask = &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             0,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessActivityRetryTimer_ActivityCompleted(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskqueue, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)
	startedEvent := addActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	completeEvent := addActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessActivityRetryTimer_Pending(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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

	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, nil,
		timerTimeout, timerTimeout, timerTimeout, timerTimeout)

	timerSequence := workflow.NewTimerSequence(mutableState)
	mutableState.InsertTasks[tasks.CategoryTimer] = nil
	modified, err := timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
	task := mutableState.InsertTasks[tasks.CategoryTimer][0]

	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	timerTask := &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Attempt:             2,
		Version:             d.version,
		TaskID:              d.mustGenerateTaskID(),
		VisibilityTimestamp: task.(*tasks.ActivityTimeoutTask).VisibilityTimestamp,
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// no-op post action
	d.mockShard.SetCurrentTime(d.clusterName, d.now)
	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	// resend history post action
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.fetchHistoryDuration))

	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Equal(t, consts.ErrTaskRetry, resp.ExecutionErr)

	// push to matching post action
	d.mockShard.SetCurrentTime(d.clusterName, d.now.Add(d.discardDuration))
	d.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		&matchingservice.AddActivityTaskRequest{
			NamespaceId: d.namespaceID.String(),
			Execution:   execution,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueueName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduledEventId:       scheduledEvent.EventId,
			ScheduleToStartTimeout: durationpb.New(timerTimeout),
			Clock:                  vclock.NewVectorClock(d.mockClusterMetadata.GetClusterID(), d.mockShard.GetShardID(), timerTask.TaskID),
			VersionDirective:       worker_versioning.MakeUseAssignmentRulesDirective(),
		},
		gomock.Any(),
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	resp = d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestExecuteStateMachineTimerTask_ExecutesAllAvailableTimers(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	reg := d.mockShard.StateMachineRegistry()
	require.NoError(t, dummy.RegisterStateMachine(reg))
	require.NoError(t, dummy.RegisterTaskSerializers(reg))

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
	ms.EXPECT().GetNextEventID().Return(int64(2))
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	require.NoError(t, err)

	dummyRoot, err := root.Child([]hsm.Key{
		{Type: dummy.StateMachineType, ID: "dummy"},
	})
	require.NoError(t, err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	require.NoError(t, err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	require.NoError(t, err)

	// Track some tasks.

	// Invalid reference, should be dropped.
	invalidTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
			},
		},
		Type: dummy.TaskTypeTimer,
	}
	staleTask := &persistencespb.StateMachineTaskInfo{
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
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineTransitionCount: 1,
		},
		Type: dummy.TaskTypeTimer,
	}

	// Past deadline, should get executed.
	workflow.TrackStateMachineTimer(ms, d.mockShard.GetTimeSource().Now().Add(-time.Hour), invalidTask)
	workflow.TrackStateMachineTimer(ms, d.mockShard.GetTimeSource().Now().Add(-time.Hour), staleTask)
	workflow.TrackStateMachineTimer(ms, d.mockShard.GetTimeSource().Now().Add(-time.Minute), staleTask)
	// Future deadline, new task should be scheduled.
	futureDeadline := d.mockShard.GetTimeSource().Now().Add(time.Hour)
	workflow.TrackStateMachineTimer(ms, futureDeadline, staleTask)

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)
	wfCtx.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), gomock.Any())

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetype, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey: tests.WorkflowKey,
		Version:     2,
	}

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockMatchingClient,
		d.mockChasmEngine,
		d.logger,
		metrics.NoopMetricsHandler,
		d.clusterName,
		d.config,
		d.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	err = timerQueueStandbyTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, 1, len(info.StateMachineTimers))
	require.Equal(t, futureDeadline, info.StateMachineTimers[0].Deadline.AsTime())
}

func TestExecuteStateMachineTimerTask_ValidConcurrentTaskIsKept(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	reg := d.mockShard.StateMachineRegistry()
	require.NoError(t, dummy.RegisterStateMachine(reg))
	require.NoError(t, dummy.RegisterTaskSerializers(reg))

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := historyi.NewMockMutableState(d.controller)
	info := &persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 1, Version: 2},
					},
				},
			},
		},
	}

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
	ms.EXPECT().GetNextEventID().Return(int64(2))
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	require.NoError(t, err)

	dummyRoot, err := root.Child([]hsm.Key{
		{Type: dummy.StateMachineType, ID: "dummy"},
	})
	require.NoError(t, err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	require.NoError(t, err)
	err = hsm.MachineTransition(dummyRoot, func(sm *dummy.Dummy) (hsm.TransitionOutput, error) {
		return dummy.Transition0.Apply(sm, dummy.Event0{})
	})
	require.NoError(t, err)

	// Track a task with a past deadline. Should get executed.
	workflow.TrackStateMachineTimer(ms, d.mockShard.GetTimeSource().Now().Add(-time.Hour), &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 0,
			},
			MachineLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 2,
			},
		},
		Type: dummy.TaskTypeTimer,
	})

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetype, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey: tests.WorkflowKey,
		Version:     2,
	}

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockMatchingClient,
		d.mockChasmEngine,
		d.logger,
		metrics.NoopMetricsHandler,
		d.clusterName,
		d.config,
		d.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	err = timerQueueStandbyTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	require.ErrorIs(t, err, consts.ErrTaskRetry)
	require.Equal(t, 1, len(info.StateMachineTimers))
}

func TestExecuteStateMachineTimerTask_StaleStateMachine(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	reg := d.mockShard.StateMachineRegistry()
	require.NoError(t, dummy.RegisterStateMachine(reg))
	require.NoError(t, dummy.RegisterTaskSerializers(reg))

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := historyi.NewMockMutableState(d.controller)
	info := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 2, TransitionCount: 2},
		},
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 1, Version: 2},
					},
				},
			},
		},
	}
	root, err := hsm.NewRoot(
		reg,
		workflow.StateMachineType,
		ms,
		make(map[string]*persistencespb.StateMachineMap),
		ms,
	)
	require.NoError(t, err)

	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes()
	ms.EXPECT().GetNextEventID().Return(int64(2))
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().HSM().Return(root).AnyTimes()

	_, err = dummy.MachineCollection(root).Add("dummy", dummy.NewDummy())
	require.NoError(t, err)

	// Track some tasks.

	validTask := &persistencespb.StateMachineTaskInfo{
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{Type: dummy.StateMachineType, Id: "dummy"},
			},
			MutableStateVersionedTransition: nil,
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          0,
			},
			MachineLastUpdateVersionedTransition: nil,
			MachineTransitionCount:               0,
		},
		Type: dummy.TaskTypeTimer,
	}

	// Past deadline, still valid task
	workflow.TrackStateMachineTimer(ms, d.mockShard.GetTimeSource().Now().Add(-time.Hour), validTask)
	// Future deadline, new task should be scheduled.
	workflow.TrackStateMachineTimer(ms, d.mockShard.GetTimeSource().Now().Add(time.Hour), validTask)

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, tests.NamespaceID, we, chasm.WorkflowArchetype, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	task := &tasks.StateMachineTimerTask{
		WorkflowKey: tests.WorkflowKey,
		Version:     2,
	}

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockMatchingClient,
		d.mockChasmEngine,
		d.logger,
		metrics.NoopMetricsHandler,
		d.clusterName,
		d.config,
		d.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	err = timerQueueStandbyTaskExecutor.executeStateMachineTimerTask(context.Background(), task)
	require.ErrorIs(t, err, consts.ErrTaskRetry)
	require.Equal(t, 2, len(info.StateMachineTimers))
}

func TestExecuteStateMachineTimerTask_ZombieWorkflow(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
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

	resp := d.timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrWorkflowZombie)
}

func TestExecuteChasmSideEffectTimerTask_ExecutesTask(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree.
	chasmTree := historyi.NewMockChasmTree(d.controller)
	expectValidate := func(isValid bool, err error) {
		chasmTree.EXPECT().ValidateSideEffectTask(
			gomock.Any(),
			gomock.Any(),
		).Times(1).Return(isValid, err)
	}

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
		Info:                &persistencespb.ChasmTaskInfo{},
	}

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil).AnyTimes()

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil).AnyTimes()

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockMatchingClient,
		d.mockChasmEngine,
		d.logger,
		metrics.NoopMetricsHandler,
		d.clusterName,
		d.config,
		d.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	// Validation succeeds, task should retry.
	expectValidate(true, nil)
	resp := timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.ErrorIs(t, consts.ErrTaskRetry, resp.ExecutionErr)

	// Validation succeeds but task is invalid.
	expectValidate(false, nil)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.NoError(t, resp.ExecutionErr)

	// Validation fails, processing should fail.
	expectedErr := errors.New("validation error")
	expectValidate(false, expectedErr)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.ErrorIs(t, expectedErr, resp.ExecutionErr)
}

func TestExecuteChasmPureTimerTask_ValidatesAllPureTimers(t *testing.T) {
	d := setupTimerQueueStandbyTaskExecutorTest(t)
	defer d.controller.Finish()
	defer d.mockShard.StopForTest()


	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree and execute interface.
	chasmTree := historyi.NewMockChasmTree(d.controller)
	expectEachPureTask := func(err error) {
		chasmTree.EXPECT().EachPureTask(gomock.Any(), gomock.Any()).
			Times(1).Return(err)
	}

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
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil).AnyTimes()

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, locks.PriorityLow,
	).Return(wfCtx, wcache.NoopReleaseFn, nil).AnyTimes()

	//nolint:revive // unchecked-type-assertion
	timerQueueStandbyTaskExecutor := newTimerQueueStandbyTaskExecutor(
		d.mockShard,
		mockCache,
		d.mockDeleteManager,
		d.mockMatchingClient,
		d.mockChasmEngine,
		d.logger,
		metrics.NoopMetricsHandler,
		d.clusterName,
		d.config,
		d.clientBean,
	).(*timerQueueStandbyTaskExecutor)

	// All tasks were invalid.
	expectEachPureTask(nil)
	resp := timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.NoError(t, resp.ExecutionErr)

	// Tasks should retry.
	expectEachPureTask(consts.ErrTaskRetry)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.ErrorIs(t, consts.ErrTaskRetry, resp.ExecutionErr)

	// Validation failed.
	expectedErr := errors.New("validation error")
	expectEachPureTask(expectedErr)
	resp = timerQueueStandbyTaskExecutor.Execute(context.Background(), d.newTaskExecutable(timerTask))
	require.NotNil(t, resp)
	require.ErrorIs(t, expectedErr, resp.ExecutionErr)
}

func (d *timerQueueStandbyTaskExecutorTestDeps) createPersistenceMutableState(
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

func (d *timerQueueStandbyTaskExecutorTestDeps) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		d.timerQueueStandbyTaskExecutor,
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

func (d *timerQueueStandbyTaskExecutorTestDeps) mustGenerateTaskID() int64 {
	taskID, err := d.mockShard.GenerateTaskID()
	if err != nil {
		panic(err)
	}
	return taskID
}

