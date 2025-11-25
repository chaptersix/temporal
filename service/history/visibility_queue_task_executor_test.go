package history

import (
	"context"
	"strconv"
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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type visibilityQueueTaskExecutorTestEnv struct {
	controller *gomock.Controller
	mockShard  *shard.ContextTest

	mockVisibilityMgr *manager.MockVisibilityManager
	mockExecutionMgr  *persistence.MockExecutionManager

	workflowCache               wcache.Cache
	logger                      log.Logger
	namespaceID                 namespace.ID
	namespace                   namespace.Name
	version                     int64
	now                         time.Time
	timeSource                  *clock.EventTimeSource
	visibilityQueueTaskExecutor queues.Executor

	enableCloseWorkflowCleanup bool
}

func setupVisibilityQueueTaskExecutorTest(t *testing.T) *visibilityQueueTaskExecutorTestEnv {
	env := &visibilityQueueTaskExecutorTestEnv{}

	env.namespaceID = tests.NamespaceID
	env.namespace = tests.Namespace
	env.version = tests.GlobalNamespaceEntry.FailoverVersion()
	env.now = time.Now().UTC()
	env.timeSource = clock.NewEventTimeSource().Update(env.now)

	env.controller = gomock.NewController(t)

	config := tests.NewDynamicConfig()
	config.EnableChasm = dynamicconfig.GetBoolPropertyFn(true)
	env.mockShard = shard.NewTestContext(
		env.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)

	// Set up expectations on the SearchAttributesMapper mocks created by NewTestContext
	mockMapper := searchattribute.NewMockMapper(env.controller)
	mockMapper.EXPECT().GetFieldName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(alias string, _ string) (string, error) {
			return alias, nil
		},
	).AnyTimes()

	mockMapperProvider := env.mockShard.Resource.SearchAttributesMapperProvider
	mockMapperProvider.EXPECT().GetMapper(gomock.Any()).Return(mockMapper, nil).AnyTimes()

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	env.mockShard.SetStateMachineRegistry(reg)

	chasmRegistry := env.mockShard.ChasmRegistry()
	err = chasmRegistry.Register(&chasm.CoreLibrary{})
	require.NoError(t, err)
	err = chasmRegistry.Register(&testChasmLibrary{})
	require.NoError(t, err)
	err = chasmRegistry.Register(chasmworkflow.NewLibrary())
	require.NoError(t, err)

	env.mockShard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		env.mockShard.GetExecutionManager(),
		env.mockShard.GetConfig(),
		env.mockShard.GetMetricsHandler(),
		env.mockShard.GetLogger(),
		false,
	))
	env.mockShard.Resource.TimeSource = env.timeSource

	env.mockExecutionMgr = env.mockShard.Resource.ExecutionMgr
	env.mockVisibilityMgr = manager.NewMockVisibilityManager(env.controller)

	mockNamespaceCache := env.mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()

	mockClusterMetadata := env.mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, env.version).Return(mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	env.workflowCache = wcache.NewHostLevelCache(env.mockShard.GetConfig(), env.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	env.logger = env.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: env.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       env.mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   env.mockExecutionMgr,
		logger:             env.logger,
		tokenSerializer:    tasktoken.NewSerializer(),
		metricsHandler:     env.mockShard.GetMetricsHandler(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
	}
	env.mockShard.SetEngineForTesting(h)

	env.enableCloseWorkflowCleanup = false
	env.visibilityQueueTaskExecutor = newVisibilityQueueTaskExecutor(
		env.mockShard,
		env.workflowCache,
		env.mockVisibilityMgr,
		env.logger,
		metrics.NoopMetricsHandler,
		config.VisibilityProcessorEnsureCloseBeforeDelete,
		func(_ string) bool { return env.enableCloseWorkflowCleanup },
		config.VisibilityProcessorRelocateAttributesMinBlobSize,
	)

	return env
}

func (env *visibilityQueueTaskExecutorTestEnv) teardown() {
	env.controller.Finish()
	env.mockShard.StopForTest()
}

func TestProcessCloseExecution(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}
	rootExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random root workflow ID",
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
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId:      parentNamespaceID,
				Namespace:        parentNamespace,
				Execution:        parentExecution,
				InitiatedId:      parentInitiatedID,
				InitiatedVersion: parentInitiatedVersion,
			},
		},
	)
	require.Nil(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	visibilityTask := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             env.version,
		TaskID:              taskID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(
		gomock.Any(),
		createRecordWorkflowExecutionClosedRequest(t, env, env.namespace, visibilityTask, mutableState, taskQueueName, parentExecution, rootExecution, map[string]any{
			sadefs.BuildIds: []string{worker_versioning.UnversionedSearchAttribute},
		}),
	).Return(nil)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessCloseExecutionWithWorkflowClosedCleanup(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	env.enableCloseWorkflowCleanup = true

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}
	rootExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random root workflow ID",
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
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId:      parentNamespaceID,
				Namespace:        parentNamespace,
				Execution:        parentExecution,
				InitiatedId:      parentInitiatedID,
				InitiatedVersion: parentInitiatedVersion,
			},
		},
	)
	require.Nil(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := int64(59)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	visibilityTask := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             env.version,
		TaskID:              taskID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	env.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(
		gomock.Any(),
		createRecordWorkflowExecutionClosedRequest(t, env, env.namespace, visibilityTask, mutableState, taskQueueName, parentExecution, rootExecution, map[string]any{
			sadefs.BuildIds: []string{worker_versioning.UnversionedSearchAttribute},
		}),
	).Return(nil)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessRecordWorkflowStartedTask(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	cronSchedule := "@every 5s"
	backoff := 5 * time.Second

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
				CronSchedule:             cronSchedule,
			},
			FirstWorkflowTaskBackoff: durationpb.New(backoff),
		},
	)
	require.Nil(t, err)

	taskID := int64(59)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	visibilityTask := &tasks.StartExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: time.Now().UTC(),
		Version:             env.version,
		TaskID:              taskID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionStarted(
		gomock.Any(),
		createRecordWorkflowExecutionStartedRequest(t, env, env.namespace, visibilityTask, mutableState, taskQueueName),
	).Return(nil)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessUpsertWorkflowSearchAttributes(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
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

	visibilityTask := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID: taskID,
	}

	persistenceMutableState := createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	env.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(
		gomock.Any(),
		createUpsertWorkflowRequest(t, env, env.namespace, visibilityTask, mutableState, taskQueueName),
	).Return(nil)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessModifyWorkflowProperties(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(
		env.mockShard,
		env.mockShard.GetEventsCache(),
		env.logger,
		env.version,
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)

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

	visibilityTask := &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			env.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		TaskID: taskID,
	}

	persistenceMutableState := createPersistenceMutableState(
		t,
		mutableState,
		wt.ScheduledEventID,
		wt.Version,
	)
	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState},
		nil,
	)
	env.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(
		gomock.Any(),
		createUpsertWorkflowRequest(t, env, env.namespace, visibilityTask, mutableState, taskQueueName),
	).Return(nil)

	resp := env.visibilityQueueTaskExecutor.Execute(
		context.Background(),
		newTaskExecutableForVisibility(env, visibilityTask),
	)
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessDeleteExecution(t *testing.T) {
	t.SkipNow()
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	workflowKey := definition.WorkflowKey{
		NamespaceID: env.namespaceID.String(),
	}
	t.Run("TaskID=0", func(t *testing.T) {
		env.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any())
		err := executeVisibilityTask(env, &tasks.DeleteExecutionVisibilityTask{
			WorkflowKey:                    workflowKey,
			CloseExecutionVisibilityTaskID: 0,
		})
		require.NoError(t, err)
	})
	t.Run("WorkflowCloseTime=1970-01-01T00:00:00Z", func(t *testing.T) {
		env.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any())
		err := executeVisibilityTask(env, &tasks.DeleteExecutionVisibilityTask{
			WorkflowKey: workflowKey,
			CloseTime:   time.Unix(0, 0).UTC(),
		})
		require.NoError(t, err)
	})
	t.Run("MultiCursorQueue", func(t *testing.T) {
		const highWatermark int64 = 5
		require.NoError(t, env.mockShard.SetQueueState(tasks.CategoryVisibility, 1, &persistencespb.QueueState{
			ReaderStates: nil,
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				TaskId:   highWatermark,
				FireTime: timestamppb.New(tasks.DefaultFireTime),
			},
		}))
		t.Run("NotAcked", func(t *testing.T) {
			err := executeVisibilityTask(env, &tasks.DeleteExecutionVisibilityTask{
				WorkflowKey:                    workflowKey,
				CloseExecutionVisibilityTaskID: highWatermark + 1,
			})
			require.ErrorIs(t, err, consts.ErrDependencyTaskNotCompleted)
		})
		t.Run("Acked", func(t *testing.T) {
			env.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any())
			err := executeVisibilityTask(env, &tasks.DeleteExecutionVisibilityTask{
				WorkflowKey:                    workflowKey,
				CloseExecutionVisibilityTaskID: highWatermark - 1,
			})
			require.NoError(t, err)
		})
	})
}

func TestProcessChasmTask_InvalidTask(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	key := definition.NewWorkflowKey(
		env.namespaceID.String(),
		"some random ID",
		uuid.New(),
	)
	mutableState := buildChasmMutableState(t, env, key, 5)

	// Case 1: invalid task with lower transition count than the state
	visibilityTask := buildChasmVisTask(t, env, key, 3)

	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)

	// Case 2: invalid task with a different initial versioned transition
	componentInitVT := mutableState.ChasmNodes["Visibility"].Metadata.InitialVersionedTransition
	visibilityTask = buildChasmVisTask(t, env, key, 5)
	visibilityTask.Info.ComponentInitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: componentInitVT.NamespaceFailoverVersion + 101,
		TransitionCount:          componentInitVT.TransitionCount,
	}

	resp = env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessChasmTask_RunningExecution(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	key := definition.NewWorkflowKey(
		env.namespaceID.String(),
		"some random ID",
		uuid.New(),
	)
	mutableState := buildChasmMutableState(t, env, key, 5)

	visibilityTask := buildChasmVisTask(t, env, key, 5)

	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	env.mockVisibilityMgr.EXPECT().UpsertWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *manager.UpsertWorkflowExecutionRequest) error {

			require.Len(t, request.SearchAttributes.IndexedFields, 2)

			v, ok := request.SearchAttributes.IndexedFields[sadefs.TemporalNamespaceDivision]
			require.True(t, ok)
			var actualArchetypeIDStr string
			err := payload.Decode(v, &actualArchetypeIDStr)
			require.NoError(t, err)
			expectedArchetypeID, ok := env.mockShard.ChasmRegistry().ComponentIDFor(&testComponent{})
			require.True(t, ok)
			require.Equal(t, strconv.FormatUint(uint64(expectedArchetypeID), 10), actualArchetypeIDStr)

			var paused bool
			// SearchAttribute now uses field name (TemporalBool01) instead of alias (PausedSA)
			err = payload.Decode(request.SearchAttributes.IndexedFields["TemporalBool01"], &paused)
			require.NoError(t, err)
			require.True(t, paused)

			require.Len(t, request.Memo.Fields, 1)
			err = payload.Decode(request.Memo.Fields[testComponentPausedSAName], &paused)
			require.NoError(t, err)
			require.True(t, paused)

			return nil
		},
	)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func TestProcessChasmTask_ClosedExecution(t *testing.T) {
	env := setupVisibilityQueueTaskExecutorTest(t)
	defer env.teardown()

	key := definition.NewWorkflowKey(
		env.namespaceID.String(),
		"some random ID",
		uuid.New(),
	)
	mutableState := buildChasmMutableState(t, env, key, 5)

	closeTime := env.now.Add(5 * time.Minute)
	mutableState.ExecutionInfo.CloseTime = timestamppb.New(closeTime)
	mutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	mutableState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED

	visibilityTask := buildChasmVisTask(t, env, key, 5)

	env.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	env.mockVisibilityMgr.EXPECT().RecordWorkflowExecutionClosed(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *manager.RecordWorkflowExecutionClosedRequest) error {
			require.True(t, closeTime.Equal(request.CloseTime))
			require.NotEmpty(t, request.ExecutionDuration)
			require.Zero(t, request.HistoryLength)
			require.Zero(t, request.HistorySizeBytes)
			require.NotEmpty(t, request.StateTransitionCount)

			// Other fields are tested in TestProcessChasmTask_RunningExecution
			return nil
		},
	)

	resp := env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, visibilityTask))
	require.Nil(t, resp.ExecutionErr)
}

func buildChasmMutableState(
	t *testing.T,
	env *visibilityQueueTaskExecutorTestEnv,
	key definition.WorkflowKey,
	visComponentTransitionCount int64,
) *persistencespb.WorkflowMutableState {
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:    env.namespaceID.String(),
		WorkflowId:     key.WorkflowID,
		LastUpdateTime: timestamp.TimeNowPtrUtc(),
		StartTime:      timestamppb.Now(),
		ExecutionTime:  timestamppb.Now(),
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: env.version, TransitionCount: 1},
		},
		StateTransitionCount: 10,
	}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:     key.RunID,
		State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: timestamppb.Now(),
	}

	visibilityComponentData := &persistencespb.ChasmVisibilityData{
		TransitionCount: visComponentTransitionCount,
	}
	data, err := visibilityComponentData.Marshal()
	require.NoError(t, err)

	testComponentTypeID, ok := env.mockShard.ChasmRegistry().ComponentIDFor(&testComponent{})
	require.True(t, ok)
	visComponentTypeID, ok := env.mockShard.ChasmRegistry().ComponentIDFor(&chasm.Visibility{})
	require.True(t, ok)

	chasmNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: env.version, TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: env.version, TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
			},
			Data: newTestComponentStateBlob(&persistencespb.ActivityInfo{Paused: true}),
		},
		"Visibility": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: env.version, TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: env.version, TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: visComponentTypeID,
					},
				},
			},
			Data: &commonpb.DataBlob{
				Data:         data,
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
	return &persistencespb.WorkflowMutableState{
		ExecutionInfo:  executionInfo,
		ExecutionState: executionState,
		ChasmNodes:     chasmNodes,
		NextEventId:    common.FirstEventID,
	}
}

func buildChasmVisTask(
	t *testing.T,
	env *visibilityQueueTaskExecutorTestEnv,
	key definition.WorkflowKey,
	taskTransitionCount int64,
) *tasks.ChasmTask {
	visTaskData := &persistencespb.ChasmVisibilityTaskData{
		TransitionCount: taskTransitionCount,
	}
	data, err := visTaskData.Marshal()
	require.NoError(t, err)

	visTaskTypeID, ok := env.mockShard.ChasmRegistry().TaskIDFor(&persistencespb.ChasmVisibilityTaskData{})
	require.True(t, ok)

	return &tasks.ChasmTask{
		WorkflowKey:         key,
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              int64(59),
		Category:            tasks.CategoryVisibility,
		Info: &persistencespb.ChasmTaskInfo{
			ComponentInitialVersionedTransition:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: env.version, TransitionCount: 1},
			ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: env.version, TransitionCount: 1},
			Path:                                   []string{"Visibility"},
			TypeId:                                 visTaskTypeID,
			Data: &commonpb.DataBlob{
				Data:         data,
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}
}

func executeVisibilityTask(env *visibilityQueueTaskExecutorTestEnv, task tasks.Task) error {
	return env.visibilityQueueTaskExecutor.Execute(context.Background(), newTaskExecutableForVisibility(env, task)).ExecutionErr
}

func createVisibilityRequestBase(
	t *testing.T,
	env *visibilityQueueTaskExecutorTestEnv,
	namespaceName namespace.Name,
	task tasks.Task,
	mutableState historyi.MutableState,
	taskQueueName string,
	parentExecution *commonpb.WorkflowExecution,
	rootExecution *commonpb.WorkflowExecution,
	searchAttributes map[string]any,
) *manager.VisibilityRequestBase {
	encodedSearchAttributes, err := searchattribute.Encode(
		searchAttributes,
		&searchattribute.NameTypeMap{},
	)
	require.NoError(t, err)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
	executionInfo := mutableState.GetExecutionInfo()

	if rootExecution == nil {
		if parentExecution != nil {
			rootExecution = parentExecution
		} else {
			rootExecution = &commonpb.WorkflowExecution{
				WorkflowId: execution.WorkflowId,
				RunId:      execution.RunId,
			}
		}
	}

	return &manager.VisibilityRequestBase{
		NamespaceID:      namespace.ID(task.GetNamespaceID()),
		Namespace:        namespaceName,
		Execution:        execution,
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTime:        timestamp.TimeValue(mutableState.GetExecutionState().GetStartTime()),
		Status:           mutableState.GetExecutionState().GetStatus(),
		ExecutionTime:    timestamp.TimeValue(executionInfo.GetExecutionTime()),
		TaskID:           task.GetTaskID(),
		ShardID:          env.mockShard.GetShardID(),
		TaskQueue:        taskQueueName,
		ParentExecution:  parentExecution,
		RootExecution:    rootExecution,
		SearchAttributes: encodedSearchAttributes,
	}
}

func createRecordWorkflowExecutionStartedRequest(
	t *testing.T,
	env *visibilityQueueTaskExecutorTestEnv,
	namespaceName namespace.Name,
	task *tasks.StartExecutionVisibilityTask,
	mutableState historyi.MutableState,
	taskQueueName string,
) gomock.Matcher {
	return protomock.Eq(&manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: createVisibilityRequestBase(
			t,
			env,
			namespaceName,
			task,
			mutableState,
			taskQueueName,
			nil,
			nil,
			nil,
		),
	})
}

func createUpsertWorkflowRequest(
	t *testing.T,
	env *visibilityQueueTaskExecutorTestEnv,
	namespaceName namespace.Name,
	task *tasks.UpsertExecutionVisibilityTask,
	mutableState historyi.MutableState,
	taskQueueName string,
) gomock.Matcher {
	return protomock.Eq(&manager.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: createVisibilityRequestBase(
			t,
			env,
			namespaceName,
			task,
			mutableState,
			taskQueueName,
			nil,
			nil,
			nil,
		),
	})
}

func createRecordWorkflowExecutionClosedRequest(
	t *testing.T,
	env *visibilityQueueTaskExecutorTestEnv,
	namespaceName namespace.Name,
	task *tasks.CloseExecutionVisibilityTask,
	mutableState historyi.MutableState,
	taskQueueName string,
	parentExecution *commonpb.WorkflowExecution,
	rootExecution *commonpb.WorkflowExecution,
	searchAttributes map[string]any,
) gomock.Matcher {
	executionInfo := mutableState.GetExecutionInfo()
	return protomock.Eq(&manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: createVisibilityRequestBase(
			t,
			env,
			namespaceName,
			task,
			mutableState,
			taskQueueName,
			parentExecution,
			rootExecution,
			searchAttributes,
		),
		CloseTime:            timestamp.TimeValue(executionInfo.GetCloseTime()),
		HistoryLength:        mutableState.GetNextEventID() - 1,
		HistorySizeBytes:     executionInfo.GetExecutionStats().GetHistorySize(),
		StateTransitionCount: executionInfo.GetStateTransitionCount(),
	})
}

func createPersistenceMutableState(
	t *testing.T,
	ms historyi.MutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
	require.NoError(t, err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEventID, lastEventVersion,
	))
	require.NoError(t, err)
	return workflow.TestCloneToProto(ms)
}

func newTaskExecutableForVisibility(
	env *visibilityQueueTaskExecutorTestEnv,
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		env.visibilityQueueTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		env.mockShard.GetTimeSource(),
		env.mockShard.GetNamespaceRegistry(),
		env.mockShard.GetClusterMetadata(),
		env.mockShard.ChasmRegistry(),
		queues.GetTaskTypeTagValue,
		nil,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
	)
}

func TestCopyMapPayload(t *testing.T) {
	var input map[string]*commonpb.Payload
	require.Nil(t, copyMapPayload(input))

	key := "key"
	val := payload.EncodeBytes([]byte{'1', '2', '3'})
	input = map[string]*commonpb.Payload{
		key: val,
	}
	result := copyMapPayload(input)
	require.Equal(t, input, result)
	result[key].GetData()[0] = '0'
	require.Equal(t, byte('1'), val.GetData()[0])
}
