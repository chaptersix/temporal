package history

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
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

type engine3TestFixture struct {
	controller              *gomock.Controller
	mockShard               *shard.ContextTest
	mockTxProcessor         *queues.MockQueue
	mockTimerProcessor      *queues.MockQueue
	mockVisibilityProcessor *queues.MockQueue
	mockEventsCache         *events.MockCache
	mockNamespaceCache      *namespace.MockRegistry
	mockClusterMetadata     *cluster.MockMetadata
	workflowCache           wcache.Cache
	historyEngine           *historyEngineImpl
	mockExecutionMgr        *persistence.MockExecutionManager
	mockVisibilityManager   *manager.MockVisibilityManager

	config *configs.Config
	logger log.Logger
}

func setupEngine3Test(t *testing.T) *engine3TestFixture {
	config := tests.NewDynamicConfig()
	controller := gomock.NewController(t)

	mockTxProcessor := queues.NewMockQueue(controller)
	mockTimerProcessor := queues.NewMockQueue(controller)
	mockVisibilityProcessor := queues.NewMockQueue(controller)
	mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	mockExecutionMgr := mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockEventsCache := mockShard.MockEventsCache
	mockVisibilityManager := mockShard.Resource.VisibilityManager

	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()
	logger := mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       mockShard,
		clusterMetadata:    mockClusterMetadata,
		executionManager:   mockExecutionMgr,
		logger:             logger,
		throttledLogger:    logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    tasktoken.NewSerializer(),
		config:             config,
		timeSource:         mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			mockTxProcessor.Category():         mockTxProcessor,
			mockTimerProcessor.Category():      mockTimerProcessor,
			mockVisibilityProcessor.Category(): mockVisibilityProcessor,
		},
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, workflowCache),
		persistenceVisibilityMgr:   mockVisibilityManager,
	}
	mockShard.SetEngineForTesting(h)

	return &engine3TestFixture{
		controller:              controller,
		mockShard:               mockShard,
		mockTxProcessor:         mockTxProcessor,
		mockTimerProcessor:      mockTimerProcessor,
		mockVisibilityProcessor: mockVisibilityProcessor,
		mockEventsCache:         mockEventsCache,
		mockNamespaceCache:      mockNamespaceCache,
		mockClusterMetadata:     mockClusterMetadata,
		workflowCache:           workflowCache,
		historyEngine:           h,
		mockExecutionMgr:        mockExecutionMgr,
		mockVisibilityManager:   mockVisibilityManager,
		config:                  config,
		logger:                  logger,
	}
}

func TestRecordWorkflowTaskStartedSuccessStickyEnabled(t *testing.T) {
	f := setupEngine3Test(t)
	defer f.controller.Finish()
	defer f.mockShard.StopForTest()

	fakeHistory := []*historypb.HistoryEvent{
		{
			EventId:   int64(1),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		},
		{
			EventId:   int64(2),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{
							"Keyword01":             payload.EncodeString("random-keyword"),
							"TemporalChangeVersion": payload.EncodeString("random-data"),
						},
					},
				},
			},
		},
		{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
	}

	f.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	f.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	f.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	f.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	f.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(f.historyEngine.shardContext, f.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	f.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	f.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: we,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{}
	expectedResponse.WorkflowType = ms.GetWorkflowType()
	executionInfo = ms.GetExecutionInfo()
	if executionInfo.LastCompletedWorkflowTaskStartedEventId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastCompletedWorkflowTaskStartedEventId
	}
	expectedResponse.ScheduledEventId = wt.ScheduledEventID
	expectedResponse.ScheduledTime = timestamppb.New(wt.ScheduledTime)
	expectedResponse.StartedEventId = wt.ScheduledEventID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = ms.GetNextEventID() + 1
	expectedResponse.Attempt = wt.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	expectedResponse.BranchToken, _ = ms.GetCurrentBranchToken()
	expectedResponse.History = &historypb.History{Events: fakeHistory}
	expectedResponse.NextPageToken = nil

	response, err := f.historyEngine.RecordWorkflowTaskStarted(context.Background(), &request)
	require.Nil(t, err)
	require.NotNil(t, response)
	require.True(t, response.StartedTime.AsTime().After(expectedResponse.ScheduledTime.AsTime()))
	expectedResponse.StartedTime = response.StartedTime
	require.Equal(t, &expectedResponse, response)
}

func TestRecordWorkflowTaskStartedSuccessStickyEnabled_WithInternalRawHistory(t *testing.T) {
	f := setupEngine3Test(t)
	defer f.controller.Finish()
	defer f.mockShard.StopForTest()

	f.config.SendRawHistoryBetweenInternalServices = func() bool { return true }
	fakeHistory := historypb.History{
		Events: []*historypb.HistoryEvent{
			{
				EventId:   int64(1),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			},
			{
				EventId:   int64(2),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
					WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"Keyword01":             payload.EncodeString("random-keyword"),
								"TemporalChangeVersion": payload.EncodeString("random-data"),
							},
						},
					},
				},
			},
			{
				EventId:   int64(3),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			},
		},
	}
	historyBlob, err := fakeHistory.Marshal()
	require.NoError(t, err)

	f.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         historyBlob,
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	f.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	f.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(f.historyEngine.shardContext, f.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	f.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	f.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	request := historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: we,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: stickyTl,
			},
			Identity: identity,
		},
	}

	expectedResponse := historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{}
	expectedResponse.WorkflowType = ms.GetWorkflowType()
	executionInfo = ms.GetExecutionInfo()
	if executionInfo.LastCompletedWorkflowTaskStartedEventId != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = executionInfo.LastCompletedWorkflowTaskStartedEventId
	}
	expectedResponse.ScheduledEventId = wt.ScheduledEventID
	expectedResponse.ScheduledTime = timestamppb.New(wt.ScheduledTime)
	expectedResponse.StartedEventId = wt.ScheduledEventID + 1
	expectedResponse.StickyExecutionEnabled = true
	expectedResponse.NextEventId = ms.GetNextEventID() + 1
	expectedResponse.Attempt = wt.Attempt
	expectedResponse.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	expectedResponse.BranchToken, _ = ms.GetCurrentBranchToken()
	expectedResponse.RawHistory = [][]byte{historyBlob}
	expectedResponse.NextPageToken = nil

	response, err := f.historyEngine.RecordWorkflowTaskStarted(context.Background(), &request)
	require.Nil(t, err)
	require.NotNil(t, response)
	require.True(t, response.StartedTime.AsTime().After(expectedResponse.ScheduledTime.AsTime()))
	expectedResponse.StartedTime = response.StartedTime
	require.Equal(t, &expectedResponse, response)
}

func TestStartWorkflowExecution_BrandNew(t *testing.T) {
	f := setupEngine3Test(t)
	defer f.controller.Finish()
	defer f.mockShard.StopForTest()

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	f.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	f.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	f.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.New()
	resp, err := f.historyEngine.StartWorkflowExecution(context.Background(), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		},
	})
	require.Nil(t, err)
	require.NotNil(t, resp.RunId)
}

func TestSignalWithStartWorkflowExecution_JustSignal(t *testing.T) {
	f := setupEngine3Test(t)
	defer f.controller.Finish()
	defer f.mockShard.StopForTest()

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	f.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	f.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := f.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	runID := tests.RunID
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:    namespaceID.String(),
			WorkflowId:   workflowID,
			WorkflowType: &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
			Identity:     identity,
			SignalName:   signalName,
			Input:        input,
			RequestId:    requestID,
		},
	}

	ms := workflow.TestLocalMutableState(f.historyEngine.shardContext, f.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	_ = addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	f.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	f.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	f.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := f.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	require.Nil(t, err)
	require.Equal(t, runID, resp.GetRunId())
}

func TestSignalWithStartWorkflowExecution_WorkflowNotExist(t *testing.T) {
	f := setupEngine3Test(t)
	defer f.controller.Finish()
	defer f.mockShard.StopForTest()

	testNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()}, &persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)}, "",
	)
	f.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()
	f.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(testNamespaceEntry, nil).AnyTimes()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := f.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := uuid.New()
	sRequest = &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			SignalName:               signalName,
			Input:                    input,
			RequestId:                requestID,
		},
	}

	notExistErr := serviceerror.NewNotFound("Workflow not exist")

	f.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, notExistErr)
	f.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := f.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	require.Nil(t, err)
	require.NotNil(t, resp.GetRunId())
}
