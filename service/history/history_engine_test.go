package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/getworkflowexecutionrawhistoryv2"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	esIndexName = ""
)

type engineSuiteDeps struct {
	controller                         *gomock.Controller
	mockShard                          *shard.ContextTest
	mockTxProcessor                    *queues.MockQueue
	mockTimerProcessor                 *queues.MockQueue
	mockVisibilityProcessor            *queues.MockQueue
	mockArchivalProcessor              *queues.MockQueue
	mockMemoryScheduledQueue           *queues.MockQueue
	mockOutboundProcessor              *queues.MockQueue
	mockNamespaceCache                 *namespace.MockRegistry
	mockMatchingClient                 *matchingservicemock.MockMatchingServiceClient
	mockHistoryClient                  *historyservicemock.MockHistoryServiceClient
	mockClusterMetadata                *cluster.MockMetadata
	mockEventsReapplier                *ndc.MockEventsReapplier
	mockWorkflowResetter               *ndc.MockWorkflowResetter
	mockErrorHandler                   *interceptor.MockErrorHandler
	workflowCache                      wcache.Cache
	historyEngine                      *historyEngineImpl
	mockExecutionMgr                   *persistence.MockExecutionManager
	mockShardManager                   *persistence.MockShardManager
	mockVisibilityMgr                  *manager.MockVisibilityManager
	mockSearchAttributesProvider       *searchattribute.MockProvider
	mockSearchAttributesMapperProvider *searchattribute.MockMapperProvider
	eventsCache                        events.Cache
	config                             *configs.Config
}

func setupEngineTest(t *testing.T) *engineSuiteDeps {
	t.Helper()
	deps := &engineSuiteDeps{}

	deps.controller = gomock.NewController(t)
	deps.mockEventsReapplier = ndc.NewMockEventsReapplier(deps.controller)
	deps.mockWorkflowResetter = ndc.NewMockWorkflowResetter(deps.controller)
	deps.mockErrorHandler = interceptor.NewMockErrorHandler(deps.controller)
	deps.mockTxProcessor = queues.NewMockQueue(deps.controller)
	deps.mockTimerProcessor = queues.NewMockQueue(deps.controller)
	deps.mockVisibilityProcessor = queues.NewMockQueue(deps.controller)
	deps.mockArchivalProcessor = queues.NewMockQueue(deps.controller)
	deps.mockMemoryScheduledQueue = queues.NewMockQueue(deps.controller)
	deps.mockOutboundProcessor = queues.NewMockQueue(deps.controller)
	deps.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	deps.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	deps.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	deps.mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	deps.mockMemoryScheduledQueue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
	deps.mockOutboundProcessor.EXPECT().Category().Return(tasks.CategoryOutbound).AnyTimes()
	deps.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockMemoryScheduledQueue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockOutboundProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	deps.config = tests.NewDynamicConfig()
	deps.mockShard = shard.NewTestContext(
		deps.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		deps.config,
	)
	deps.workflowCache = wcache.NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	deps.eventsCache = events.NewHostLevelEventsCache(
		deps.mockShard.GetExecutionManager(),
		deps.mockShard.GetConfig(),
		deps.mockShard.GetMetricsHandler(),
		deps.mockShard.GetLogger(),
		false,
	)
	deps.mockShard.SetEventsCacheForTesting(s.eventsCache)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	deps.mockShard.SetStateMachineRegistry(reg)

	deps.mockMatchingClient = deps.mockShard.Resource.MatchingClient
	deps.mockHistoryClient = deps.mockShard.Resource.HistoryClient
	deps.mockExecutionMgr = deps.mockShard.Resource.ExecutionMgr
	deps.mockShardManager = deps.mockShard.Resource.ShardMgr
	deps.mockVisibilityMgr = deps.mockShard.Resource.VisibilityManager
	deps.mockSearchAttributesProvider = deps.mockShard.Resource.SearchAttributesProvider
	deps.mockSearchAttributesMapperProvider = deps.mockShard.Resource.SearchAttributesMapperProvider
	deps.mockClusterMetadata = deps.mockShard.Resource.ClusterMetadata
	deps.mockNamespaceCache = deps.mockShard.Resource.NamespaceCache
	deps.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	eventNotifier := events.NewNotifier(
		clock.NewRealTimeSource(),
		deps.mockShard.Resource.MetricsHandler,
		func(namespaceID namespace.ID, workflowID string) int32 {
			key := namespaceID.String() + "_" + workflowID
			return int32(len(key))
		},
	)

	h := &historyEngineImpl{
		currentClusterName: deps.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       deps.mockShard,
		clusterMetadata:    deps.mockClusterMetadata,
		executionManager:   deps.mockExecutionMgr,
		logger:             deps.mockShard.GetLogger(),
		metricsHandler:     deps.mockShard.GetMetricsHandler(),
		tokenSerializer:    tasktoken.NewSerializer(),
		eventNotifier:      eventNotifier,
		config:             deps.config,
		queueProcessors: map[tasks.Category]queues.Queue{
			deps.mockTxProcessor.Category():          deps.mockTxProcessor,
			deps.mockTimerProcessor.Category():       deps.mockTimerProcessor,
			deps.mockVisibilityProcessor.Category():  deps.mockVisibilityProcessor,
			deps.mockArchivalProcessor.Category():    deps.mockArchivalProcessor,
			deps.mockMemoryScheduledQueue.Category(): deps.mockMemoryScheduledQueue,
			deps.mockOutboundProcessor.Category():    deps.mockOutboundProcessor,
		},
		eventsReapplier:            deps.mockEventsReapplier,
		workflowResetter:           deps.mockWorkflowResetter,
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(deps.mockShard, deps.workflowCache),
		throttledLogger:            log.NewNoopLogger(),
		persistenceVisibilityMgr:   deps.mockVisibilityMgr,
		versionChecker:             headers.NewDefaultVersionChecker(),
	}
	deps.mockShard.SetEngineForTesting(h)

	h.eventNotifier.Start()

	deps.historyEngine = h

	return deps
}

func TestGetMutableStateSync(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry,
		execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// test get the next event ID instantly
	response, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: tests.NamespaceID.String(),
		Execution:   &execution,
	})
	require.Nil(t, err)
	require.Equal(t, int64(4), response.GetNextEventId())
	require.Equal(t, tests.RunID, response.GetFirstExecutionRunId())

}

func TestGetMutableState_IntestRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      "run-id-not-valid-uuid",
	}

	_, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: tests.NamespaceID.String(),
		Execution:   &execution,
	})
	require.Equal(t, errRunIDNotValid, err)

}

func TestGetMutableState_EmptyRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
	}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: tests.NamespaceID.String(),
		Execution:   &execution,
	})
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestGetMutableStateLongPoll(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	ctx := context.Background()

	namespaceID := tests.NamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry,
		execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// test long poll on next event ID change
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asycWorkflowUpdate := func(delay time.Duration) {
		tt := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID.String(),
			WorkflowId:       execution.WorkflowId,
			RunId:            execution.RunId,
			ScheduledEventId: 2,
		}
		taskToken, _ := tt.Marshal()
		deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		timer := time.NewTimer(delay)

		<-timer.C
		_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
				Identity:  identity,
			},
		})
		require.Nil(t, err)
		waitGroup.Done()
		// right now the next event ID is 5
	}

	// return immediately, since the expected next event ID appears
	response, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           &execution,
		ExpectedNextEventId: 3,
	})
	require.Nil(t, err)
	require.Equal(t, int64(4), response.NextEventId)

	// long poll, new event happen before long poll timeout
	go asycWorkflowUpdate(time.Second)
	start := time.Now().UTC()
	pollResponse, err := deps.historyEngine.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           &execution,
		ExpectedNextEventId: 4,
	})
	require.True(t, time.Now().UTC().After(start.Add(time.Second)))
	require.Nil(t, err)
	require.Equal(t, int64(5), pollResponse.GetNextEventId())
	waitGroup.Wait()

}

func TestGetMutableStateLongPoll_CurrentBranchChanged(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	ctx := context.Background()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(
		deps.historyEngine.shardContext,
		deps.eventsCache,
		tests.LocalNamespaceEntry,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		log.NewTestLogger(),
	)
	addWorkflowExecutionStartedEvent(ms, execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	// test long poll on next event ID change
	asyncBranchTokenUpdate := func(delay time.Duration) {
		timer := time.NewTimer(delay)
		<-timer.C
		deps.historyEngine.eventNotifier.NotifyNewHistoryEvent(events.NewNotification(
			ms.GetWorkflowKey().NamespaceID,
			execution,
			int64(1),
			int64(0),
			int64(12),
			int64(1),
			enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			ms.GetExecutionInfo().GetVersionHistories(),
			ms.GetExecutionInfo().GetTransitionHistory()),
		)
	}

	// return immediately, since the expected next event ID appears
	response0, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           execution,
		ExpectedNextEventId: 3,
	})
	require.Nil(t, err)
	require.Equal(t, int64(4), response0.GetNextEventId())

	// long poll, new event happen before long poll timeout
	go asyncBranchTokenUpdate(time.Second)
	start := time.Now().UTC()
	response1, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           execution,
		ExpectedNextEventId: 10,
	})
	require.True(t, time.Now().UTC().After(start.Add(time.Second)))
	require.Nil(t, err)
	require.Equal(t, response0.GetCurrentBranchToken(), response1.GetCurrentBranchToken())

}

func TestGetMutableStateLongPollTimeout(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	ctx := context.Background()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "test-get-workflow-execution-event-id",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	// right now the next event ID is 4
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	deps.mockShard.GetConfig().LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second)

	// long poll, no event happen after long poll timeout
	response, err := deps.historyEngine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         tests.NamespaceID.String(),
		Execution:           &execution,
		ExpectedNextEventId: 4,
	})
	require.Nil(t, err)
	require.Equal(t, int64(4), response.GetNextEventId())

}

func TestQueryWorkflow_RejectBasedOnCompleted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_RejectBasedOnCompleted",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	event := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	addCompleteWorkflowEvent(ms, event.GetEventId(), nil)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &querypb.WorkflowQuery{},
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}
	resp, err := deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.NoError(t, err)
	require.Nil(t, resp.GetResponse().QueryResult)
	require.NotNil(t, resp.GetResponse().QueryRejected)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, resp.GetResponse().GetQueryRejected().GetStatus())

}

func TestQueryWorkflow_RejectBasedOnFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_RejectBasedOnFailed",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	event := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	addFailWorkflowEvent(ms, event.GetEventId(), failure.NewServerFailure("failure reason", true), enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE)
	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &querypb.WorkflowQuery{},
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}
	resp, err := deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.NoError(t, err)
	require.Nil(t, resp.GetResponse().QueryResult)
	require.NotNil(t, resp.GetResponse().QueryRejected)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resp.GetResponse().GetQueryRejected().GetStatus())

	request = &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution:            &execution,
			Query:                &querypb.WorkflowQuery{},
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY,
		},
	}
	resp, err = deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.NoError(t, err)
	require.Nil(t, resp.GetResponse().QueryResult)
	require.NotNil(t, resp.GetResponse().QueryRejected)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resp.GetResponse().GetQueryRejected().GetStatus())

}

func TestQueryWorkflow_DirectlyThroughMatching(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_DirectlyThroughMatching",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)
	deps.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&matchingservice.QueryWorkflowResponse{QueryResult: payloads.EncodeBytes([]byte{1, 2, 3})}, nil)
	deps.historyEngine.matchingClient = deps.mockMatchingClient
	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}
	resp, err := deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp.GetResponse().QueryResult)
	require.Nil(t, resp.GetResponse().QueryRejected)

	var queryResult []byte
	err = payloads.Decode(resp.GetResponse().GetQueryResult(), &queryResult)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, queryResult)

}

func TestQueryWorkflow_WorkflowTaskDispatch_Timeout(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_WorkflowTaskDispatch_Timeout",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)
	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
			// since workflow is open this filter does not reject query
			QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		resp, err := deps.historyEngine.QueryWorkflow(ctx, request)
		require.Error(t, err)
		require.Nil(t, resp)
		wg.Done()
	}()

	time.Sleep(time.Second)
	ms1 := deps.getMutableState(tests.NamespaceID, &execution)
	require.NotNil(t, ms1)
	qr := ms1.GetQueryRegistry()
	require.True(t, qr.HasBufferedQuery())
	require.False(t, qr.HasCompletedQuery())
	require.False(t, qr.HasUnblockedQuery())
	require.False(t, qr.HasFailedQuery())
	wg.Wait()
	require.False(t, qr.HasBufferedQuery())
	require.False(t, qr.HasCompletedQuery())
	require.False(t, qr.HasUnblockedQuery())
	require.False(t, qr.HasFailedQuery())

}

func TestQueryWorkflow_ConsistentQueryBufferFull(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_ConsistentQueryBufferFull",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	ctx, release, err := deps.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		tests.NamespaceID,
		&execution,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	loadedMS, err := ctx.LoadMutableState(context.Background(), deps.mockShard)
	require.NoError(t, err)

	// buffer query so that when historyEngine.QueryWorkflow() is called buffer is already full
	qr := workflow.NewQueryRegistry()
	queryId, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	loadedMS.(*workflow.MutableStateImpl).QueryRegistry = qr
	release(nil)

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
		},
	}
	resp, err := deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.Nil(t, resp)
	require.Equal(t, consts.ErrConsistentQueryBufferExceeded, err)

	// verify that after last query error, the previous pending query is still in the buffer
	pendingBufferedQueries := qr.GetBufferedIDs()
	require.Equal(t, 1, len(pendingBufferedQueries))
	require.Equal(t, queryId, pendingBufferedQueries[0])

}

func TestQueryWorkflow_WorkflowTaskDispatch_Complete(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_WorkflowTaskDispatch_Complete",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		time.Sleep(delay)
		ms1 := deps.getMutableState(tests.NamespaceID, &execution)
		require.NotNil(t, ms1)
		qr := ms1.GetQueryRegistry()
		buffered := qr.GetBufferedIDs()
		for _, id := range buffered {
			resultType := enumspb.QUERY_RESULT_TYPE_ANSWERED
			succeededCompletionState := &historyi.QueryCompletionState{
				Type: workflow.QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: resultType,
					Answer:     payloads.EncodeBytes(answer),
				},
			}
			err := qr.SetCompletionState(id, succeededCompletionState)
			require.NoError(t, err)
			state, err := qr.GetCompletionState(id)
			require.NoError(t, err)
			require.Equal(t, workflow.QueryCompletionTypeSucceeded, state.Type)
		}
	}

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now().UTC()
	resp, err := deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.True(t, time.Now().UTC().After(start.Add(time.Second)))
	require.NoError(t, err)

	var queryResult []byte
	err = payloads.Decode(resp.GetResponse().GetQueryResult(), &queryResult)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, queryResult)

	ms1 := deps.getMutableState(tests.NamespaceID, &execution)
	require.NotNil(t, ms1)
	qr := ms1.GetQueryRegistry()
	require.False(t, qr.HasBufferedQuery())
	require.False(t, qr.HasCompletedQuery())
	waitGroup.Wait()

}

func TestQueryWorkflow_WorkflowTaskDispatch_Unblocked(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "TestQueryWorkflow_WorkflowTaskDispatch_Unblocked",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache, tests.LocalNamespaceEntry, execution.GetWorkflowId(), execution.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)
	wt = addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskqueue, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gweResponse, nil)
	deps.mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&matchingservice.QueryWorkflowResponse{QueryResult: payloads.EncodeBytes([]byte{1, 2, 3})}, nil)
	deps.historyEngine.matchingClient = deps.mockMatchingClient
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	asyncQueryUpdate := func(delay time.Duration, answer []byte) {
		defer waitGroup.Done()
		time.Sleep(delay)
		ms1 := deps.getMutableState(tests.NamespaceID, &execution)
		require.NotNil(t, ms1)
		qr := ms1.GetQueryRegistry()
		buffered := qr.GetBufferedIDs()
		for _, id := range buffered {
			require.NoError(t, qr.SetCompletionState(id, &historyi.QueryCompletionState{Type: workflow.QueryCompletionTypeUnblocked}))
			state, err := qr.GetCompletionState(id)
			require.NoError(t, err)
			require.Equal(t, workflow.QueryCompletionTypeUnblocked, state.Type)
		}
	}

	request := &historyservice.QueryWorkflowRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Execution: &execution,
			Query:     &querypb.WorkflowQuery{},
		},
	}
	go asyncQueryUpdate(time.Second*2, []byte{1, 2, 3})
	start := time.Now().UTC()
	resp, err := deps.historyEngine.QueryWorkflow(context.Background(), request)
	require.True(t, time.Now().UTC().After(start.Add(time.Second)))
	require.NoError(t, err)

	var queryResult []byte
	err = payloads.Decode(resp.GetResponse().GetQueryResult(), &queryResult)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, queryResult)

	ms1 := deps.getMutableState(tests.NamespaceID, &execution)
	require.NotNil(t, ms1)
	qr := ms1.GetQueryRegistry()
	require.False(t, qr.HasBufferedQuery())
	require.False(t, qr.HasCompletedQuery())
	require.False(t, qr.HasUnblockedQuery())
	waitGroup.Wait()

}

func TestRespondWorkflowTaskCompletedInvalidToken(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: invalidToken,
			Commands:  nil,
			Identity:  identity,
		},
	})

	require.NotNil(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)

}

func TestRespondWorkflowTaskCompletedIfNoExecution(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondWorkflowTaskCompletedIfGetExecutionFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("FAILED"))

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "FAILED")

}

func TestRespondWorkflowTaskCompletedUpdateExecutionFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tq, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, errors.New("FAILED"))
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes() // might be called in background goroutine

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.EqualError(t, err, "FAILED")

}

func TestRespondWorkflowTaskCompletedIfTaskCompleted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	startedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tq, identity)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, startedEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondWorkflowTaskCompletedIfTaskNotStarted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondWorkflowTaskCompletedConflictOnUpdate(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tq := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := payloads.EncodeString("input2")
	activity2Result := payloads.EncodeString("activity2_result")
	activity3ID := "activity3"
	activity3Type := "activity_type3"
	activity3Input := payloads.EncodeString("input3")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tq, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tq, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tq, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity2ID, activity2Type, tq, activity2Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(ms, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tq, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             activity3ID,
			ActivityType:           &commonpb.ActivityType{Name: activity3Type},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
			Input:                  activity3Input,
			ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
			ScheduleToStartTimeout: durationpb.New(10 * time.Second),
			StartToCloseTimeout:    durationpb.New(50 * time.Second),
			HeartbeatTimeout:       durationpb.New(5 * time.Second),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	addActivityTaskCompletedEvent(ms, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, &persistence.ConditionFailedError{})

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.Equal(t, &persistence.ConditionFailedError{}, err)

}

func TestValidateSignalRequest(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	workflowType := "testType"
	input := payloads.EncodeString("input")
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               "ID",
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: "taskptr"},
		Input:                    input,
		WorkflowExecutionTimeout: durationpb.New(20 * time.Second),
		WorkflowRunTimeout:       durationpb.New(10 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
		Identity:                 "identity",
	}
	err := api.ValidateStartWorkflowExecutionRequest(
		context.Background(), startRequest, deps.historyEngine.shardContext, tests.LocalNamespaceEntry, "SignalWithStartWorkflowExecution")
	require.Error(t, err, "startRequest doesn't have request id, it should error out")

	startRequest.RequestId = "request-id"
	startRequest.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{
		"data": payload.EncodeBytes(make([]byte, 4*1024*1024)),
	}}
	err = api.ValidateStartWorkflowExecutionRequest(
		context.Background(), startRequest, deps.historyEngine.shardContext, tests.LocalNamespaceEntry, "SignalWithStartWorkflowExecution")
	require.Error(t, err, "memo should be too big")

}

func TestRespondWorkflowTaskCompleted_StaleCache(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	tt.ScheduledEventId = 4 // Set it to 4 to emulate stale cache.

	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "activity1",
			ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
			Input:                  input,
			ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
			ScheduleToStartTimeout: durationpb.New(10 * time.Second),
			StartToCloseTimeout:    durationpb.New(50 * time.Second),
			HeartbeatTimeout:       durationpb.New(5 * time.Second),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondWorkflowTaskCompletedCompleteWorkflowFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := payloads.EncodeString("input2")
	activity2Result := payloads.EncodeString("activity2_result")
	workflowResult := payloads.EncodeString("workflow result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tl, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity2ID, activity2Type, tl, activity2Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(ms, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)
	addActivityTaskCompletedEvent(ms, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: workflowResult,
		}},
	}}

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	ms2 := common.CloneProto(ms1)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)

	var updatedWorkflowMutation persistence.WorkflowMutation
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "UnhandledCommand", err.Error())

	require.NotNil(t, updatedWorkflowMutation)
	require.Equal(t, int64(15), updatedWorkflowMutation.NextEventID)
	require.Equal(t, workflowTaskStartedEvent1.EventId, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	require.Equal(t, updatedWorkflowMutation.NextEventID-1, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId)
	require.Equal(t, int32(1), updatedWorkflowMutation.ExecutionInfo.Attempt)

}

func TestRespondWorkflowTaskCompletedFailWorkflowFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := payloads.EncodeString("input2")
	activity2Result := payloads.EncodeString("activity2_result")
	reason := "workflow fail reason"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tl, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity2ID, activity2Type, tl, activity2Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(ms, activity2ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)
	addActivityTaskCompletedEvent(ms, activity2ScheduledEvent.EventId,
		activity2StartedEvent.EventId, activity2Result, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure.NewServerFailure(reason, false),
		}},
	}}

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	ms2 := common.CloneProto(ms1)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)

	var updatedWorkflowMutation persistence.WorkflowMutation
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "UnhandledCommand", err.Error())

	require.NotNil(t, updatedWorkflowMutation)
	require.Equal(t, int64(15), updatedWorkflowMutation.NextEventID)
	require.Equal(t, workflowTaskStartedEvent1.EventId, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	require.Equal(t, updatedWorkflowMutation.NextEventID-1, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId)
	require.Equal(t, int32(1), updatedWorkflowMutation.ExecutionInfo.Attempt)

}

func TestRespondWorkflowTaskCompletedBadCommandAttributes(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := payloads.EncodeString("input1")
	activity1Result := payloads.EncodeString("activity1_result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	wt1 := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent1 := addWorkflowTaskStartedEvent(ms, wt1.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent1 := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt1.ScheduledEventID, workflowTaskStartedEvent1.EventId, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent1.EventId, activity1ID, activity1Type, tl, activity1Input, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activity1StartedEvent := addActivityTaskStartedEvent(ms, activity1ScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activity1ScheduledEvent.EventId,
		activity1StartedEvent.EventId, activity1Result, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken, _ := tt.Marshal()

	// commands with nil attributes
	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
	}}

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "BadCompleteWorkflowExecutionAttributes: CompleteWorkflowExecutionCommandAttributes is not set on CompleteWorkflowExecutionCommand.", err.Error())

}

// This test unit tests the activity schedule timeout validation logic of HistoryEngine's RespondWorkflowTaskComplete function.
// A ScheduleActivityTask command and the corresponding ActivityTaskScheduledEvent have 3 timeouts: ScheduleToClose, ScheduleToStart and StartToClose.
// This test verifies that when either ScheduleToClose or ScheduleToStart and StartToClose are specified,
// HistoryEngine's validateActivityScheduleAttribute will deduce the missing timeout and fill it in
// instead of returning a BadRequest error and only when all three are missing should a BadRequest be returned.
func TestRespondWorkflowTaskCompletedSingleActivityScheduledAttribute(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	runTimeout := int32(100)
	testIterationVariables := []struct {
		scheduleToClose         int32
		scheduleToStart         int32
		startToClose            int32
		heartbeat               int32
		expectedScheduleToClose int32
		expectedScheduleToStart int32
		expectedStartToClose    int32
		expectWorkflowTaskFail  bool
	}{
		// No ScheduleToClose timeout, will use runTimeout
		{0, 3, 7, 0,
			runTimeout, 3, 7, false},
		// Has ScheduleToClose timeout but not ScheduleToStart or StartToClose,
		// will use ScheduleToClose for ScheduleToStart and StartToClose
		{7, 0, 0, 0,
			7, 7, 7, false},
		// Only StartToClose timeout
		{0, 0, 7, 0,
			runTimeout, runTimeout, 7, false},
		// No ScheduleToClose timeout, ScheduleToStart or StartToClose, expect error return
		{0, 0, 0, 0,
			0, 0, 0, true},
		// Negative ScheduleToClose, expect error return
		{-1, 0, 0, 0,
			0, 0, 0, true},
		// Negative ScheduleToStart, expect error return
		{0, -1, 0, 0,
			0, 0, 0, true},
		// Negative StartToClose, expect error return
		{0, 0, -1, 0,
			0, 0, 0, true},
		// Negative HeartBeat, expect error return
		{0, 0, 0, -1,
			0, 0, 0, true},
		// Use workflow timeout
		{runTimeout, 0, 0, 0,
			runTimeout, runTimeout, runTimeout, false},
		// Timeout larger than workflow timeout
		{runTimeout + 1, 0, 0, 0,
			runTimeout, runTimeout, runTimeout, false},
		{0, runTimeout + 1, 0, 0,
			0, 0, 0, true},
		{0, 0, runTimeout + 1, 0,
			runTimeout, runTimeout, runTimeout, false},
		{0, 0, 0, runTimeout + 1,
			0, 0, 0, true},
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose, but exceed limit
		{0, runTimeout, 10, 0,
			runTimeout, runTimeout, 10, false},
	}

	for _, iVar := range testIterationVariables {
		namespaceID := tests.NamespaceID
		we := commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}
		tl := "testTaskQueue"
		tt := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID.String(),
			WorkflowId:       tests.WorkflowID,
			RunId:            we.GetRunId(),
			ScheduledEventId: 2,
		}
		taskToken, _ := tt.Marshal()
		identity := "testIdentity"
		input := payloads.EncodeString("input")

		ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
			tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
		addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), time.Duration(runTimeout*10)*time.Second, time.Duration(runTimeout)*time.Second, 200*time.Second, identity)
		wt := addWorkflowTaskScheduledEvent(ms)
		addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

		commands := []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: durationpb.New(time.Duration(iVar.scheduleToClose) * time.Second),
				ScheduleToStartTimeout: durationpb.New(time.Duration(iVar.scheduleToStart) * time.Second),
				StartToCloseTimeout:    durationpb.New(time.Duration(iVar.startToClose) * time.Second),
				HeartbeatTimeout:       durationpb.New(time.Duration(iVar.heartbeat) * time.Second),
			}},
		}}

		gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
		deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
		ms2 := workflow.TestCloneToProto(ms)
		if iVar.expectWorkflowTaskFail {
			gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
			deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
		}

		var updatedWorkflowMutation persistence.WorkflowMutation
		deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			updatedWorkflowMutation = request.UpdateWorkflowMutation
			return tests.UpdateWorkflowExecutionResponse, nil
		})

		_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tests.NamespaceID.String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: taskToken,
				Commands:  commands,
				Identity:  identity,
			},
		})

		if !iVar.expectWorkflowTaskFail {
			require.NoError(t, err)
			ms := deps.getMutableState(tests.NamespaceID, &we)
			require.Equal(t, int64(6), ms.GetNextEventID())
			require.Equal(t, int64(3), ms.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
			require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms.GetExecutionState().State)
			require.False(t, ms.HasPendingWorkflowTask())

			activity1Attributes := deps.getActivityScheduledEvent(ms, int64(5)).GetActivityTaskScheduledEventAttributes()
			require.Equal(t, time.Duration(iVar.expectedScheduleToClose)*time.Second, timestamp.DurationValue(activity1Attributes.GetScheduleToCloseTimeout()), iVar)
			require.Equal(t, time.Duration(iVar.expectedScheduleToStart)*time.Second, timestamp.DurationValue(activity1Attributes.GetScheduleToStartTimeout()), iVar)
			require.Equal(t, time.Duration(iVar.expectedStartToClose)*time.Second, timestamp.DurationValue(activity1Attributes.GetStartToCloseTimeout()), iVar)
		} else {
			require.Error(t, err)
			require.IsType(t, &serviceerror.InvalidArgument{}, err)
			require.True(t, strings.HasPrefix(err.Error(), "BadScheduleActivityAttributes"), err.Error())
			require.NotNil(t, updatedWorkflowMutation)
			require.Equal(t, int64(5), updatedWorkflowMutation.NextEventID, iVar)
			require.Equal(t, common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId, iVar)
			require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State, iVar)
			require.True(t, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID, iVar)
		}
		deps.TearDownTest()
		deps.SetupTest()
	}

}

func TestRespondWorkflowTaskCompletedBadBinary(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ns := tests.LocalNamespaceEntry.Clone(
		namespace.WithID(uuid.New()),
		namespace.WithBadBinary("test-bad-binary"),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(ns.ID()).Return(ns, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespace(ns.ID()).Return(ns, nil).AnyTimes()
	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		ns, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	var commands []*commandpb.Command

	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	ms2 := workflow.TestCloneToProto(ms)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: ns.ID().String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:      taskToken,
			Commands:       commands,
			Identity:       identity,
			BinaryChecksum: "test-bad-binary",
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "BadBinary: binary test-bad-binary is marked as bad deployment", err.Error())

	require.NotNil(t, updatedWorkflowMutation)
	require.Equal(t, int64(5), updatedWorkflowMutation.NextEventID)
	require.Equal(t, common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	require.True(t, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID)

}

func TestRespondWorkflowTaskCompletedSingleActivityScheduledWorkflowTask(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "activity1",
			ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
			Input:                  input,
			ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
			ScheduleToStartTimeout: durationpb.New(10 * time.Second),
			StartToCloseTimeout:    durationpb.New(50 * time.Second),
			HeartbeatTimeout:       durationpb.New(5 * time.Second),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

	activity1Attributes := deps.getActivityScheduledEvent(ms2, int64(5)).GetActivityTaskScheduledEventAttributes()
	require.Equal(t, "activity1", activity1Attributes.ActivityId)
	require.Equal(t, "activity_type1", activity1Attributes.ActivityType.Name)
	require.Equal(t, int64(4), activity1Attributes.WorkflowTaskCompletedEventId)
	require.Equal(t, tl, activity1Attributes.TaskQueue.Name)
	require.Equal(t, input, activity1Attributes.Input)
	require.Equal(t, 90*time.Second, timestamp.DurationValue(activity1Attributes.ScheduleToCloseTimeout)) // runTimeout
	require.Equal(t, 10*time.Second, timestamp.DurationValue(activity1Attributes.ScheduleToStartTimeout))
	require.Equal(t, 50*time.Second, timestamp.DurationValue(activity1Attributes.StartToCloseTimeout))
	require.Equal(t, 5*time.Second, timestamp.DurationValue(activity1Attributes.HeartbeatTimeout))

}

func TestRespondWorkflowTaskCompleted_SignalTaskGeneration(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	resp := deps.testRespondWorkflowTaskCompletedSignalGeneration()
	require.NotNil(t, resp.GetStartedResponse())

}

func (s *engineSuite) testRespondWorkflowTaskCompletedSignalGeneration() *historyservice.RespondWorkflowTaskCompletedResponse {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	signal := workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         tests.NamespaceID.String(),
		WorkflowExecution: &we,
		Identity:          identity,
		SignalName:        "test signal name",
		Input:             payloads.EncodeString("test input"),
		RequestId:         uuid.New(),
	}
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId:   tests.NamespaceID.String(),
		SignalRequest: &signal,
	}

	ms := workflow.TestLocalMutableState(s.historyEngine.shardContext, s.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).AnyTimes()

	_, err := s.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.NoError(err)

	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{}}, nil)

	var commands []*commandpb.Command
	resp, err := s.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:             taskToken,
			Commands:              commands,
			Identity:              identity,
			ReturnNewWorkflowTask: true,
		},
	})
	s.NoError(err)
	s.NotNil(resp)

	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms.GetExecutionState().State)

	return resp
}

func TestRespondWorkflowTaskCompleted_ActivityEagerExecution_NotCancelled(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	scheduleToCloseTimeout := durationpb.New(90 * time.Second)
	scheduleToStartTimeout := durationpb.New(10 * time.Second)
	startToCloseTimeout := durationpb.New(50 * time.Second)
	heartbeatTimeout := durationpb.New(5 * time.Second)
	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  false,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity2",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type2"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  true,
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(7), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

	ai1, ok := ms2.GetActivityByActivityID("activity1")
	require.True(t, ok)
	require.Equal(t, common.EmptyEventID, ai1.StartedEventId)

	ai2, ok := ms2.GetActivityByActivityID("activity2")
	require.True(t, ok)
	require.Equal(t, common.TransientEventID, ai2.StartedEventId)
	deps.NotZero(ai2.StartedTime)

	scheduledEvent := deps.getActivityScheduledEvent(ms2, ai2.ScheduledEventId)

	require.Len(t, resp.ActivityTasks, 1)
	activityTask := resp.ActivityTasks[0]
	require.Equal(t, "activity2", activityTask.ActivityId)
	require.Equal(t, "activity_type2", activityTask.ActivityType.GetName())
	require.Equal(t, input, activityTask.Input)
	protorequire.ProtoEqual(t, &we, activityTask.WorkflowExecution)
	require.Equal(t, scheduledEvent.EventTime, activityTask.CurrentAttemptScheduledTime)
	require.Equal(t, scheduledEvent.EventTime, activityTask.ScheduledTime)
	require.Equal(t, scheduleToCloseTimeout.AsDuration(), activityTask.ScheduleToCloseTimeout.AsDuration())
	protorequire.ProtoEqual(t, startToCloseTimeout, activityTask.StartToCloseTimeout)
	protorequire.ProtoEqual(t, heartbeatTimeout, activityTask.HeartbeatTimeout)
	require.Equal(t, int32(1), activityTask.Attempt)
	require.Nil(t, activityTask.HeartbeatDetails)
	require.Equal(t, tests.LocalNamespaceEntry.Name().String(), activityTask.WorkflowNamespace)

}

func TestRespondWorkflowTaskCompleted_ActivityEagerExecution_Cancelled(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	scheduleToCloseTimeout := durationpb.New(90 * time.Second)
	scheduleToStartTimeout := durationpb.New(10 * time.Second)
	startToCloseTimeout := durationpb.New(50 * time.Second)
	heartbeatTimeout := durationpb.New(5 * time.Second)
	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  true,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
			Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
				ScheduledEventId: 5,
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	deps.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	deps.mockSearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
	deps.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()
	deps.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{}}, nil)

	resp, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:             taskToken,
			Commands:              commands,
			Identity:              identity,
			ReturnNewWorkflowTask: true,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(10), ms2.GetNextEventID()) // activity scheduled, request cancel, cancelled, workflow task scheduled, started
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.True(t, ms2.HasPendingWorkflowTask())

	_, ok := ms2.GetActivityByActivityID("activity1")
	require.False(t, ok)

	require.Len(t, resp.ActivityTasks, 0)
	require.NotNil(t, resp.StartedResponse)
	require.Equal(t, int64(10), resp.StartedResponse.NextEventId)
	require.Equal(t, int64(3), resp.StartedResponse.PreviousStartedEventId)

}

func TestRespondWorkflowTaskCompleted_ActivityEagerExecution_WorkflowClosed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	input := payloads.EncodeString("input")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 90*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	scheduleToCloseTimeout := durationpb.New(90 * time.Second)
	scheduleToStartTimeout := durationpb.New(10 * time.Second)
	startToCloseTimeout := durationpb.New(50 * time.Second)
	heartbeatTimeout := durationpb.New(5 * time.Second)
	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "activity1",
				ActivityType:           &commonpb.ActivityType{Name: "activity_type1"},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
				Input:                  input,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				ScheduleToStartTimeout: scheduleToStartTimeout,
				StartToCloseTimeout:    startToCloseTimeout,
				HeartbeatTimeout:       heartbeatTimeout,
				RequestEagerExecution:  true,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("complete"),
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:             taskToken,
			Commands:              commands,
			Identity:              identity,
			ReturnNewWorkflowTask: true,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(7), ms2.GetNextEventID()) // activity scheduled, workflow completed
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

	activityInfo, ok := ms2.GetActivityByActivityID("activity1")
	require.True(t, ok)
	require.Equal(t, int64(5), activityInfo.ScheduledEventId)          // activity scheduled
	require.Equal(t, common.EmptyEventID, activityInfo.StartedEventId) // activity not started

	require.Len(t, resp.ActivityTasks, 0)
	require.Nil(t, resp.StartedResponse)

}

func TestRespondWorkflowTaskCompleted_WorkflowTaskHeartbeatTimeout(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	ms.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = timestamppb.New(time.Now().UTC().Add(-time.Hour))

	var commands []*commandpb.Command

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			TaskToken:                  taskToken,
			Commands:                   commands,
			Identity:                   identity,
		},
	})
	require.Error(t, err, "workflow task heartbeat timeout")

}

func TestRespondWorkflowTaskCompleted_WorkflowTaskHeartbeatNotTimeout(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	ms.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = timestamppb.New(time.Now().UTC().Add(-time.Minute))

	var commands []*commandpb.Command

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			TaskToken:                  taskToken,
			Commands:                   commands,
			Identity:                   identity,
		},
	})
	require.Nil(t, err)

}

func TestRespondWorkflowTaskCompleted_WorkflowTaskHeartbeatNotTimeout_ZeroOrignalScheduledTime(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	ms.GetExecutionInfo().WorkflowTaskOriginalScheduledTime = nil

	var commands []*commandpb.Command

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			ForceCreateNewWorkflowTask: true,
			TaskToken:                  taskToken,
			Commands:                   commands,
			Identity:                   identity,
		},
	})
	require.Nil(t, err)

}

func TestRespondWorkflowTaskCompletedCompleteWorkflowSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	workflowResult := payloads.EncodeString("success")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: workflowResult,
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestRespondWorkflowTaskCompletedFailWorkflowSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	reason := "fail workflow reason"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure.NewServerFailure(reason, false),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestRespondWorkflowTaskCompletedSignalExternalWorkflowSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      payloads.EncodeString("test input"),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)

}

func TestRespondWorkflowTaskCompletedStartChildWorkflowWithAbandonPolicy(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	abandon := enumspb.PARENT_CLOSE_POLICY_ABANDON
	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:  tests.Namespace.String(),
			WorkflowId: "child-workflow-id",
			WorkflowType: &commonpb.WorkflowType{
				Name: "child-workflow-type",
			},
			ParentClosePolicy: abandon,
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	deps.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, 1, len(ms2.GetPendingChildExecutionInfos()))
	var childID int64
	for c := range ms2.GetPendingChildExecutionInfos() {
		childID = c
		break
	}
	require.Equal(t, "child-workflow-id", ms2.GetPendingChildExecutionInfos()[childID].StartedWorkflowId)
	require.Equal(t, enumspb.PARENT_CLOSE_POLICY_ABANDON, ms2.GetPendingChildExecutionInfos()[childID].ParentClosePolicy)

}

func TestRespondWorkflowTaskCompletedStartChildWorkflowWithTerminatePolicy(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	terminate := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:  tests.Namespace.String(),
			WorkflowId: "child-workflow-id",
			WorkflowType: &commonpb.WorkflowType{
				Name: "child-workflow-type",
			},
			ParentClosePolicy: terminate,
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	deps.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, 1, len(ms2.GetPendingChildExecutionInfos()))
	var childID int64
	for c := range ms2.GetPendingChildExecutionInfos() {
		childID = c
		break
	}
	require.Equal(t, "child-workflow-id", ms2.GetPendingChildExecutionInfos()[childID].StartedWorkflowId)
	require.Equal(t, enumspb.PARENT_CLOSE_POLICY_TERMINATE, ms2.GetPendingChildExecutionInfos()[childID].ParentClosePolicy)

}

func TestRespondWorkflowTaskCompletedSignalExternalWorkflowFailed_UnKnownNamespace(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	foreignNamespace := namespace.Name("unknown namespace")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Namespace: foreignNamespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: "signal",
			Input:      payloads.EncodeString("test input"),
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespace(foreignNamespace).Return(
		nil, errors.New("get foreign namespace error"),
	)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})

	require.NotNil(t, err)

}

func TestRespondActivityTaskCompletedInvalidToken(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
			Result:    nil,
			Identity:  identity,
		},
	})

	require.NotNil(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)

}

func TestRespondActivityTaskCompletedIfNoExecution(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskCompletedIfNoRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskCompletedIfGetExecutionFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("FAILED"))

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "FAILED")

}

func TestRespondActivityTaskCompletedIfNoAIdProvided(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "activityID cannot be empty")

}

func TestRespondActivityTaskCompletedIfNotFound(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       "aid",
	}
	taskToken, _ := tt.Marshal()
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.Error(t, err)

}

func TestRespondActivityTaskCompletedUpdateExecutionFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, errors.New("FAILED"))
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes() // might be called in background goroutine

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "FAILED")

}

func TestRespondActivityTaskCompletedIfTaskCompleted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activityStartedEvent := addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	addActivityTaskCompletedEvent(ms, activityScheduledEvent.EventId, activityStartedEvent.EventId,
		activityResult, identity)
	addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskCompletedIfTaskNotStarted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskCompletedConflictOnUpdate(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, &persistence.ConditionFailedError{})

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	require.Equal(t, &persistence.ConditionFailedError{}, err)

}

func TestRespondActivityTaskCompletedSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(9), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(8))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(8), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

}

func TestRespondActivityTaskCompletedByIdSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	activityResult := payloads.EncodeString("activity result")
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	workflowTaskScheduledEvent := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, workflowTaskScheduledEvent.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, workflowTaskScheduledEvent.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCompleted(context.Background(), &historyservice.RespondActivityTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  identity,
		},
	})
	require.NoError(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(9), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt := ms2.GetWorkflowTaskByID(int64(8))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(8), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

}

func TestRespondActivityTaskFailedInvalidToken(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: invalidToken,
			Identity:  identity,
		},
	})

	require.NotNil(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)

}

func TestRespondActivityTaskFailedIfNoExecution(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskFailedIfNoRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil,
		serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskFailedIfGetExecutionFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil,
		errors.New("FAILED"))

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "FAILED")

}

func TestRespondActivityTaskFailededIfNoAIdProvided(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "activityID cannot be empty")

}

func TestRespondActivityTaskFailededIfNotFound(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       "aid",
	}
	taskToken, _ := tt.Marshal()
	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.Error(t, err)

}

func TestRespondActivityTaskFailedUpdateExecutionFailed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, errors.New("FAILED"))
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes() // might be called in background goroutine

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "FAILED")

}

func TestRespondActivityTaskFailedIfTaskCompleted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	serverFailure := failure.NewServerFailure("fail reason", true)

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	activityStartedEvent := addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	addActivityTaskFailedEvent(ms, activityScheduledEvent.EventId, activityStartedEvent.EventId, serverFailure, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, identity)
	addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Failure:   serverFailure,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskFailedIfTaskNotStarted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskFailedConflictOnUpdate(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, &persistence.ConditionFailedError{})

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.Equal(t, &persistence.ConditionFailedError{}, err)

}

func TestRespondActivityTaskFailedSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	serverFailure := failure.NewServerFailure("failed", false)

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Failure:   serverFailure,
			Identity:  identity,
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(9), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(8))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(8), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

}

func TestRespondActivityTaskFailedWithHeartbeatSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	serverFailure := failure.NewServerFailure("failed", false)

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, activityInfo := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ActivityInfos[activityInfo.ScheduledEventId] = activityInfo
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	details := payloads.EncodeString("details")

	require.Nil(t, activityInfo.GetLastHeartbeatDetails())

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken:            taskToken,
			Failure:              serverFailure,
			Identity:             identity,
			LastHeartbeatDetails: details,
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(9), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(8))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(8), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

	require.NotNil(t, activityInfo.GetLastHeartbeatDetails())

}

func TestRespondActivityTaskFailedByIdSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	serverFailure := failure.NewServerFailure("failed", false)
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	workflowTaskScheduledEvent := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, workflowTaskScheduledEvent.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, workflowTaskScheduledEvent.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskFailed(context.Background(), &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: tests.NamespaceID.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Failure:   serverFailure,
			Identity:  identity,
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(9), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt := ms2.GetWorkflowTaskByID(int64(8))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(8), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

}

func TestRecordActivityTaskHeartBeatSuccess_NoTimer(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	detais := payloads.EncodeString("details")

	_, err := deps.historyEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	require.Nil(t, err)

}

func TestRecordActivityTaskHeartBeatSuccess_TimerRunning(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	// HeartBeat timer running.
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	detais := payloads.EncodeString("details")

	_, err := deps.historyEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(7), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestRecordActivityTaskHeartBeatByIDSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)

	// No HeartBeat timer running.
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	detais := payloads.EncodeString("details")

	_, err := deps.historyEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   detais,
		},
	})
	require.Nil(t, err)

}

func TestRespondActivityTaskCanceled_Scheduled(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskCanceled_Started(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 5,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	_, _, err := ms.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEvent.EventId, activityScheduledEvent.EventId, identity)
	require.Nil(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(10), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt = ms2.GetWorkflowTaskByID(int64(9))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(9), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

}

func TestRespondActivityTaskCanceledById_Started(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       activityID,
	}
	taskToken, _ := tt.Marshal()

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	workflowTaskScheduledEvent := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, workflowTaskScheduledEvent.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, workflowTaskScheduledEvent.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	_, _, err := ms.AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEvent.EventId, activityScheduledEvent.EventId, identity)
	require.Nil(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: we.RunId}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(10), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	require.True(t, ms2.HasPendingWorkflowTask())
	wt := ms2.GetWorkflowTaskByID(int64(9))
	require.NotNil(t, wt)
	require.EqualValues(t, int64(100), wt.WorkflowTaskTimeout.Seconds())
	require.Equal(t, int64(9), wt.ScheduledEventID)
	require.Equal(t, common.EmptyEventID, wt.StartedEventID)

}

func TestRespondActivityTaskCanceledIfNoRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRespondActivityTaskCanceledIfNoAIdProvided(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-respond-activity-task-canceled-if-no-activity-id-provided",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, workflowExecution.WorkflowId, workflowExecution.RunId, log.NewTestLogger())
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.EqualError(t, err, "activityID cannot be empty")

}

func TestRespondActivityTaskCanceledIfNotFound(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-respond-activity-task-canceled-if-not-found",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"

	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       "aid",
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, workflowExecution.WorkflowId, workflowExecution.RunId, log.NewTestLogger())
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  identity,
		},
	})
	require.Error(t, err)

}

func TestRequestCancel_RespondWorkflowTaskCompleted_NotScheduled(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityScheduledEventID := int64(99)

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEventID,
		}},
	}}

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}
	ms2 := workflow.TestCloneToProto(ms)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "BadRequestCancelActivityAttributes: invalid history builder state for action: add-activitytask-cancel-requested-event, ScheduledEventID: 99", err.Error())
	require.NotNil(t, updatedWorkflowMutation)
	require.Equal(t, int64(5), updatedWorkflowMutation.NextEventID)
	require.Equal(t, common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	require.True(t, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID)

}

func TestRequestCancel_RespondWorkflowTaskCompleted_Scheduled(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	_, aInfo := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: aInfo.ScheduledEventId,
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(12), ms2.GetNextEventID())
	require.Equal(t, int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.True(t, ms2.HasPendingWorkflowTask())
	wt2 = ms2.GetWorkflowTaskByID(ms2.GetNextEventID() - 1)
	require.NotNil(t, wt2)
	require.Equal(t, ms2.GetNextEventID()-1, wt2.ScheduledEventID)
	require.Equal(t, int32(1), wt2.Attempt)

}

func TestRequestCancel_RespondWorkflowTaskCompleted_Started(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(11), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestRequestCancel_RespondWorkflowTaskCompleted_Completed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")
	workflowResult := payloads.EncodeString("workflow result")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	_, aInfo := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{
		{
			CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
			Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
				ScheduledEventId: aInfo.ScheduledEventId,
			}},
		},
		{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: workflowResult,
			}},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(11), ms2.GetNextEventID())
	require.Equal(t, int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestRequestCancel_RespondWorkflowTaskCompleted_NoHeartBeat(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 0*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(11), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

	// Try recording activity heartbeat
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	att := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := deps.historyEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)
	require.NotNil(t, hbResponse)
	require.True(t, hbResponse.CancelRequested)

	// Try cancelling the request.
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)

	ms2 = deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(13), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.True(t, ms2.HasPendingWorkflowTask())

}

func TestRequestCancel_RespondWorkflowTaskCompleted_Success(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(11), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

	// Try recording activity heartbeat
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	att := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := deps.historyEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)
	require.NotNil(t, hbResponse)
	require.True(t, hbResponse.CancelRequested)

	// Try cancelling the request.
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)

	ms2 = deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(13), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.True(t, ms2.HasPendingWorkflowTask())

}

func TestRequestCancel_RespondWorkflowTaskCompleted_SuccessWithQueries(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 7,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 1*time.Second)
	addActivityTaskStartedEvent(ms, activityScheduledEvent.EventId, identity)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
		Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
			ScheduledEventId: activityScheduledEvent.GetEventId(),
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := deps.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		tests.NamespaceID,
		&we,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	loadedMS, err := ctx.LoadMutableState(context.Background(), deps.mockShard)
	require.NoError(t, err)
	qr := workflow.NewQueryRegistry()
	id1, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	id2, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	id3, _ := qr.BufferQuery(&querypb.WorkflowQuery{})
	loadedMS.(*workflow.MutableStateImpl).QueryRegistry = qr
	release(nil)
	result1 := &querypb.WorkflowQueryResult{
		ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
		Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
	}
	result2 := &querypb.WorkflowQueryResult{
		ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
		ErrorMessage: "error reason",
	}
	queryResults := map[string]*querypb.WorkflowQueryResult{
		id1: result1,
		id2: result2,
	}
	_, err = deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken:    taskToken,
			Commands:     commands,
			Identity:     identity,
			QueryResults: queryResults,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(11), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())
	require.Len(t, qr.GetCompletedIDs(), 2)
	succeeded1, err := qr.GetCompletionState(id1)
	require.NoError(t, err)
	require.EqualValues(t, succeeded1.Result, result1)
	require.Equal(t, workflow.QueryCompletionTypeSucceeded, succeeded1.Type)
	succeeded2, err := qr.GetCompletionState(id2)
	require.NoError(t, err)
	require.EqualValues(t, succeeded2.Result, result2)
	require.Equal(t, workflow.QueryCompletionTypeSucceeded, succeeded2.Type)
	require.Len(t, qr.GetBufferedIDs(), 0)
	require.Len(t, qr.GetFailedIDs(), 0)
	require.Len(t, qr.GetUnblockedIDs(), 1)
	unblocked1, err := qr.GetCompletionState(id3)
	require.NoError(t, err)
	require.Nil(t, unblocked1.Result)
	require.Equal(t, workflow.QueryCompletionTypeUnblocked, unblocked1.Type)

	// Try recording activity heartbeat
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	att := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 5,
	}
	activityTaskToken, _ := att.Marshal()

	hbResponse, err := deps.historyEngine.RecordActivityTaskHeartbeat(context.Background(), &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId: tests.NamespaceID.String(),
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)
	require.NotNil(t, hbResponse)
	require.True(t, hbResponse.CancelRequested)

	// Try cancelling the request.
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.RespondActivityTaskCanceled(context.Background(), &historyservice.RespondActivityTaskCanceledRequest{
		NamespaceId: tests.NamespaceID.String(),
		CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  identity,
			Details:   payloads.EncodeString("details"),
		},
	})
	require.Nil(t, err)

	ms2 = deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(13), ms2.GetNextEventID())
	require.Equal(t, int64(8), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.True(t, ms2.HasPendingWorkflowTask())

}

func TestStarTimer_DuplicateTimerID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())

	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(1 * time.Second),
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)

	// Try to add the same timer ID again.
	wt2 := addWorkflowTaskScheduledEvent(ms2)
	addWorkflowTaskStartedEvent(ms2, wt2.ScheduledEventID, tl, identity)
	tt2 := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: wt2.ScheduledEventID,
	}
	taskToken2, _ := tt2.Marshal()

	wfMs2 := workflow.TestCloneToProto(ms2)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: wfMs2}

	workflowTaskFailedEvent := false
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		for _, newEvents := range request.UpdateWorkflowEvents {
			decTaskIndex := len(newEvents.Events) - 1
			if decTaskIndex >= 0 && newEvents.Events[decTaskIndex].EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
				workflowTaskFailedEvent = true
			}
		}
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err = deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken2,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "StartTimerDuplicateId: invalid history builder state for action: add-timer-started-event, TimerID: t1", err.Error())

	require.True(t, workflowTaskFailedEvent)

	require.NotNil(t, updatedWorkflowMutation)
	require.Equal(t, int64(9), updatedWorkflowMutation.NextEventID)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	require.Equal(t, updatedWorkflowMutation.NextEventID, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId)
	require.Equal(t, int32(2), updatedWorkflowMutation.ExecutionInfo.WorkflowTaskAttempt)

}

func TestUserTimer_RespondWorkflowTaskCompleted(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addTimerStartedEvent(ms, workflowTaskCompletedEvent.EventId, timerID, 10*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(10), ms2.GetNextEventID())
	require.Equal(t, int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestCancelTimer_RespondWorkflowTaskCompleted_NoStartTimer(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 2,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	ms2 := workflow.TestCloneToProto(ms)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse2, nil)
	var updatedWorkflowMutation persistence.WorkflowMutation
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		updatedWorkflowMutation = request.UpdateWorkflowMutation
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Equal(t, "BadCancelTimerAttributes: invalid history builder state for action: add-timer-canceled-event, TimerID: t1", err.Error())

	require.NotNil(t, updatedWorkflowMutation)
	require.Equal(t, int64(5), updatedWorkflowMutation.NextEventID)
	require.Equal(t, common.EmptyEventID, updatedWorkflowMutation.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, updatedWorkflowMutation.ExecutionState.State)
	require.True(t, updatedWorkflowMutation.ExecutionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID)

}

func TestCancelTimer_RespondWorkflowTaskCompleted_TimerFired(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	tt := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       we.WorkflowId,
		RunId:            we.RunId,
		ScheduledEventId: 6,
	}
	taskToken, _ := tt.Marshal()
	identity := "testIdentity"
	timerID := "t1"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(ms, &we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 100*time.Second, 100*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTaskStartedEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, workflowTaskStartedEvent.EventId, identity)
	addTimerStartedEvent(ms, workflowTaskCompletedEvent.EventId, timerID, 10*time.Second)
	wt2 := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt2.ScheduledEventID, tl, identity)
	addTimerFiredEvent(ms, timerID)
	_, _, err := ms.CloseTransactionAsMutation(historyi.TransactionPolicyActive)
	require.Nil(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	require.True(t, len(gwmsResponse.State.BufferedEvents) > 0)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
		Attributes: &commandpb.Command_CancelTimerCommandAttributes{CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
			TimerId: timerID,
		}},
	}}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.True(t, request.UpdateWorkflowMutation.ClearBufferedEvents)
			return tests.UpdateWorkflowExecutionResponse, nil
		})

	_, err = deps.historyEngine.RespondWorkflowTaskCompleted(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(tests.NamespaceID, &we)
	require.Equal(t, int64(10), ms2.GetNextEventID())
	require.Equal(t, int64(7), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())
	require.False(t, ms2.HasBufferedEvents())

}

func TestSignalWorkflowExecution(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.Nil(t, err)

}

// Test signal workflow task by adding request ID
func TestSignalWorkflowExecution_DuplicateRequest(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: "wId2",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := payloads.EncodeString("test input 2")
	requestID := uuid.New()
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
			RequestId:         requestID,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	// assume duplicate request id
	wfMs.SignalRequestedIds = []string{requestID}
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.Nil(t, err)

}

// Test signal workflow task by dedup request ID & workflow finished
func TestSignalWorkflowExecution_DuplicateRequest_Completed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: "wId2",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name 2"
	input := payloads.EncodeString("test input 2")
	requestID := uuid.New()
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
			RequestId:         requestID,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	// assume duplicate request id
	wfMs.SignalRequestedIds = []string{requestID}
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.Nil(t, err)

}

func TestSignalWorkflowExecution_Failed(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             input,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.EqualError(t, err, "workflow execution already completed")

}

func TestSignalWorkflowExecution_WorkflowTaskBackoff(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	signalRequest := &historyservice.SignalWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	signalInput := payloads.EncodeString("test input")
	signalRequest = &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         tests.NamespaceID.String(),
			WorkflowExecution: &we,
			Identity:          identity,
			SignalName:        signalName,
			Input:             signalInput,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               we.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: "wType"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue},
		Input:                    payloads.EncodeString("input"),
		WorkflowExecutionTimeout: durationpb.New(100 * time.Second),
		WorkflowRunTimeout:       durationpb.New(50 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(200 * time.Second),
		Identity:                 identity,
	}

	_, err = ms.AddWorkflowExecutionStartedEvent(
		&we,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:                  1,
			NamespaceId:              tests.NamespaceID.String(),
			StartRequest:             startRequest,
			ContinueAsNewInitiator:   enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY,
			FirstWorkflowTaskBackoff: durationpb.New(time.Second * 10),
		},
	)
	require.NoError(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		require.Len(t, request.UpdateWorkflowEvents[0].Events, 1) // no workflow task scheduled event
		// require.Empty(t, request.UpdateWorkflowMutation.Tasks[tasks.CategoryTransfer]) // no workflow transfer task
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	_, err = deps.historyEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	require.Nil(t, err)

}

func TestRemoveSignalMutableState(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	removeRequest := &historyservice.RemoveSignalMutableStateRequest{}
	_, err := deps.historyEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	execution := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	requestID := uuid.New()
	removeRequest = &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId:       tests.NamespaceID.String(),
		WorkflowExecution: &execution,
		RequestId:         requestID,
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.eventsCache,
		tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &execution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionInfo.NamespaceId = tests.NamespaceID.String()
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err = deps.historyEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	require.Nil(t, err)

}

func TestReapplyEvents_ReturnSuccess(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reapply",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	eventVersion := int64(100)
	history := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   eventVersion,
		},
	}
	globalNamespaceID := uuid.New()
	globalNamespaceName := "global-namespace-name"
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: globalNamespaceID, Name: globalNamespaceName},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		tests.Version,
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespace(namespaceEntry.Name()).Return(namespaceEntry, nil).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, eventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	ms := workflow.TestGlobalMutableState(
		deps.historyEngine.shardContext,
		deps.eventsCache,
		log.NewTestLogger(),
		namespaceEntry.FailoverVersion(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
	)
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}
	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())

	err := deps.historyEngine.ReapplyEvents(
		context.Background(),
		namespaceEntry.ID(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		history,
	)
	require.NoError(t, err)

}

func TestReapplyEvents_IgnoreSameClusterEvents(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reapply-same-cluster",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"

	// TODO: Figure out why version is empty?
	history := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
			Version:   common.EmptyVersion,
		},
	}
	ms := workflow.TestLocalMutableState(
		deps.historyEngine.shardContext,
		deps.eventsCache,
		tests.LocalNamespaceEntry,
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		log.NewTestLogger(),
	)
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}
	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	err := deps.historyEngine.ReapplyEvents(
		context.Background(),
		tests.NamespaceID,
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		history,
	)
	require.NoError(t, err)

}

func TestReapplyEvents_ResetWorkflow(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reapply-reset-workflow",
		RunId:      tests.RunID,
	}
	taskqueue := "testTaskQueue"
	identity := "testIdentity"
	eventVersion := int64(100)
	history := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   eventVersion,
		},
	}
	globalNamespaceID := uuid.New()
	globalNamespaceName := "global-namespace-name"
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: globalNamespaceID, Name: globalNamespaceName},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		tests.Version,
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespace(namespaceEntry.Name()).Return(namespaceEntry, nil).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, eventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	ms := workflow.TestGlobalMutableState(
		deps.historyEngine.shardContext,
		deps.eventsCache,
		log.NewTestLogger(),
		namespaceEntry.FailoverVersion(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
	)
	// Add dummy event
	addWorkflowExecutionStartedEvent(ms, &workflowExecution, "wType", taskqueue, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)

	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	wfMs.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId = 1
	token, err := ms.GetCurrentBranchToken()
	require.NoError(t, err)
	item := versionhistory.NewVersionHistoryItem(1, 1)
	versionHistory := versionhistory.NewVersionHistory(token, []*historyspb.VersionHistoryItem{item})
	wfMs.ExecutionInfo.VersionHistories = versionhistory.NewVersionHistories(versionHistory)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: tests.RunID}
	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockEventsReapplier.EXPECT().ReapplyEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	deps.mockWorkflowResetter.EXPECT().ResetWorkflow(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(),
	).Return(nil)

	err = deps.historyEngine.ReapplyEvents(
		context.Background(),
		namespaceEntry.ID(),
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		history,
	)
	require.NoError(t, err)

}

func TestEagerWorkflowStart_DoesNotCreateTransferTask(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	var recordedTasks []tasks.Task

	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		recordedTasks = request.NewWorkflowSnapshot.Tasks[tasks.CategoryTransfer]
		persistenceResponse := persistence.CreateWorkflowExecutionResponse{NewMutableStateStats: tests.CreateWorkflowExecutionResponse.NewMutableStateStats}
		return &persistenceResponse, nil
	})

	i := interceptor.NewTelemetryInterceptor(deps.mockShard.GetNamespaceRegistry(),
		deps.mockShard.GetMetricsHandler(),
		deps.mockShard.Resource.Logger,
		deps.config.LogAllReqErrors,
		deps.mockErrorHandler)
	response, err := i.UnaryIntercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "StartWorkflowExecution"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		response, err := deps.historyEngine.StartWorkflowExecution(ctx, &historyservice.StartWorkflowExecutionRequest{
			NamespaceId: tests.NamespaceID.String(),
			Attempt:     1,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:            "test",
				Namespace:             tests.Namespace.String(),
				WorkflowType:          &commonpb.WorkflowType{Name: "test"},
				TaskQueue:             &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "test"},
				Identity:              "test",
				RequestId:             "test",
				RequestEagerExecution: true,
			},
		})
		return response, err
	})
	require.NoError(t, err)
	startResp := response.(*historyservice.StartWorkflowExecutionResponse)
	require.Len(t, startResp.EagerWorkflowTask.History.Events, 3)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startResp.EagerWorkflowTask.History.Events[0].EventType)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, startResp.EagerWorkflowTask.History.Events[1].EventType)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, startResp.EagerWorkflowTask.History.Events[2].EventType)
	require.Empty(t, recordedTasks)

}

func TestEagerWorkflowStart_FromCron_SkipsEager(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	var recordedTasks []tasks.Task

	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		recordedTasks = request.NewWorkflowSnapshot.Tasks[tasks.CategoryTransfer]
		persistenceResponse := persistence.CreateWorkflowExecutionResponse{NewMutableStateStats: tests.CreateWorkflowExecutionResponse.NewMutableStateStats}
		return &persistenceResponse, nil
	})

	i := interceptor.NewTelemetryInterceptor(deps.mockShard.GetNamespaceRegistry(),
		deps.mockShard.GetMetricsHandler(),
		deps.mockShard.Resource.Logger,
		deps.config.LogAllReqErrors,
		deps.mockErrorHandler)
	response, err := i.UnaryIntercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "StartWorkflowExecution"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		firstWorkflowTaskBackoff := time.Second
		response, err := deps.historyEngine.StartWorkflowExecution(ctx, &historyservice.StartWorkflowExecutionRequest{
			NamespaceId:              tests.NamespaceID.String(),
			Attempt:                  1,
			ContinueAsNewInitiator:   enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
			FirstWorkflowTaskBackoff: durationpb.New(firstWorkflowTaskBackoff),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:            "test",
				Namespace:             tests.Namespace.String(),
				WorkflowType:          &commonpb.WorkflowType{Name: "test"},
				TaskQueue:             &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "test"},
				Identity:              "test",
				RequestId:             "test",
				CronSchedule:          "* * * * *",
				RequestEagerExecution: true,
			},
		})
		return response, err
	})
	require.NoError(t, err)
	require.Nil(t, response.(*historyservice.StartWorkflowExecutionResponse).EagerWorkflowTask)
	require.Equal(t, len(recordedTasks), 0)

}

func TestEagerWorkflowStart_WithSearchAttributes(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	var recordedTasks []tasks.Task

	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		recordedTasks = request.NewWorkflowSnapshot.Tasks[tasks.CategoryTransfer]
		persistenceResponse := persistence.CreateWorkflowExecutionResponse{NewMutableStateStats: tests.CreateWorkflowExecutionResponse.NewMutableStateStats}
		return &persistenceResponse, nil
	})

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Keyword01": payload.EncodeString("random-keyword"),
		},
	}
	i := interceptor.NewTelemetryInterceptor(deps.mockShard.GetNamespaceRegistry(),
		deps.mockShard.GetMetricsHandler(),
		deps.mockShard.Resource.Logger,
		deps.config.LogAllReqErrors,
		deps.mockErrorHandler)
	response, err := i.UnaryIntercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "StartWorkflowExecution"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		response, err := deps.historyEngine.StartWorkflowExecution(ctx, &historyservice.StartWorkflowExecutionRequest{
			NamespaceId: tests.NamespaceID.String(),
			Attempt:     1,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:            "test",
				Namespace:             tests.Namespace.String(),
				WorkflowType:          &commonpb.WorkflowType{Name: "test"},
				TaskQueue:             &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "test"},
				Identity:              "test",
				RequestId:             "test",
				SearchAttributes:      searchAttributes,
				RequestEagerExecution: true,
			},
		})
		return response, err
	})
	require.NoError(t, err)
	startResp := response.(*historyservice.StartWorkflowExecutionResponse)
	require.Len(t, startResp.EagerWorkflowTask.History.Events, 3)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startResp.EagerWorkflowTask.History.Events[0].EventType)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, startResp.EagerWorkflowTask.History.Events[1].EventType)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, startResp.EagerWorkflowTask.History.Events[2].EventType)
	workflowStartedEventAttr := startResp.EagerWorkflowTask.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	require.Equal(t, searchAttributes.IndexedFields, workflowStartedEventAttr.GetSearchAttributes().GetIndexedFields())
	require.Empty(t, recordedTasks)

}

func TestGetHistory(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	firstEventID := int64(100)
	nextEventID := int64(102)
	branchToken := []byte{1}
	we := commonpb.WorkflowExecution{
		WorkflowId: "wid",
		RunId:      "rid",
	}
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      2,
		NextPageToken: []byte{},
		ShardID:       1,
	}
	deps.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(100),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			},
			{
				EventId:   int64(101),
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
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	deps.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	deps.mockSearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()
	deps.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()

	history, token, err := api.GetHistory(
		context.Background(),
		deps.mockShard,
		tests.Namespace,
		tests.NamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: we.WorkflowId,
			RunId:      we.RunId,
		},
		firstEventID,
		nextEventID,
		2,
		[]byte{},
		nil,
		branchToken,
		deps.mockVisibilityMgr,
	)
	require.NoError(t, err)
	require.NotNil(t, history)
	require.Equal(t, []byte{}, token)

	require.EqualValues(t, "Keyword", history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["AliasForKeyword01"].GetMetadata()["type"])
	require.EqualValues(t, `"random-data"`, history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["TemporalChangeVersion"].GetData())

}

func TestGetWorkflowExecutionHistory(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}
	newRunID := uuid.New()

	req := &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Execution:              &we,
			MaximumPageSize:        10,
			NextPageToken:          nil,
			WaitNewEvent:           true,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			SkipArchival:           true,
		},
	}

	// set up mocks to simulate a failed workflow with a retry policy. the failure event is id 5.
	branchToken := []byte{1, 2, 3}

	deps.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  we.RunId,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		NextEventId: 6,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:         tests.NamespaceID.String(),
			WorkflowId:          we.WorkflowId,
			VersionHistories:    versionHistories,
			WorkflowTypeName:    "mytype",
			LastFirstEventId:    5,
			LastFirstEventTxnId: 100,
		},
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  we.WorkflowId,
		RunID:       we.RunId,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()
	// GetWorkflowExecutionHistory will request the last event
	deps.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    5,
		MaxEventID:    6,
		PageSize:      10,
		NextPageToken: nil,
		ShardID:       1,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(5),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure:                      &failurepb.Failure{Message: "this workflow failed"},
						RetryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
						WorkflowTaskCompletedEventId: 4,
						NewExecutionRunId:            newRunID,
					},
				},
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil).Times(2)

	deps.mockExecutionMgr.EXPECT().TrimHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	deps.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
	deps.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	oldGoSDKVersion := "1.9.1"
	newGoSDKVersion := "1.10.1"

	// new sdk: should see failed event
	ctx := headers.SetVersionsForTests(context.Background(), newGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	resp, err := engine.GetWorkflowExecutionHistory(ctx, req)
	require.NoError(t, err)
	require.False(t, resp.Response.Archived)
	event := resp.Response.History.Events[0]
	require.Equal(t, int64(5), event.EventId)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, event.EventType)
	attrs := event.GetWorkflowExecutionFailedEventAttributes()
	require.Equal(t, "this workflow failed", attrs.Failure.Message)
	require.Equal(t, newRunID, attrs.NewExecutionRunId)
	require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, attrs.RetryState)

	// old sdk: should see continued-as-new event
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// See comment in workflowHandler.go:GetWorkflowExecutionHistory
	ctx = headers.SetVersionsForTests(context.Background(), oldGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, "")
	resp, err = engine.GetWorkflowExecutionHistory(ctx, req)
	require.NoError(t, err)
	require.False(t, resp.Response.Archived)
	event = resp.Response.History.Events[0]
	require.Equal(t, int64(5), event.EventId)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, event.EventType)
	attrs2 := event.GetWorkflowExecutionContinuedAsNewEventAttributes()
	require.Equal(t, newRunID, attrs2.NewExecutionRunId)
	require.Equal(t, "this workflow failed", attrs2.Failure.Message)

}

func TestGetWorkflowExecutionHistoryWhenInternalRawHistoryIsEnabled(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.config.SendRawHistoryBetweenInternalServices = func() bool { return true }
	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{},
		"")
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil).AnyTimes()
	newRunID := uuid.New()

	req := &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Execution:              &we,
			MaximumPageSize:        10,
			NextPageToken:          nil,
			WaitNewEvent:           true,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			SkipArchival:           true,
		},
	}

	// set up mocks to simulate a failed workflow with a retry policy. the failure event is id 5.
	branchToken := []byte{1, 2, 3}

	deps.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  we.RunId,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
		NextEventId: 6,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:         tests.NamespaceID.String(),
			WorkflowId:          we.WorkflowId,
			VersionHistories:    versionHistories,
			WorkflowTypeName:    "mytype",
			LastFirstEventId:    5,
			LastFirstEventTxnId: 100,
		},
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  we.WorkflowId,
		RunID:       we.RunId,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()
	// GetWorkflowExecutionHistory will request the last event
	history := historypb.History{
		Events: []*historypb.HistoryEvent{
			{
				EventId:   int64(5),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure:                      &failurepb.Failure{Message: "this workflow failed"},
						RetryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
						WorkflowTaskCompletedEventId: 4,
						NewExecutionRunId:            newRunID,
					},
				},
			},
		},
	}

	historyBlob, err := history.Marshal()
	require.NoError(t, err)

	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    5,
		MaxEventID:    6,
		PageSize:      10,
		NextPageToken: nil,
		ShardID:       1,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         historyBlob,
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil).Times(1)

	deps.mockExecutionMgr.EXPECT().TrimHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	deps.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil).AnyTimes()
	deps.mockVisibilityMgr.EXPECT().GetIndexName().Return(esIndexName).AnyTimes()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	resp, err := engine.GetWorkflowExecutionHistory(context.Background(), req)
	require.NoError(t, err)
	require.False(t, resp.Response.Archived)
	require.Len(t, resp.History, 1)
	err = history.Unmarshal(resp.History[0])
	require.NoError(t, err)
	event := history.Events[0]
	require.Equal(t, int64(5), event.EventId)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, event.EventType)
	attrs := event.GetWorkflowExecutionFailedEventAttributes()
	require.Equal(t, "this workflow failed", attrs.Failure.Message)
	require.Equal(t, newRunID, attrs.NewExecutionRunId)
	require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, attrs.RetryState)

}

func TestGetWorkflowExecutionHistory_RawHistoryWithTransientDecision(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	we := commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	branchToken := []byte{1, 2, 3}
	persistenceToken := []byte("some random persistence token")
	nextPageToken, err := api.SerializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:            we.GetRunId(),
		FirstEventId:     common.FirstEventID,
		NextEventId:      5,
		PersistenceToken: persistenceToken,
		TransientWorkflowTask: &historyspb.TransientWorkflowTaskInfo{
			HistorySuffix: []*historypb.HistoryEvent{
				{
					EventId:   5,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   6,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
		},
		BranchToken: branchToken,
	})
	require.NoError(t, err)
	deps.config.SendRawWorkflowHistory = func(string) bool { return true }
	req := &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Execution:              &we,
			MaximumPageSize:        10,
			NextPageToken:          nextPageToken,
			WaitNewEvent:           false,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
			SkipArchival:           true,
		},
	}

	deps.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	historyBlob1, err := deps.mockShard.GetPayloadSerializer().SerializeEvents(
		[]*historypb.HistoryEvent{
			{
				EventId:   int64(3),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			},
		},
	)
	require.NoError(t, err)
	historyBlob2, err := deps.mockShard.GetPayloadSerializer().SerializeEvents(
		[]*historypb.HistoryEvent{
			{
				EventId:   int64(4),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
			},
		},
	)
	require.NoError(t, err)
	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    1,
		MaxEventID:    5,
		PageSize:      10,
		NextPageToken: persistenceToken,
		ShardID:       1,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{historyBlob1, historyBlob2},
		NextPageToken:     []byte{},
		Size:              1,
	}, nil).Times(1)

	ctx := headers.SetVersionsForTests(context.Background(), "1.10.1", headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	resp, err := engine.GetWorkflowExecutionHistory(ctx, req)
	require.NoError(t, err)
	require.False(t, resp.Response.Archived)
	require.Empty(t, resp.Response.History.Events)
	require.Len(t, resp.Response.RawHistory, 3)
	historyEvents, err := deps.mockShard.GetPayloadSerializer().DeserializeEvents(resp.Response.RawHistory[2])
	require.NoError(t, err)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, historyEvents[0].EventType)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, historyEvents[1].EventType)

}

func Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidWorkflowID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Error(t, err)

}

func Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      "runID",
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Error(t, err)

}

func Test_GetWorkflowExecutionRawHistoryV2_FailedOnNamespaceCache(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(nil, fmt.Errorf("test"))
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Error(t, err)

}

func Test_GetWorkflowExecutionRawHistoryV2(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()

	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err = engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   10,
				NextPageToken:     nil,
			},
		})
	require.NoError(t, err)

}

func Test_GetWorkflowExecutionRawHistoryV2_SameStartIDAndEndID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()

	resp, err := engine.GetWorkflowExecutionRawHistoryV2(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      10,
				StartEventVersion: 100,
				EndEventId:        common.EmptyEventID,
				EndEventVersion:   common.EmptyVersion,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Nil(t, resp.Response.NextPageToken)
	require.NoError(t, err)

}

func Test_GetWorkflowExecutionRawHistory_FailedOnInvalidWorkflowID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Error(t, err)

}

func Test_GetWorkflowExecutionRawHistory_FailedOnInvalidRunID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id: namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespaceEntry, nil).AnyTimes()
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      "runID",
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Error(t, err)

}

func Test_GetWorkflowExecutionRawHistory_FailedOnNamespaceCache(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceID := namespace.ID(uuid.New())
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(nil, fmt.Errorf("test"))
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: namespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Error(t, err)

}

func Test_GetWorkflowExecutionRawHistory(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()

	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err = engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      1,
				StartEventVersion: 100,
				EndEventId:        10,
				EndEventVersion:   100,
				MaximumPageSize:   10,
				NextPageToken:     nil,
			},
		})
	require.NoError(t, err)

}

func Test_GetWorkflowExecutionRawHistory_SameStartIDAndEndID(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	engine, err := deps.historyEngine.shardContext.GetEngine(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	namespaceEntry := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
			Id:   tests.NamespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 11,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:      tests.NamespaceID.String(),
			VersionHistories: versionHistories,
		},
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mState}, nil).AnyTimes()
	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	resp, err := engine.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: tests.NamespaceID.String(),
			Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: tests.NamespaceID.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflowID",
					RunId:      uuid.New(),
				},
				StartEventId:      10,
				StartEventVersion: 100,
				EndEventId:        common.EmptyEventID,
				EndEventVersion:   common.EmptyVersion,
				MaximumPageSize:   1,
				NextPageToken:     nil,
			},
		})
	require.Nil(t, resp.Response.NextPageToken)
	require.NoError(t, err)

}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartAndEnd(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	inputStartEventID := int64(1)
	inputStartVersion := int64(10)
	inputEndEventID := int64(100)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	endItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, endItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      inputStartEventID,
			StartEventVersion: inputStartVersion,
			EndEventId:        inputEndEventID,
			EndEventVersion:   inputEndVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	require.Equal(t, request.GetRequest().GetStartEventId(), inputStartEventID)
	require.Equal(t, request.GetRequest().GetEndEventId(), inputEndEventID)
	require.Equal(t, targetVersionHistory, versionHistory)
	require.NoError(t, err)

}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedEndEvent(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      common.EmptyEventID,
			StartEventVersion: common.EmptyVersion,
			EndEventId:        inputEndEventID,
			EndEventVersion:   inputEndVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	require.Equal(t, request.GetRequest().GetStartEventId(), inputStartEventID-1)
	require.Equal(t, request.GetRequest().GetEndEventId(), inputEndEventID)
	require.Equal(t, targetVersionHistory, versionHistory)
	require.NoError(t, err)

}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartEvent(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      inputStartEventID,
			StartEventVersion: inputStartVersion,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	require.Equal(t, request.GetRequest().GetStartEventId(), inputStartEventID)
	require.Equal(t, request.GetRequest().GetEndEventId(), inputEndEventID+1)
	require.Equal(t, targetVersionHistory, versionHistory)
	require.NoError(t, err)

}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_NonCurrentBranch(t *testing.T) {
	deps := setupEngineTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(101)
	item1 := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	item2 := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory1 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item2})
	item3 := versionhistory.NewVersionHistoryItem(int64(10), int64(20))
	item4 := versionhistory.NewVersionHistoryItem(int64(20), int64(51))
	versionHistory2 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item3, item4})
	versionHistories := versionhistory.NewVersionHistories(versionHistory1)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory2)
	require.NoError(t, err)
	namespaceID := namespace.ID(uuid.New())
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: namespaceID.String(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      9,
			StartEventVersion: 20,
			EndEventId:        inputEndEventID,
			EndEventVersion:   inputEndVersion,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		},
	}

	targetVersionHistory, err := getworkflowexecutionrawhistoryv2.SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	require.Equal(t, request.GetRequest().GetStartEventId(), inputStartEventID)
	require.Equal(t, request.GetRequest().GetEndEventId(), inputEndEventID)
	require.Equal(t, targetVersionHistory, versionHistory1)
	require.NoError(t, err)

}

func (s *engineSuite) getMutableState(testNamespaceID namespace.ID, we *commonpb.WorkflowExecution) historyi.MutableState {
	context, release, err := s.workflowCache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		we,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.(*workflow.ContextImpl).MutableState
}

func (s *engineSuite) getActivityScheduledEvent(
	ms historyi.MutableState,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event, _ := ms.GetActivityScheduledEvent(context.Background(), scheduledEventID)
	return event
}

func addWorkflowExecutionStartedEventWithParent(ms historyi.MutableState, workflowExecution *commonpb.WorkflowExecution,
	workflowType, taskQueue string, input *commonpb.Payloads, executionTimeout, runTimeout, taskTimeout time.Duration,
	parentInfo *workflowspb.ParentExecutionInfo, identity string) *historypb.HistoryEvent {

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               workflowExecution.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                    input,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
		WorkflowRunTimeout:       durationpb.New(runTimeout),
		WorkflowTaskTimeout:      durationpb.New(taskTimeout),
		Identity:                 identity,
	}

	event, _ := ms.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:             1,
			NamespaceId:         tests.NamespaceID.String(),
			StartRequest:        startRequest,
			ParentExecutionInfo: parentInfo,
		},
	)

	return event
}

func addWorkflowExecutionStartedEvent(ms historyi.MutableState, workflowExecution *commonpb.WorkflowExecution,
	workflowType, taskQueue string, input *commonpb.Payloads, executionTimeout, runTimeout, taskTimeout time.Duration,
	identity string) *historypb.HistoryEvent {
	return addWorkflowExecutionStartedEventWithParent(ms, workflowExecution, workflowType, taskQueue, input,
		executionTimeout, runTimeout, taskTimeout, nil, identity)
}

func addWorkflowTaskScheduledEvent(ms historyi.MutableState) *historyi.WorkflowTaskInfo {
	workflowTask, _ := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	return workflowTask
}

func addWorkflowTaskStartedEvent(ms historyi.MutableState, scheduledEventID int64, taskQueue,
	identity string) *historypb.HistoryEvent {
	return addWorkflowTaskStartedEventWithRequestID(ms, scheduledEventID, tests.RunID, taskQueue, identity)
}

func addWorkflowTaskStartedEventWithRequestID(ms historyi.MutableState, scheduledEventID int64, requestID string,
	taskQueue, identity string) *historypb.HistoryEvent {
	event, _, _ := ms.AddWorkflowTaskStartedEvent(
		scheduledEventID,
		requestID,
		&taskqueuepb.TaskQueue{Name: taskQueue},
		identity,
		nil,
		nil,
		nil,
		false,
	)

	return event
}

func addWorkflowTaskCompletedEvent(t testing.TB, ms historyi.MutableState, scheduledEventID, startedEventID int64, identity string) *historypb.HistoryEvent {
	workflowTask := ms.GetWorkflowTaskByID(scheduledEventID)
	require.NotNil(t, workflowTask)
	require.Equal(t, startedEventID, workflowTask.StartedEventID)

	event, _ := ms.AddWorkflowTaskCompletedEvent(workflowTask, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: identity,
	}, defaultWorkflowTaskCompletionLimits)

	ms.FlushBufferedEvents()

	return event
}

func addActivityTaskScheduledEvent(
	ms historyi.MutableState,
	workflowTaskCompletedID int64,
	activityID, activityType,
	taskQueue string,
	input *commonpb.Payloads,
	scheduleToCloseTimeout time.Duration,
	scheduleToStartTimeout time.Duration,
	startToCloseTimeout time.Duration,
	heartbeatTimeout time.Duration,
) (*historypb.HistoryEvent,
	*persistencespb.ActivityInfo) {

	event, ai, _ := ms.AddActivityTaskScheduledEvent(workflowTaskCompletedID, &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                  input,
		ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
		ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
		StartToCloseTimeout:    durationpb.New(startToCloseTimeout),
		HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
	}, false)

	return event, ai
}

func addActivityTaskScheduledEventWithRetry(
	ms historyi.MutableState,
	workflowTaskCompletedID int64,
	activityID, activityType,
	taskQueue string,
	input *commonpb.Payloads,
	scheduleToCloseTimeout time.Duration,
	scheduleToStartTimeout time.Duration,
	startToCloseTimeout time.Duration,
	heartbeatTimeout time.Duration,
	retryPolicy *commonpb.RetryPolicy,
) (*historypb.HistoryEvent, *persistencespb.ActivityInfo) {

	event, ai, _ := ms.AddActivityTaskScheduledEvent(workflowTaskCompletedID, &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                  input,
		ScheduleToCloseTimeout: durationpb.New(scheduleToCloseTimeout),
		ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
		StartToCloseTimeout:    durationpb.New(startToCloseTimeout),
		HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
		RetryPolicy:            retryPolicy,
	}, false)

	return event, ai
}

func addActivityTaskStartedEvent(ms historyi.MutableState, scheduledEventID int64, identity string) *historypb.HistoryEvent {
	ai, _ := ms.GetActivityInfo(scheduledEventID)
	event, _ := ms.AddActivityTaskStartedEvent(
		ai,
		scheduledEventID,
		tests.RunID,
		identity,
		nil,
		nil,
		nil,
	)
	return event
}

func addActivityTaskCompletedEvent(ms historyi.MutableState, scheduledEventID, startedEventID int64, result *commonpb.Payloads,
	identity string) *historypb.HistoryEvent {
	event, _ := ms.AddActivityTaskCompletedEvent(scheduledEventID, startedEventID, &workflowservice.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: identity,
	})

	return event
}

func addActivityTaskFailedEvent(
	ms historyi.MutableState,
	scheduledEventID, startedEventID int64,
	failureInfo *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event, _ := ms.AddActivityTaskFailedEvent(scheduledEventID, startedEventID, failureInfo, retryState, identity, nil)
	return event
}

func addTimerStartedEvent(ms historyi.MutableState, workflowTaskCompletedEventID int64, timerID string,
	timeout time.Duration) (*historypb.HistoryEvent, *persistencespb.TimerInfo) {
	event, ti, _ := ms.AddTimerStartedEvent(workflowTaskCompletedEventID,
		&commandpb.StartTimerCommandAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(timeout),
		})
	return event, ti
}

func addTimerFiredEvent(ms historyi.MutableState, timerID string) *historypb.HistoryEvent {
	event, _ := ms.AddTimerFiredEvent(timerID)
	return event
}

func addRequestCancelInitiatedEvent(
	ms historyi.MutableState,
	workflowTaskCompletedEventID int64,
	cancelRequestID string,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	workflowID, runID string,
	childWorkflowOnly bool,
) (*historypb.HistoryEvent, *persistencespb.RequestCancelInfo) {
	event, rci, _ := ms.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID,
		cancelRequestID, &commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
			Namespace:         namespaceName.String(),
			WorkflowId:        workflowID,
			RunId:             runID,
			ChildWorkflowOnly: childWorkflowOnly,
			Reason:            "cancellation reason",
		},
		namespaceID)

	return event, rci
}

func addCancelRequestedEvent(
	ms historyi.MutableState,
	initiatedID int64,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	workflowID,
	runID string,
) *historypb.HistoryEvent {
	event, _ := ms.AddExternalWorkflowExecutionCancelRequested(initiatedID, namespaceName, namespaceID, workflowID, runID)
	return event
}

func addRequestSignalInitiatedEvent(
	ms historyi.MutableState,
	workflowTaskCompletedEventID int64,
	signalRequestID string,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	workflowID, runID string,
	childWorkflowOnly bool,
	signalName string,
	input *commonpb.Payloads,
	control string,
	header *commonpb.Header,
) (*historypb.HistoryEvent, *persistencespb.SignalInfo) {
	event, si, _ := ms.AddSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, signalRequestID,
		&commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Namespace: namespaceName.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			ChildWorkflowOnly: childWorkflowOnly,
			SignalName:        signalName,
			Input:             input,
			Control:           control,
			Header:            header,
		}, namespaceID)

	return event, si
}

func addSignaledEvent(
	ms historyi.MutableState,
	initiatedID int64,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	workflowID,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event, _ := ms.AddExternalWorkflowExecutionSignaled(initiatedID, namespaceName, namespaceID, workflowID, runID, control)
	return event
}

func addStartChildWorkflowExecutionInitiatedEvent(
	ms historyi.MutableState,
	workflowTaskCompletedID int64,
	namespace namespace.Name,
	namespaceID namespace.ID,
	workflowID, workflowType, taskQueue string,
	input *commonpb.Payloads,
	executionTimeout, runTimeout, taskTimeout time.Duration,
	parentClosePolicy enumspb.ParentClosePolicy,
) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo) {

	event, cei, _ := ms.AddStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedID,
		&commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:                namespace.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: durationpb.New(executionTimeout),
			WorkflowRunTimeout:       durationpb.New(runTimeout),
			WorkflowTaskTimeout:      durationpb.New(taskTimeout),
			Control:                  "",
			ParentClosePolicy:        parentClosePolicy,
		}, namespaceID)
	return event, cei
}

func addChildWorkflowExecutionStartedEvent(ms historyi.MutableState, initiatedID int64, workflowID, runID string,
	workflowType string, clock *clockspb.VectorClock) *historypb.HistoryEvent {
	event, _ := ms.AddChildWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		&commonpb.WorkflowType{Name: workflowType},
		initiatedID,
		&commonpb.Header{},
		clock,
	)
	return event
}

func addChildWorkflowExecutionCompletedEvent(ms historyi.MutableState, initiatedID int64, childExecution *commonpb.WorkflowExecution,
	attributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	event, _ := ms.AddChildWorkflowExecutionCompletedEvent(initiatedID, childExecution, attributes)
	return event
}

func addCompleteWorkflowEvent(ms historyi.MutableState, workflowTaskCompletedEventID int64,
	result *commonpb.Payloads) *historypb.HistoryEvent {
	event, _ := ms.AddCompletedWorkflowEvent(
		workflowTaskCompletedEventID,
		&commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: result,
		},
		"")
	return event
}

func addFailWorkflowEvent(
	ms historyi.MutableState,
	workflowTaskCompletedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event, _ := ms.AddFailWorkflowEvent(
		workflowTaskCompletedEventID,
		retryState,
		&commandpb.FailWorkflowExecutionCommandAttributes{
			Failure: failure,
		},
		"",
	)
	return event
}
