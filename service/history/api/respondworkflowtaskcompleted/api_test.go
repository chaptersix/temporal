package respondworkflowtaskcompleted

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

type testDeps struct {
	controller                       *gomock.Controller
	mockShard                        *shard.ContextTest
	mockEventsCache                  *events.MockCache
	mockExecutionMgr                 *persistence.MockExecutionManager
	workflowCache                    wcache.Cache
	mockNamespaceCache               *namespace.MockRegistry
	logger                           log.Logger
	workflowTaskCompletedHandler     *WorkflowTaskCompletedHandler
	protoAssertions                  protorequire.ProtoAssertions
	historyRequire                   historyrequire.HistoryRequire
	updateUtils                      updateutils.UpdateUtils
}

func setupSubTest(t *testing.T) *testDeps {
	t.Helper()

	controller := gomock.NewController(t)
	config := tests.NewDynamicConfig()
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

	mockEngine := historyi.NewMockEngine(controller)
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockShard.SetEngineForTesting(mockEngine)

	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockExecutionMgr := mockShard.Resource.ExecutionMgr

	mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(nil).AnyTimes()

	mockEventsCache := mockShard.MockEventsCache
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	logger := mockShard.GetLogger()

	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	workflowTaskCompletedHandler := NewWorkflowTaskCompletedHandler(
		mockShard,
		tasktoken.NewSerializer(),
		events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		nil,
		nil,
		nil,
		api.NewWorkflowConsistencyChecker(mockShard, workflowCache),
		nil)

	t.Cleanup(func() {
		controller.Finish()
	})

	return &testDeps{
		controller:                   controller,
		mockShard:                    mockShard,
		mockEventsCache:              mockEventsCache,
		mockExecutionMgr:             mockExecutionMgr,
		workflowCache:                workflowCache,
		mockNamespaceCache:           mockNamespaceCache,
		logger:                       logger,
		workflowTaskCompletedHandler: workflowTaskCompletedHandler,
		protoAssertions:              protorequire.New(t),
		historyRequire:               historyrequire.New(t),
		updateUtils:                  updateutils.New(t),
	}
}

func TestUpdateWorkflow(t *testing.T) {
	t.Parallel()

	createWrittenHistoryCh := func(t *testing.T, deps *testDeps, expectedUpdateWorkflowExecutionCalls int) <-chan []*historypb.HistoryEvent {
		writtenHistoryCh := make(chan []*historypb.HistoryEvent, expectedUpdateWorkflowExecutionCalls)
		deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			var wfEvents []*persistence.WorkflowEvents
			if len(request.UpdateWorkflowEvents) > 0 {
				wfEvents = request.UpdateWorkflowEvents
			} else {
				wfEvents = request.NewWorkflowEvents
			}

			var historyEvents []*historypb.HistoryEvent
			for _, uwe := range wfEvents {
				for _, event := range uwe.Events {
					historyEvents = append(historyEvents, event)
				}
			}
			writtenHistoryCh <- historyEvents
			return tests.UpdateWorkflowExecutionResponse, nil
		}).Times(expectedUpdateWorkflowExecutionCalls)

		return writtenHistoryCh
	}

	t.Run("Accept Complete", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)
		writtenHistoryCh := createWrittenHistoryCh(t, deps, 1)

		_, err := wfContext.LoadMutableState(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		_, err = deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Commands:  deps.updateUtils.UpdateAcceptCompleteCommands(tv),
				Messages:  deps.updateUtils.UpdateAcceptCompleteMessages(tv, updRequestMsg),
				Identity:  tv.Any().String(),
			},
		})
		require.NoError(t, err)

		updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
		deps.protoAssertions.ProtoEqual(payloads.EncodeString("success-result-of-"+tv.UpdateID()), updStatus.Outcome.GetSuccess())

		deps.historyRequire.EqualHistoryEvents(`
  2 WorkflowTaskScheduled // Speculative WFT events are persisted on WFT completion.
  3 WorkflowTaskStarted // Speculative WFT events are persisted on WFT completion.
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, <-writtenHistoryCh)
	})

	t.Run("Reject", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		_, err := deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Messages:  deps.updateUtils.UpdateRejectMessages(tv, updRequestMsg),
				Identity:  tv.Any().String(),
			},
		})
		require.NoError(t, err)

		updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
		require.Equal(t, "rejection-of-"+tv.UpdateID(), updStatus.Outcome.GetFailure().GetMessage())
	})

	t.Run("Write failed on normal task queue", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)

		writeErr := errors.New("write failed")
		deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, writeErr)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		_, err := deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Commands:  deps.updateUtils.UpdateAcceptCompleteCommands(tv),
				Messages:  deps.updateUtils.UpdateAcceptCompleteMessages(tv, updRequestMsg),
				Identity:  tv.Any().String(),
			},
		})
		require.ErrorIs(t, err, writeErr)

		require.Nil(t, wfContext.(*workflow.ContextImpl).MutableState, "mutable state must be cleared")
	})

	t.Run("Write failed on sticky task queue", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)

		writeErr := serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT, "write failed")
		// First write of MS
		deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, writeErr)
		// Second write of MS to clear stickiness
		deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		_, err := deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken:        serializedTaskToken,
				Commands:         deps.updateUtils.UpdateAcceptCompleteCommands(tv),
				Messages:         deps.updateUtils.UpdateAcceptCompleteMessages(tv, updRequestMsg),
				Identity:         tv.Any().String(),
				StickyAttributes: tv.StickyExecutionAttributes(tv.Any().InfiniteTimeout().AsDuration()),
			},
		})
		require.ErrorIs(t, err, writeErr)

		require.Nil(t, wfContext.(*workflow.ContextImpl).MutableState, "mutable state must be cleared")
	})

	t.Run("GetHistory failed", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		deps.mockNamespaceCache.EXPECT().GetNamespaceName(tv.NamespaceID()).Return(tv.NamespaceName(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)
		writtenHistoryCh := createWrittenHistoryCh(t, deps, 1)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		readHistoryErr := errors.New("get history failed")
		deps.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, readHistoryErr)

		_, err := deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken:                  serializedTaskToken,
				Commands:                   deps.updateUtils.UpdateAcceptCompleteCommands(tv),
				Messages:                   deps.updateUtils.UpdateAcceptCompleteMessages(tv, updRequestMsg),
				Identity:                   tv.Any().String(),
				ReturnNewWorkflowTask:      true,
				ForceCreateNewWorkflowTask: true,
			},
		})
		require.ErrorIs(t, err, readHistoryErr)

		updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
		deps.protoAssertions.ProtoEqual(payloads.EncodeString("success-result-of-"+tv.UpdateID()), updStatus.Outcome.GetSuccess())

		deps.historyRequire.EqualHistoryEvents(`
  2 WorkflowTaskScheduled // Speculative WFT events are persisted on WFT completion.
  3 WorkflowTaskStarted // Speculative WFT events are persisted on WFT completion.
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted`, <-writtenHistoryCh)
	})

	t.Run("Discard speculative WFT with events", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)
		// Expect only 2 calls to UpdateWorkflowExecution: for timer started and timer fired events but not Update or WFT events.
		writtenHistoryCh := createWrittenHistoryCh(t, deps, 2)
		ms, err := wfContext.LoadMutableState(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)

		_, _, err = ms.AddTimerStartedEvent(
			1,
			&commandpb.StartTimerCommandAttributes{
				TimerId:            tv.TimerID(),
				StartToFireTimeout: tv.Any().InfiniteTimeout(),
			},
		)
		require.NoError(t, err)
		err = wfContext.UpdateWorkflowExecutionAsActive(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)

		deps.historyRequire.EqualHistoryEvents(`
  2 TimerStarted
`, <-writtenHistoryCh)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		_, err = deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Messages:  deps.updateUtils.UpdateRejectMessages(tv, updRequestMsg),
				Identity:  tv.Any().String(),
				Capabilities: &workflowservice.RespondWorkflowTaskCompletedRequest_Capabilities{
					DiscardSpeculativeWorkflowTaskWithEvents: true,
				},
			},
		})
		require.NoError(t, err)

		updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
		require.Equal(t, "rejection-of-"+tv.UpdateID(), updStatus.Outcome.GetFailure().GetMessage())

		ms, err = wfContext.LoadMutableState(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)
		_, err = ms.AddTimerFiredEvent(tv.TimerID())
		require.NoError(t, err)
		err = wfContext.UpdateWorkflowExecutionAsActive(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)

		deps.historyRequire.EqualHistoryEvents(`
  3 TimerFired // No WFT events in between 2 and 3.
`, <-writtenHistoryCh)
	})

	t.Run("Do not discard speculative WFT with more than 10 events", func(t *testing.T) {
		deps := setupSubTest(t)
		tv := testvars.New(t)
		tv = tv.WithRunID(tv.Any().RunID())
		deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil).AnyTimes()
		wfContext := createStartedWorkflow(t, deps, tv)
		// Expect 2 calls to UpdateWorkflowExecution: for timer started and WFT events.
		writtenHistoryCh := createWrittenHistoryCh(t, deps, 2)
		ms, err := wfContext.LoadMutableState(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)

		for i := 0; i < 11; i++ {
			_, _, err = ms.AddTimerStartedEvent(
				1,
				&commandpb.StartTimerCommandAttributes{
					TimerId:            tv.WithTimerIDNumber(i).TimerID(),
					StartToFireTimeout: tv.Any().InfiniteTimeout(),
				},
			)
			require.NoError(t, err)
		}
		err = wfContext.UpdateWorkflowExecutionAsActive(context.Background(), deps.workflowTaskCompletedHandler.shardContext)
		require.NoError(t, err)

		deps.historyRequire.EqualHistoryEvents(`
  2 TimerStarted
  3 TimerStarted
  4 TimerStarted
  5 TimerStarted
  6 TimerStarted
  7 TimerStarted
  8 TimerStarted
  9 TimerStarted
 10 TimerStarted
 11 TimerStarted
 12 TimerStarted
`, <-writtenHistoryCh)

		updRequestMsg, upd, serializedTaskToken := createSentUpdate(t, deps, tv, wfContext)
		require.NotNil(t, upd)

		_, err = deps.workflowTaskCompletedHandler.Invoke(context.Background(), &historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId: tv.NamespaceID().String(),
			CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
				TaskToken: serializedTaskToken,
				Messages:  deps.updateUtils.UpdateRejectMessages(tv, updRequestMsg),
				Identity:  tv.Any().String(),
				Capabilities: &workflowservice.RespondWorkflowTaskCompletedRequest_Capabilities{
					DiscardSpeculativeWorkflowTaskWithEvents: true,
				},
			},
		})
		require.NoError(t, err)

		updStatus, err := upd.WaitLifecycleStage(context.Background(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED, time.Duration(0))
		require.NoError(t, err)
		require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED.String(), updStatus.Stage.String())
		require.Equal(t, "rejection-of-"+tv.UpdateID(), updStatus.Outcome.GetFailure().GetMessage())

		deps.historyRequire.EqualHistoryEvents(`
 13 WorkflowTaskScheduled // WFT events were created even if it was a rejection (because number of events > 10).
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
`, <-writtenHistoryCh)
	})
}

func TestHandleBufferedQueries(t *testing.T) {
	t.Parallel()

	constructQueryResults := func(ids []string, resultSize int) map[string]*querypb.WorkflowQueryResult {
		results := make(map[string]*querypb.WorkflowQueryResult)
		for _, id := range ids {
			results[id] = &querypb.WorkflowQueryResult{
				ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				Answer:     payloads.EncodeBytes(make([]byte, resultSize)),
			}
		}
		return results
	}

	constructQueryRegistry := func(numQueries int) historyi.QueryRegistry {
		queryRegistry := workflow.NewQueryRegistry()
		for i := 0; i < numQueries; i++ {
			queryRegistry.BufferQuery(&querypb.WorkflowQuery{})
		}
		return queryRegistry
	}

	assertQueryCounts := func(t *testing.T, queryRegistry historyi.QueryRegistry, buffered, completed, unblocked, failed int) {
		require.Len(t, queryRegistry.GetBufferedIDs(), buffered)
		require.Len(t, queryRegistry.GetCompletedIDs(), completed)
		require.Len(t, queryRegistry.GetUnblockedIDs(), unblocked)
		require.Len(t, queryRegistry.GetFailedIDs(), failed)
	}

	setupBufferedQueriesMocks := func(t *testing.T, controller *gomock.Controller) (historyi.QueryRegistry, *historyi.MockMutableState) {
		queryRegistry := constructQueryRegistry(10)
		mockMutableState := historyi.NewMockMutableState(controller)
		mockMutableState.EXPECT().GetQueryRegistry().Return(queryRegistry)
		mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			WorkflowId: tests.WorkflowID,
		}).AnyTimes()
		mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId: tests.RunID,
		}).AnyTimes()
		return queryRegistry, mockMutableState
	}

	t.Run("New WorkflowTask", func(t *testing.T) {
		deps := setupSubTest(t)
		queryRegistry, mockMutableState := setupBufferedQueriesMocks(t, deps.controller)
		assertQueryCounts(t, queryRegistry, 10, 0, 0, 0)
		queryResults := constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
		deps.workflowTaskCompletedHandler.handleBufferedQueries(mockMutableState, queryResults, true, tests.GlobalNamespaceEntry)
		assertQueryCounts(t, queryRegistry, 5, 5, 0, 0)
	})

	t.Run("No New WorkflowTask", func(t *testing.T) {
		deps := setupSubTest(t)
		queryRegistry, mockMutableState := setupBufferedQueriesMocks(t, deps.controller)
		assertQueryCounts(t, queryRegistry, 10, 0, 0, 0)
		queryResults := constructQueryResults(queryRegistry.GetBufferedIDs()[0:5], 10)
		deps.workflowTaskCompletedHandler.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry)
		assertQueryCounts(t, queryRegistry, 0, 5, 5, 0)
	})

	t.Run("Query Too Large", func(t *testing.T) {
		deps := setupSubTest(t)
		queryRegistry, mockMutableState := setupBufferedQueriesMocks(t, deps.controller)
		assertQueryCounts(t, queryRegistry, 10, 0, 0, 0)
		bufferedIDs := queryRegistry.GetBufferedIDs()
		queryResults := constructQueryResults(bufferedIDs[0:5], 10)
		largeQueryResults := constructQueryResults(bufferedIDs[5:10], 10*1024*1024)
		maps.Copy(queryResults, largeQueryResults)
		deps.workflowTaskCompletedHandler.handleBufferedQueries(mockMutableState, queryResults, false, tests.GlobalNamespaceEntry)
		assertQueryCounts(t, queryRegistry, 0, 5, 0, 5)
	})
}

func createStartedWorkflow(t *testing.T, deps *testDeps, tv *testvars.TestVars) historyi.WorkflowContext {
	t.Helper()

	ms := workflow.TestLocalMutableState(deps.workflowTaskCompletedHandler.shardContext, deps.mockEventsCache, tv.Namespace(),
		tv.WorkflowID(), tv.RunID(), log.NewTestLogger())

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Input:                    tv.Any().Payloads(),
		WorkflowExecutionTimeout: tv.Any().InfiniteTimeout(),
		WorkflowRunTimeout:       tv.Any().InfiniteTimeout(),
		WorkflowTaskTimeout:      tv.Any().InfiniteTimeout(),
		Identity:                 tv.ClientIdentity(),
	}

	_, _ = ms.AddWorkflowExecutionStartedEvent(
		tv.WorkflowExecution(),
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:             1,
			NamespaceId:         tv.NamespaceID().String(),
			StartRequest:        startRequest,
			ParentExecutionInfo: nil,
		},
	)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			return &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}, nil
		}).AnyTimes()

	// Create WF context in the cache and load MS for it.
	wfContext, release, err := deps.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		deps.mockShard,
		tv.NamespaceID(),
		tv.WorkflowExecution(),
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.NotNil(t, wfContext)

	loadedMS, err := wfContext.LoadMutableState(context.Background(), deps.mockShard)
	require.NoError(t, err)
	require.NotNil(t, loadedMS)
	release(nil)

	return wfContext
}

func createSentUpdate(t *testing.T, deps *testDeps, tv *testvars.TestVars, wfContext historyi.WorkflowContext) (*protocolpb.Message, *update.Update, []byte) {
	t.Helper()

	ctx := context.Background()

	ms, err := wfContext.LoadMutableState(ctx, deps.workflowTaskCompletedHandler.shardContext)
	require.NoError(t, err)

	// 1. Create speculative WFT for update.
	wt, _ := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	_, _, _ = ms.AddWorkflowTaskStartedEvent(
		wt.ScheduledEventID,
		tv.RunID(),
		tv.StickyTaskQueue(),
		tv.Any().String(),
		nil,
		nil,
		nil,
		false,
	)
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      tv.NamespaceID().String(),
		WorkflowId:       tv.WorkflowID(),
		RunId:            tv.RunID(),
		ScheduledEventId: wt.ScheduledEventID,
	}
	serializedTaskToken, err := taskToken.Marshal()
	require.NoError(t, err)

	// 2. Create update.
	upd, alreadyExisted, err := wfContext.UpdateRegistry(ctx).FindOrCreate(ctx, tv.UpdateID())
	require.False(t, alreadyExisted)
	require.NoError(t, err)

	updReq := &updatepb.Request{
		Meta: &updatepb.Meta{UpdateId: tv.UpdateID()},
		Input: &updatepb.Input{
			Name: tv.HandlerName(),
			Args: payloads.EncodeString("args-value-of-" + tv.UpdateID()),
		}}

	eventStore := workflow.WithEffects(effect.Immediate(ctx), ms)

	err = upd.Admit(updReq, eventStore)
	require.NoError(t, err)

	seqID := &protocolpb.Message_EventId{EventId: tv.Any().EventID()}
	msg := upd.Send(false, seqID)
	require.NotNil(t, msg)

	updRequestMsg := &protocolpb.Message{
		Id:                 tv.Any().String(),
		ProtocolInstanceId: tv.UpdateID(),
		SequencingId:       seqID,
		Body:               protoutils.MarshalAny(t, updReq),
	}

	return updRequestMsg, upd, serializedTaskToken
}
