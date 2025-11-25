package history

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type engine2SuiteDeps struct {
	controller                  *gomock.Controller
	mockShard                   *shard.ContextTest
	mockTxProcessor             *queues.MockQueue
	mockTimerProcessor          *queues.MockQueue
	mockVisibilityProcessor     *queues.MockQueue
	mockArchivalProcessor       *queues.MockQueue
	mockMemoryScheduledQueue    *queues.MockQueue
	mockEventsCache             *events.MockCache
	mockNamespaceCache          *namespace.MockRegistry
	mockClusterMetadata         *cluster.MockMetadata
	mockVisibilityManager       *manager.MockVisibilityManager
	mockWorkflowStateReplicator *ndc.MockWorkflowStateReplicator

	workflowCache    wcache.Cache
	historyEngine    *historyEngineImpl
	mockExecutionMgr *persistence.MockExecutionManager

	config        *configs.Config
	logger        *log.MockLogger
	errorMessages []string
	tv            *testvars.TestVars
}

func TestRecordWorkflowTaskStartedSuccessStickyEnabled(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

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

	deps.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)
	deps.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	deps.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

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
	expectedResponse.Version = tests.GlobalNamespaceEntry.FailoverVersion()
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
	currentBranchTokken, err := ms.GetCurrentBranchToken()
	require.NoError(t, err)
	expectedResponse.BranchToken = currentBranchTokken
	expectedResponse.History = &historypb.History{Events: fakeHistory}
	expectedResponse.NextPageToken = nil

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &request)
	require.Nil(t, err)
	require.NotNil(t, response)
	require.True(t, response.StartedTime.AsTime().After(expectedResponse.ScheduledTime.AsTime()))
	expectedResponse.StartedTime = response.StartedTime
	require.Equal(t, &expectedResponse, response)

}

func TestRecordWorkflowTaskStartedSuccessStickyEnabled_WithInternalRawHistory(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.config.SendRawHistoryBetweenInternalServices = func() bool { return true }
	serializer := serialization.NewSerializer()
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
	historyBlob, err := serializer.SerializeEvents(fakeHistory)
	require.NoError(t, err)

	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         historyBlob.Data,
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	stickyTl := "stickyTaskQueue"
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	executionInfo := ms.GetExecutionInfo()
	executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	executionInfo.StickyTaskQueue = stickyTl

	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)

	wfMs := workflow.TestCloneToProto(ms)

	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

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
	expectedResponse.Version = tests.GlobalNamespaceEntry.FailoverVersion()
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
	currentBranchTokken, err := ms.GetCurrentBranchToken()
	require.NoError(t, err)
	expectedResponse.BranchToken = currentBranchTokken
	expectedResponse.RawHistory = [][]byte{historyBlob.Data}
	expectedResponse.NextPageToken = nil

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &request)
	require.Nil(t, err)
	require.NotNil(t, response)
	require.True(t, response.StartedTime.AsTime().After(expectedResponse.ScheduledTime.AsTime()))
	expectedResponse.StartedTime = response.StartedTime
	require.Equal(t, &expectedResponse, response)

}

func TestRecordWorkflowTaskStartedIfNoExecution(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	require.Nil(t, response)
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRecordWorkflowTaskStarted_NoMessages(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, false, false)
	// Use UpdateCurrentVersion explicitly here,
	// because there is no call to CloseTransactionAsSnapshot,
	// because it converts speculative WT to normal, but WT needs to be speculative for this test.
	err := ms.UpdateCurrentVersion(tests.GlobalNamespaceEntry.FailoverVersion(), true)
	require.NoError(t, err)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			wfMs := ms.CloneToProto()
			gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
			return gwmsResponse, nil
		},
	)

	wt, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	require.NoError(t, err)
	require.NotNil(t, wt)

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  wt.ScheduledEventID,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	require.Nil(t, response)
	require.Error(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err, err.Error())
	require.EqualError(t, err, "No messages for speculative workflow task.")

}

func TestRecordWorkflowTaskStartedIfGetExecutionFailed(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("FAILED"))

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	require.Nil(t, response)
	require.NotNil(t, err)
	require.EqualError(t, err, "FAILED")

}

func TestRecordWorkflowTaskStartedIfTaskAlreadyStarted(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, true)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	require.Nil(t, response)
	require.NotNil(t, err)
	require.IsType(t, &serviceerrors.TaskAlreadyStarted{}, err)
	deps.logger.Error("RecordWorkflowTaskStarted failed with", tag.Error(err))

}

func TestRecordWorkflowTaskStartedIfTaskAlreadyCompleted(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, true)
	addWorkflowTaskCompletedEvent(&deps.Suite, ms, int64(2), int64(3), identity)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	require.Nil(t, response)
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)
	deps.logger.Error("RecordWorkflowTaskStarted failed with", tag.Error(err))

}

func TestRecordWorkflowTaskStartedConflictOnUpdate(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &persistence.ConditionFailedError{})

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	require.NotNil(t, err)
	require.Nil(t, response)
	require.Equal(t, &persistence.ConditionFailedError{}, err)

}

func TestRecordWorkflowTaskStartedSuccess(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

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

	deps.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: fakeHistory,
		NextPageToken: []byte{},
		Size:          1,
	}, nil)
	deps.mockShard.Resource.SearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap(), nil)
	deps.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).AnyTimes()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := deps.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		deps.mockShard,
		tests.NamespaceID,
		workflowExecution,
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

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	require.Nil(t, err)
	require.NotNil(t, response)
	require.Equal(t, "wType", response.WorkflowType.Name)
	require.True(t, response.PreviousStartedEventId == 0)
	require.Equal(t, int64(3), response.StartedEventId)
	expectedQueryMap := map[string]*querypb.WorkflowQuery{
		id1: {},
		id2: {},
		id3: {},
	}
	require.Equal(t, expectedQueryMap, response.Queries)

}

func TestRecordWorkflowTaskStartedSuccessWithInternalRawHistory(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.config.SendRawHistoryBetweenInternalServices = func() bool { return true }
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

	deps.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         historyBlob,
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	tl := "testTaskQueue"
	identity := "testIdentity"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	// load mutable state such that it already exists in memory when respond workflow task is called
	// this enables us to set query registry on it
	ctx, release, err := deps.workflowCache.GetOrCreateWorkflowExecution(
		metrics.AddMetricsContext(context.Background()),
		deps.mockShard,
		tests.NamespaceID,
		workflowExecution,
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

	response, err := deps.historyEngine.RecordWorkflowTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  2,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})

	require.Nil(t, err)
	require.NotNil(t, response)
	require.Equal(t, "wType", response.WorkflowType.Name)
	require.True(t, response.PreviousStartedEventId == 0)
	require.Equal(t, int64(3), response.StartedEventId)
	expectedQueryMap := map[string]*querypb.WorkflowQuery{
		id1: {},
		id2: {},
		id3: {},
	}
	require.Equal(t, expectedQueryMap, response.Queries)

}

func TestRecordActivityTaskStartedIfNoExecution(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	response, err := deps.historyEngine.RecordActivityTaskStarted(
		metrics.AddMetricsContext(context.Background()),
		&historyservice.RecordActivityTaskStartedRequest{
			NamespaceId:       namespaceID.String(),
			WorkflowExecution: workflowExecution,
			ScheduledEventId:  5,
			RequestId:         "reqId",
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: tl,
				},
				Identity: identity,
			},
		},
	)
	if err != nil {
		deps.logger.Error("Unexpected Error", tag.Error(err))
	}
	require.Nil(t, response)
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRecordActivityTaskStartedSuccess(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := payloads.EncodeString("input1")

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, true)
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, int64(2), int64(3), identity)
	scheduledEvent, _ := addActivityTaskScheduledEvent(ms, workflowTaskCompletedEvent.EventId, activityID, activityType, tl, activityInput, 100*time.Second, 10*time.Second, 1*time.Second, 5*time.Second)

	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	deps.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		gomock.Any(),
		events.EventKey{
			NamespaceID: namespaceID,
			WorkflowID:  workflowExecution.GetWorkflowId(),
			RunID:       workflowExecution.GetRunId(),
			EventID:     scheduledEvent.GetEventId(),
			Version:     0,
		},
		workflowTaskCompletedEvent.GetEventId(),
		gomock.Any(),
	).Return(scheduledEvent, nil)
	response, err := deps.historyEngine.RecordActivityTaskStarted(metrics.AddMetricsContext(context.Background()), &historyservice.RecordActivityTaskStartedRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: workflowExecution,
		ScheduledEventId:  5,
		RequestId:         "reqId",
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
			},
			Identity: identity,
		},
	})
	require.Nil(t, err)
	require.NotNil(t, response)
	require.Equal(t, scheduledEvent, response.ScheduledEvent)

}

func TestRequestCancelWorkflowExecution_Running(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	require.Nil(t, err)

	ms2 := deps.getMutableState(namespaceID, workflowExecution)
	require.Equal(t, int64(4), ms2.GetNextEventID())

}

func TestRequestCancelWorkflowExecution_Finished(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := deps.createExecutionStartedState(workflowExecution, tl, identity, true, false)
	ms.GetExecutionState().State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	_, err := deps.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	require.Nil(t, err)

}

func TestRequestCancelWorkflowExecution_NotFound(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))

	_, err := deps.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
	})
	require.NotNil(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestRequestCancelWorkflowExecution_ParentMismatch(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	parentInfo := &workflowspb.ParentExecutionInfo{
		NamespaceId: tests.ParentNamespaceID.String(),
		Namespace:   tests.ParentNamespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "parent wId",
			RunId:      "parent rId",
		},
		InitiatedId:      123,
		InitiatedVersion: 456,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := deps.createExecutionStartedStateWithParent(workflowExecution, tl, parentInfo, identity, true, false)
	ms1 := workflow.TestCloneToProto(ms)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	_, err := deps.historyEngine.RequestCancelWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: "identity",
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "unknown wId",
			RunId:      "unknown rId",
		},
		ChildWorkflowOnly: true,
	})
	require.Equal(t, consts.ErrWorkflowParent, err)

}

func TestTerminateWorkflowExecution_ParentMismatch(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	parentInfo := &workflowspb.ParentExecutionInfo{
		NamespaceId: tests.ParentNamespaceID.String(),
		Namespace:   tests.ParentNamespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "parent wId",
			RunId:      "parent rId",
		},
		InitiatedId:      123,
		InitiatedVersion: 456,
	}

	identity := "testIdentity"
	tl := "testTaskQueue"

	ms := deps.createExecutionStartedStateWithParent(workflowExecution, tl, parentInfo, identity, true, false)
	ms1 := workflow.TestCloneToProto(ms)
	currentExecutionResp := &persistence.GetCurrentExecutionResponse{
		RunID: tests.RunID,
	}
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(currentExecutionResp, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse1, nil)

	_, err := deps.historyEngine.TerminateWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
			},
			Identity:            "identity",
			FirstExecutionRunId: workflowExecution.RunId,
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: "unknown wId",
			RunId:      "unknown rId",
		},
		ChildWorkflowOnly: true,
	})
	require.Equal(t, consts.ErrWorkflowParent, err)

}

func TestRespondWorkflowTaskCompletedRecordMarkerCommand(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       "wId",
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	serializedTaskToken, _ := taskToken.Marshal()
	identity := "testIdentity"
	markerDetails := payloads.EncodeString("marker details")
	markerName := "marker name"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
			MarkerName: markerName,
			Details: map[string]*commonpb.Payloads{
				"data": markerDetails,
			},
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(metrics.AddMetricsContext(context.Background()), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: namespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)
	ms2 := deps.getMutableState(namespaceID, we)
	require.Equal(t, int64(6), ms2.GetNextEventID())
	require.Equal(t, int64(3), ms2.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, ms2.GetExecutionState().State)
	require.False(t, ms2.HasPendingWorkflowTask())

}

func TestRespondWorkflowTaskCompleted_StartChildWithSearchAttributes(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       "wId",
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	serializedTaskToken, _ := taskToken.Marshal()
	identity := "testIdentity"

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		we.GetWorkflowId(), we.GetRunId(), log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, nil, 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, tl, identity)

	commands := []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:    tests.Namespace.String(),
			WorkflowId:   tests.WorkflowID,
			WorkflowType: &commonpb.WorkflowType{Name: "wType"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: tl},
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"AliasForText01": payload.EncodeString("search attribute value")},
			},
		}},
	}}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		eventsToSave := request.UpdateWorkflowEvents[0].Events
		require.Len(t, eventsToSave, 2)
		require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, eventsToSave[0].GetEventType())
		require.Equal(t, enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, eventsToSave[1].GetEventType())
		startChildEventAttributes := eventsToSave[1].GetStartChildWorkflowExecutionInitiatedEventAttributes()
		// Search attribute name was mapped and saved under field name.
		protorequire.ProtoEqual(t,
			payload.EncodeString("search attribute value"),
			startChildEventAttributes.GetSearchAttributes().GetIndexedFields()["Text01"])
		return tests.UpdateWorkflowExecutionResponse, nil
	})

	deps.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil)

	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(metrics.AddMetricsContext(context.Background()), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: serializedTaskToken,
			Commands:  commands,
			Identity:  identity,
		},
	})
	require.Nil(t, err)

}

func TestRespondWorkflowTaskCompleted_StartChildWorkflow_ExceedsLimit(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	workflowType := "testWorkflowType"

	we := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	ms := workflow.TestLocalMutableState(
		deps.historyEngine.shardContext,
		deps.mockEventsCache,
		tests.LocalNamespaceEntry,
		we.GetWorkflowId(),
		we.GetRunId(),
		log.NewTestLogger(),
	)

	addWorkflowExecutionStartedEvent(
		ms,
		we,
		workflowType,
		taskQueue,
		nil,
		time.Minute,
		time.Minute,
		time.Minute,
		identity,
	)

	deps.mockShard.SetLoggers(deps.logger)
	deps.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	var commands []*commandpb.Command
	for i := 0; i < 6; i++ {
		commands = append(
			commands,
			&commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace:    tests.Namespace.String(),
						WorkflowId:   tests.WorkflowID,
						WorkflowType: &commonpb.WorkflowType{Name: workflowType},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
					}},
			},
		)
	}

	wt := addWorkflowTaskScheduledEvent(ms)
	addWorkflowTaskStartedEvent(
		ms,
		wt.ScheduledEventID,
		taskQueue,
		identity,
	)
	taskToken := &tokenspb.Task{
		Attempt:          1,
		NamespaceId:      namespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            we.GetRunId(),
		ScheduledEventId: 2,
	}
	taskTokenBytes, _ := taskToken.Marshal()
	response := &persistence.GetWorkflowExecutionResponse{State: workflow.TestCloneToProto(ms)}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(response, nil).AnyTimes()
	deps.mockShard.Resource.SearchAttributesMapperProvider.EXPECT().
		GetMapper(tests.Namespace).
		Return(&searchattribute.TestMapper{Namespace: tests.Namespace.String()}, nil).
		AnyTimes()
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	deps.historyEngine.shardContext.GetConfig().NumPendingChildExecutionsLimit = func(namespace string) int {
		return 5
	}
	_, err := deps.historyEngine.RespondWorkflowTaskCompleted(metrics.AddMetricsContext(context.Background()), &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
			TaskToken: taskTokenBytes,
			Commands:  commands,
			Identity:  identity,
		},
	})

	require.Error(t, err)
	require.IsType(t, &serviceerror.InvalidArgument{}, err)
	require.Len(t, deps.errorMessages, 1)
	require.Equal(t, "the number of pending child workflow executions, 5, has reached the per-workflow limit of 5", deps.errorMessages[0])

}

func TestStartWorkflowExecution_BrandNew(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	requestID := uuid.New()
	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: durationpb.New(20 * time.Second),
			WorkflowRunTimeout:       durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
		},
	})
	require.Nil(t, err)
	require.NotNil(t, resp.RunId)
	require.True(t, resp.Started)
	require.Nil(t, resp.EagerWorkflowTask)

}

func TestStartWorkflowExecution_BrandNew_SearchAttributes(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"

	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
		eventsToSave := request.NewWorkflowEvents[0].Events
		require.Len(t, eventsToSave, 2)
		require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, eventsToSave[0].GetEventType())
		startEventAttributes := eventsToSave[0].GetWorkflowExecutionStartedEventAttributes()
		// Search attribute name was mapped and saved under field name.
		protorequire.ProtoEqual(t,
			payload.EncodeString("test"),
			startEventAttributes.GetSearchAttributes().GetIndexedFields()["Keyword01"])
		return tests.CreateWorkflowExecutionResponse, nil
	})

	requestID := uuid.New()
	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: namespaceID.String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowExecutionTimeout: durationpb.New(20 * time.Second),
			WorkflowRunTimeout:       durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"Keyword01": payload.EncodeString("test"),
			}}},
	})
	require.Nil(t, err)
	require.NotNil(t, resp.RunId)
	require.True(t, resp.Started)
	require.Nil(t, resp.EagerWorkflowTask)

}

func makeMockStartRequest(
	tv *testvars.TestVars,
	wfReusePolicy enumspb.WorkflowIdReusePolicy,
	wfConflictPolicy enumspb.WorkflowIdConflictPolicy,
) *historyservice.StartWorkflowExecutionRequest {
	return &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: tv.NamespaceID().String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                tv.NamespaceID().String(),
			WorkflowId:               tv.WorkflowID(),
			WorkflowType:             tv.WorkflowType(),
			TaskQueue:                tv.TaskQueue(),
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			WorkflowIdReusePolicy:    wfReusePolicy,
			WorkflowIdConflictPolicy: wfConflictPolicy,
			Identity:                 tv.WorkerIdentity(),
			RequestId:                tv.Any().String(),
		},
	}
}

func makeCurrentWorkflowConditionFailedError(
	tv *testvars.TestVars,
	startTime *timestamppb.Timestamp,
) error {
	lastWriteVersion := common.EmptyVersion
	tv1 := tv.WithRequestID("AttachedRequestID1")
	tv2 := tv.WithRequestID("AttachedRequestID2")
	return &persistence.CurrentWorkflowConditionFailedError{
		Msg: "random message",
		RequestIDs: map[string]*persistencespb.RequestIDInfo{
			tv.RequestID(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   common.FirstEventID,
			},
			tv1.RequestID(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventId:   3,
			},
			tv2.RequestID(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventId:   common.BufferedEventID,
			},
		},
		RunID:            tv.RunID(),
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: lastWriteVersion,
		StartTime:        timestamp.TimeValuePtr(startTime),
	}
}

func TestStartWorkflowExecution_Dedup_Running_CalledTooSoon(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	// error when id reuse policy is WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING but called too soon
	deps.setupStartWorkflowExecutionForRunning()

	startRequest := makeMockStartRequest(deps.tv, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)

	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)

	var expectedErr *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &expectedErr)
	require.Nil(t, resp)

}

func TestStartWorkflowExecution_Dedup_Running_SameRequestID(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	// no error when request ID is the same
	deps.setupStartWorkflowExecutionForRunning()
	startRequest := makeMockStartRequest(deps.tv, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)
	startRequest.StartRequest.RequestId = deps.tv.RequestID()

	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)

	require.NoError(t, err)
	require.True(t, resp.Started)
	require.Equal(t, deps.tv.RunID(), resp.GetRunId())

}

func TestStartWorkflowExecution_Dedup_Running_SameAttachedRequestID(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	// no error when request ID is the same
	deps.setupStartWorkflowExecutionForRunning()
	startRequest := makeMockStartRequest(
		deps.tv,
		enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
	)

	tv1 := deps.tv.WithRequestID("AttachedRequestID1")
	startRequest.StartRequest.RequestId = tv1.RequestID()
	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)
	require.NoError(t, err)
	require.False(t, resp.Started)
	require.Equal(t, deps.tv.RunID(), resp.GetRunId())

	tv2 := deps.tv.WithRequestID("AttachedRequestID2")
	startRequest.StartRequest.RequestId = tv2.RequestID()
	resp, err = deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)
	require.NoError(t, err)
	require.False(t, resp.Started)
	require.Equal(t, deps.tv.RunID(), resp.GetRunId())

}

func TestStartWorkflowExecution_Dedup_Running_PolicyFail(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	// error when id conflict policy is POLICY_FAIL
	deps.setupStartWorkflowExecutionForRunning()

	startRequest := makeMockStartRequest(deps.tv, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL)

	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)

	var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
	require.ErrorAs(t, err, &expectedErr)
	require.Nil(t, resp)

}

func TestStartWorkflowExecution_Dedup_Running_UseExisting(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	// ignore error when id conflict policy is USE_EXISTING
	deps.setupStartWorkflowExecutionForRunning()

	startRequest := makeMockStartRequest(deps.tv, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)

	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)

	require.NoError(t, err)
	require.False(t, resp.Started)
	require.Equal(t, deps.tv.RunID(), resp.GetRunId())

}

func TestStartWorkflowExecution_Terminate_Running(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.setupStartWorkflowExecutionForTerminate()

	startRequest := makeMockStartRequest(deps.tv, enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING, enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED)

	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)

	require.NoError(t, err)
	require.True(t, resp.Started)
	require.NotEqual(t, deps.tv.RunID(), resp.GetRunId())

}

func TestStartWorkflowExecution_Terminate_Existing(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.setupStartWorkflowExecutionForTerminate()

	startRequest := makeMockStartRequest(deps.tv, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)

	resp, err := deps.historyEngine.StartWorkflowExecution(metrics.AddMetricsContext(context.Background()), startRequest)

	require.NoError(t, err)
	require.True(t, resp.Started)
	require.NotEqual(t, deps.tv.RunID(), resp.GetRunId())

}

func TestStartWorkflowExecution_Dedup(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	namespaceID := tests.NamespaceID
	workflowID := "workflowID"
	prevRunID := uuid.New()
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	requestID := "requestID"
	prevRequestID := "oldRequestID"
	lastWriteVersion := common.EmptyVersion

	brandNewExecutionRequest := mock.MatchedBy(func(request *persistence.CreateWorkflowExecutionRequest) bool {
		return request.Mode == persistence.CreateWorkflowModeBrandNew
	})

	makeStartRequest := func(
		wfReusePolicy enumspb.WorkflowIdReusePolicy,
		wfConflictPolicy enumspb.WorkflowIdConflictPolicy,
	) *historyservice.StartWorkflowExecutionRequest {
		return &historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                namespaceID.String(),
				WorkflowId:               workflowID,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
				WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
				WorkflowIdReusePolicy:    wfReusePolicy,
				WorkflowIdConflictPolicy: wfConflictPolicy,
				Identity:                 identity,
				RequestId:                requestID,
			},
		}
	}

	now := deps.historyEngine.shardContext.GetTimeSource().Now()
	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, workflowID, prevRunID)
	ms.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{{Version: 0, EventId: 0}}
	ms.GetExecutionState().StartTime = timestamppb.New(now.Add(-2 * time.Second))

	deps.Run("when workflow completed", func() {
		makeCurrentWorkflowConditionFailedError := func(
			requestID string,
		) error {
			return &persistence.CurrentWorkflowConditionFailedError{
				Msg: "random message",
				RequestIDs: map[string]*persistencespb.RequestIDInfo{
					requestID: {
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						EventId:   common.FirstEventID,
					},
				},
				RunID:            prevRunID,
				State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				LastWriteVersion: lastWriteVersion,
				StartTime:        nil,
			}
		}

		updateExecutionRequest := mock.MatchedBy(func(request *persistence.CreateWorkflowExecutionRequest) bool {
			return request.Mode == persistence.CreateWorkflowModeUpdateCurrent &&
				request.PreviousRunID == prevRunID &&
				request.PreviousLastWriteVersion == lastWriteVersion
		})

		deps.Run("ignore error when request ID is the same", func() {
			deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
				Return(nil, makeCurrentWorkflowConditionFailedError(requestID)) // *same* request ID!

			resp, err := deps.historyEngine.StartWorkflowExecution(
				metrics.AddMetricsContext(context.Background()),
				makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

			require.NoError(t, err)
			require.Equal(t, prevRunID, resp.GetRunId())
		})

		deps.Run("with success", func() {

			deps.Run("and id reuse policy is ALLOW_DUPLICATE", func() {
				deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))
				deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), updateExecutionRequest).
					Return(tests.CreateWorkflowExecutionResponse, nil)

				resp, err := deps.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				require.NoError(t, err)
				require.True(t, resp.Started)
				require.NotEqual(t, prevRunID, resp.GetRunId())
			})

			deps.Run("and id reuse policy is TERMINATE_IF_RUNNING", func() {
				deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))
				deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), updateExecutionRequest).
					Return(tests.CreateWorkflowExecutionResponse, nil)

				resp, err := deps.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				require.NoError(t, err)
				require.True(t, resp.Started)
				require.NotEqual(t, prevRunID, resp.GetRunId())
			})

			deps.Run("and id reuse policy ALLOW_DUPLICATE_FAILED_ONLY", func() {
				deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

				resp, err := deps.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
				require.ErrorAs(t, err, &expectedErr)
				require.Nil(t, resp)
			})

			deps.Run("and id reuse policy REJECT_DUPLICATE", func() {
				deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
					Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

				resp, err := deps.historyEngine.StartWorkflowExecution(
					metrics.AddMetricsContext(context.Background()),
					makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

				var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
				require.ErrorAs(t, err, &expectedErr)
				require.Nil(t, resp)
			})
		})

		deps.Run("with failure", func() {
			failureStatuses := []enumspb.WorkflowExecutionStatus{
				enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
				enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
				enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
			}

			for _, status := range failureStatuses {
				makeCurrentWorkflowConditionFailedError := func(
					requestID string,
				) error {
					return &persistence.CurrentWorkflowConditionFailedError{
						Msg: "random message",
						RequestIDs: map[string]*persistencespb.RequestIDInfo{
							requestID: {
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
								EventId:   common.FirstEventID,
							},
						},
						RunID:            prevRunID,
						State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
						Status:           status,
						LastWriteVersion: lastWriteVersion,
						StartTime:        nil,
					}
				}

				deps.Run(fmt.Sprintf("status %v", status), func() {
					deps.Run("and id reuse policy ALLOW_DUPLICATE", func() {
						deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
							Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

						resp, err := deps.historyEngine.StartWorkflowExecution(
							metrics.AddMetricsContext(context.Background()),
							makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

						var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
						require.ErrorAs(t, err, &expectedErr)
						require.Nil(t, resp)
					})

					deps.Run("and id reuse policy ALLOW_DUPLICATE_FAILED_ONLY", func() {
						deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
							Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))
						deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), updateExecutionRequest).
							Return(tests.CreateWorkflowExecutionResponse, nil)

						resp, err := deps.historyEngine.StartWorkflowExecution(
							metrics.AddMetricsContext(context.Background()),
							makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

						require.NoError(t, err)
						require.True(t, resp.Started)
						require.NotEqual(t, prevRunID, resp.GetRunId())
					})

					deps.Run("and id reuse policy REJECT_DUPLICATE", func() {
						deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), brandNewExecutionRequest).
							Return(nil, makeCurrentWorkflowConditionFailedError(prevRequestID))

						resp, err := deps.historyEngine.StartWorkflowExecution(
							metrics.AddMetricsContext(context.Background()),
							makeStartRequest(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL))

						var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
						require.ErrorAs(t, err, &expectedErr)
						require.Nil(t, resp)
					})
				})
			}
		})
	})

}

func TestSignalWithStartWorkflowExecution_JustSignal(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
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

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, identity)
	_ = addWorkflowTaskScheduledEvent(ms)
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)

	resp, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	require.Nil(t, err)
	require.Equal(t, runID, resp.GetRunId())

}

func TestSignalWithStartWorkflowExecution_WorkflowNotExist(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
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

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, notExistErr)
	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	require.Nil(t, err)
	require.NotNil(t, resp.GetRunId())

}

func TestSignalWithStartWorkflowExecution_WorkflowNotRunning(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.config.EnableWorkflowIdReuseStartTimeValidation = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"

	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{}
	_, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	require.EqualError(t, err, "Missing namespace UUID.")

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	runID := tests.RunID
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
			Input:                    input,
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			SignalName:               signalName,
			SignalInput:              nil,
			Control:                  "",
			RetryPolicy:              nil,
			CronSchedule:             "",
			Memo:                     nil,
			SearchAttributes:         nil,
			Header:                   nil,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.CreateWorkflowExecutionResponse, nil)

	resp, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	require.Nil(t, err)
	require.NotNil(t, resp.GetRunId())
	require.NotEqual(t, runID, resp.GetRunId())

}

func TestSignalWithStartWorkflowExecution_Start_DuplicateRequests(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.config.EnableWorkflowIdReuseStartTimeValidation = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	runID := tests.RunID
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := "testRequestID"
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			SignalName:               signalName,
			SignalInput:              nil,
			Control:                  "",
			RetryPolicy:              nil,
			CronSchedule:             "",
			Memo:                     nil,
			SearchAttributes:         nil,
			Header:                   nil,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &persistence.CurrentWorkflowConditionFailedError{
		Msg: "random message",
		RequestIDs: map[string]*persistencespb.RequestIDInfo{
			// use same requestID
			requestID: {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   common.FirstEventID,
			},
		},
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		LastWriteVersion: common.EmptyVersion,
		StartTime:        nil,
	}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, workflowAlreadyStartedErr)

	ctx := metrics.AddMetricsContext(context.Background())
	resp, err := deps.historyEngine.SignalWithStartWorkflowExecution(ctx, sRequest)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRunId())
	require.Equal(t, runID, resp.GetRunId())

}

func TestSignalWithStartWorkflowExecution_Start_WorkflowAlreadyStarted(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	deps.config.EnableWorkflowIdReuseStartTimeValidation = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	namespaceID := tests.NamespaceID
	workflowID := "wId"
	runID := tests.RunID
	workflowType := "workflowType"
	taskQueue := "testTaskQueue"
	identity := "testIdentity"
	signalName := "my signal name"
	input := payloads.EncodeString("test input")
	requestID := "testRequestID"
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	sRequest := &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalWithStartRequest: &workflowservice.SignalWithStartWorkflowExecutionRequest{
			Namespace:                namespaceID.String(),
			WorkflowId:               workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                    input,
			WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(2 * time.Second),
			Identity:                 identity,
			RequestId:                requestID,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			SignalName:               signalName,
			SignalInput:              nil,
			Control:                  "",
			RetryPolicy:              nil,
			CronSchedule:             "",
			Memo:                     nil,
			SearchAttributes:         nil,
			Header:                   nil,
		},
	}

	ms := workflow.TestLocalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, tests.LocalNamespaceEntry,
		workflowID, runID, log.NewTestLogger())
	addWorkflowExecutionStartedEvent(ms, we, "wType", tl, payloads.EncodeString("input"), 100*time.Second, 50*time.Second, 200*time.Second, identity)
	wfMs := workflow.TestCloneToProto(ms)
	wfMs.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &persistence.CurrentWorkflowConditionFailedError{
		Msg: "random message",
		RequestIDs: map[string]*persistencespb.RequestIDInfo{
			"new request ID": {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   common.FirstEventID,
			},
		},
		RunID:            runID,
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastWriteVersion: common.EmptyVersion,
		StartTime:        nil,
	}

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(gceResponse, nil).AnyTimes()
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, workflowAlreadyStartedErr)

	resp, err := deps.historyEngine.SignalWithStartWorkflowExecution(metrics.AddMetricsContext(context.Background()), sRequest)
	require.Nil(t, resp)
	require.NotNil(t, err)

}

func TestRecordChildExecutionCompleted(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		CompletionEvent: &historypb.HistoryEvent{
			EventId:   456,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
			},
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	// reload mutable state due to potential stale mutable state (initiated event not found)
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)
	_, err := deps.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	require.IsType(t, &serviceerror.NotFound{}, err)

	// add child init event
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
	request.ParentInitiatedId = initiatedEvent.GetEventId()
	request.ParentInitiatedVersion = initiatedEvent.GetVersion()
	request.ChildFirstExecutionRunId = childRunID

	// add child started event
	addChildWorkflowExecutionStartedEvent(ms, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	_, err = deps.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	require.NoError(t, err)

}

func TestRecordChildExecutionCompleted_ChildFirstRunId(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	testcases := []struct {
		name            string
		childFirstRunID string
		expectedError   error
	}{
		{
			name:            "empty child first run id",
			childFirstRunID: "",
		},
		{
			name:            "child first run id is set",
			childFirstRunID: childRunID,
		},
		{
			name:            "child first run id doesnt match",
			childFirstRunID: "wrong_run_id",
			expectedError:   consts.ErrChildExecutionNotFound,
		},
	}

	for _, tc := range testcases {
		deps.Run(tc.name, func() {
			request := &historyservice.RecordChildExecutionCompletedRequest{
				NamespaceId: tests.NamespaceID.String(),
				ParentExecution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      tests.RunID,
				},
				ChildExecution: &commonpb.WorkflowExecution{
					WorkflowId: childWorkflowID,
					RunId:      childRunID,
				},
				CompletionEvent: &historypb.HistoryEvent{
					EventId:   456,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
						WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
					},
				},
				ParentInitiatedId:      123,
				ParentInitiatedVersion: 100,
			}

			ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
			addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
			wfMs := workflow.TestCloneToProto(ms)
			gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

			// reload mutable state due to potential stale mutable state (initiated event not found)
			deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil).Times(2)
			_, err := deps.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
			require.IsType(t, &serviceerror.NotFound{}, err)

			// add child init event
			wt := addWorkflowTaskScheduledEvent(ms)
			workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", uuid.New())
			wt.StartedEventID = workflowTasksStartEvent.GetEventId()
			workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

			initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(),
				tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
			request.ParentInitiatedId = initiatedEvent.GetEventId()
			request.ParentInitiatedVersion = initiatedEvent.GetVersion()
			request.ChildFirstExecutionRunId = tc.childFirstRunID

			// add child started event
			addChildWorkflowExecutionStartedEvent(ms, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

			wfMs = workflow.TestCloneToProto(ms)
			gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
			deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
			if tc.expectedError == nil {
				deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
			}
			_, err = deps.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

}

func TestRecordChildExecutionCompleted_MissingChildStartedEvent(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		CompletionEvent: &historypb.HistoryEvent{
			EventId:   456,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
			},
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	// add child init event
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
	request.ParentInitiatedId = initiatedEvent.GetEventId()
	request.ParentInitiatedVersion = initiatedEvent.GetVersion()
	request.ChildFirstExecutionRunId = childRunID

	// started event not found, should automatically be added
	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(initiatedEvent, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	_, err := deps.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
	require.NoError(t, err)

}

func TestRecordChildExecutionCompleted_MissingChildStartedEvent_ChildFirstRunId(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	testcases := []struct {
		name            string
		childFirstRunID string
	}{
		{
			name:            "empty child first run id",
			childFirstRunID: "",
		},
		{
			name:            "child first run id is same as current run id",
			childFirstRunID: childRunID,
		},
		{
			name:            "child first run id is different",
			childFirstRunID: "wrong_run_id",
		},
	}

	for _, tc := range testcases {
		deps.Run(tc.name, func() {
			request := &historyservice.RecordChildExecutionCompletedRequest{
				NamespaceId: tests.NamespaceID.String(),
				ParentExecution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      tests.RunID,
				},
				ChildExecution: &commonpb.WorkflowExecution{
					WorkflowId: childWorkflowID,
					RunId:      childRunID,
				},
				CompletionEvent: &historypb.HistoryEvent{
					EventId:   456,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
						WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
					},
				},
				ParentInitiatedId:      123,
				ParentInitiatedVersion: 100,
			}

			ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
			addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
			wfMs := workflow.TestCloneToProto(ms)
			gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

			// add child init event
			wt := addWorkflowTaskScheduledEvent(ms)
			workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, "testTaskQueue", uuid.New())
			wt.StartedEventID = workflowTasksStartEvent.GetEventId()
			workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

			initiatedEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(),
				tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)
			request.ParentInitiatedId = initiatedEvent.GetEventId()
			request.ParentInitiatedVersion = initiatedEvent.GetVersion()
			request.ChildFirstExecutionRunId = tc.childFirstRunID

			// started event not found, should automatically be added
			wfMs = workflow.TestCloneToProto(ms)
			gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
			deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
			deps.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(initiatedEvent, nil)
			deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
			_, err := deps.historyEngine.RecordChildExecutionCompleted(metrics.AddMetricsContext(context.Background()), request)
			require.NoError(t, err)
		})
	}

}

func TestVerifyChildExecutionCompletionRecorded_WorkflowNotExist(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.ParentNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NotFound{})

	_, err := deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestVerifyChildExecutionCompletionRecorded_ResendParent(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.ParentNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
		ResendParent:           true,
	}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NotFound{})

	mockClusterMetadata := cluster.NewMockMetadata(deps.controller)
	mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo)
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockShard.SetClusterMetadata(mockClusterMetadata)

	// Add expectation for SyncWorkflowState
	req := &adminservice.SyncWorkflowStateRequest{
		NamespaceId: request.NamespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: request.ParentExecution.WorkflowId,
			RunId:      request.ParentExecution.RunId,
		},
		VersionedTransition: nil,
		VersionHistories:    nil,
		TargetClusterId:     int32(cluster.TestAlternativeClusterInitialFailoverVersion),
	}
	resp := &adminservice.SyncWorkflowStateResponse{
		VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{
			StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
				SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
					State: &persistencespb.WorkflowMutableState{
						Checksum: &persistencespb.Checksum{
							Value: []byte("test-checksum"),
						},
					},
				},
			},
		},
	}
	deps.mockShard.Resource.RemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		req,
	).Return(resp, nil)
	deps.mockWorkflowStateReplicator.EXPECT().ReplicateVersionedTransition(gomock.Any(), resp.VersionedTransitionArtifact, cluster.TestCurrentClusterName).Return(nil)

	// prepare closed workflow
	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	_, err := ms.AddTimeoutWorkflowEvent(
		ms.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	require.NoError(t, err)
	ms.GetExecutionInfo().VersionHistories = &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{4, 5, 6},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 456, Version: 100},
				},
			},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.NoError(t, err)

}

func TestVerifyChildExecutionCompletionRecorded_WorkflowClosed(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.ParentNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	_, err := ms.AddTimeoutWorkflowEvent(
		ms.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	require.NoError(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.NoError(t, err)

}

func TestVerifyChildExecutionCompletionRecorded_InitiatedEventNotFound(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: 100,
	}

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.IsType(t, &serviceerror.WorkflowNotReady{}, err)

}

func TestVerifyChildExecutionCompletionRecorded_InitiatedEventFoundOnNonCurrentBranch(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	inititatedVersion := tests.Version - 100
	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child workflowId",
			RunId:      "child runId",
		},
		ParentInitiatedId:      123,
		ParentInitiatedVersion: inititatedVersion,
	}

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	ms.GetExecutionInfo().VersionHistories = &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{1, 2, 3},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 100, Version: inititatedVersion},
					{EventId: 456, Version: tests.Version},
				},
			},
			{
				BranchToken: []byte{4, 5, 6},
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 456, Version: inititatedVersion},
				},
			},
		},
	}

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.IsType(t, &serviceerror.NotFound{}, err)

}

func TestVerifyChildExecutionCompletionRecorded_InitiatedEventFoundOnCurrentBranch(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	taskQueueName := "testTaskQueue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	addWorkflowExecutionStartedEvent(ms, &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}, "wType", taskQueueName, payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	wt := addWorkflowTaskScheduledEvent(ms)
	workflowTasksStartEvent := addWorkflowTaskStartedEvent(ms, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()
	workflowTaskCompletedEvent := addWorkflowTaskCompletedEvent(&deps.Suite, ms, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	initiatedEvent, ci := addStartChildWorkflowExecutionInitiatedEvent(ms, workflowTaskCompletedEvent.GetEventId(),
		tests.ChildNamespace, tests.ChildNamespaceID, childWorkflowID, childWorkflowType, childTaskQueueName, nil, 1*time.Second, 1*time.Second, 1*time.Second, enumspb.PARENT_CLOSE_POLICY_TERMINATE)

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		ParentInitiatedId:      initiatedEvent.GetEventId(),
		ParentInitiatedVersion: initiatedEvent.GetVersion(),
	}

	// child workflow not started in mutable state
	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err := deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.IsType(t, &serviceerror.WorkflowNotReady{}, err)

	// child workflow started but not completed
	addChildWorkflowExecutionStartedEvent(ms, initiatedEvent.GetEventId(), childWorkflowID, childRunID, childWorkflowType, nil)

	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.IsType(t, &serviceerror.WorkflowNotReady{}, err)

	// child completion recorded
	addChildWorkflowExecutionCompletedEvent(
		ms,
		ci.InitiatedEventId,
		&commonpb.WorkflowExecution{
			WorkflowId: childWorkflowID,
			RunId:      childRunID,
		},
		&historypb.WorkflowExecutionCompletedEventAttributes{
			Result:                       payloads.EncodeString("some random child workflow execution result"),
			WorkflowTaskCompletedEventId: workflowTaskCompletedEvent.GetEventId(),
		},
	)

	wfMs = workflow.TestCloneToProto(ms)
	gwmsResponse = &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	_, err = deps.historyEngine.VerifyChildExecutionCompletionRecorded(metrics.AddMetricsContext(context.Background()), request)
	require.NoError(t, err)

}

func TestRefreshWorkflowTasks(t *testing.T) {
	deps := setupEngine2Test(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()
	defer deps.historyEngine.eventNotifier.Stop()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	ms := workflow.TestGlobalMutableState(deps.historyEngine.shardContext, deps.mockEventsCache, log.NewTestLogger(), tests.Version, tests.WorkflowID, tests.RunID)
	startEvent := addWorkflowExecutionStartedEvent(ms, execution, "wType", "testTaskQueue", payloads.EncodeString("input"), 25*time.Second, 20*time.Second, 200*time.Second, "identity")
	startVersion := startEvent.GetVersion()
	timeoutEvent, err := ms.AddTimeoutWorkflowEvent(
		ms.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	require.NoError(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	deps.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		gomock.Any(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     common.FirstEventID,
			Version:     startVersion,
		},
		common.FirstEventID,
		gomock.Any(),
	).Return(startEvent, nil).AnyTimes()
	deps.mockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		gomock.Any(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     timeoutEvent.GetEventId(),
			Version:     startVersion,
		},
		timeoutEvent.GetEventId(),
		gomock.Any(),
	).Return(startEvent, nil).AnyTimes()

	err = deps.historyEngine.RefreshWorkflowTasks(metrics.AddMetricsContext(context.Background()), tests.NamespaceID, execution)
	require.NoError(t, err)

}

func setupEngine2Test(t *testing.T) *engine2SuiteDeps {
	t.Helper()
	deps := &engine2SuiteDeps{}

	deps.controller = gomock.NewController(t)

	deps.mockTxProcessor = queues.NewMockQueue(deps.controller)
	deps.mockTimerProcessor = queues.NewMockQueue(deps.controller)
	deps.mockVisibilityProcessor = queues.NewMockQueue(deps.controller)
	deps.mockArchivalProcessor = queues.NewMockQueue(deps.controller)
	deps.mockMemoryScheduledQueue = queues.NewMockQueue(deps.controller)
	deps.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	deps.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	deps.mockVisibilityProcessor.EXPECT().Category().Return(tasks.CategoryVisibility).AnyTimes()
	deps.mockArchivalProcessor.EXPECT().Category().Return(tasks.CategoryArchival).AnyTimes()
	deps.mockMemoryScheduledQueue.EXPECT().Category().Return(tasks.CategoryMemoryTimer).AnyTimes()
	deps.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockVisibilityProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockArchivalProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockMemoryScheduledQueue.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	deps.config = tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		s.config,
	)
	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	deps.mockShard = mockShard
	deps.mockNamespaceCache = deps.mockShard.Resource.NamespaceCache
	deps.mockExecutionMgr = deps.mockShard.Resource.ExecutionMgr
	deps.mockClusterMetadata = deps.mockShard.Resource.ClusterMetadata
	deps.mockVisibilityManager = deps.mockShard.Resource.VisibilityManager

	deps.mockEventsCache = deps.mockShard.MockEventsCache
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	deps.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	deps.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	deps.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	deps.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	deps.mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()
	deps.workflowCache = wcache.NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	deps.logger = log.NewMockLogger(deps.controller)
	deps.logger.EXPECT().Debug(gomock.Any(), gomock.Any()).AnyTimes()
	deps.logger.EXPECT().Info(gomock.Any(), gomock.Any()).AnyTimes()
	deps.logger.EXPECT().Warn(gomock.Any(), gomock.Any()).AnyTimes()
	deps.errorMessages = make([]string, 0)
	deps.logger.EXPECT().Error(gomock.Any(), gomock.Any()).AnyTimes().Do(func(msg string, tags ...tag.Tag) {
		deps.errorMessages = append(s.errorMessages, msg)
	})

	deps.mockWorkflowStateReplicator = ndc.NewMockWorkflowStateReplicator(deps.controller)

	h := &historyEngineImpl{
		currentClusterName: deps.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		executionManager:   s.mockExecutionMgr,
		logger:             s.logger,
		throttledLogger:    s.logger,
		metricsHandler:     metrics.NoopMetricsHandler,
		tokenSerializer:    tasktoken.NewSerializer(),
		config:             s.config,
		timeSource:         deps.mockShard.GetTimeSource(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			deps.mockArchivalProcessor.Category():    s.mockArchivalProcessor,
			deps.mockTxProcessor.Category():          s.mockTxProcessor,
			deps.mockTimerProcessor.Category():       s.mockTimerProcessor,
			deps.mockVisibilityProcessor.Category():  s.mockVisibilityProcessor,
			deps.mockMemoryScheduledQueue.Category(): deps.mockMemoryScheduledQueue,
		},
		searchAttributesValidator: searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			deps.mockShard.Resource.SearchAttributesMapperProvider,
			deps.config.SearchAttributesNumberOfKeysLimit,
			deps.config.SearchAttributesSizeOfValueLimit,
			deps.config.SearchAttributesTotalSizeLimit,
			deps.mockVisibilityManager,
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		),
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(mockShard, deps.workflowCache),
		persistenceVisibilityMgr:   deps.mockVisibilityManager,
		nDCWorkflowStateReplicator: deps.mockWorkflowStateReplicator,
	}
	deps.mockShard.SetEngineForTesting(h)

	deps.historyEngine = h

	deps.tv = testvars.New(t).WithNamespaceID(tests.NamespaceID)
	deps.tv = deps.tv.WithRunID(deps.tv.Any().RunID())

	return deps
}

type createWorkflowExecutionRequestMatcher struct {
	f func(request *persistence.CreateWorkflowExecutionRequest) bool
}

func newCreateWorkflowExecutionRequestMatcher(f func(request *persistence.CreateWorkflowExecutionRequest) bool) gomock.Matcher {
	return &createWorkflowExecutionRequestMatcher{
		f: f,
	}
}

func (m *createWorkflowExecutionRequestMatcher) Matches(x interface{}) bool {
	request, ok := x.(*persistence.CreateWorkflowExecutionRequest)
	if !ok {
		return false
	}
	return m.f(request)
}

func (m *createWorkflowExecutionRequestMatcher) String() string {
	return "CreateWorkflowExecutionRequest match condition"
}
