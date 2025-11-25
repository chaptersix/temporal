package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

type stateRebuilderTestDeps struct {
	controller           *gomock.Controller
	mockShard            *shard.ContextTest
	mockEventsCache      *events.MockCache
	mockTaskRefresher    *workflow.MockTaskRefresher
	mockNamespaceCache   *namespace.MockRegistry
	mockClusterMetadata  *cluster.MockMetadata
	mockExecutionManager *persistence.MockExecutionManager
	logger               log.Logger
	namespaceID          namespace.ID
	workflowID           string
	runID                string
	now                  time.Time
	nDCStateRebuilder    *StateRebuilderImpl
}

func setupStateRebuilderTest(t *testing.T) *stateRebuilderTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockTaskRefresher := workflow.NewMockTaskRefresher(controller)
	config := tests.NewDynamicConfig()
	config.EnableTransitionHistory = dynamicconfig.GetBoolPropertyFn(true)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	mockExecutionManager := mockShard.Resource.ExecutionMgr
	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockEventsCache := mockShard.MockEventsCache
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	logger := mockShard.GetLogger()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	now := time.Now().UTC()
	nDCStateRebuilder := NewStateRebuilder(
		mockShard, logger,
	)
	nDCStateRebuilder.taskRefresher = mockTaskRefresher

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &stateRebuilderTestDeps{
		controller:           controller,
		mockShard:            mockShard,
		mockEventsCache:      mockEventsCache,
		mockTaskRefresher:    mockTaskRefresher,
		mockNamespaceCache:   mockNamespaceCache,
		mockClusterMetadata:  mockClusterMetadata,
		mockExecutionManager: mockExecutionManager,
		logger:               logger,
		workflowID:           workflowID,
		runID:                runID,
		now:                  now,
		nDCStateRebuilder:    nDCStateRebuilder,
	}
}

func TestInitializeBuilders(t *testing.T) {
	t.Parallel()
	deps := setupStateRebuilderTest(t)

	mutableState, stateBuilder := deps.nDCStateRebuilder.initializeBuilders(tests.GlobalNamespaceEntry, tests.WorkflowKey, deps.now)
	require.NotNil(t, mutableState)
	require.NotNil(t, stateBuilder)
	require.NotNil(t, mutableState.GetExecutionInfo().GetVersionHistories())
}

func TestApplyEvents(t *testing.T) {
	t.Parallel()
	deps := setupStateRebuilderTest(t)

	requestID := uuid.New()
	events := []*historypb.HistoryEvent{
		{
			EventId:    1,
			EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
		},
		{
			EventId:    2,
			EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{}},
		},
	}

	workflowKey := definition.NewWorkflowKey(deps.namespaceID.String(), deps.workflowID, deps.runID)

	mockStateRebuilder := workflow.NewMockMutableStateRebuilder(deps.controller)
	mockStateRebuilder.EXPECT().ApplyEvents(
		gomock.Any(),
		deps.namespaceID,
		requestID,
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.runID,
		}),
		[][]*historypb.HistoryEvent{events},
		[]*historypb.HistoryEvent(nil),
		"",
	).Return(nil, nil)

	err := deps.nDCStateRebuilder.applyEvents(context.Background(), workflowKey, mockStateRebuilder, events, requestID)
	require.NoError(t, err)
}

func TestPagination(t *testing.T) {
	t.Parallel()
	deps := setupStateRebuilderTest(t)

	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")

	event1 := &historypb.HistoryEvent{
		EventId:    1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    2,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	event4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	event5 := &historypb.HistoryEvent{
		EventId:    5,
		EventType:  enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}
	history1 := []*historypb.History{{Events: []*historypb.HistoryEvent{event1, event2, event3}}}
	transactionID1 := int64(10)
	history2 := []*historypb.History{{Events: []*historypb.HistoryEvent{event4, event5}}}
	transactionID2 := int64(20)
	expectedHistory := append(history1, history2...)
	expectedTransactionIDs := []int64{transactionID1, transactionID2}
	pageToken := []byte("some random token")

	shardID := deps.mockShard.GetShardID()
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{transactionID1},
		NextPageToken:  pageToken,
		Size:           12345,
	}, nil)
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{transactionID2},
		NextPageToken:  nil,
		Size:           67890,
	}, nil)

	paginationFn := deps.nDCStateRebuilder.getPaginationFn(context.Background(), firstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	var result []HistoryBlobsPaginationItem
	for iter.HasNext() {
		item, err := iter.Next()
		require.NoError(t, err)
		result = append(result, item)
	}
	var historyResult []*historypb.History
	var transactionIDsResult []int64
	for _, item := range result {
		historyResult = append(historyResult, item.History)
		transactionIDsResult = append(transactionIDsResult, item.TransactionID)
	}

	require.Equal(t, expectedHistory, historyResult)
	require.Equal(t, expectedTransactionIDs, transactionIDsResult)
}

func TestRebuild(t *testing.T) {
	t.Parallel()
	deps := setupStateRebuilderTest(t)

	requestID := uuid.New()
	version := int64(12)
	lastEventID := int64(2)
	branchToken := []byte("other random branch token")
	targetBranchToken := []byte("some other random branch token")

	targetNamespaceID := namespace.ID(uuid.New())
	targetNamespace := namespace.Name("other random namespace name")
	targetWorkflowID := "other random workflow ID"
	targetRunID := uuid.New()

	firstEventID := common.FirstEventID
	nextEventID := lastEventID + 1
	events1 := []*historypb.HistoryEvent{{
		EventId:   1,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: "some random workflow type"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "some random workflow type"},
			Input:                    payloads.EncodeString("some random input"),
			WorkflowExecutionTimeout: durationpb.New(123 * time.Second),
			WorkflowRunTimeout:       durationpb.New(233 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(45 * time.Second),
			Identity:                 "some random identity",
		}},
	}}
	events2 := []*historypb.HistoryEvent{{
		EventId:   2,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}},
	}}
	history1 := []*historypb.History{{Events: events1}}
	history2 := []*historypb.History{{Events: events2}}
	pageToken := []byte("some random pagination token")

	historySize1 := 12345
	historySize2 := 67890
	shardID := deps.mockShard.GetShardID()
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{10},
		NextPageToken:  pageToken,
		Size:           historySize1,
	}, nil)
	expectedLastFirstTransactionID := int64(20)
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{expectedLastFirstTransactionID},
		NextPageToken:  nil,
		Size:           historySize2,
	}, nil)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(targetNamespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: targetNamespaceID.String(), Name: targetNamespace.String()},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	), nil).AnyTimes()
	deps.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	rebuildMutableState, rebuiltHistorySize, err := deps.nDCStateRebuilder.Rebuild(
		context.Background(),
		deps.now,
		definition.NewWorkflowKey(deps.namespaceID.String(), deps.workflowID, deps.runID),
		branchToken,
		lastEventID,
		util.Ptr(version),
		definition.NewWorkflowKey(targetNamespaceID.String(), targetWorkflowID, targetRunID),
		targetBranchToken,
		requestID,
	)
	require.NoError(t, err)
	require.NotNil(t, rebuildMutableState)
	rebuildExecutionInfo := rebuildMutableState.GetExecutionInfo()
	require.Equal(t, targetNamespaceID, namespace.ID(rebuildExecutionInfo.NamespaceId))
	require.Equal(t, targetWorkflowID, rebuildExecutionInfo.WorkflowId)
	require.Equal(t, targetRunID, rebuildMutableState.GetExecutionState().RunId)
	require.Equal(t, int64(historySize1+historySize2), rebuiltHistorySize)
	protorequire.ProtoEqual(t, versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(
			targetBranchToken,
			[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
		),
	), rebuildMutableState.GetExecutionInfo().GetVersionHistories())
	require.Equal(t, timestamp.TimeValue(rebuildMutableState.GetExecutionState().StartTime), deps.now)
	require.Equal(t, expectedLastFirstTransactionID, rebuildExecutionInfo.LastFirstEventTxnId)
}

func TestRebuildWithCurrentMutableState(t *testing.T) {
	t.Parallel()
	deps := setupStateRebuilderTest(t)

	requestID := uuid.New()
	version := int64(12)
	lastEventID := int64(2)
	branchToken := []byte("other random branch token")
	targetBranchToken := []byte("some other random branch token")

	targetNamespaceID := namespace.ID(uuid.New())
	targetNamespace := namespace.Name("other random namespace name")
	targetWorkflowID := "other random workflow ID"
	targetRunID := uuid.New()

	firstEventID := common.FirstEventID
	nextEventID := lastEventID + 1
	events1 := []*historypb.HistoryEvent{{
		EventId:   1,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: "some random workflow type"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "some random workflow type"},
			Input:                    payloads.EncodeString("some random input"),
			WorkflowExecutionTimeout: durationpb.New(123 * time.Second),
			WorkflowRunTimeout:       durationpb.New(233 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(45 * time.Second),
			Identity:                 "some random identity",
		}},
	}}
	events2 := []*historypb.HistoryEvent{{
		EventId:   2,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}},
	}}
	history1 := []*historypb.History{{Events: events1}}
	history2 := []*historypb.History{{Events: events2}}
	pageToken := []byte("some random pagination token")

	historySize1 := 12345
	historySize2 := 67890
	shardID := deps.mockShard.GetShardID()
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{10},
		NextPageToken:  pageToken,
		Size:           historySize1,
	}, nil)
	expectedLastFirstTransactionID := int64(20)
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{expectedLastFirstTransactionID},
		NextPageToken:  nil,
		Size:           historySize2,
	}, nil)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(targetNamespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: targetNamespaceID.String(), Name: targetNamespace.String()},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	), nil).AnyTimes()

	deps.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	currentMutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					TransitionCount:          10,
					NamespaceFailoverVersion: 12,
				},
			},
		},
	}
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, int64(12)).Return(cluster.TestCurrentClusterName).AnyTimes()
	rebuildMutableState, rebuiltHistorySize, err := deps.nDCStateRebuilder.RebuildWithCurrentMutableState(
		context.Background(),
		deps.now,
		definition.NewWorkflowKey(deps.namespaceID.String(), deps.workflowID, deps.runID),
		branchToken,
		lastEventID,
		util.Ptr(version),
		definition.NewWorkflowKey(targetNamespaceID.String(), targetWorkflowID, targetRunID),
		targetBranchToken,
		requestID,
		currentMutableState,
	)
	require.NoError(t, err)
	require.NotNil(t, rebuildMutableState)
	rebuildExecutionInfo := rebuildMutableState.GetExecutionInfo()
	require.Equal(t, targetNamespaceID, namespace.ID(rebuildExecutionInfo.NamespaceId))
	require.Equal(t, targetWorkflowID, rebuildExecutionInfo.WorkflowId)
	require.Equal(t, targetRunID, rebuildMutableState.GetExecutionState().RunId)
	require.Equal(t, int64(historySize1+historySize2), rebuiltHistorySize)
	protorequire.ProtoEqual(t, versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(
			targetBranchToken,
			[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
		),
	), rebuildMutableState.GetExecutionInfo().GetVersionHistories())
	require.Equal(t, timestamp.TimeValue(rebuildMutableState.GetExecutionState().StartTime), deps.now)
	require.Equal(t, expectedLastFirstTransactionID, rebuildExecutionInfo.LastFirstEventTxnId)
	require.Equal(t, int64(11), rebuildExecutionInfo.TransitionHistory[0].TransitionCount)
}
