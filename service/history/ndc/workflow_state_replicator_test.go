package ndc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type workflowReplicatorTestDeps struct {
	controller                *gomock.Controller
	mockShard                 *shard.ContextTest
	mockEventCache            *events.MockCache
	mockWorkflowCache         *wcache.MockCache
	mockNamespaceCache        *namespace.MockRegistry
	mockRemoteAdminClient     *adminservicemock.MockAdminServiceClient
	mockExecutionManager      *persistence.MockExecutionManager
	logger                    log.Logger
	workflowID                string
	runID                     string
	now                       time.Time
	workflowStateReplicator   *WorkflowStateReplicatorImpl
}

func setupWorkflowReplicatorTest(t *testing.T) *workflowReplicatorTestDeps {
	controller := gomock.NewController(t)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	mockExecutionManager := mockShard.Resource.ExecutionMgr
	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockWorkflowCache := wcache.NewMockCache(controller)
	mockEventCache := mockShard.MockEventsCache
	mockRemoteAdminClient := mockShard.Resource.RemoteAdminClient
	eventReapplier := NewMockEventsReapplier(controller)
	logger := mockShard.GetLogger()

	workflowID := "some random workflow ID"
	runID := uuid.New()
	now := time.Now().UTC()
	workflowStateReplicator := NewWorkflowStateReplicator(
		mockShard,
		mockWorkflowCache,
		eventReapplier,
		serialization.NewSerializer(),
		logger,
	)

	return &workflowReplicatorTestDeps{
		controller:              controller,
		mockShard:               mockShard,
		mockEventCache:          mockEventCache,
		mockWorkflowCache:       mockWorkflowCache,
		mockNamespaceCache:      mockNamespaceCache,
		mockRemoteAdminClient:   mockRemoteAdminClient,
		mockExecutionManager:    mockExecutionManager,
		logger:                  logger,
		workflowID:              workflowID,
		runID:                   runID,
		now:                     now,
		workflowStateReplicator: workflowStateReplicator,
	}
}

func TestApplyWorkflowState_BrandNew(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	namespaceName := "namespaceName"
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    deps.runID,
		BranchId:  uuid.New(),
		Ancestors: nil,
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	require.NoError(t, err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:  deps.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: int64(100),
									Version: int64(100),
								},
							},
						},
					},
				},
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  deps.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
		NamespaceId:   namespaceID,
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: deps.workflowID,
		RunId:      deps.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil).Times(1)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(nil, serviceerror.NewNotFound("ms not found"))
	mockWeCtx.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
		gomock.Any(),
		gomock.Any(),
		[]*persistence.WorkflowEvents{},
	).Return(nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: namespaceName},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{},
		nil,
	)
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("test"))
	deps.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	fakeStartHistory := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		},
	}
	fakeCompletionEvent := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
		},
	}
	deps.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	deps.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), completionEventBatchId, gomock.Any()).Return(fakeCompletionEvent, nil).AnyTimes()
	err = deps.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	require.NoError(t, err)
}

func TestApplyWorkflowState_Ancestors(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	namespaceName := "namespaceName"
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   uuid.New(),
		BranchId: uuid.New(),
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    uuid.New(),
				BeginNodeId: 1,
				EndNodeId:   3,
			},
			{
				BranchId:    uuid.New(),
				BeginNodeId: 3,
				EndNodeId:   4,
			},
		},
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	require.NoError(t, err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:  deps.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: int64(100),
									Version: int64(100),
								},
							},
						},
					},
				},
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  deps.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
		NamespaceId:   namespaceID,
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: deps.workflowID,
		RunId:      deps.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil).Times(1)
	deps.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      branchInfo.GetTreeId(),
		},
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil).Times(1)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(nil, serviceerror.NewNotFound("ms not found"))
	mockWeCtx.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
		gomock.Any(),
		gomock.Any(),
		[]*persistence.WorkflowEvents{},
	).Return(nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: namespaceName},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	expectedHistory := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 1,
				},
				{
					EventId: 2,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 3,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 4,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 5,
				},
				{
					EventId: 6,
				},
			},
		},
	}
	serializer := serialization.NewSerializer()
	var historyBlobs []*commonpb.DataBlob
	var nodeIds []int64
	for _, history := range expectedHistory {
		blob, err := serializer.SerializeEvents(history.GetEvents())
		require.NoError(t, err)
		historyBlobs = append(historyBlobs, blob)
		nodeIds = append(nodeIds, history.GetEvents()[0].GetEventId())
	}
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: historyBlobs,
			HistoryNodeIds: nodeIds,
		},
		nil,
	)
	deps.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId: 1,
					},
					{
						EventId: 2,
					},
				},
			},
		},
	}, nil)
	deps.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), gomock.Any()).Return(nil, nil).Times(3)
	fakeStartHistory := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		},
	}
	fakeCompletionEvent := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
		},
	}
	deps.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	deps.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), completionEventBatchId, gomock.Any()).Return(fakeCompletionEvent, nil).AnyTimes()
	err = deps.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	require.NoError(t, err)
}

func TestApplyWorkflowState_NoClosedWorkflow_Error(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	err := deps.workflowStateReplicator.SyncWorkflowState(context.Background(), &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId: deps.workflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  deps.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
		RemoteCluster: "test",
	})
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
}

func TestApplyWorkflowState_ExistWorkflow_Resend(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    uuid.New(),
		BranchId:  uuid.New(),
		Ancestors: nil,
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	require.NoError(t, err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:  deps.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: int64(100),
									Version: int64(100),
								},
							},
						},
					},
				},
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  deps.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: deps.workflowID,
		RunId:      deps.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					Items: []*historyspb.VersionHistoryItem{
						{
							EventId: int64(1),
							Version: int64(1),
						},
					},
				},
			},
		},
	})
	err = deps.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	var expectedErr *serviceerrors.RetryReplication
	require.ErrorAs(t, err, &expectedErr)
	require.Equal(t, namespaceID, expectedErr.NamespaceId)
	require.Equal(t, deps.workflowID, expectedErr.WorkflowId)
	require.Equal(t, deps.runID, expectedErr.RunId)
	require.Equal(t, int64(1), expectedErr.StartEventId)
	require.Equal(t, int64(1), expectedErr.StartEventVersion)
}

func TestApplyWorkflowState_ExistWorkflow_SyncHSM(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    uuid.New(),
		BranchId:  uuid.New(),
		Ancestors: nil,
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	require.NoError(t, err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: historyBranch.GetData(),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(100),
						Version: int64(100),
					},
				},
			},
		},
	}
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:             deps.workflowID,
				NamespaceId:            namespaceID,
				VersionHistories:       versionHistories,
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  deps.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: deps.workflowID,
		RunId:      deps.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	})
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, deps.workflowID, deps.runID)).AnyTimes()

	engine := historyi.NewMockEngine(deps.controller)
	deps.mockShard.SetEngineForTesting(engine)
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	require.NoError(t, err)
	engine.EXPECT().SyncHSM(gomock.Any(), &historyi.SyncHSMRequest{
		WorkflowKey: definition.NewWorkflowKey(namespaceID, deps.workflowID, deps.runID),
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: request.WorkflowState.ExecutionInfo.SubStateMachinesByType,
		},
		EventVersionHistory: currentVersionHistory,
	}).Times(1)
	engine.EXPECT().Stop().AnyTimes()

	err = deps.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	require.NoError(t, err)
}

type VersionedTransitionMatcher struct {
	expected *persistencespb.VersionedTransition
}

// Matches implements gomock.Matcher
func (m *VersionedTransitionMatcher) Matches(x interface{}) bool {
	// Type assertion to ensure the argument is of the correct type
	got, ok := x.(*persistencespb.VersionedTransition)
	if !ok {
		return false
	}

	// Use proto.Equal to compare the actual protobuf messages
	return m.expected.TransitionCount == got.TransitionCount && m.expected.NamespaceFailoverVersion == got.NamespaceFailoverVersion
}

// String is used for descriptive error messages when the matcher fails
func (m *VersionedTransitionMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.expected)
}

// Helper function to create the matcher
func EqVersionedTransition(expected *persistencespb.VersionedTransition) gomock.Matcher {
	return &VersionedTransitionMatcher{expected: expected}
}

func TestReplicateVersionedTransition_SameBranch_SyncSnapshot(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	workflowStateReplicator := NewWorkflowStateReplicator(
		deps.mockShard,
		deps.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		deps.logger,
	)
	mockTransactionManager := NewMockTransactionManager(deps.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(deps.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       deps.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: deps.runID,
					},
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 18},
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplySnapshot(versionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().State)
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}), nil, false,
		).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	require.NoError(t, err)
}

func TestReplicateVersionedTransition_DifferentBranch_SyncState(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	workflowStateReplicator := NewWorkflowStateReplicator(
		deps.mockShard,
		deps.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		deps.logger,
	)
	mockTransactionManager := NewMockTransactionManager(deps.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(deps.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       deps.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: deps.runID,
					},
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 13}, // local transition is stale
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplySnapshot(versionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().State)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), true, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), mockMutableState, gomock.Any()).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	require.NoError(t, err)
}

func TestReplicateVersionedTransition_SameBranch_SyncMutation(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	workflowStateReplicator := NewWorkflowStateReplicator(
		deps.mockShard,
		deps.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		deps.logger,
	)
	mockTransactionManager := NewMockTransactionManager(deps.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(deps.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       deps.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: deps.runID,
					},
				},
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1, TransitionCount: 10,
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 18},
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplyMutation(versionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes().StateMutation)
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}), nil, false,
		).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	require.NoError(t, err)
}

func TestReplicateVersionedTransition_FirstTask_SyncMutation(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	workflowStateReplicator := NewWorkflowStateReplicator(
		deps.mockShard,
		deps.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		deps.logger,
	)
	mockTransactionManager := NewMockTransactionManager(deps.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(deps.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 2, TransitionCount: 10},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:        deps.workflowID,
						NamespaceId:       namespaceID,
						TransitionHistory: transitionHistory,
						VersionHistories:  versionHistories,
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:  deps.runID,
						State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					},
				},
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2, TransitionCount: 0,
				},
			},
		},
		IsFirstSync: true,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(),
		gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).DoAndReturn(func(ctx context.Context, wf Workflow) error {
		// Capture localMutableState from the workflow
		localMutableState := wf.GetMutableState()

		// Perform your comparisons here
		require.Equal(t, localMutableState.GetExecutionInfo().TransitionHistory, transitionHistory)

		return nil
	}).Times(1)
	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	require.NoError(t, err)

}

func TestReplicateVersionedTransition_MutationProvidedWithGap_ReturnSyncStateError(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	workflowStateReplicator := NewWorkflowStateReplicator(
		deps.mockShard,
		deps.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		deps.logger,
	)
	mockTransactionManager := NewMockTransactionManager(deps.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(deps.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       deps.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: deps.runID,
					},
				},
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2, TransitionCount: 15,
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	deps.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		deps.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: deps.workflowID,
			RunId:      deps.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	require.IsType(t, &serviceerrors.SyncState{}, err)
}

type historyEventMatcher struct {
	expected *historypb.HistoryEvent
}

func (m *historyEventMatcher) Matches(x interface{}) bool {
	evt, ok := x.(*historypb.HistoryEvent)
	return ok && proto.Equal(evt, m.expected)
}

func (m *historyEventMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.expected)
}

func TestBringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	localVersionHistoryies := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(20),
						Version: int64(2),
					},
				},
			},
			{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(20),
						Version: int64(1),
					},
				},
			},
		},
	}
	serializer := serialization.NewSerializer()
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 21, Version: 2}, {EventId: 22, Version: 2},
		{EventId: 23, Version: 2}, {EventId: 24, Version: 2},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 25, Version: 2}, {EventId: 26, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 27, Version: 2}, {EventId: 28, Version: 2},
		{EventId: 29, Version: 2}, {EventId: 30, Version: 2},
	}
	blobs, err := serializer.SerializeEvents(requestedEvents)
	require.NoError(t, err)
	gapBlobs, err := serializer.SerializeEvents(gapEvents)
	require.NoError(t, err)
	tailBlobs, err := serializer.SerializeEvents(tailEvents)
	require.NoError(t, err)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 100,
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, deps.workflowID, deps.runID)).AnyTimes()
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)

	allEvents := append(gapEvents, requestedEvents...)
	allEvents = append(allEvents, tailEvents...)
	for _, event := range allEvents {
		mockMutableState.EXPECT().AddReapplyCandidateEvent(&historyEventMatcher{expected: event}).
			Times(1)
	}

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	sourceClusterName := "test-cluster"
	mockShard := historyi.NewMockShardContext(deps.controller)
	taskId1 := int64(46)
	taskId2 := int64(47)
	taskId3 := int64(48)
	mockShard.EXPECT().GenerateTaskID().Return(taskId1, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId2, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId3, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(deps.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	deps.workflowStateReplicator.shardContext = mockShard
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: deps.workflowID, RunId: deps.runID},
		StartEventId:      20,
		StartEventVersion: 2,
		EndEventId:        25,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{21},
	}, nil)
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: deps.workflowID, RunId: deps.runID},
		StartEventId:      26,
		StartEventVersion: 2,
		EndEventId:        31,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{27},
	}, nil)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            21,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            25,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           tailBlobs,
		PrevTransactionID: taskId2,
		TransactionID:     taskId3,
		NodeID:            27,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	_, err = deps.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		deps.workflowID,
		deps.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{blobs},
		false)
	require.NoError(t, err)
	require.Equal(t, versionHistories.Histories[0].Items, mockMutableState.GetExecutionInfo().VersionHistories.Histories[0].Items)
}

func TestBringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents_NewMutableState(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(3),
						Version: int64(1),
					},
					{
						EventId: int64(6),
						Version: int64(2),
					},
				},
			},
		},
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
			},
		},
	}
	serializer := serialization.NewSerializer()
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 1}, {EventId: 2, Version: 1}, {EventId: 3, Version: 1},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 4, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 5, Version: 2}, {EventId: 6, Version: 2},
	}
	blobs, err := serializer.SerializeEvents(requestedEvents)
	require.NoError(t, err)
	gapBlobs, err := serializer.SerializeEvents(gapEvents)
	require.NoError(t, err)
	tailBlobs, err := serializer.SerializeEvents(tailEvents)
	require.NoError(t, err)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
		ExecutionStats:   &persistencespb.ExecutionStats{HistorySize: 0},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, deps.workflowID, deps.runID)).AnyTimes()
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)

	allEvents := append(gapEvents, requestedEvents...)
	allEvents = append(allEvents, tailEvents...)
	for _, event := range allEvents {
		mockMutableState.EXPECT().AddReapplyCandidateEvent(&historyEventMatcher{expected: event}).
			Times(1)
	}

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	sourceClusterName := "test-cluster"
	mockShard := historyi.NewMockShardContext(deps.controller)
	taskId1 := int64(46)
	taskId2 := int64(47)
	taskId3 := int64(48)
	mockShard.EXPECT().GenerateTaskID().Return(taskId1, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId2, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId3, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(deps.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	deps.workflowStateReplicator.shardContext = mockShard
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: deps.workflowID, RunId: deps.runID},
		StartEventId:      0,
		StartEventVersion: 0,
		EndEventId:        4,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{1},
	}, nil)
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: deps.workflowID, RunId: deps.runID},
		StartEventId:      4,
		StartEventVersion: 2,
		EndEventId:        7,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{5},
	}, nil)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       true,
		BranchToken:       localVersionHistories.Histories[0].BranchToken,
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            1,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistories.Histories[0].BranchToken,
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            4,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistories.Histories[0].BranchToken,
		History:           tailBlobs,
		PrevTransactionID: taskId2,
		TransactionID:     taskId3,
		NodeID:            5,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	_, err = deps.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		deps.workflowID,
		deps.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{blobs},
		true)
	require.NoError(t, err)
	require.Equal(t, versionHistories.Histories[0].Items, mockMutableState.GetExecutionInfo().VersionHistories.Histories[0].Items)
}

func TestBringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents_NotConsecutive(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	localVersionHistoryies := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(20),
						Version: int64(2),
					},
				},
			},
			{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(20),
						Version: int64(1),
					},
				},
			},
		},
	}
	serializer := serialization.NewSerializer()
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 21, Version: 2}, {EventId: 22, Version: 2},
		{EventId: 23, Version: 2}, {EventId: 24, Version: 2},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 25, Version: 2}, {EventId: 26, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 28, Version: 2},
		{EventId: 29, Version: 2}, {EventId: 30, Version: 2},
	}
	blobs, err := serializer.SerializeEvents(requestedEvents)
	require.NoError(t, err)
	gapBlobs, err := serializer.SerializeEvents(gapEvents)
	require.NoError(t, err)
	tailBlobs, err := serializer.SerializeEvents(tailEvents)
	require.NoError(t, err)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 100,
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, deps.workflowID, deps.runID)).AnyTimes()

	allEvents := append(gapEvents, requestedEvents...)
	for _, event := range allEvents {
		mockMutableState.EXPECT().AddReapplyCandidateEvent(&historyEventMatcher{expected: event}).
			Times(1)
	}

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	sourceClusterName := "test-cluster"
	mockShard := historyi.NewMockShardContext(deps.controller)
	taskId1 := int64(46)
	taskId2 := int64(47)
	taskId3 := int64(48)
	mockShard.EXPECT().GenerateTaskID().Return(taskId1, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId2, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId3, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(deps.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	deps.workflowStateReplicator.shardContext = mockShard
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: deps.workflowID, RunId: deps.runID},
		StartEventId:      20,
		StartEventVersion: 2,
		EndEventId:        25,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{21},
	}, nil)
	deps.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: deps.workflowID, RunId: deps.runID},
		StartEventId:      26,
		StartEventVersion: 2,
		EndEventId:        31,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{27},
	}, nil)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            21,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	deps.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            25,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
	}).Return(nil, nil).Times(1)
	_, err = deps.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		deps.workflowID,
		deps.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{blobs},
		false)
	require.ErrorIs(t, err, ErrEventSlicesNotConsecutive)
}

func TestBringLocalEventsUpToSourceCurrentBranch_CreateNewBranch(t *testing.T) {
	deps := setupWorkflowReplicatorTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(30),
						Version: int64(1),
					},
				},
			},
		},
	}
	localVersionHistoryies := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(40),
						Version: int64(1),
					},
				},
			},
			{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(20),
						Version: int64(1),
					},
				},
			},
		},
	}

	mockMutableState := historyi.NewMockMutableState(deps.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 100,
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	sourceClusterName := "test-cluster"

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	forkedBranchToken := []byte("forked-branchToken")
	deps.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: localVersionHistoryies.Histories[0].BranchToken,
		ForkNodeID:      31,
		NamespaceID:     namespaceID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, deps.workflowID, deps.runID),
		ShardID:         deps.mockShard.GetShardID(),
		NewRunID:        deps.runID,
	}).Return(&persistence.ForkHistoryBranchResponse{
		NewBranchToken: forkedBranchToken,
	}, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	newRunBranch, err := deps.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		deps.workflowID,
		deps.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		nil,
		false)
	require.NoError(t, err)

	require.Equal(t, forkedBranchToken, localVersionHistoryies.Histories[2].BranchToken)
	require.Equal(t, int32(2), localVersionHistoryies.CurrentVersionHistoryIndex)
	require.NotNil(t, newRunBranch)
}
