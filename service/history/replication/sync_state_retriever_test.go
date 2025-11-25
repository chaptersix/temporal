package replication

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type syncWorkflowStateTestDeps struct {
	controller                 *gomock.Controller
	workflowCache              *wcache.MockCache
	eventBlobCache             persistence.XDCCache
	logger                     log.Logger
	mockShard                  *shard.ContextTest
	releaseFunc                func(err error)
	workflowContext            *historyi.MockWorkflowContext
	newRunWorkflowContext      *historyi.MockWorkflowContext
	namespaceID                string
	execution                  *commonpb.WorkflowExecution
	newRunId                   string
	workflowKey                definition.WorkflowKey
	syncStateRetriever         *SyncStateRetrieverImpl
	workflowConsistencyChecker *api.MockWorkflowConsistencyChecker
}

func setupSyncWorkflowStateTest(t *testing.T) *syncWorkflowStateTestDeps {
	controller := gomock.NewController(t)
	workflowContext := historyi.NewMockWorkflowContext(controller)
	newRunWorkflowContext := historyi.NewMockWorkflowContext(controller)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	releaseFunc := func(err error) {}
	workflowCache := wcache.NewMockCache(controller)
	logger := mockShard.GetLogger()
	namespaceID := tests.NamespaceID.String()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: uuid.New(),
		RunId:      uuid.New(),
	}
	newRunId := uuid.New()
	workflowKey := definition.WorkflowKey{
		NamespaceID: namespaceID,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}
	workflowConsistencyChecker := api.NewMockWorkflowConsistencyChecker(controller)
	eventBlobCache := persistence.NewEventsBlobCache(
		1024*1024,
		20*time.Second,
		logger,
	)
	syncStateRetriever := NewSyncStateRetriever(mockShard, workflowCache, workflowConsistencyChecker, eventBlobCache, logger)

	return &syncWorkflowStateTestDeps{
		controller:                 controller,
		workflowCache:              workflowCache,
		eventBlobCache:             eventBlobCache,
		logger:                     logger,
		mockShard:                  mockShard,
		releaseFunc:                releaseFunc,
		workflowContext:            workflowContext,
		newRunWorkflowContext:      newRunWorkflowContext,
		namespaceID:                namespaceID,
		execution:                  execution,
		newRunId:                   newRunId,
		workflowKey:                workflowKey,
		syncStateRetriever:         syncStateRetriever,
		workflowConsistencyChecker: workflowConsistencyChecker,
	}
}

func TestSyncWorkflowState_TransitionHistoryDisabled(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: nil, // transition history is disabled
	}
	mu.EXPECT().HasBufferedEvents().Return(false)
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		nil,
	)
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, consts.ErrTransitionHistoryDisabled)
}

func TestSyncWorkflowState_UnFlushedBufferedEvents(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)

	mu.EXPECT().HasBufferedEvents().Return(true)
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		nil,
	)
	require.Nil(t, result)
	require.Error(t, err)
	require.IsType(t, &serviceerror.WorkflowNotReady{}, err)
}

func TestSyncWorkflowState_ReturnMutation(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:    versionHistories,
		LastFirstEventTxnId: 1234, // some state that should be sanitized
	}
	mu.EXPECT().HasBufferedEvents().Return(false)
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mu.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	mu.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{
		// should not be included in the mutation
		"timerID": {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}},
	})
	mu.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{
		// should be included in the mutation
		13: {
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
			Clock:                         &clockspb.VectorClock{ShardId: s.mockShard.GetShardID(), Clock: 1234},
		},
	})
	mu.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistencespb.RequestCancelInfo{
		// should not be included in the mutation
		5: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 5}},
	})
	mu.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistencespb.SignalInfo{
		// should be included in the mutation
		15: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 15}},
	})
	mu.EXPECT().HSM().Return(nil)
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mockChasmTree.EXPECT().Snapshot(&persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12}).
		Return(chasm.NodesSnapshot{
			Nodes: map[string]*persistencespb.ChasmNode{
				"node-path": {
					Metadata: &persistencespb.ChasmNodeMetadata{
						LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
					},
				},
			},
		})
	mu.EXPECT().ChasmTree().Return(mockChasmTree)

	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          12,
		},
		versionHistories)
	require.NoError(t, err)
	require.NotNil(t, result)
	syncAttributes := result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes()
	require.NotNil(t, result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes())

	mutation := syncAttributes.StateMutation
	// ensure it's a copy by checking the pointers are pointing to different memory addresses
	require.True(t, executionInfo != mutation.ExecutionInfo)
	require.Nil(t, mutation.ExecutionInfo.UpdateInfos)
	require.Nil(t, mutation.ExecutionInfo.SubStateMachinesByType)
	require.Nil(t, mutation.ExecutionInfo.SubStateMachineTombstoneBatches)
	require.Zero(t, mutation.ExecutionInfo.LastFirstEventTxnId) // field should be sanitized
	require.Empty(t, mutation.UpdatedActivityInfos)
	require.Len(t, mutation.UpdatedTimerInfos, 0)
	require.Len(t, mutation.UpdatedChildExecutionInfos, 1)
	require.Len(t, mutation.UpdatedRequestCancelInfos, 0)
	require.Len(t, mutation.UpdatedSignalInfos, 1)
	require.Len(t, mutation.UpdatedChasmNodes, 1)
	require.Nil(t, mutation.UpdatedChildExecutionInfos[13].Clock) // field should be sanitized

	require.Nil(t, result.VersionedTransitionArtifact.EventBatches)
	require.Nil(t, result.VersionedTransitionArtifact.NewRunInfo)
}

func TestGetSyncStateRetrieverForNewWorkflow_WithEvents(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)

	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
			},
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:    versionHistories,
		LastFirstEventTxnId: 1234, // some state that should be sanitized
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mu.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	mu.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{
		// should not be included in the mutation
		"timerID": {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}},
	})
	mu.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{
		// should be included in the mutation
		13: {
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
			Clock:                         &clockspb.VectorClock{ShardId: s.mockShard.GetShardID(), Clock: 1234},
		},
	})
	mu.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistencespb.RequestCancelInfo{
		// should not be included in the mutation
		5: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 5}},
	})
	mu.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistencespb.SignalInfo{
		// should be included in the mutation
		15: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 15}},
	})
	mu.EXPECT().HSM().Return(nil)
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mockChasmTree.EXPECT().Snapshot(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          0,
	}).
		Return(chasm.NodesSnapshot{
			Nodes: map[string]*persistencespb.ChasmNode{
				"node-path": {
					Metadata: &persistencespb.ChasmNodeMetadata{
						LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
					},
				},
			},
		})
	mu.EXPECT().ChasmTree().Return(mockChasmTree)

	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistories.Histories[0].GetBranchToken(),
		MinEventID:  1,
		MaxEventID:  versionHistories.Histories[0].Items[0].GetEventId() + 1,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: getEventBlobs(1, 10)}, nil)

	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
		context.Background(),
		s.namespaceID,
		s.execution,
		mu,
		func(err error) {},
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          12,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	syncAttributes := result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes()
	require.NotNil(t, result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes())

	mutation := syncAttributes.StateMutation
	// ensure it's a copy by checking the pointers are pointing to different memory addresses
	require.True(t, executionInfo != mutation.ExecutionInfo)
	require.Nil(t, mutation.ExecutionInfo.UpdateInfos)
	require.Nil(t, mutation.ExecutionInfo.SubStateMachinesByType)
	require.Nil(t, mutation.ExecutionInfo.SubStateMachineTombstoneBatches)
	require.Zero(t, mutation.ExecutionInfo.LastFirstEventTxnId) // field should be sanitized
	require.Empty(t, mutation.UpdatedActivityInfos)
	require.Len(t, mutation.UpdatedTimerInfos, 1)
	require.Len(t, mutation.UpdatedChildExecutionInfos, 1)
	require.Len(t, mutation.UpdatedRequestCancelInfos, 1)
	require.Len(t, mutation.UpdatedSignalInfos, 1)
	require.Len(t, mutation.UpdatedChasmNodes, 1)
	require.Nil(t, mutation.UpdatedChildExecutionInfos[13].Clock) // field should be sanitized

	require.Nil(t, result.VersionedTransitionArtifact.NewRunInfo)
}

func TestGetSyncStateRetrieverForNewWorkflow_NoEvents(t *testing.T) {
	// Test that sync state retriever logic handles the case where mutable state has no event at all
	// and current version history is empty.

	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)

	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*historyspb.VersionHistory{{}},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
			},
		},
		VersionHistories:    versionHistories,
		LastFirstEventTxnId: 1234, // some state that should be sanitized
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mu.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	mu.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{})
	mu.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{})
	mu.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistencespb.RequestCancelInfo{})
	mu.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistencespb.SignalInfo{})
	mu.EXPECT().HSM().Return(nil)
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mockChasmTree.EXPECT().Snapshot(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          0,
	}).Return(chasm.NodesSnapshot{
		Nodes: map[string]*persistencespb.ChasmNode{
			"node-path": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
				},
			},
		},
	})
	mu.EXPECT().ChasmTree().Return(mockChasmTree)

	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
		context.Background(),
		s.namespaceID,
		s.execution,
		mu,
		func(err error) {},
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          5,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes())
}

func TestSyncWorkflowState_ReturnSnapshot(t *testing.T) {
	testCases := []struct {
		name   string
		infoFn func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition)
	}{
		{
			name: "tombstone batch is empty",
			infoFn: func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition) {
				versionHistories := &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte("branchToken1"),
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 1, Version: 10},
								{EventId: 2, Version: 13},
							},
						},
					},
				}
				return versionHistories, []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 12},
					{NamespaceFailoverVersion: 2, TransitionCount: 15},
				}, nil, nil
			},
		},
		{
			name: "tombstone batch is not empty",
			infoFn: func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition) {
				versionHistories := &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte("branchToken1"),
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 1, Version: 10},
								{EventId: 2, Version: 13},
							},
						},
					},
				}
				return versionHistories, []*persistencespb.VersionedTransition{
						{NamespaceFailoverVersion: 1, TransitionCount: 12},
						{NamespaceFailoverVersion: 2, TransitionCount: 15},
					}, []*persistencespb.StateMachineTombstoneBatch{
						{
							VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
						},
					}, nil
			},
		},
		{
			name: "tombstone batch is not empty, but target state transition is before break point",
			infoFn: func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition) {
				versionHistories := &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte("branchToken1"),
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 1, Version: 10},
								{EventId: 2, Version: 13},
							},
						},
					},
				}
				return versionHistories, []*persistencespb.VersionedTransition{
						{NamespaceFailoverVersion: 1, TransitionCount: 13},
						{NamespaceFailoverVersion: 2, TransitionCount: 15},
					}, []*persistencespb.StateMachineTombstoneBatch{
						{
							VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
						},
					}, &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 13}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupSyncWorkflowStateTest(t)
			defer s.controller.Finish()
			defer s.mockShard.StopForTest()

			mu := historyi.NewMockMutableState(s.controller)
			s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
				NamespaceID: s.namespaceID,
				WorkflowID:  s.execution.WorkflowId,
				RunID:       s.execution.RunId,
			}, chasm.ArchetypeAny, locks.PriorityLow).Return(
				api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
			versionHistories, transitions, tombstoneBatches, breakPoint := tc.infoFn()
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				TransitionHistory:               transitions,
				SubStateMachineTombstoneBatches: tombstoneBatches,
				VersionHistories:                versionHistories,
				LastTransitionHistoryBreakPoint: breakPoint,
			}
			mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			mu.EXPECT().HasBufferedEvents().Return(false)
			mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
				ExecutionInfo: executionInfo,
			})
			result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
				context.Background(),
				s.namespaceID,
				s.execution,
				&persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          13,
				},
				versionHistories)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotNil(t, result.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes())
			require.Nil(t, result.VersionedTransitionArtifact.EventBatches)
			require.Nil(t, result.VersionedTransitionArtifact.NewRunInfo)
		})
	}
}

func TestSyncWorkflowState_NoVersionTransitionProvided_ReturnSnapshot(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories: versionHistories,
	}
	mu.EXPECT().HasBufferedEvents().Return(false)
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
		ExecutionInfo: executionInfo,
	})
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		versionHistories)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes())
	require.Nil(t, result.VersionedTransitionArtifact.EventBatches)
	require.Nil(t, result.VersionedTransitionArtifact.NewRunInfo)
}

func TestGetNewRunInfo(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	// New logic queries start version and checks cluster affinity
	mu.EXPECT().GetStartVersion().Return(int64(1), nil)
	cm := cluster.NewMockMetadata(s.controller)
	cm.EXPECT().GetClusterID().Return(int64(1))
	cm.EXPECT().IsVersionFromSameCluster(int64(1), int64(1)).Return(true)
	s.mockShard.SetClusterMetadata(cm)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &commonpb.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(mu, nil).Times(1)

	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistories.Histories[0].GetBranchToken(),
		MinEventID:  1,
		MaxEventID:  2,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			{Data: []byte("event1")}},
	}, nil)
	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	require.NoError(t, err)
	require.NotNil(t, newRunInfo)
}

func TestGetNewRunInfo_NewRunFromDifferentCluster_ReturnNil(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 7},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 7, TransitionCount: 12},
			{NamespaceFailoverVersion: 13, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	// New logic queries start version and checks cluster affinity
	mu.EXPECT().GetStartVersion().Return(int64(7), nil)
	cm := cluster.NewMockMetadata(s.controller)
	cm.EXPECT().GetClusterID().Return(int64(1))
	cm.EXPECT().IsVersionFromSameCluster(int64(7), int64(1)).Return(false)
	s.mockShard.SetClusterMetadata(cm)

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &commonpb.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(mu, nil).Times(1)

	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	require.NoError(t, err)
	require.Nil(t, newRunInfo)
}

func TestGetNewRunInfo_NotFound(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	mu := historyi.NewMockMutableState(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &commonpb.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(nil, serviceerror.NewNotFound("not found")).Times(1)

	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	require.NoError(t, err)
	require.Nil(t, newRunInfo)
}

func addXDCCache(eventBlobCache persistence.XDCCache, workflowKey definition.WorkflowKey, minEventID int64, version int64, nextEventID int64, eventBlobs []*commonpb.DataBlob, versionHistoryItems []*historyspb.VersionHistoryItem) {
	eventBlobCache.Put(persistence.NewXDCCacheKey(
		workflowKey,
		minEventID,
		version,
	), persistence.NewXDCCacheValue(
		nil,
		versionHistoryItems,
		eventBlobs,
		nextEventID,
	))
}

func getEventBlobs(firstEventID, nextEventID int64) []*commonpb.DataBlob {
	eventBlob := &commonpb.DataBlob{Data: []byte("event1")}
	eventBlobs := make([]*commonpb.DataBlob, nextEventID-firstEventID)
	for i := 0; i < int(nextEventID-firstEventID); i++ {
		eventBlobs[i] = eventBlob
	}
	return eventBlobs
}

func TestGetSyncStateEvents(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	targetVersionHistoriesItems := [][]*historyspb.VersionHistoryItem{
		{
			{EventId: 1, Version: 10},
			{EventId: 18, Version: 13},
		},
		{
			{EventId: 10, Version: 10},
		},
	}
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{EventId: 1, Version: 10},
		{EventId: 30, Version: 13},
	}
	sourceVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("source branchToken1"),
				Items:       versionHistoryItems,
			},
		},
	}

	// get [19, 31) from DB
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: sourceVersionHistories.Histories[0].GetBranchToken(),
		MinEventID:  19,
		MaxEventID:  31,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: getEventBlobs(19, 31)}, nil)

	events, err := s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)
	require.NoError(t, err)
	require.Len(t, events, 31-19)

	// get [19,21) from cache, [21, 31) from DB
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: sourceVersionHistories.Histories[0].GetBranchToken(),
		MinEventID:  21,
		MaxEventID:  31,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: getEventBlobs(21, 31)}, nil)

	addXDCCache(s.eventBlobCache, s.workflowKey, 19, 13, 21, getEventBlobs(19, 21), versionHistoryItems)
	events, err = s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)
	require.NoError(t, err)
	require.Len(t, events, 31-19)

	// get [19,31) from cache
	addXDCCache(s.eventBlobCache, s.workflowKey, 21, 13, 41, getEventBlobs(21, 41), versionHistoryItems)
	events, err = s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)
	require.NoError(t, err)
	require.Len(t, events, 31-19)
}

func TestGetEventsBlob_NewRun(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branchToken1"),
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 1, Version: 1},
		},
	}

	// get [1,4) from DB
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistory.BranchToken,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.FirstEventID + 1,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: getEventBlobs(1, 4)}, nil)
	events, err := s.syncStateRetriever.getEventsBlob(context.Background(), s.workflowKey, versionHistory, common.FirstEventID, common.FirstEventID+1, true)
	require.NoError(t, err)
	require.Len(t, events, 4-1)

	// get [1,4) from cache
	addXDCCache(s.eventBlobCache, s.workflowKey, 1, 1, 4, getEventBlobs(1, 4), versionHistory.Items)
	events, err = s.syncStateRetriever.getEventsBlob(context.Background(), s.workflowKey, versionHistory, common.FirstEventID, common.FirstEventID+1, true)
	require.NoError(t, err)
	require.Len(t, events, 4-1)
}

func TestGetSyncStateEvents_EventsUpToDate_ReturnNothing(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	targetVersionHistoriesItems := [][]*historyspb.VersionHistoryItem{
		{
			{EventId: 1, Version: 10},
			{EventId: 18, Version: 13},
		},
	}
	sourceVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("source branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 18, Version: 13},
				},
			},
		},
	}

	events, err := s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)

	require.NoError(t, err)
	require.Nil(t, events)
}

func TestGetUpdatedSubStateMachine(t *testing.T) {
	s := setupSyncWorkflowStateTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	reg := hsm.NewRegistry()
	var def1 = hsmtest.NewDefinition("type1")
	err := reg.RegisterMachine(def1)
	require.NoError(t, err)

	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	root.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 10}
	child1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State1))
	require.Nil(t, err)
	child1.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}
	child2, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "child2"}, hsmtest.NewData(hsmtest.State1))
	require.Nil(t, err)
	child2.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 10}

	result, err := s.syncStateRetriever.getUpdatedSubStateMachine(root, &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 9})
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, len(child2.Path()), len(result[0].Path.Path))
	require.Equal(t, child2.Path()[0].ID, result[0].Path.Path[0].Id)
}
