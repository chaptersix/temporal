package ndc

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type hsmStateReplicatorTestDeps struct {
	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockNamespaceCache    *namespace.MockRegistry
	mockClusterMetadata   *cluster.MockMetadata
	mockMutableState      *historyi.MockMutableState
	mockExecutionMgr      *persistence.MockExecutionManager
	workflowCache         wcache.Cache
	logger                log.Logger
	workflowKey           definition.WorkflowKey
	namespaceEntry        *namespace.Namespace
	stateMachineDef       hsm.StateMachineDefinition
	nDCHSMStateReplicator *HSMStateReplicatorImpl
}

func setupHSMStateReplicatorTest(t *testing.T) *hsmStateReplicatorTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockMutableState := historyi.NewMockMutableState(controller)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	mockEngine := historyi.NewMockEngine(controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().Stop().MaxTimes(1)
	mockShard.SetEngineForTesting(mockEngine)

	stateMachineRegistry := mockShard.StateMachineRegistry()
	err := workflow.RegisterStateMachine(stateMachineRegistry)
	require.NoError(t, err)
	stateMachineDef := hsmtest.NewDefinition("test")
	err = stateMachineRegistry.RegisterMachine(stateMachineDef)
	require.NoError(t, err)
	err = stateMachineRegistry.RegisterTaskSerializer(hsmtest.TaskType, hsmtest.TaskSerializer{})
	require.NoError(t, err)

	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceEntry := tests.GlobalNamespaceEntry
	workflowKey := definition.NewWorkflowKey(namespaceEntry.ID().String(), tests.WorkflowID, tests.RunID)

	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()

	mockExecutionMgr := mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	mockClusterMetadata.EXPECT().IsVersionFromSameCluster(cluster.TestCurrentClusterInitialFailoverVersion, namespaceEntry.FailoverVersion()).Return(true).AnyTimes()

	logger := mockShard.GetLogger()

	nDCHSMStateReplicator := NewHSMStateReplicator(
		mockShard,
		workflowCache,
		logger,
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &hsmStateReplicatorTestDeps{
		controller:            controller,
		mockShard:             mockShard,
		mockNamespaceCache:    mockNamespaceCache,
		mockClusterMetadata:   mockClusterMetadata,
		mockMutableState:      mockMutableState,
		mockExecutionMgr:      mockExecutionMgr,
		workflowCache:         workflowCache,
		logger:                logger,
		workflowKey:           workflowKey,
		namespaceEntry:        namespaceEntry,
		stateMachineDef:       stateMachineDef,
		nDCHSMStateReplicator: nDCHSMStateReplicator,
	}
}

func TestSyncHSM_WorkflowNotFound(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	nonExistKey := definition.NewWorkflowKey(
		deps.namespaceEntry.ID().String(),
		"non-exist workflowID",
		uuid.New(),
	)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: nonExistKey.NamespaceID,
		WorkflowID:  nonExistKey.WorkflowID,
		RunID:       nonExistKey.RunID,
	}).Return(nil, serviceerror.NewNotFound("")).Times(1)

	lastEventID := int64(10)
	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey: nonExistKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				{EventId: lastEventID, Version: deps.namespaceEntry.FailoverVersion()},
			},
		},
	})
	require.Error(t, err)
	retryReplicationErr, ok := err.(*serviceerrors.RetryReplication)
	require.True(t, ok)
	require.Equal(t, nonExistKey.NamespaceID, retryReplicationErr.NamespaceId)
	require.Equal(t, nonExistKey.WorkflowID, retryReplicationErr.WorkflowId)
	require.Equal(t, nonExistKey.RunID, retryReplicationErr.RunId)
	require.Equal(t, common.EmptyEventID, retryReplicationErr.StartEventId)
	require.Equal(t, common.EmptyVersion, retryReplicationErr.StartEventVersion)
	require.Equal(t, lastEventID+1, retryReplicationErr.EndEventId)
	require.Equal(t, deps.namespaceEntry.FailoverVersion(), retryReplicationErr.EndEventVersion)
}

func TestSyncHSM_Diverge_LocalEventVersionLarger(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey: deps.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming version smaller, should not sync
				{EventId: 102, Version: deps.namespaceEntry.FailoverVersion() - 100},
			},
		},
	})
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncHSM_Diverge_IncomingEventVersionLarger(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey: deps.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming version large, should resend history
				{EventId: 80, Version: deps.namespaceEntry.FailoverVersion() - 100},
				{EventId: 202, Version: deps.namespaceEntry.FailoverVersion() + 100},
			},
		},
	})
	require.Error(t, err)
	retryReplicationErr, ok := err.(*serviceerrors.RetryReplication)
	require.True(t, ok)
	require.Equal(t, deps.workflowKey.NamespaceID, retryReplicationErr.NamespaceId)
	require.Equal(t, deps.workflowKey.WorkflowID, retryReplicationErr.WorkflowId)
	require.Equal(t, deps.workflowKey.RunID, retryReplicationErr.RunId)
	require.Equal(t, int64(50), retryReplicationErr.StartEventId) // LCA
	require.Equal(t, deps.namespaceEntry.FailoverVersion()-100, retryReplicationErr.StartEventVersion)
	require.Equal(t, int64(203), retryReplicationErr.EndEventId)
	require.Equal(t, deps.namespaceEntry.FailoverVersion()+100, retryReplicationErr.EndEventVersion)
}

func TestSyncHSM_LocalEventVersionSuperSet(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	// Only asserting state sync happens here
	// There are other tests asserting the actual state sync result
	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey: deps.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming is a subset of local version history, should sync
				{EventId: 50, Version: deps.namespaceEntry.FailoverVersion() - 100},
			},
		},
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							// despite local has more events, incoming state could still be newer for a certain node
							// and state should be synced
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestSyncHSM_IncomingEventVersionSuperSet(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey: deps.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming version large, should resend history
				{EventId: 50, Version: deps.namespaceEntry.FailoverVersion() - 100},
				{EventId: 202, Version: deps.namespaceEntry.FailoverVersion()},
				{EventId: 302, Version: deps.namespaceEntry.FailoverVersion() + 100},
			},
		},
	})
	require.Error(t, err)
	retryReplicationErr, ok := err.(*serviceerrors.RetryReplication)
	require.True(t, ok)
	require.Equal(t, deps.workflowKey.NamespaceID, retryReplicationErr.NamespaceId)
	require.Equal(t, deps.workflowKey.WorkflowID, retryReplicationErr.WorkflowId)
	require.Equal(t, deps.workflowKey.RunID, retryReplicationErr.RunId)
	require.Equal(t, int64(102), retryReplicationErr.StartEventId)
	require.Equal(t, deps.namespaceEntry.FailoverVersion(), retryReplicationErr.StartEventVersion)
	require.Equal(t, int64(303), retryReplicationErr.EndEventId)
	require.Equal(t, deps.namespaceEntry.FailoverVersion()+100, retryReplicationErr.EndEventVersion)
}

func TestSyncHSM_IncomingStateStale(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State1), // stale state
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncHSM_IncomingLastUpdateVersionStale(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3), // newer state
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								// smaller than current node last updated version
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 50,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncHSM_IncomingLastUpdateVersionedTransitionStale(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3), // newer state
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
								// smaller than current node last update transition count
								TransitionCount: 49,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncHSM_IncomingLastUpdateVersionNewer(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State1), // state stale
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								// newer than current node last update version
								// should sync despite state is older than the current node
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 200,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestSyncHSM_IncomingLastUpdateVersionedTransitionNewer(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
								// higher transition count
								TransitionCount: 51,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestSyncHSM_IncomingStateNewer_WorkflowOpen(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.Equal(t, persistence.UpdateWorkflowModeUpdateCurrent, request.Mode)

			subStateMachineByType := request.UpdateWorkflowMutation.ExecutionInfo.SubStateMachinesByType
			require.Len(t, subStateMachineByType, 1)
			machines := subStateMachineByType[deps.stateMachineDef.Type()]
			require.Len(t, machines.MachinesById, 1)
			machine := machines.MachinesById["child1"]
			require.Equal(t, []byte(hsmtest.State3), machine.Data)
			require.Equal(t, int64(24), machine.TransitionCount) // transition count is cluster local and should only be increamented by 1
			require.Len(t, request.UpdateWorkflowMutation.Tasks[tasks.CategoryTimer], 1)
			require.Len(t, request.UpdateWorkflowMutation.Tasks[tasks.CategoryOutbound], 1)
			require.Empty(t, request.UpdateWorkflowEvents)
			require.Empty(t, request.NewWorkflowEvents)
			require.Empty(t, request.NewWorkflowSnapshot)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestSyncHSM_IncomingStateNewer_WorkflowZombie(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)
	persistedState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	persistedState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.Equal(t, persistence.UpdateWorkflowModeBypassCurrent, request.Mode)
			// other fields are tested in TestSyncHSM_IncomingStateNewer_WorkflowOpen
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestSyncHSM_IncomingStateNewer_WorkflowClosed(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)
	persistedState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	persistedState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	deps.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			require.Equal(t, persistence.UpdateWorkflowModeIgnoreCurrent, request.Mode)
			subStateMachineByType := request.UpdateWorkflowMutation.ExecutionInfo.SubStateMachinesByType
			require.Len(t, subStateMachineByType, 1)
			machines := subStateMachineByType[deps.stateMachineDef.Type()]
			require.Len(t, machines.MachinesById, 1)
			machine := machines.MachinesById["child1"]
			require.Equal(t, []byte(hsmtest.State3), machine.Data)
			require.Equal(t, int64(24), machine.TransitionCount) // transition count is cluster local and should only be increamented by 1
			require.Len(t, request.UpdateWorkflowMutation.Tasks[tasks.CategoryTimer], 1)
			require.Len(t, request.UpdateWorkflowMutation.Tasks[tasks.CategoryOutbound], 1)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	).Times(1)

	err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
		WorkflowKey:         deps.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				deps.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestSyncHSM_StateMachineNotFound(t *testing.T) {
	t.Parallel()
	deps := setupHSMStateReplicatorTest(t)

	const (
		deletedMachineID = "child1"
		initialCount     = 50
	)

	baseVersion := deps.namespaceEntry.FailoverVersion()
	persistedState := buildWorkflowMutableState(deps.workflowKey, deps.namespaceEntry, deps.stateMachineDef)

	// Remove the state machine to simulate deletion
	delete(persistedState.ExecutionInfo.SubStateMachinesByType[deps.stateMachineDef.Type()].MachinesById, deletedMachineID)

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: deps.workflowKey.NamespaceID,
		WorkflowID:  deps.workflowKey.WorkflowID,
		RunID:       deps.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).AnyTimes()

	testCases := []struct {
		name           string
		versionHistory *historyspb.VersionHistory
		expectedError  error
	}{
		{
			name: "local version higher - ignore missing state machine",
			versionHistory: &historyspb.VersionHistory{
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 50, Version: baseVersion - 100},
					{EventId: 102, Version: baseVersion - 50},
				},
			},
			expectedError: consts.ErrDuplicate,
		},
		{
			name: "incoming version higher - ignored",
			versionHistory: &historyspb.VersionHistory{
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 50, Version: baseVersion - 100},
					{EventId: 102, Version: baseVersion},
				},
			},
			expectedError: consts.ErrDuplicate,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lastVersion := tc.versionHistory.Items[len(tc.versionHistory.Items)-1].Version

			err := deps.nDCHSMStateReplicator.SyncHSMState(context.Background(), &historyi.SyncHSMRequest{
				WorkflowKey:         deps.workflowKey,
				EventVersionHistory: tc.versionHistory,
				StateMachineNode: &persistencespb.StateMachineNode{
					Children: map[string]*persistencespb.StateMachineMap{
						deps.stateMachineDef.Type(): {
							MachinesById: map[string]*persistencespb.StateMachineNode{
								deletedMachineID: {
									Data: []byte(hsmtest.State3),
									InitialVersionedTransition: &persistencespb.VersionedTransition{
										NamespaceFailoverVersion: lastVersion,
									},
									LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
										NamespaceFailoverVersion: lastVersion,
									},
									TransitionCount: initialCount,
								},
							},
						},
					},
				},
			})

			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func buildWorkflowMutableState(workflowKey definition.WorkflowKey, namespaceEntry *namespace.Namespace, stateMachineDef hsm.StateMachineDefinition) *persistencespb.WorkflowMutableState {
	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: workflowKey.NamespaceID,
		WorkflowId:  workflowKey.WorkflowID,
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 1234,
		},
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 50, Version: namespaceEntry.FailoverVersion() - 100},
						{EventId: 102, Version: namespaceEntry.FailoverVersion()},
					},
				},
			},
		},
		SubStateMachinesByType: map[string]*persistencespb.StateMachineMap{
			stateMachineDef.Type(): {
				MachinesById: map[string]*persistencespb.StateMachineNode{
					"child1": {
						Data: []byte(hsmtest.State2),
						InitialVersionedTransition: &persistencespb.VersionedTransition{
							NamespaceFailoverVersion: namespaceEntry.FailoverVersion(),
							TransitionCount:          10,
						},
						LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
							NamespaceFailoverVersion: namespaceEntry.FailoverVersion() + 100,
							TransitionCount:          50,
						},
						TransitionCount: 23,
					},
				},
			},
		},
	}

	state := &persistencespb.WorkflowExecutionState{
		RunId:  workflowKey.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	return &persistencespb.WorkflowMutableState{
		ExecutionInfo:  info,
		ExecutionState: state,
		NextEventId:    int64(103),
	}
}
