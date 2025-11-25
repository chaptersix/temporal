package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type contextTestDeps struct {
	mockShard       *shard.ContextTest
	workflowContext *ContextImpl
}

func setupContextTest(t *testing.T) *contextTestDeps {
	configs := tests.NewDynamicConfig()

	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{ShardId: 1},
		configs,
	)
	mockEngine := historyi.NewMockEngine(controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	mockShard.SetEngineForTesting(mockEngine)
	require.NoError(t, RegisterStateMachine(mockShard.StateMachineRegistry()))
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	workflowContext := NewContext(
		configs,
		tests.WorkflowKey,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &contextTestDeps{
		mockShard:       mockShard,
		workflowContext: workflowContext,
	}
}

func TestMergeReplicationTasks_NoNewRun(t *testing.T) {
	deps := setupContextTest(t)

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		nil, // no new run
	)
	require.NoError(t, err)
	require.Empty(t, currentWorkflowMutation.Tasks)
}

func TestMergeReplicationTasks_LocalNamespace(t *testing.T) {
	deps := setupContextTest(t)

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		// no replication tasks
	}
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Empty(t, currentWorkflowMutation.Tasks) // verify no change to tasks
	require.Empty(t, newWorkflowSnapshot.Tasks)     // verify no change to tasks
}

func TestMergeReplicationTasks_SingleReplicationTask(t *testing.T) {
	deps := setupContextTest(t)

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         10,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        10,
					NextEventID:         20,
					Version:             tests.Version,
				},
			},
		},
	}

	newRunID := uuid.New()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
			},
		},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Len(t, currentWorkflowMutation.Tasks[tasks.CategoryReplication], 2)
	require.Empty(t, newWorkflowSnapshot.Tasks[tasks.CategoryReplication]) // verify no change to tasks

	mergedReplicationTasks := currentWorkflowMutation.Tasks[tasks.CategoryReplication]
	require.Empty(t, mergedReplicationTasks[0].(*tasks.HistoryReplicationTask).NewRunID)
	require.Equal(t, newRunID, mergedReplicationTasks[1].(*tasks.HistoryReplicationTask).NewRunID)
}

func TestMergeReplicationTasks_SyncVersionedTransitionTask_ShouldMergeTaskAndEquivalent(t *testing.T) {
	deps := setupContextTest(t)

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.SyncVersionedTransitionTask{
					WorkflowKey:  tests.WorkflowKey,
					FirstEventID: 5,
					NextEventID:  10,
					VersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: 1,
						TransitionCount:          1,
					},
					TaskEquivalents: []tasks.Task{
						&tasks.HistoryReplicationTask{
							WorkflowKey:         tests.WorkflowKey,
							VisibilityTimestamp: time.Now(),
							FirstEventID:        5,
							NextEventID:         10,
							Version:             tests.Version,
						},
					},
				},
			},
		},
	}

	newRunID := uuid.New()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
			},
		},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Len(t, currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1)
	require.Empty(t, newWorkflowSnapshot.Tasks[tasks.CategoryReplication]) // verify no change to tasks

	mergedReplicationTasks := currentWorkflowMutation.Tasks[tasks.CategoryReplication]
	require.Equal(t, newRunID, mergedReplicationTasks[0].(*tasks.SyncVersionedTransitionTask).NewRunID)
	require.Equal(t, newRunID, mergedReplicationTasks[0].(*tasks.SyncVersionedTransitionTask).TaskEquivalents[0].(*tasks.HistoryReplicationTask).NewRunID)

}

func TestMergeReplicationTasks_MultipleReplicationTasks(t *testing.T) {
	deps := setupContextTest(t)

	// The case can happen when importing a workflow:
	// current workflow will be terminated and imported workflow can contain multiple replication tasks
	// This case is not supported right now
	// NOTE: ^ should be the case and both current and new runs should have replication tasks. However, the
	// actual implementation in WorkflowImporter will close the transaction of the new run with Passive
	// policy resulting in 0 replication tasks.
	// However the implementation of mergeUpdateWithNewReplicationTasks should still handle this case and not error out.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        9,
					NextEventID:         10,
					Version:             tests.Version,
				},
			},
		},
	}

	newRunID := uuid.New()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        3,
					NextEventID:         6,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        6,
					NextEventID:         10,
					Version:             tests.Version,
				},
			},
		},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Len(t, currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
	require.Len(t, newWorkflowSnapshot.Tasks[tasks.CategoryReplication], 3)     // verify no change to tasks
}

func TestMergeReplicationTasks_CurrentRunRunning(t *testing.T) {
	deps := setupContextTest(t)

	// The case can happen when suppressing a current running workflow to be zombie
	// and creating a new workflow at the same time

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Empty(t, currentWorkflowMutation.Tasks) // verify no change to tasks
	require.Empty(t, newWorkflowSnapshot.Tasks)     // verify no change to tasks
}

func TestMergeReplicationTasks_OnlyCurrentRunHasReplicationTasks(t *testing.T) {
	deps := setupContextTest(t)

	// The case can happen when importing a workflow (via replication task)
	// current workflow may be terminated and the imported workflow since it's received via replication task
	// will not generate replication tasks again.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         6,
					Version:             tests.Version,
				},
			},
		},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Len(t, currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
	require.Empty(t, newWorkflowSnapshot.Tasks)                                 // verify no change to tasks
}

func TestMergeReplicationTasks_OnlyNewRunHasReplicationTasks(t *testing.T) {
	deps := setupContextTest(t)

	// TODO: check if this case can happen or not.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         6,
					Version:             tests.Version,
				},
			},
		},
	}

	err := deps.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	require.NoError(t, err)
	require.Empty(t, currentWorkflowMutation.Tasks)                         // verify no change to tasks
	require.Len(t, newWorkflowSnapshot.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
}

func TestRefreshTask(t *testing.T) {
	deps := setupContextTest(t)

	now := time.Now()

	baseMutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:        tests.NamespaceID.String(),
			WorkflowId:         tests.WorkflowID,
			WorkflowRunTimeout: timestamp.DurationFromSeconds(200),
			ExecutionTime:      timestamppb.New(now),
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("token#1"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 1, Version: common.EmptyVersion},
						},
					},
				},
			},
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: common.EmptyVersion,
					TransitionCount:          1,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{
				HistorySize: 128,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     tests.RunID,
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(now),
		},
		NextEventId: 2,
	}

	testCases := []struct {
		name                  string
		persistedMutableState func() *persistencespb.WorkflowMutableState
		setupMock             func(t *testing.T, mockShard *shard.ContextTest)
	}{
		{
			name: "open workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				return common.CloneProto(baseMutableState)
			},
			setupMock: func(t *testing.T, mockShard *shard.ContextTest) {
				mockShard.MockEventsCache.EXPECT().GetEvent(
					gomock.Any(),
					mockShard.GetShardID(),
					gomock.Any(),
					common.FirstEventID,
					gomock.Any(),
				).Return(&historypb.HistoryEvent{
					EventId:    1,
					EventTime:  timestamppb.New(now),
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					Version:    common.EmptyVersion,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
				}, nil).Times(2)
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						require.Equal(t, persistence.UpdateWorkflowModeUpdateCurrent, request.Mode)
						require.NotEmpty(t, request.UpdateWorkflowMutation.Tasks)
						require.Empty(t, request.UpdateWorkflowEvents)
						return tests.UpdateWorkflowExecutionResponse, nil
					}).Times(1)
			},
		},
		{
			name: "completed workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				base := common.CloneProto(baseMutableState)
				base.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
				base.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
				base.NextEventId = 3
				base.ExecutionInfo.VersionHistories.Histories[0].Items[0].EventId = 2
				base.ExecutionInfo.TransitionHistory[0].TransitionCount = 2
				base.ExecutionInfo.CloseTime = timestamppb.New(now.Add(time.Second))
				return base
			},
			setupMock: func(t *testing.T, mockShard *shard.ContextTest) {
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						require.NotEmpty(t, request.UpdateWorkflowMutation.Tasks)
						require.Equal(t, persistence.UpdateWorkflowModeIgnoreCurrent, request.Mode)
						return tests.UpdateWorkflowExecutionResponse, nil
					}).Times(1)
			},
		},
		{
			name: "zombie workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				base := common.CloneProto(baseMutableState)
				base.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
				return base
			},
			setupMock: func(t *testing.T, mockShard *shard.ContextTest) {
				mockShard.MockEventsCache.EXPECT().GetEvent(
					gomock.Any(),
					mockShard.GetShardID(),
					gomock.Any(),
					common.FirstEventID,
					gomock.Any(),
				).Return(&historypb.HistoryEvent{
					EventId:    1,
					EventTime:  timestamppb.New(now),
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					Version:    common.EmptyVersion,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
				}, nil).Times(2)
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						require.NotEmpty(t, request.UpdateWorkflowMutation.Tasks)
						require.Equal(t, persistence.UpdateWorkflowModeBypassCurrent, request.Mode)
						return tests.UpdateWorkflowExecutionResponse, nil
					}).Times(1)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			deps.workflowContext.MutableState, err = NewMutableStateFromDB(
				deps.mockShard,
				deps.mockShard.MockEventsCache,
				deps.mockShard.GetLogger(),
				tests.LocalNamespaceEntry,
				tc.persistedMutableState(),
				1,
			)
			require.NoError(t, err)

			tc.setupMock(t, deps.mockShard)

			err = deps.workflowContext.RefreshTasks(context.Background(), deps.mockShard)
			require.NoError(t, err)
		})
	}
}
