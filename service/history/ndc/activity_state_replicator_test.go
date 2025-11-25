package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type activityStateReplicatorTestDeps struct {
	controller                 *gomock.Controller
	mockShard                  *shard.ContextTest
	mockNamespaceCache         *namespace.MockRegistry
	mockClusterMetadata        *cluster.MockMetadata
	mockMutableState           *historyi.MockMutableState
	mockExecutionMgr           *persistence.MockExecutionManager
	workflowCache              wcache.Cache
	logger                     log.Logger
	nDCActivityStateReplicator *ActivityStateReplicatorImpl
}

func setupActivityStateReplicatorTest(t *testing.T) *activityStateReplicatorTestDeps {
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

	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)

	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockExecutionMgr := mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	logger := mockShard.GetLogger()

	nDCActivityStateReplicator := NewActivityStateReplicator(
		mockShard,
		workflowCache,
		logger,
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &activityStateReplicatorTestDeps{
		controller:                 controller,
		mockShard:                  mockShard,
		mockNamespaceCache:         mockNamespaceCache,
		mockClusterMetadata:        mockClusterMetadata,
		mockMutableState:           mockMutableState,
		mockExecutionMgr:           mockExecutionMgr,
		workflowCache:              workflowCache,
		logger:                     logger,
		nDCActivityStateReplicator: nDCActivityStateReplicator,
	}
}

func TestActivity_LocalVersionLarger(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version + 1,
		Attempt: attempt,
		Stamp:   stamp,
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		localActivityInfo.Stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.False(t, apply)
}

func TestActivity_DifferentStamp(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt,
		Stamp:   stamp - 1,
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.True(t, apply)
}

func TestActivity_IncomingVersionLarger(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version - 1,
		Attempt: attempt,
		Stamp:   stamp,
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.True(t, apply)
}

func TestActivity_SameVersion_LocalAttemptLarger(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt + 1,
		Stamp:   stamp,
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.False(t, apply)
}

func TestActivity_SameVersion_IncomingAttemptLarger(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt - 1,
		Stamp:   stamp,
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.True(t, apply)
}

func TestActivity_SameVersion_SameAttempt_LocalHeartbeatLater(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version:                 version,
		Attempt:                 attempt,
		Stamp:                   stamp,
		LastHeartbeatUpdateTime: timestamppb.New(lastHeartbeatTime.Add(time.Second)),
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.False(t, apply)
}

func TestActivity_SameVersion_SameAttempt_IncomingHeartbeatLater(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	version := int64(123)
	attempt := int32(1)
	stamp := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version:                 version,
		Attempt:                 attempt,
		Stamp:                   stamp,
		LastHeartbeatUpdateTime: timestamppb.New(lastHeartbeatTime.Add(-time.Second)),
	}

	apply := deps.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		stamp,
		lastHeartbeatTime,
		localActivityInfo,
	)
	require.True(t, apply)
}

func TestVersionHistory_LocalIsSuperSet(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := deps.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		deps.mockMutableState,
		incomingVersionHistory,
	)
	require.NoError(t, err)
	require.True(t, apply)
}

func TestVersionHistory_IncomingIsSuperSet_NoResend(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID + 10,
				Version: version,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := deps.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		deps.mockMutableState,
		incomingVersionHistory,
	)
	require.NoError(t, err)
	require.True(t, apply)
}

func TestVersionHistory_IncomingIsSuperSet_Resend(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID - 1,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID + 10,
				Version: version,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := deps.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		deps.mockMutableState,
		incomingVersionHistory,
	)
	require.Equal(t, serviceerrors.NewRetryReplication(
		resendMissingEventMessage,
		namespaceID.String(),
		workflowID,
		runID,
		scheduledEventID-1,
		version,
		common.EmptyEventID,
		common.EmptyVersion,
	), err)
	require.False(t, apply)
}

func TestVersionHistory_Diverge_LocalLarger(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID,
					Version: version,
				},
				{
					EventId: scheduledEventID + 1,
					Version: version + 2,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID + 10,
				Version: version,
			},
			{
				EventId: scheduledEventID + 1,
				Version: version + 1,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := deps.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		deps.mockMutableState,
		incomingVersionHistory,
	)
	require.NoError(t, err)
	require.False(t, apply)
}

func TestVersionHistory_Diverge_IncomingLarger(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID,
					Version: version,
				},
				{
					EventId: scheduledEventID + 1,
					Version: version + 1,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
			{
				EventId: scheduledEventID + 1,
				Version: version + 2,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := deps.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		deps.mockMutableState,
		incomingVersionHistory,
	)
	require.Equal(t, serviceerrors.NewRetryReplication(
		resendHigherVersionMessage,
		namespaceID.String(),
		workflowID,
		runID,
		scheduledEventID,
		version,
		common.EmptyEventID,
		common.EmptyVersion,
	), err)
	require.False(t, apply)
}

func TestSyncActivity_WorkflowNotFound(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := namespace.Name("some random namespace name")
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &historyservice.SyncActivityRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			version,
		), nil,
	).AnyTimes()

	err := deps.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	require.Nil(t, err)
}

func TestSyncActivities_WorkflowNotFound(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := namespace.Name("some random namespace name")
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
	}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))
	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			version,
		), nil,
	).AnyTimes()

	err := deps.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	require.Nil(t, err)
}

func TestSyncActivity_WorkflowClosed(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Clear().AnyTimes()
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		VersionHistory:   incomingVersionHistory,
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).AnyTimes()

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	require.Error(t, err)
}

func TestSyncActivities_WorkflowClosed(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().Clear().AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).AnyTimes()

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncActivity_ActivityNotFound(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Clear().AnyTimes()
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		VersionHistory:   incomingVersionHistory,
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	deps.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncActivities_ActivityNotFound(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)
	weContext.EXPECT().Clear().AnyTimes()

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	deps.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestSyncActivity_ActivityFound_Zombie(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	now := time.Now()
	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	deps.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)

	deps.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	deps.mockMutableState.EXPECT().ShouldResetActivityTimerTaskMask(
		&persistencespb.ActivityInfo{
			Version: version,
		},
		&persistencespb.ActivityInfo{
			Version: version,
			Attempt: 0,
		}).Return(false)
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	weContext.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), deps.mockShard).Return(nil)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	require.Nil(t, err)
}

func TestSyncActivities_ActivityFound_Zombie(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	now := time.Now()
	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
				ScheduledTime:    timestamppb.New(now),
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	deps.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)
	deps.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	deps.mockMutableState.EXPECT().ShouldResetActivityTimerTaskMask(
		&persistencespb.ActivityInfo{
			Version: version,
		},
		&persistencespb.ActivityInfo{
			Version: version,
			Attempt: 0,
		}).Return(false)
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	weContext.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), deps.mockShard).Return(nil)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	require.Nil(t, err)
}

func TestSyncActivity_ActivityFound_NonZombie(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	now := time.Now()
	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	deps.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)
	deps.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	deps.mockMutableState.EXPECT().ShouldResetActivityTimerTaskMask(
		&persistencespb.ActivityInfo{
			Version: version,
		},
		&persistencespb.ActivityInfo{
			Version: version,
			Attempt: 0,
		}).Return(false)
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	weContext.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), deps.mockShard).Return(nil)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	require.Nil(t, err)
}

func TestSyncActivities_ActivityFound_NonZombie(t *testing.T) {
	t.Parallel()
	deps := setupActivityStateReplicatorTest(t)

	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   deps.mockShard.GetOwner(),
	}
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), deps.mockShard).Return(deps.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), locks.PriorityHigh).Return(nil)
	weContext.EXPECT().Unlock()
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()
	weContext.EXPECT().SetArchetype(chasm.WorkflowArchetype).Times(1)

	err := wcache.PutContextIfNotExist(deps.workflowCache, key, weContext)
	require.NoError(t, err)

	now := time.Now()
	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
				ScheduledTime:    timestamppb.New(now),
			},
		},
	}

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	deps.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)
	deps.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	deps.mockMutableState.EXPECT().ShouldResetActivityTimerTaskMask(
		&persistencespb.ActivityInfo{
			Version: version,
		},
		&persistencespb.ActivityInfo{
			Version: version,
			Attempt: 0,
		}).Return(false)
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	weContext.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), deps.mockShard).Return(nil)

	deps.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = deps.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	require.Nil(t, err)
}
