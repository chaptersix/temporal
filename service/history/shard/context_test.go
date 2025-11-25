package shard

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type contextTestDeps struct {
	controller           *gomock.Controller
	shardID              int32
	mockShard            *ContextTest
	mockClusterMetadata  *cluster.MockMetadata
	mockShardManager     *persistence.MockShardManager
	mockExecutionManager *persistence.MockExecutionManager
	mockNamespaceCache   *namespace.MockRegistry
	mockHistoryEngine    *historyi.MockEngine
	timeSource           *clock.EventTimeSource
}

func setupContextTest(t *testing.T) *contextTestDeps {
	controller := gomock.NewController(t)

	shardID := int32(1)
	timeSource := clock.NewEventTimeSource()
	shardContext := NewTestContextWithTimeSource(
		controller,
		&persistencespb.ShardInfo{
			ShardId: shardID,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
		timeSource,
	)
	mockShard := shardContext

	shardContext.Resource.HostInfoProvider.EXPECT().HostInfo().Return(shardContext.Resource.GetHostInfo()).AnyTimes()

	mockNamespaceCache := shardContext.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	mockClusterMetadata := shardContext.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	mockExecutionManager := shardContext.Resource.ExecutionMgr
	mockShardManager := shardContext.Resource.ShardMgr
	mockHistoryEngine := historyi.NewMockEngine(controller)
	shardContext.engineFuture.Set(mockHistoryEngine, nil)

	return &contextTestDeps{
		controller:           controller,
		shardID:              shardID,
		mockShard:            mockShard,
		mockClusterMetadata:  mockClusterMetadata,
		mockShardManager:     mockShardManager,
		mockExecutionManager: mockExecutionManager,
		mockNamespaceCache:   mockNamespaceCache,
		mockHistoryEngine:    mockHistoryEngine,
		timeSource:           timeSource,
	}
}

func TestOverwriteScheduledTaskTimestamp(t *testing.T) {
	deps := setupContextTest(t)

	now := time.Now()
	deps.timeSource.Update(now)
	maxReadLevel := deps.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTimer)

	now = now.Add(time.Minute)
	deps.timeSource.Update(now)

	workflowKey := definition.NewWorkflowKey(
		tests.NamespaceID.String(),
		tests.WorkflowID,
		tests.RunID,
	)
	fakeTask := tasks.NewFakeTask(
		workflowKey,
		tasks.CategoryTimer,
		time.Time{},
	)
	testTasks := map[tasks.Category][]tasks.Task{
		tasks.CategoryTimer: {fakeTask},
	}

	deps.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(testTasks).AnyTimes()

	testCases := []struct {
		taskTimestamp     time.Time
		expectedTimestamp time.Time
	}{
		{
			// task timestamp is lower than both scheduled queue max read level and now
			// should be overwritten to be later than both
			taskTimestamp:     maxReadLevel.FireTime.Add(-time.Minute),
			expectedTimestamp: now.Add(common.ScheduledTaskMinPrecision).Truncate(common.ScheduledTaskMinPrecision),
		},
		{
			// task timestamp is lower than now but higher than scheduled queue max read level
			// should still be overwritten to be later than both
			taskTimestamp:     now.Add(-time.Minute),
			expectedTimestamp: now.Add(common.ScheduledTaskMinPrecision).Truncate(common.ScheduledTaskMinPrecision),
		},
		{
			// task timestamp is later than both now and scheduled queue max read level
			// should not be overwritten
			taskTimestamp:     now.Add(time.Minute),
			expectedTimestamp: now.Add(time.Minute).Add(common.ScheduledTaskMinPrecision).Truncate(common.ScheduledTaskMinPrecision),
		},
	}

	for _, tc := range testCases {
		fakeTask.SetVisibilityTime(tc.taskTimestamp)
		err := deps.mockShard.AddTasks(
			context.Background(),
			&persistence.AddHistoryTasksRequest{
				ShardID:     deps.mockShard.GetShardID(),
				NamespaceID: workflowKey.NamespaceID,
				WorkflowID:  workflowKey.WorkflowID,
				Tasks:       testTasks,
			},
		)
		require.NoError(t, err)
		fmt.Println(fakeTask.GetVisibilityTime())
		fmt.Println(tc.expectedTimestamp)
		require.True(t, fakeTask.GetVisibilityTime().After(now))
		require.True(t, fakeTask.GetVisibilityTime().After(maxReadLevel.FireTime))
		require.True(t, fakeTask.GetVisibilityTime().Equal(tc.expectedTimestamp))
	}
}

func TestAddTasks_Success(t *testing.T) {
	deps := setupContextTest(t)

	testTasks := map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer:    {&tasks.ActivityTask{}},           // Just for testing purpose. In the real code ActivityTask can't be passed to shardContext.AddTasks.
		tasks.CategoryTimer:       {&tasks.ActivityRetryTimerTask{}}, // Just for testing purpose. In the real code ActivityRetryTimerTask can't be passed to shardContext.AddTasks.
		tasks.CategoryReplication: {&tasks.HistoryReplicationTask{}}, // Just for testing purpose. In the real code HistoryReplicationTask can't be passed to shardContext.AddTasks.
		tasks.CategoryVisibility:  {&tasks.DeleteExecutionVisibilityTask{}},
	}

	addTasksRequest := &persistence.AddHistoryTasksRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,

		Tasks: testTasks,
	}

	deps.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), addTasksRequest).Return(nil)
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(testTasks)

	err := deps.mockShard.AddTasks(context.Background(), addTasksRequest)
	require.NoError(t, err)
}

func TestDeleteWorkflowExecution_Success(t *testing.T) {
	deps := setupContextTest(t)

	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")
	stage := tasks.DeleteWorkflowExecutionStageNone

	deps.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any())
	deps.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)

	err := deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)

	require.NoError(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)
}

func TestDeleteWorkflowExecution_Continue_Success(t *testing.T) {
	deps := setupContextTest(t)

	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")

	deps.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	stage := tasks.DeleteWorkflowExecutionStageVisibility
	err := deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.NoError(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)

	deps.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	stage = tasks.DeleteWorkflowExecutionStageVisibility | tasks.DeleteWorkflowExecutionStageCurrent
	err = deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.NoError(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)

	deps.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	stage = tasks.DeleteWorkflowExecutionStageVisibility | tasks.DeleteWorkflowExecutionStageCurrent | tasks.DeleteWorkflowExecutionStageMutableState
	err = deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.NoError(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)
}

func TestDeleteWorkflowExecution_ErrorAndContinue_Success(t *testing.T) {
	deps := setupContextTest(t)

	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")

	deps.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any())
	deps.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
	stage := tasks.DeleteWorkflowExecutionStageNone
	err := deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.Error(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageVisibility, stage)

	deps.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
	err = deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.Error(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageVisibility|tasks.DeleteWorkflowExecutionStageCurrent, stage)

	deps.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
	err = deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.Error(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageVisibility, stage)

	deps.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	err = deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.NoError(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageVisibility|tasks.DeleteWorkflowExecutionStageHistory, stage)
}

func TestDeleteWorkflowExecution_DeleteVisibilityTaskNotifiction(t *testing.T) {
	deps := setupContextTest(t)

	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")
	stage := tasks.DeleteWorkflowExecutionStageNone

	// add task fails with error that suggests operation can't possibly succeed, no task notification
	deps.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(persistence.ErrPersistenceSystemLimitExceeded).Times(1)
	err := deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.Error(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageNone, stage)

	// add task succeeds but second operation fails, send task notification
	deps.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)
	deps.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(persistence.ErrPersistenceSystemLimitExceeded).Times(1)
	err = deps.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		0,
		time.Time{},
		&stage,
	)
	require.Error(t, err)
	require.Equal(t, tasks.DeleteWorkflowExecutionStageVisibility, stage)
}

func TestAcquireShardOwnershipLostErrorIsNotRetried(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquiring
	deps.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(&persistence.ShardOwnershipLostError{}).Times(1)

	deps.mockShard.acquireShard()

	require.Equal(t, contextStateStopping, deps.mockShard.state)
}

func TestAcquireShardNonOwnershipLostErrorIsRetried(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquiring
	deps.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("temp error")).Times(5)

	deps.mockShard.acquireShard()

	require.Equal(t, contextStateStopping, deps.mockShard.state)
}

func TestAcquireShardEventuallySucceeds(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquiring
	deps.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("temp error")).Times(3)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).MinTimes(1)

	deps.mockShard.acquireShard()

	require.Equal(t, contextStateAcquired, deps.mockShard.state)
}

func TestAcquireShardNoError(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquiring
	deps.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).MinTimes(1)

	deps.mockShard.acquireShard()

	require.Equal(t, contextStateAcquired, deps.mockShard.state)
}

func TestHandoverNamespace(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(1),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
			State: enumspb.REPLICATION_STATE_HANDOVER,
		},
		tests.Version,
	)
	deps.mockShard.UpdateHandoverNamespace(namespaceEntry, false)
	_, handoverNS, err := deps.mockShard.GetReplicationStatus([]string{})
	require.NoError(t, err)

	handoverInfo, ok := handoverNS[namespaceEntry.Name().String()]
	require.True(t, ok)
	require.Equal(t,
		deps.mockShard.taskKeyManager.getExclusiveReaderHighWatermark(tasks.CategoryReplication).TaskID-1,
		handoverInfo.HandoverReplicationTaskId,
	)

	// make shard status invalid
	// ideally we should use deps.mockShard.transition() method
	// but that will cause shard trying to re-acquire the shard in the background
	deps.mockShard.stateLock.Lock()
	deps.mockShard.state = contextStateAcquiring
	deps.mockShard.stateLock.Unlock()

	// note: no mock for NotifyNewTasks

	deps.mockShard.UpdateHandoverNamespace(namespaceEntry, false)
	_, handoverNS, err = deps.mockShard.GetReplicationStatus([]string{})
	require.NoError(t, err)

	handoverInfo, ok = handoverNS[namespaceEntry.Name().String()]
	require.True(t, ok)
	require.Equal(t,
		deps.mockShard.taskKeyManager.getExclusiveReaderHighWatermark(tasks.CategoryReplication).TaskID-1,
		handoverInfo.HandoverReplicationTaskId,
	)

	// delete namespace
	deps.mockShard.UpdateHandoverNamespace(namespaceEntry, true)
	_, handoverNS, err = deps.mockShard.GetReplicationStatus([]string{})
	require.NoError(t, err)

	_, ok = handoverNS[namespaceEntry.Name().String()]
	require.False(t, ok)
}

func TestUpdateGetRemoteClusterInfo_Legacy_8_4(t *testing.T) {
	deps := setupContextTest(t)

	clusterMetadata := cluster.NewMockMetadata(deps.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             8,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	deps.mockShard.clusterMetadata = clusterMetadata

	ackTaskID := rand.Int63()
	ackTimestamp := time.Unix(0, rand.Int63())
	deps.mockShard.UpdateRemoteClusterInfo(
		cluster.TestAlternativeClusterName,
		ackTaskID,
		ackTimestamp,
	)
	remoteAckStatus, _, err := deps.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	require.NoError(t, err)
	require.Equal(t, map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamppb.New(ackTimestamp),
		},
	}, remoteAckStatus)
}

func TestUpdateGetRemoteClusterInfo_Legacy_4_8(t *testing.T) {
	deps := setupContextTest(t)

	clusterMetadata := cluster.NewMockMetadata(deps.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             8,
		},
	}).AnyTimes()
	deps.mockShard.clusterMetadata = clusterMetadata

	ackTaskID := rand.Int63()
	ackTimestamp := time.Unix(0, rand.Int63())
	deps.mockShard.UpdateRemoteClusterInfo(
		cluster.TestAlternativeClusterName,
		ackTaskID,
		ackTimestamp,
	)
	remoteAckStatus, _, err := deps.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	require.NoError(t, err)
	require.Equal(t, map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamppb.New(ackTimestamp),
		},
	}, remoteAckStatus)
}

func TestUpdateGetRemoteReaderInfo_8_4(t *testing.T) {
	deps := setupContextTest(t)

	clusterMetadata := cluster.NewMockMetadata(deps.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             8,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	deps.mockShard.clusterMetadata = clusterMetadata

	ackTaskID := rand.Int63()
	ackTimestamp := time.Unix(0, rand.Int63())
	err := deps.mockShard.UpdateRemoteReaderInfo(
		ReplicationReaderIDFromClusterShardID(
			cluster.TestAlternativeClusterInitialFailoverVersion,
			1,
		),
		ackTaskID,
		ackTimestamp,
	)
	require.NoError(t, err)
	remoteAckStatus, _, err := deps.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	require.NoError(t, err)
	require.Equal(t, map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamppb.New(ackTimestamp),
		},
	}, remoteAckStatus)
}

func TestUpdateGetRemoteReaderInfo_4_8(t *testing.T) {
	deps := setupContextTest(t)

	clusterMetadata := cluster.NewMockMetadata(deps.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             8,
		},
	}).AnyTimes()
	deps.mockShard.clusterMetadata = clusterMetadata

	ack1TaskID := rand.Int63()
	ack1Timestamp := time.Unix(0, rand.Int63())
	err := deps.mockShard.UpdateRemoteReaderInfo(
		ReplicationReaderIDFromClusterShardID(
			cluster.TestAlternativeClusterInitialFailoverVersion,
			1, // maps to local shard 1
		),
		ack1TaskID,
		ack1Timestamp,
	)
	require.NoError(t, err)
	ack5TaskID := rand.Int63()
	ack5Timestamp := time.Unix(0, rand.Int63())
	err = deps.mockShard.UpdateRemoteReaderInfo(
		ReplicationReaderIDFromClusterShardID(
			cluster.TestAlternativeClusterInitialFailoverVersion,
			5, // maps to local shard 1
		),
		ack5TaskID,
		ack5Timestamp,
	)
	require.NoError(t, err)

	ackTaskID := ack1TaskID
	ackTimestamp := ack1Timestamp
	if ackTaskID > ack5TaskID {
		ackTaskID = ack5TaskID
		ackTimestamp = ack5Timestamp
	}

	remoteAckStatus, _, err := deps.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	require.NoError(t, err)
	require.Equal(t, map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamppb.New(ackTimestamp),
		},
	}, remoteAckStatus)
}

func TestShardStopReasonAssertOwnership(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired
	deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).
		Return(&persistence.ShardOwnershipLostError{}).Times(1)

	err := deps.mockShard.AssertOwnership(context.Background())
	require.Error(t, err)

	require.False(t, deps.mockShard.IsValid())
	require.True(t, deps.mockShard.stoppedForOwnershipLost())
}

func TestShardStopReasonShardRead(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired
	deps.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).
		Return(nil, &persistence.ShardOwnershipLostError{}).Times(1)

	_, err := deps.mockShard.GetCurrentExecution(context.Background(), nil)
	require.Error(t, err)

	require.False(t, deps.mockShard.IsValid())
	require.True(t, deps.mockShard.stoppedForOwnershipLost())
}

func TestShardStopReasonAcquireShard(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquiring
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(&persistence.ShardOwnershipLostError{}).Times(1)

	deps.mockShard.acquireShard()

	require.Equal(t, contextStateStopping, deps.mockShard.state)
	require.False(t, deps.mockShard.IsValid())
	require.True(t, deps.mockShard.stoppedForOwnershipLost())
}

func TestShardStopReasonUnload(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired

	deps.mockShard.UnloadForOwnershipLost()

	require.Equal(t, contextStateStopping, deps.mockShard.state)
	require.False(t, deps.mockShard.IsValid())
	require.True(t, deps.mockShard.stoppedForOwnershipLost())
}

func TestShardStopReasonCloseShard(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired
	deps.mockHistoryEngine.EXPECT().Stop().Times(1)

	deps.mockShard.FinishStop()

	require.False(t, deps.mockShard.IsValid())
	require.False(t, deps.mockShard.stoppedForOwnershipLost())
}

func TestUpdateShardInfo_CallbackIsInvoked_EvenWhenNotPersisted(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired

	var timesCalled int
	callback := func() {
		timesCalled++
	}

	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	err := deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)

	// No time has passed and too few tasks completed: shouldn't update the database
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(0)
	err = deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)

	require.Equal(t, 2, timesCalled)
}

func TestUpdateShardInfo_PersistsAfterInterval_RegardlessOfTasksCompleted(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired

	// We only expect the first and third calls to updateShardInfo to hit the database

	var timesCalled int
	callback := func() {
		timesCalled++
	}

	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	err := deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)

	// No time has passed: shouldn't update the database.
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(0)
	err = deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)

	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	deps.timeSource.Update(time.Now().Add(deps.mockShard.config.ShardUpdateMinInterval()))
	err = deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)
	require.Equal(t, 3, timesCalled)
}

func TestUpdateShardInfo_PersistsBeforeInterval_WhenEnoughTasksCompleted(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired
	var timesCalled int
	callback := func() {
		timesCalled++
	}
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	tasksNecessaryForUpdate := deps.mockShard.config.ShardUpdateMinTasksCompleted()
	err := deps.mockShard.updateShardInfo(tasksNecessaryForUpdate, callback)
	require.NoError(t, err)

	// No time has passed and too few tasks completed: shouldn't update
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(0)
	err = deps.mockShard.updateShardInfo(tasksNecessaryForUpdate-1, callback)
	require.NoError(t, err)
	require.Equal(t, 2, timesCalled, "Should call provided callback even when not persisting updates")

	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	err = deps.mockShard.updateShardInfo(1, callback)
	require.NoError(t, err)
	require.Equal(t, 3, timesCalled)
}

func TestUpdateShardInfo_OnlyPersistsAfterInterval_WhenTaskCheckingDisabled(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.state = contextStateAcquired

	// Anything less than one disables the task counting logic
	deps.mockShard.config.ShardUpdateMinTasksCompleted = func() int { return 0 }

	var timesCalled int
	callback := func() {
		timesCalled++
	}

	// Initial call to set the last called time
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	err := deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)
	require.Equal(t, 1, timesCalled)

	// Not enough time passed and with task tracking disabled, this is ignored
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(0)
	err = deps.mockShard.updateShardInfo(10000000, callback)
	require.NoError(t, err)
	require.Equal(t, 2, timesCalled, "Should call provided callback even when not persisting updates")

	// Time passes
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	deps.timeSource.Update(time.Now().Add(deps.mockShard.config.ShardUpdateMinInterval()))
	err = deps.mockShard.updateShardInfo(0, callback)
	require.NoError(t, err)
	require.Equal(t, 3, timesCalled)
}

func TestUpdateShardInfo_FailsUnlessShardAcquired(t *testing.T) {
	deps := setupContextTest(t)

	for _, state := range []contextState{
		contextStateInitialized, contextStateAcquiring, contextStateStopping, contextStateStopped,
	} {
		deps.mockShard.state = state
		require.Error(t, deps.mockShard.updateShardInfo(0, func() {
			require.Fail(t, "Should not have called update callback when in state %v", state)
		}))

	}
	// This is the only state we should succeed in
	deps.mockShard.state = contextStateAcquired
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	var called bool
	require.NoError(t, deps.mockShard.updateShardInfo(0, func() {
		called = true
	}))
	require.True(t, called)
}

func TestUpdateShardInfo_FirstUpdate(t *testing.T) {
	deps := setupContextTest(t)

	deps.mockShard.config.ShardUpdateMinInterval = func() time.Duration { return 5 * time.Minute }
	deps.mockShard.config.ShardFirstUpdateInterval = func() time.Duration { return 10 * time.Second }
	deps.timeSource.Update(time.Now())

	deps.mockShard.initLastUpdatesTime()
	deps.mockShard.tasksCompletedSinceLastUpdate = 1
	_any := gomock.Any()
	deps.mockShardManager.EXPECT().UpdateShard(_any, _any).Times(0)

	// updating too early
	var called bool
	updateFunc := func() { called = true }

	err := deps.mockShard.updateShardInfo(1, updateFunc)

	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, 2, deps.mockShard.tasksCompletedSinceLastUpdate)

	// update after ShardFirstUpdateInterval
	deps.mockShard.initLastUpdatesTime()
	deps.timeSource.Update(time.Now().Add(deps.mockShard.config.ShardFirstUpdateInterval() + 10*time.Second))
	called = false
	deps.mockShardManager.EXPECT().UpdateShard(_any, _any).Return(nil).Times(1)
	err = deps.mockShard.updateShardInfo(1, updateFunc)

	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, 0, deps.mockShard.tasksCompletedSinceLastUpdate)

	// update again. This time update will not work since shard lastUpdate time was set during previous update
	deps.timeSource.Update(time.Now().Add(deps.mockShard.config.ShardFirstUpdateInterval() + 15*time.Second))
	called = false
	deps.mockShardManager.EXPECT().UpdateShard(_any, _any).Times(0)
	err = deps.mockShard.updateShardInfo(1, updateFunc)

	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, 1, deps.mockShard.tasksCompletedSinceLastUpdate)

	// now move past last updated interval. This time hard info should be updated/persisted
	deps.timeSource.Update(deps.mockShard.lastUpdated.Add(deps.mockShard.config.ShardUpdateMinInterval() + 10*time.Second))
	called = false
	deps.mockShardManager.EXPECT().UpdateShard(_any, _any).Return(nil).Times(1)
	err = deps.mockShard.updateShardInfo(1, updateFunc)

	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, 0, deps.mockShard.tasksCompletedSinceLastUpdate)
}
