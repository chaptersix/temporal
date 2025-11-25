package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type taskExecutorTestDeps struct {
	controller                  *gomock.Controller
	remoteCluster               string
	mockResource                *resourcetest.Test
	mockShard                   *shard.ContextTest
	config                      *configs.Config
	historyClient               *historyservicemock.MockHistoryServiceClient
	mockNamespaceCache          *namespace.MockRegistry
	clusterMetadata             *cluster.MockMetadata
	workflowCache               *wcache.MockCache
	nDCHistoryResender          *eventhandler.MockResendHandler
	replicationTaskExecutor     *taskExecutorImpl
}

func setupTaskExecutorTest(t *testing.T) *taskExecutorTestDeps {
	controller := gomock.NewController(t)
	remoteCluster := cluster.TestAlternativeClusterName

	config := tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			ReplicationDlqAckLevel: map[string]int64{
				cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID,
			},
		},
		config,
	)
	mockResource := mockShard.Resource
	mockNamespaceCache := mockResource.NamespaceCache
	clusterMetadata := mockResource.ClusterMetadata
	nDCHistoryResender := eventhandler.NewMockResendHandler(controller)
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	workflowCache := wcache.NewMockCache(controller)

	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	mockShard.SetHistoryClientForTesting(historyClient)
	replicationTaskExecutor := NewTaskExecutor(
		remoteCluster,
		mockShard,
		nDCHistoryResender,
		deletemanager.NewMockDeleteManager(controller),
		workflowCache,
	).(*taskExecutorImpl)

	return &taskExecutorTestDeps{
		controller:              controller,
		remoteCluster:           remoteCluster,
		mockResource:            mockResource,
		mockShard:               mockShard,
		config:                  config,
		historyClient:           historyClient,
		mockNamespaceCache:      mockNamespaceCache,
		clusterMetadata:         clusterMetadata,
		workflowCache:           workflowCache,
		nDCHistoryResender:      nDCHistoryResender,
		replicationTaskExecutor: replicationTaskExecutor,
	}
}

func TestFilterTask_Apply(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(namespace.NewGlobalNamespaceForTest(
			nil,
			nil,
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			}},
			0,
		), nil)
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestFilterTask_NotApply(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(namespace.NewGlobalNamespaceForTest(
			nil,
			nil,
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{cluster.TestAlternativeClusterName}},
			0,
		), nil)
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFilterTask_Error(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, fmt.Errorf("random error"))
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	require.Error(t, err)
	require.False(t, ok)
}

func TestFilterTask_EnforceApply(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, true)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestFilterTask_NamespaceNotFound(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, &serviceerror.NamespaceNotFound{})
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestProcessTaskOnce_SyncActivityReplicationTask(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:                namespaceID.String(),
				WorkflowId:                 workflowID,
				RunId:                      runID,
				Version:                    1234,
				ScheduledEventId:           2345,
				ScheduledTime:              nil,
				StartedEventId:             2346,
				StartedTime:                nil,
				LastHeartbeatTime:          nil,
				Attempt:                    10,
				LastFailure:                nil,
				LastWorkerIdentity:         "",
				LastStartedBuildId:         "ABC",
				LastStartedRedirectCounter: 8,
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:                namespaceID.String(),
		WorkflowId:                 workflowID,
		RunId:                      runID,
		Version:                    1234,
		ScheduledEventId:           2345,
		ScheduledTime:              nil,
		StartedEventId:             2346,
		StartedTime:                nil,
		LastHeartbeatTime:          nil,
		Attempt:                    10,
		LastFailure:                nil,
		LastWorkerIdentity:         "",
		LastStartedBuildId:         "ABC",
		LastStartedRedirectCounter: 8,
	}

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	require.NoError(t, err)
}

func TestProcessTaskOnce_SyncActivityReplicationTask_Resend(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID.String(),
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            1234,
				ScheduledEventId:   2345,
				ScheduledTime:      nil,
				StartedEventId:     2346,
				StartedTime:        nil,
				LastHeartbeatTime:  nil,
				Attempt:            10,
				LastFailure:        nil,
				LastWorkerIdentity: "",
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:        namespaceID.String(),
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            1234,
		ScheduledEventId:   2345,
		ScheduledTime:      nil,
		StartedEventId:     2346,
		StartedTime:        nil,
		LastHeartbeatTime:  nil,
		Attempt:            10,
		LastFailure:        nil,
		LastWorkerIdentity: "",
	}

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID.String(),
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(nil, resendErr)
	s.nDCHistoryResender.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		s.remoteCluster,
		namespaceID,
		workflowID,
		runID,
		int64(123),
		int64(234),
		int64(345),
		int64(456),
	)

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	require.NoError(t, err)
}

func TestProcess_HistoryReplicationTask(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         namespaceID.String(),
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	require.NoError(t, err)
}

func TestProcess_HistoryReplicationTask_Resend(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         namespaceID.String(),
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID.String(),
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil, resendErr)
	s.nDCHistoryResender.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		s.remoteCluster,
		namespaceID,
		workflowID,
		runID,
		int64(123),
		int64(234),
		int64(345),
		int64(456),
	)

	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	require.NoError(t, err)
}

func TestProcessTaskOnce_SyncWorkflowStateTask(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
					},
				},
			},
		},
	}
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateWorkflowStateResponse{}, nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	require.NoError(t, err)
}

func TestProcessTaskOnce_SyncHSMTask(t *testing.T) {
	s := setupTaskExecutorTest(t)
	defer s.controller.Finish()
	defer s.mockShard.StopForTest()

	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
			NamespaceId:      namespaceID.String(),
			WorkflowId:       workflowID,
			RunId:            runID,
			VersionHistory:   &historyspb.VersionHistory{},
			StateMachineNode: &persistencespb.StateMachineNode{},
		}},
	}

	// not handling SyncHSMTask in deprecated replication task processing code path
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	require.ErrorIs(t, err, ErrUnknownReplicationTask)
}
