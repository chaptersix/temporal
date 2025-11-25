package replication

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

func setupDLQHandler(t *testing.T) (*gomock.Controller, *shard.ContextTest, *configs.Config, *client.MockBean, *adminservicemock.MockAdminServiceClient, *cluster.MockMetadata, *persistence.MockExecutionManager, *persistence.MockShardManager, *MockTaskExecutor, map[string]TaskExecutor, string, *dlqHandlerImpl) {
	controller := gomock.NewController(t)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId:                0,
			RangeId:                1,
			ReplicationDlqAckLevel: map[string]int64{cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID},
		},
		tests.NewDynamicConfig(),
	)
	mockResource := mockShard.Resource
	mockClientBean := mockResource.ClientBean
	adminClient := mockResource.RemoteAdminClient
	clusterMetadata := mockResource.ClusterMetadata
	executionManager := mockResource.ExecutionMgr
	shardManager := mockResource.ShardMgr
	config := tests.NewDynamicConfig()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	taskExecutors := make(map[string]TaskExecutor)
	taskExecutor := NewMockTaskExecutor(controller)
	sourceCluster := cluster.TestAlternativeClusterName
	taskExecutors[sourceCluster] = taskExecutor

	replicationMessageHandler := newDLQHandler(
		mockShard,
		deletemanager.NewMockDeleteManager(controller),
		wcache.NewMockCache(controller),
		mockClientBean,
		taskExecutors,
		func(params TaskExecutorParams) TaskExecutor {
			return NewTaskExecutor(
				params.RemoteCluster,
				params.Shard,
				params.HistoryResender,
				params.DeleteManager,
				params.WorkflowCache,
			)
		},
	)

	return controller, mockShard, config, mockClientBean, adminClient, clusterMetadata, executionManager, shardManager, taskExecutor, taskExecutors, sourceCluster, replicationMessageHandler
}

func TestReadMessages_OK(t *testing.T) {
	controller, mockShard, _, mockClientBean, adminClient, _, executionManager, _, _, _, sourceCluster, replicationMessageHandler := setupDLQHandler(t)
	defer controller.Finish()
	defer mockShard.StopForTest()
	ctx := context.Background()

	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	taskID := int64(12345)
	version := int64(2333)
	firstEventID := int64(144)
	nextEventID := int64(233)

	lastMessageID := int64(1394)
	pageSize := 1
	pageToken := []byte("some random token")
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				namespaceID,
				workflowID,
				runID,
			),
			Version:      version,
			FirstEventID: firstEventID,
			NextEventID:  nextEventID,
			TaskID:       taskID,
		}},
		NextPageToken: pageToken,
	}

	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{
					Version: version,
					EventId: nextEventID - 1,
				}},
				Events: &commonpb.DataBlob{},
			},
		},
	}

	executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             mockShard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: sourceCluster,
	}).Return(dbResp, nil)

	mockClientBean.EXPECT().GetRemoteAdminClient(sourceCluster).Return(adminClient, nil).AnyTimes()
	adminClient.EXPECT().GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask},
		}, nil)
	taskList, tasksInfo, token, err := replicationMessageHandler.GetMessages(ctx, sourceCluster, lastMessageID, pageSize, pageToken)
	require.NoError(t, err)
	require.Equal(t, pageToken, token)
	require.Equal(t, []*replicationspb.ReplicationTask{remoteTask}, taskList)
	require.Equal(t, namespaceID, tasksInfo[0].GetNamespaceId())
	require.Equal(t, workflowID, tasksInfo[0].GetWorkflowId())
	require.Equal(t, taskID, tasksInfo[0].GetTaskId())
	require.Equal(t, version, tasksInfo[0].GetVersion())
	require.Equal(t, firstEventID, tasksInfo[0].GetFirstEventId())
	require.Equal(t, nextEventID, tasksInfo[0].GetNextEventId())
}

func TestPurgeMessages(t *testing.T) {
	controller, mockShard, _, _, _, _, executionManager, shardManager, _, _, sourceCluster, replicationMessageHandler := setupDLQHandler(t)
	defer controller.Finish()
	defer mockShard.StopForTest()

	lastMessageID := int64(1)

	executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(
		gomock.Any(),
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             mockShard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			},
			SourceClusterName: sourceCluster,
		}).Return(nil)

	shardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	err := replicationMessageHandler.PurgeMessages(context.Background(), sourceCluster, lastMessageID)
	require.NoError(t, err)
}
func TestMergeMessages(t *testing.T) {
	controller, mockShard, _, mockClientBean, adminClient, _, executionManager, shardManager, taskExecutor, _, sourceCluster, replicationMessageHandler := setupDLQHandler(t)
	defer controller.Finish()
	defer mockShard.StopForTest()
	ctx := context.Background()

	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	taskID := int64(12345)
	version := int64(2333)
	firstEventID := int64(144)
	nextEventID := int64(233)

	lastMessageID := int64(1394)
	pageSize := 1
	pageToken := []byte("some random token")

	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				namespaceID,
				workflowID,
				runID,
			),
			Version:      version,
			FirstEventID: firstEventID,
			NextEventID:  nextEventID,
			TaskID:       taskID,
		}},
		NextPageToken: pageToken,
	}

	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{
					Version: version,
					EventId: nextEventID - 1,
				}},
				Events: &commonpb.DataBlob{},
			},
		},
	}

	executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             mockShard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: sourceCluster,
	}).Return(dbResp, nil)

	mockClientBean.EXPECT().GetRemoteAdminClient(sourceCluster).Return(adminClient, nil).AnyTimes()
	adminClient.EXPECT().GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask},
		}, nil)
	taskExecutor.EXPECT().Execute(gomock.Any(), remoteTask, true).Return(nil)
	executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(gomock.Any(), &persistence.RangeDeleteReplicationTaskFromDLQRequest{
		RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             mockShard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
		},
		SourceClusterName: sourceCluster,
	}).Return(nil)

	shardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)

	token, err := replicationMessageHandler.MergeMessages(ctx, sourceCluster, lastMessageID, pageSize, pageToken)
	require.NoError(t, err)
	require.Equal(t, pageToken, token)
}
