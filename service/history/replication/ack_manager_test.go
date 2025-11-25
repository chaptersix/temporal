package replication

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type ackManagerTestSetup struct {
	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockNamespaceRegistry *namespace.MockRegistry
	mockMutableState      *historyi.MockMutableState
	mockClusterMetadata   *cluster.MockMetadata
	syncStateRetriever    *MockSyncStateRetriever
	mockExecutionMgr      *persistence.MockExecutionManager
	logger                 log.Logger
	replicationAckManager *ackMgrImpl
}

func setupAckManagerTest(t *testing.T) *ackManagerTestSetup {
	controller := gomock.NewController(t)
	mockMutableState := historyi.NewMockMutableState(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	mockNamespaceRegistry := mockShard.Resource.NamespaceCache
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	mockExecutionMgr := mockShard.Resource.ExecutionMgr

	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return(cluster.TestCurrentClusterName).AnyTimes()

	logger := mockShard.GetLogger()
	workflowCache := wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	replicationProgressCache := NewProgressCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler)
	syncStateRetriever := NewMockSyncStateRetriever(controller)
	replicationAckManager := NewAckManager(
		mockShard, workflowCache, nil, replicationProgressCache, mockExecutionMgr, syncStateRetriever, logger,
	).(*ackMgrImpl)

	return &ackManagerTestSetup{
		controller:            controller,
		mockShard:             mockShard,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockMutableState:      mockMutableState,
		mockClusterMetadata:   mockClusterMetadata,
		syncStateRetriever:    syncStateRetriever,
		mockExecutionMgr:      mockExecutionMgr,
		logger:                logger,
		replicationAckManager: replicationAckManager,
	}
}

func TestNotifyNewTasks_NotInitialized(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	setup.replicationAckManager.maxTaskID = nil

	setup.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 456},
		&tasks.HistoryReplicationTask{TaskID: 123},
	})

	require.Equal(t, int64(456), *setup.replicationAckManager.maxTaskID)
}

func TestNotifyNewTasks_Initialized(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	setup.replicationAckManager.maxTaskID = util.Ptr(int64(123))

	setup.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 100},
	})
	require.Equal(t, int64(123), *setup.replicationAckManager.maxTaskID)

	setup.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 234},
	})
	require.Equal(t, int64(234), *setup.replicationAckManager.maxTaskID)
}

func TestTaskIDRange_NotInitialized(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	setup.replicationAckManager.sanityCheckTime = time.Time{}
	expectMaxTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
	expectMinTaskID := expectMaxTaskID - 100
	setup.replicationAckManager.maxTaskID = util.Ptr(expectMinTaskID - 100)

	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(expectMinTaskID)
	require.Equal(t, expectMinTaskID, minTaskID)
	require.Equal(t, expectMaxTaskID, maxTaskID)
	require.NotEqual(t, time.Time{}, setup.replicationAckManager.sanityCheckTime)
	require.Equal(t, expectMaxTaskID, *setup.replicationAckManager.maxTaskID)
}

func TestTaskIDRange_Initialized_UseHighestReplicationTaskID(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	now := time.Now().UTC()
	sanityCheckTime := now.Add(2 * time.Minute)
	setup.replicationAckManager.sanityCheckTime = sanityCheckTime
	expectMinTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 100
	expectMaxTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 50
	setup.replicationAckManager.maxTaskID = util.Ptr(expectMaxTaskID)

	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(expectMinTaskID)
	require.Equal(t, expectMinTaskID, minTaskID)
	require.Equal(t, expectMaxTaskID, maxTaskID)
	require.Equal(t, sanityCheckTime, setup.replicationAckManager.sanityCheckTime)
	require.Equal(t, expectMaxTaskID, *setup.replicationAckManager.maxTaskID)
}

func TestTaskIDRange_Initialized_NoHighestReplicationTaskID(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	now := time.Now().UTC()
	sanityCheckTime := now.Add(2 * time.Minute)
	setup.replicationAckManager.sanityCheckTime = sanityCheckTime
	expectMinTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID - 100
	expectMaxTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
	setup.replicationAckManager.maxTaskID = nil

	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(expectMinTaskID)
	require.Equal(t, expectMinTaskID, minTaskID)
	require.Equal(t, expectMaxTaskID, maxTaskID)
	require.Equal(t, sanityCheckTime, setup.replicationAckManager.sanityCheckTime)
	require.Equal(t, expectMaxTaskID, *setup.replicationAckManager.maxTaskID)
}

func TestTaskIDRange_Initialized_UseHighestTransferTaskID(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	now := time.Now().UTC()
	sanityCheckTime := now.Add(-2 * time.Minute)
	setup.replicationAckManager.sanityCheckTime = sanityCheckTime
	expectMinTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID - 100
	expectMaxTaskID := setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
	setup.replicationAckManager.maxTaskID = util.Ptr(setup.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 50)

	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(expectMinTaskID)
	require.Equal(t, expectMinTaskID, minTaskID)
	require.Equal(t, expectMaxTaskID, maxTaskID)
	require.NotEqual(t, sanityCheckTime, setup.replicationAckManager.sanityCheckTime)
	require.Equal(t, expectMaxTaskID, *setup.replicationAckManager.maxTaskID)
}

func Test_GetMaxTaskInfo(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	now := time.Now()
	taskSet := []tasks.Task{
		&tasks.HistoryReplicationTask{
			TaskID:              1,
			VisibilityTimestamp: now,
		},
		&tasks.HistoryReplicationTask{
			TaskID:              6,
			VisibilityTimestamp: now.Add(time.Second),
		},
		&tasks.HistoryReplicationTask{
			TaskID:              3,
			VisibilityTimestamp: now.Add(time.Hour),
		},
	}
	setup.replicationAckManager.NotifyNewTasks(taskSet)

	maxTaskID, maxVisibilityTimestamp := setup.replicationAckManager.GetMaxTaskInfo()
	require.Equal(t, int64(6), maxTaskID)
	require.Equal(t, now.Add(time.Hour), maxVisibilityTimestamp)
}

func TestGetTasks_NoTasksInDB(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(getHistoryTasksResponse(0), nil)

	replicationTasks, lastTaskID, err := setup.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	require.NoError(t, err)
	require.Empty(t, replicationTasks)
	require.Equal(t, maxTaskID, lastTaskID)
}

func TestGetTasks_FirstPersistenceErrorReturnsErrorAndEmptyResult(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	tasksResponse := getHistoryTasksResponse(2)
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	gweErr := serviceerror.NewUnavailable("random error")
	setup.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     setup.mockShard.GetShardID(),
		NamespaceID: tasksResponse.Tasks[0].GetNamespaceID(),
		WorkflowID:  tasksResponse.Tasks[0].GetWorkflowID(),
		RunID:       tasksResponse.Tasks[0].GetRunID(),
	}).Return(nil, gweErr)

	replicationTasks, lastTaskID, err := setup.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	require.Error(t, err)
	require.ErrorIs(t, err, gweErr)
	require.Empty(t, replicationTasks)
	require.EqualValues(t, 0, lastTaskID)
}

func TestGetTasks_SecondPersistenceErrorReturnsPartialResult(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	tasksResponse := getHistoryTasksResponse(2)
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	eventsCache := events.NewHostLevelEventsCache(
		setup.mockShard.GetExecutionManager(),
		setup.mockShard.GetConfig(),
		setup.mockShard.GetMetricsHandler(),
		setup.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(setup.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	setup.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil)
	setup.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("some random error"))
	setup.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil)

	replicationTasks, lastTaskID, err := setup.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	require.NoError(t, err)
	require.Equal(t, 1, len(replicationTasks))
	require.Equal(t, tasksResponse.Tasks[0].GetTaskID(), lastTaskID)
}

func TestGetTasks_FullPage(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	tasksResponse := getHistoryTasksResponse(setup.replicationAckManager.pageSize())
	tasksResponse.NextPageToken = []byte{22, 3, 83} // There is more in DB.
	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(22)
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	eventsCache := events.NewHostLevelEventsCache(
		setup.mockShard.GetExecutionManager(),
		setup.mockShard.GetConfig(),
		setup.mockShard.GetMetricsHandler(),
		setup.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(setup.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	setup.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil).Times(setup.replicationAckManager.pageSize())
	setup.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(setup.replicationAckManager.pageSize())

	replicationMessages, err := setup.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	require.NoError(t, err)
	require.NotNil(t, replicationMessages)
	require.Len(t, replicationMessages.ReplicationTasks, setup.replicationAckManager.pageSize())
	require.Equal(t, tasksResponse.Tasks[len(tasksResponse.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)
}
func TestGetTasks_PartialPage(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	numTasks := setup.replicationAckManager.pageSize() / 2
	tasksResponse := getHistoryTasksResponse(numTasks)
	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(22)
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	eventsCache := events.NewHostLevelEventsCache(
		setup.mockShard.GetExecutionManager(),
		setup.mockShard.GetConfig(),
		setup.mockShard.GetMetricsHandler(),
		setup.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(setup.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	setup.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil).Times(numTasks)
	setup.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(numTasks)

	replicationMessages, err := setup.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	require.NoError(t, err)
	require.NotNil(t, replicationMessages)
	require.Len(t, replicationMessages.ReplicationTasks, numTasks)
	require.Equal(t, tasksResponse.Tasks[len(tasksResponse.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)
}

func TestGetTasks_FilterNamespace(t *testing.T) {
	setup := setupAckManagerTest(t)
	defer setup.controller.Finish()
	defer setup.mockShard.StopForTest()

	notExistOnTestClusterNamespaceID := namespace.ID("not-exist-on-" + cluster.TestCurrentClusterName)
	notExistOnTestClusterNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		&persistencespb.NamespaceConfig{},
		"not-a-"+cluster.TestCurrentClusterName,
	)
	setup.mockNamespaceRegistry.EXPECT().GetNamespaceByID(notExistOnTestClusterNamespaceID).Return(notExistOnTestClusterNamespaceEntry, nil).AnyTimes()

	minTaskID, maxTaskID := setup.replicationAckManager.taskIDsRange(22)

	tasksResponse1 := getHistoryTasksResponse(setup.replicationAckManager.pageSize())
	// 2 of 25 tasks are for namespace that doesn't exist on poll cluster.
	tasksResponse1.Tasks[1].(*tasks.HistoryReplicationTask).NamespaceID = notExistOnTestClusterNamespaceID.String()
	tasksResponse1.Tasks[3].(*tasks.HistoryReplicationTask).NamespaceID = notExistOnTestClusterNamespaceID.String()
	tasksResponse1.NextPageToken = []byte{22, 3, 83} // There is more in DB.
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse1, nil)

	tasksResponse2 := getHistoryTasksResponse(2)
	// 1 of 2 task is for namespace that doesn't exist on poll cluster.
	tasksResponse2.Tasks[1].(*tasks.HistoryReplicationTask).NamespaceID = notExistOnTestClusterNamespaceID.String()
	tasksResponse2.NextPageToken = []byte{22, 8, 78} // There is more in DB.
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       []byte{22, 3, 83}, // previous token
	}).Return(tasksResponse2, nil)

	tasksResponse3 := getHistoryTasksResponse(1)
	setup.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             setup.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           setup.replicationAckManager.pageSize(),
		NextPageToken:       []byte{22, 8, 78}, // previous token
	}).Return(tasksResponse3, nil)

	eventsCache := events.NewHostLevelEventsCache(
		setup.mockShard.GetExecutionManager(),
		setup.mockShard.GetConfig(),
		setup.mockShard.GetMetricsHandler(),
		setup.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(setup.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	setup.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil).Times(setup.replicationAckManager.pageSize())
	setup.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(setup.replicationAckManager.pageSize())

	replicationMessages, err := setup.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	require.NoError(t, err)
	require.NotNil(t, replicationMessages)
	require.Len(t, replicationMessages.ReplicationTasks, setup.replicationAckManager.pageSize())
	require.Equal(t, tasksResponse3.Tasks[len(tasksResponse3.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)
}

func getHistoryTasksResponse(size int) *persistence.GetHistoryTasksResponse {
	result := &persistence.GetHistoryTasksResponse{}
	for i := 1; i <= size; i++ {
		result.Tasks = append(result.Tasks, &tasks.HistoryReplicationTask{
			WorkflowKey: definition.WorkflowKey{
				NamespaceID: tests.NamespaceID.String(),
				WorkflowID:  tests.WorkflowID + strconv.Itoa(i),
				RunID:       uuid.New(),
			},
			TaskID:       int64(i),
			FirstEventID: 1,
			NextEventID:  1,
			Version:      1,
		},
		)
	}

	return result
}
