package replication

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type taskProcessorManagerTestDeps struct {
	controller                        *gomock.Controller
	mockShard                         *historyi.MockShardContext
	mockEngine                        *historyi.MockEngine
	mockClientBean                    *client.MockBean
	mockClusterMetadata               *cluster.MockMetadata
	mockHistoryClient                 *historyservicemock.MockHistoryServiceClient
	mockReplicationTaskExecutor       *MockTaskExecutor
	mockReplicationTaskFetcherFactory *MockTaskFetcherFactory
	mockExecutionManager              *persistence.MockExecutionManager
	shardID                           int32
	shardOwner                        string
	config                            *configs.Config
	requestChan                       chan *replicationTaskRequest
	taskProcessorManager              *taskProcessorManagerImpl
}

func setupTaskProcessorManagerTest(t *testing.T) *taskProcessorManagerTestDeps {
	controller := gomock.NewController(t)

	config := tests.NewDynamicConfig()
	requestChan := make(chan *replicationTaskRequest, 10)

	shardID := rand.Int31()
	shardOwner := "test-shard-owner"
	mockShard := historyi.NewMockShardContext(controller)
	mockEngine := historyi.NewMockEngine(controller)
	mockClientBean := client.NewMockBean(controller)

	mockReplicationTaskExecutor := NewMockTaskExecutor(controller)
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(controller)
	mockReplicationTaskFetcherFactory := NewMockTaskFetcherFactory(controller)
	serializer := serialization.NewSerializer()
	mockClusterMetadata := cluster.NewMockMetadata(controller)
	mockClientBean.EXPECT().GetHistoryClient().Return(mockHistoryClient).AnyTimes()
	mockShard.EXPECT().GetClusterMetadata().Return(mockClusterMetadata).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	mockShard.EXPECT().GetHistoryClient().Return(nil).AnyTimes()
	mockShard.EXPECT().GetNamespaceRegistry().Return(namespace.NewMockRegistry(controller)).AnyTimes()
	mockShard.EXPECT().GetConfig().Return(config).AnyTimes()
	mockShard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	mockShard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	mockShard.EXPECT().GetPayloadSerializer().Return(serializer).AnyTimes()
	mockExecutionManager := persistence.NewMockExecutionManager(controller)
	mockShard.EXPECT().GetExecutionManager().Return(mockExecutionManager).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(shardID).AnyTimes()
	mockShard.EXPECT().GetOwner().Return(shardOwner).AnyTimes()
	taskProcessorManager := NewTaskProcessorManager(
		config,
		mockShard,
		mockEngine,
		nil,
		nil,
		mockClientBean,
		serializer,
		mockReplicationTaskFetcherFactory,
		func(params TaskExecutorParams) TaskExecutor {
			return mockReplicationTaskExecutor
		},
		NewExecutionManagerDLQWriter(mockExecutionManager),
	)

	return &taskProcessorManagerTestDeps{
		controller:                        controller,
		mockShard:                         mockShard,
		mockEngine:                        mockEngine,
		mockClientBean:                    mockClientBean,
		mockClusterMetadata:               mockClusterMetadata,
		mockHistoryClient:                 mockHistoryClient,
		mockReplicationTaskExecutor:       mockReplicationTaskExecutor,
		mockReplicationTaskFetcherFactory: mockReplicationTaskFetcherFactory,
		mockExecutionManager:              mockExecutionManager,
		shardID:                           shardID,
		shardOwner:                        shardOwner,
		config:                            config,
		requestChan:                       requestChan,
		taskProcessorManager:              taskProcessorManager,
	}
}

func TestCleanupReplicationTask_Noop(t *testing.T) {
	s := setupTaskProcessorManagerTest(t)
	defer s.controller.Finish()

	ackedTaskID := int64(12345)
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(ackedTaskID + 2)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			shard.ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, s.shardID): {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackedTaskID + 1),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}, true)

	s.taskProcessorManager.minTxAckedTaskID = ackedTaskID
	err := s.taskProcessorManager.cleanupReplicationTasks()
	require.NoError(t, err)
}

func TestCleanupReplicationTask_Cleanup(t *testing.T) {
	s := setupTaskProcessorManagerTest(t)
	defer s.controller.Finish()

	ackedTaskID := int64(12345)
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(ackedTaskID + 2)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			shard.ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, common.MapShardID(
				cluster.TestAllClusterInfo[cluster.TestCurrentClusterName].ShardCount,
				cluster.TestAllClusterInfo[cluster.TestAlternativeClusterName].ShardCount,
				s.shardID,
			)[0]): {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackedTaskID + 1),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}, true)
	s.taskProcessorManager.minTxAckedTaskID = ackedTaskID - 1
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(
		gomock.Any(),
		&persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             s.shardID,
			TaskCategory:        tasks.CategoryReplication,
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(ackedTaskID + 1),
		},
	).Return(nil).Times(1)
	err := s.taskProcessorManager.cleanupReplicationTasks()
	require.NoError(t, err)
}
