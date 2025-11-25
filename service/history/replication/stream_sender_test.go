package replication

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type streamSenderTestDeps struct {
	controller           *gomock.Controller
	server               *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
	shardContext         *historyi.MockShardContext
	historyEngine        *historyi.MockEngine
	taskConverter        *MockSourceTaskConverter
	clientShardKey       ClusterShardKey
	serverShardKey       ClusterShardKey
	streamSender         *StreamSenderImpl
	senderFlowController *MockSenderFlowController
	config               *configs.Config
}

func setupStreamSenderTest(t *testing.T) *streamSenderTestDeps {
	controller := gomock.NewController(t)
	server := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(controller)
	server.EXPECT().Context().Return(context.Background()).AnyTimes()
	shardContext := historyi.NewMockShardContext(controller)
	historyEngine := historyi.NewMockEngine(controller)
	taskConverter := NewMockSourceTaskConverter(controller)
	config := tests.NewDynamicConfig()
	clientShardKey := NewClusterShardKey(rand.Int31(), 1)
	serverShardKey := NewClusterShardKey(rand.Int31(), 1)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(historyEngine, nil).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()

	streamSender := NewStreamSender(
		server,
		shardContext,
		historyEngine,
		quotas.NoopRequestRateLimiter,
		taskConverter,
		"target_cluster",
		2,
		clientShardKey,
		serverShardKey,
		config,
	)
	senderFlowController := NewMockSenderFlowController(controller)
	streamSender.flowController = senderFlowController

	return &streamSenderTestDeps{
		controller:           controller,
		server:               server,
		shardContext:         shardContext,
		historyEngine:        historyEngine,
		taskConverter:        taskConverter,
		clientShardKey:       clientShardKey,
		serverShardKey:       serverShardKey,
		streamSender:         streamSender,
		senderFlowController: senderFlowController,
		config:               config,
	}
}

func TestRecvSyncReplicationState_SingleStack_Success(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = false
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	deps.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
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
	).Return(nil)
	deps.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.InclusiveLowWatermark-1,
		replicationState.InclusiveLowWatermarkTime.AsTime(),
	).Return(nil)

	err := deps.streamSender.recvSyncReplicationState(replicationState)
	require.NoError(t, err)
}

func TestRecvSyncReplicationState_SingleStack_Error(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = false
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}

	deps.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
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
	).Return(ownershipLost)

	err := deps.streamSender.recvSyncReplicationState(replicationState)
	require.Error(t, err)
	require.Equal(t, ownershipLost, err)
}

func TestRecvSyncReplicationState_TieredStack_Success(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	lowPriorityInclusiveWatermark := int64(1234)
	highPriorityInclusiveWatermark := lowPriorityInclusiveWatermark + 10

	timestamp := timestamppb.New(time.Unix(0, rand.Int63()))
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     lowPriorityInclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     highPriorityInclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     lowPriorityInclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
	}
	deps.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(replicationState).Return().Times(1)

	deps.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.HighPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.LowPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		},
	).Return(nil)
	deps.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.HighPriorityState.InclusiveLowWatermark-1,
		replicationState.InclusiveLowWatermarkTime.AsTime(),
	).Return(nil)

	err := deps.streamSender.recvSyncReplicationState(replicationState)
	require.NoError(t, err)
}

func TestRecvSyncReplicationState_TieredStack_Error(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	inclusiveWatermark := int64(1234)
	timestamp := timestamppb.New(time.Unix(0, rand.Int63()))
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     inclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     inclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     inclusiveWatermark + 10,
			InclusiveLowWatermarkTime: timestamp,
		},
	}

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}
	deps.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(replicationState).Return().Times(1)

	deps.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.HighPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.LowPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		},
	).Return(ownershipLost)

	err := deps.streamSender.recvSyncReplicationState(replicationState)
	require.Error(t, err)
	require.Equal(t, ownershipLost, err)
}

func TestSendCatchUp_SingleStack(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 1
	deps.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
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
	deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	require.NoError(t, err)
	require.Equal(t, endExclusiveWatermark, taskID)
}

func TestSendCatchUp_TieredStack_SingleReaderScope(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 1
	deps.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{{ // only has one scope
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
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
	}, true).Times(2)
	deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil).Times(2)
	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	}).Times(2)

	highPriorityCatchupTaskID, highPriorityCatchupErr := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_HIGH)
	lowPriorityCatchupTaskID, lowPriorityCatchupErr := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_LOW)
	require.NoError(t, highPriorityCatchupErr)
	require.Equal(t, endExclusiveWatermark, highPriorityCatchupTaskID)
	require.NoError(t, lowPriorityCatchupErr)
	require.Equal(t, endExclusiveWatermark, lowPriorityCatchupTaskID)
}

func TestSendCatchUp_TieredStack_TieredReaderScope(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(deps.clientShardKey.ClusterID),
		deps.clientShardKey.ShardID,
	)
	beginInclusiveWatermarkHighPriority := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermarkHighPriority + 1
	beginInclusiveWatermarkLowPriority := beginInclusiveWatermarkHighPriority - 100
	deps.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkLowPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkHighPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkLowPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
				},
			},
		},
	}, true).Times(2)
	deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)

	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermarkHighPriority,
		endExclusiveWatermark,
	).Return(iter, nil).Times(1)

	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermarkLowPriority,
		endExclusiveWatermark,
	).Return(iter, nil).Times(1)

	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	}).Times(2)

	lowPriorityCatchupTaskID, lowPriorityCatchupErr := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_LOW)
	highPriorityCatchupTaskID, highPriorityCatchupErr := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_HIGH)
	require.NoError(t, highPriorityCatchupErr)
	require.Equal(t, endExclusiveWatermark, highPriorityCatchupTaskID)
	require.NoError(t, lowPriorityCatchupErr)
	require.Equal(t, endExclusiveWatermark, lowPriorityCatchupTaskID)
}

func TestSendCatchUp_SingleStack_NoReaderState(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	endExclusiveWatermark := int64(1234)
	deps.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
	}, true)
	deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	require.NoError(t, err)
	require.Equal(t, endExclusiveWatermark, taskID)
}

func TestSendCatchUp_TieredStack_NoReaderState(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	endExclusiveWatermark := int64(1234)
	deps.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
	}, true).Times(2)
	deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	}).Times(2)

	taskID, err := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_HIGH)
	require.NoError(t, err)
	require.Equal(t, endExclusiveWatermark, taskID)
	taskID, err = deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_LOW)
	require.NoError(t, err)
	require.Equal(t, endExclusiveWatermark, taskID)
}

func TestSendCatchUp_SingleStack_NoQueueState(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	endExclusiveWatermark := int64(1234)
	deps.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(nil, false)
	deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := deps.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	require.NoError(t, err)
	require.Equal(t, endExclusiveWatermark, taskID)
}

func TestSendLive(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	channel := make(chan struct{})
	watermark0 := rand.Int63()
	watermark1 := watermark0 + 1 + rand.Int63n(100)
	watermark2 := watermark1 + 1 + rand.Int63n(100)

	gomock.InOrder(
		deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
			tasks.NewImmediateKey(watermark1),
		),
		deps.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
			tasks.NewImmediateKey(watermark2),
		),
	)
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	gomock.InOrder(
		deps.historyEngine.EXPECT().GetReplicationTasksIter(
			gomock.Any(),
			string(deps.clientShardKey.ClusterID),
			watermark0,
			watermark1,
		).Return(iter, nil),
		deps.historyEngine.EXPECT().GetReplicationTasksIter(
			gomock.Any(),
			string(deps.clientShardKey.ClusterID),
			watermark1,
			watermark2,
		).Return(iter, nil),
	)
	gomock.InOrder(
		deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			require.Equal(t, watermark1, resp.GetMessages().ExclusiveHighWatermark)
			require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
		deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			require.Equal(t, watermark2, resp.GetMessages().ExclusiveHighWatermark)
			require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)
	go func() {
		channel <- struct{}{}
		channel <- struct{}{}
		deps.streamSender.shutdownChan.Shutdown()
	}()
	err := deps.streamSender.sendLive(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		channel,
		watermark0,
	)
	require.Nil(t, err)
	require.True(t, !deps.streamSender.IsValid())
}

func TestSendTasks_Noop(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark

	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	err := deps.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	require.NoError(t, err)
}

func TestSendTasks_WithoutTasks(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	err := deps.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	require.NoError(t, err)
}

func TestSendTasks_WithTasks(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = false
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := tasks.NewMockTask(deps.controller)
	item1 := tasks.NewMockTask(deps.controller)
	item2 := tasks.NewMockTask(deps.controller)
	item3 := tasks.NewMockTask(deps.controller)
	item0.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item1.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item2.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item3.EXPECT().GetNamespaceID().Return("2").AnyTimes()
	item0.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item1.EXPECT().GetWorkflowID().Return("3").AnyTimes()
	item2.EXPECT().GetWorkflowID().Return("2").AnyTimes()
	item3.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item0.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item1.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item2.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item3.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item0.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item1.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item2.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item3.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	task0 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	task2 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark + 2,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2, item3}, nil, nil
		},
	)
	mockRegistry := namespace.NewMockRegistry(deps.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("2")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster"},
		}, 100), nil).AnyTimes()
	deps.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	deps.taskConverter.EXPECT().Convert(item0, deps.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(task0, nil)
	deps.taskConverter.EXPECT().Convert(item1, deps.clientShardKey.ClusterID, gomock.Any()).Times(0)
	deps.taskConverter.EXPECT().Convert(item2, deps.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(task2, nil)
	deps.taskConverter.EXPECT().Convert(item3, deps.clientShardKey.ClusterID, gomock.Any()).Times(0)
	gomock.InOrder(
		deps.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
					ExclusiveHighWatermark:     task0.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task0.VisibilityTime,
				},
			},
		}).Return(nil),
		deps.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
					ExclusiveHighWatermark:     task2.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task2.VisibilityTime,
				},
			},
		}).Return(nil),
		deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := deps.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	require.NoError(t, err)
}

func TestSendTasks_TieredStack_HighPriority(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}

	item1 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_HIGH,
	}
	item2 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}
	task1 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	mockRegistry := namespace.NewMockRegistry(deps.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	deps.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	deps.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil).Times(1)
	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	deps.taskConverter.EXPECT().Convert(item1, deps.clientShardKey.ClusterID, item1.Priority).Return(task1, nil)

	gomock.InOrder(
		deps.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task1},
					ExclusiveHighWatermark:     task1.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task1.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		}).Return(nil),
		deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			require.Equal(t, enumsspb.TASK_PRIORITY_HIGH, resp.GetMessages().Priority)
			require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := deps.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_HIGH,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	require.NoError(t, err)
}

func TestSendTasks_TieredStack_LowPriority(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.isTieredStackEnabled = true
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}
	item1 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_HIGH,
	}
	item2 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}

	task0 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	task2 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	mockRegistry := namespace.NewMockRegistry(deps.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	mockRegistry.EXPECT().GetNamespaceName(namespace.ID("1")).Return(namespace.Name("test"), nil).AnyTimes()
	deps.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	deps.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_LOW).Return(nil).Times(2)
	deps.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(deps.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	deps.taskConverter.EXPECT().Convert(item0, deps.clientShardKey.ClusterID, item0.Priority).Return(task0, nil)
	deps.taskConverter.EXPECT().Convert(item0, deps.clientShardKey.ClusterID, item0.Priority).Return(task2, nil)

	gomock.InOrder(
		deps.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
					ExclusiveHighWatermark:     task0.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task0.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		}).Return(nil),
		deps.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
					ExclusiveHighWatermark:     task2.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task2.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		}).Return(nil),
		deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			require.Equal(t, enumsspb.TASK_PRIORITY_LOW, resp.GetMessages().Priority)
			require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := deps.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_LOW,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	require.NoError(t, err)
}

func TestSendEventLoop_Panic_ShouldCaptureAsError(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.historyEngine.EXPECT().SubscribeReplicationNotification("target_cluster").Do(func(_ string) {
		panic("panic")
	})
	err := deps.streamSender.sendEventLoop(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	require.Error(t, err) // panic is captured as error
}

func TestRecvEventLoop_Panic_ShouldCaptureAsError(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.streamSender.shutdownChan = nil // mimic nil pointer panic
	err := deps.streamSender.recvEventLoop()
	require.Error(t, err) // panic is captured as error
}

func TestSendEventLoop_StreamSendError_ShouldReturnStreamError(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark

	deps.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		require.Equal(t, endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		require.NotNil(t, resp.GetMessages().ExclusiveHighWatermarkTime)
		return errors.New("rpc error")
	})

	err := deps.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	require.Error(t, err, "rpc error")
	require.IsType(t, &StreamError{}, err)
}

func TestRecvEventLoop_RpcError_ShouldReturnStreamError(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	deps.server.EXPECT().Recv().Return(nil, errors.New("rpc error"))
	err := deps.streamSender.recvEventLoop()
	require.Error(t, err)
	require.Error(t, err, "rpc error")
	require.IsType(t, &StreamError{}, err)
}

func TestLivenessMonitor(t *testing.T) {
	deps := setupStreamSenderTest(t)
	defer deps.controller.Finish()

	livenessMonitor(
		deps.streamSender.recvSignalChan,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetIntPropertyFn(1),
		deps.streamSender.shutdownChan,
		deps.streamSender.Stop,
		deps.streamSender.logger,
	)
	require.False(t, deps.streamSender.IsValid())
}
