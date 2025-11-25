package replication

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type streamReceiverMonitorTestHelper struct {
	controller      *gomock.Controller
	clusterMetadata *cluster.MockMetadata
	clientBean      *client.MockBean
	shardController *shard.MockController

	streamReceiverMonitor *StreamReceiverMonitorImpl
}

func setupStreamReceiverMonitorTest(t *testing.T) *streamReceiverMonitorTestHelper {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clientBean := client.NewMockBean(controller)
	shardController := shard.NewMockController(controller)

	processToolBox := ProcessToolBox{
		Config: configs.NewConfig(
			dynamicconfig.NewNoopCollection(),
			1,
		),
		ClusterMetadata: clusterMetadata,
		ClientBean:      clientBean,
		ShardController: shardController,
		MetricsHandler:  metrics.NoopMetricsHandler,
		Logger:          log.NewNoopLogger(),
		DLQWriter:       NoopDLQWriter{},
	}
	streamReceiverMonitor := NewStreamReceiverMonitor(
		processToolBox,
		NewExecutableTaskConverter(processToolBox),
		true,
	)
	streamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(controller)
	streamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	streamClient.EXPECT().Recv().Return(&adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           []*replicationspb.ReplicationTask{},
				ExclusiveHighWatermark:     100,
				ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, 100)),
			},
		},
	}, nil).AnyTimes()
	streamClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	adminClient := adminservicemock.NewMockAdminServiceClient(controller)
	adminClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(streamClient, nil).AnyTimes()
	clientBean.EXPECT().GetRemoteAdminClient(cluster.TestAlternativeClusterName).Return(adminClient, nil).AnyTimes()

	return &streamReceiverMonitorTestHelper{
		controller:            controller,
		clusterMetadata:       clusterMetadata,
		clientBean:            clientBean,
		shardController:       shardController,
		streamReceiverMonitor: streamReceiverMonitor,
	}
}

func (h *streamReceiverMonitorTestHelper) tearDown() {
	h.controller.Finish()

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	for serverKey, stream := range h.streamReceiverMonitor.outboundStreams {
		stream.Stop()
		delete(h.streamReceiverMonitor.outboundStreams, serverKey)
	}
}

func TestGenerateInboundStreamKeys_1From4(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestAlternativeClusterInitialFailoverVersion).AnyTimes()
	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	h.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()

	streamKeys := h.streamReceiverMonitor.generateInboundStreamKeys()
	require.Equal(t, map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 2),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 3),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 4),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
	}, streamKeys)
}

func TestGenerateInboundStreamKeys_4From1(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestAlternativeClusterInitialFailoverVersion).AnyTimes()
	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             1,
		},
	}).AnyTimes()
	h.shardController.EXPECT().ShardIDs().Return([]int32{1, 2, 3, 4}).AnyTimes()

	streamKeys := h.streamReceiverMonitor.generateInboundStreamKeys()
	require.Equal(t, map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 2),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 3),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 4),
		}: {},
	}, streamKeys)
}

func TestGenerateOutboundStreamKeys_1To4(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	h.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()

	streamKeys := h.streamReceiverMonitor.generateOutboundStreamKeys()
	require.Equal(t, map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 2),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 3),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 4),
		}: {},
	}, streamKeys)
}

func TestGenerateOutboundStreamKeys_4To1(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
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
			ShardCount:             1,
		},
	}).AnyTimes()
	h.shardController.EXPECT().ShardIDs().Return([]int32{1, 2, 3, 4}).AnyTimes()

	streamKeys := h.streamReceiverMonitor.generateOutboundStreamKeys()
	require.Equal(t, map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 2),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 3),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 4),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
	}, streamKeys)
}

func TestDoReconcileInboundStreams_Add(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())

	h.streamReceiverMonitor.Lock()
	require.Equal(t, 0, len(h.streamReceiverMonitor.inboundStreams))
	h.streamReceiverMonitor.Unlock()

	streamKeys := map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	}
	streamSender := NewMockStreamSender(h.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSender.EXPECT().IsValid().Return(true)
	h.streamReceiverMonitor.RegisterInboundStream(streamSender)
	h.streamReceiverMonitor.doReconcileInboundStreams(streamKeys)

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	require.Equal(t, 1, len(h.streamReceiverMonitor.inboundStreams))
	stream, ok := h.streamReceiverMonitor.inboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	require.True(t, ok)
	require.Equal(t, streamSender, stream)
}

func TestDoReconcileInboundStreams_Remove(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSender := NewMockStreamSender(h.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSender.EXPECT().IsValid().Return(false)
	streamSender.EXPECT().Stop()
	h.streamReceiverMonitor.RegisterInboundStream(streamSender)

	h.streamReceiverMonitor.Lock()
	require.Equal(t, 1, len(h.streamReceiverMonitor.inboundStreams))
	h.streamReceiverMonitor.Unlock()

	h.streamReceiverMonitor.doReconcileInboundStreams(map[ClusterShardKeyPair]struct{}{})

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	require.Equal(t, 0, len(h.streamReceiverMonitor.inboundStreams))
}

func TestDoReconcileInboundStreams_Reactivate(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSenderStale := NewMockStreamSender(h.controller)
	streamSenderStale.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSenderStale.EXPECT().Stop()
	h.streamReceiverMonitor.RegisterInboundStream(streamSenderStale)

	h.streamReceiverMonitor.Lock()
	require.Equal(t, 1, len(h.streamReceiverMonitor.inboundStreams))
	h.streamReceiverMonitor.Unlock()

	streamSenderValid := NewMockStreamSender(h.controller)
	streamSenderValid.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	h.streamReceiverMonitor.RegisterInboundStream(streamSenderValid)

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	require.Equal(t, 1, len(h.streamReceiverMonitor.inboundStreams))
	stream, ok := h.streamReceiverMonitor.inboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	require.True(t, ok)
	require.Equal(t, streamSenderValid, stream)
}

func TestDoReconcileOutboundStreams_Add(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	h.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some cluster name").AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())

	h.streamReceiverMonitor.Lock()
	require.Equal(t, 0, len(h.streamReceiverMonitor.outboundStreams))
	h.streamReceiverMonitor.Unlock()

	streamKeys := map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	}
	h.streamReceiverMonitor.doReconcileOutboundStreams(streamKeys)

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	require.Equal(t, 1, len(h.streamReceiverMonitor.outboundStreams))
	stream, ok := h.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	require.True(t, ok)
	require.True(t, stream.IsValid())
}

func TestDoReconcileOutboundStreams_Remove(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	streamReceiver := NewMockStreamReceiver(h.controller)
	streamReceiver.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamReceiver.EXPECT().IsValid().Return(true)
	streamReceiver.EXPECT().Stop()

	h.streamReceiverMonitor.Lock()
	require.Equal(t, 0, len(h.streamReceiverMonitor.outboundStreams))
	h.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}] = streamReceiver
	h.streamReceiverMonitor.Unlock()

	h.streamReceiverMonitor.doReconcileOutboundStreams(map[ClusterShardKeyPair]struct{}{})

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	require.Equal(t, 0, len(h.streamReceiverMonitor.outboundStreams))
}

func TestDoReconcileOutboundStreams_Reactivate(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	h.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	h.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some cluster name").AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	streamReceiverStale := NewMockStreamReceiver(h.controller)
	streamReceiverStale.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamReceiverStale.EXPECT().IsValid().Return(false)
	streamReceiverStale.EXPECT().Stop()

	h.streamReceiverMonitor.Lock()
	require.Equal(t, 0, len(h.streamReceiverMonitor.outboundStreams))
	h.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}] = streamReceiverStale
	h.streamReceiverMonitor.Unlock()

	h.streamReceiverMonitor.doReconcileOutboundStreams(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	})

	h.streamReceiverMonitor.Lock()
	defer h.streamReceiverMonitor.Unlock()
	require.Equal(t, 1, len(h.streamReceiverMonitor.outboundStreams))
	stream, ok := h.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	require.True(t, ok)
	require.True(t, stream.IsValid())
}

func TestGenerateStatusMap_Success(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	inboundKeys := make(map[ClusterShardKeyPair]struct{})
	key1 := ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}
	key2 := ClusterShardKeyPair{
		Client: NewClusterShardKey(3, 1),
		Server: NewClusterShardKey(1, 1),
	}
	key3 := ClusterShardKeyPair{
		Client: NewClusterShardKey(4, 2),
		Server: NewClusterShardKey(1, 2),
	}
	inboundKeys[key1] = struct{}{}
	inboundKeys[key2] = struct{}{}
	inboundKeys[key3] = struct{}{}
	ctx1 := historyi.NewMockShardContext(h.controller)
	ctx2 := historyi.NewMockShardContext(h.controller)
	engine1 := historyi.NewMockEngine(h.controller)
	engine2 := historyi.NewMockEngine(h.controller)
	engine1.EXPECT().GetMaxReplicationTaskInfo().Return(int64(1000), time.Now())
	engine2.EXPECT().GetMaxReplicationTaskInfo().Return(int64(2000), time.Now())
	readerId1 := shard.ReplicationReaderIDFromClusterShardID(int64(key1.Client.ClusterID), key1.Client.ShardID)
	ackLevel1 := int64(100)
	ackLevel2 := int64(200)
	ackLevel3 := int64(300)
	readerId2 := shard.ReplicationReaderIDFromClusterShardID(int64(key2.Client.ClusterID), key2.Client.ShardID)
	readerId3 := shard.ReplicationReaderIDFromClusterShardID(int64(key3.Client.ClusterID), key3.Client.ShardID)
	queueState1 := &persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerId1: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackLevel1),
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
			readerId2: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackLevel2),
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
	}
	queueState2 := &persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerId3: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackLevel3),
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
	}
	ctx1.EXPECT().GetQueueState(tasks.CategoryReplication).Return(queueState1, true)
	ctx2.EXPECT().GetQueueState(tasks.CategoryReplication).Return(queueState2, true)
	h.shardController.EXPECT().GetShardByID(int32(1)).Return(ctx1, nil)
	h.shardController.EXPECT().GetShardByID(int32(2)).Return(ctx2, nil)
	ctx1.EXPECT().GetEngine(gomock.Any()).Return(engine1, nil)
	ctx2.EXPECT().GetEngine(gomock.Any()).Return(engine2, nil)
	statusMap := h.streamReceiverMonitor.generateStatusMap(inboundKeys)
	require.Equal(t, map[ClusterShardKeyPair]*streamStatus{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(2, 1),
			Server: NewClusterShardKey(1, 1),
		}: {
			defaultAckLevel:      ackLevel1,
			maxReplicationTaskId: int64(1000),
			isTieredStackEnabled: false,
		},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(3, 1),
			Server: NewClusterShardKey(1, 1),
		}: {
			defaultAckLevel:      ackLevel2,
			maxReplicationTaskId: int64(1000),
			isTieredStackEnabled: false,
		},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(4, 2),
			Server: NewClusterShardKey(1, 2),
		}: {
			defaultAckLevel:      ackLevel3,
			maxReplicationTaskId: int64(2000),
			isTieredStackEnabled: false,
		},
	}, statusMap)
}

func TestEvaluateStreamStatus(t *testing.T) {
	h := setupStreamReceiverMonitorTest(t)
	defer h.tearDown()

	keyPair := &ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}

	require.True(t, h.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			defaultAckLevel:      100,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: false,
		},
	),
	)

	require.False(t, h.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: false,
		},
	),
	)

	require.False(t, h.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			highPriorityAckLevel: 100,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			highPriorityAckLevel: 50,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: true,
		},
	),
	)

	require.False(t, h.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			highPriorityAckLevel: 20,
			lowPriorityAckLevel:  50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			highPriorityAckLevel: 20,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: true,
		},
	),
	)

	require.True(t, h.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			highPriorityAckLevel: 51,
			lowPriorityAckLevel:  21,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			highPriorityAckLevel: 50,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: true,
		},
	),
	)
}
