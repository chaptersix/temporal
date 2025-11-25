package shard

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type controllerTestDeps struct {
	controller          *gomock.Controller
	mockResource        *resourcetest.Test
	mockHistoryEngine   *historyi.MockEngine
	mockClusterMetadata *cluster.MockMetadata
	mockServiceResolver *membership.MockServiceResolver

	hostInfo          membership.HostInfo
	otherHostInfo     membership.HostInfo
	mockShardManager  *persistence.MockShardManager
	mockEngineFactory *MockEngineFactory

	config               *configs.Config
	logger               log.Logger
	shardController      *ControllerImpl
	mockHostInfoProvider *membership.MockHostInfoProvider
	metricsTestHandler   *metricstest.Handler
}

func NewTestController(
	engineFactory *MockEngineFactory,
	config *configs.Config,
	resource *resourcetest.Test,
	hostInfoProvider *membership.MockHostInfoProvider,
	metricsTestHandler *metricstest.Handler,
) *ControllerImpl {
	contextFactory := ContextFactoryProvider(ContextFactoryParams{
		ArchivalMetadata:            resource.GetArchivalMetadata(),
		ClientBean:                  resource.GetClientBean(),
		ClusterMetadata:             resource.GetClusterMetadata(),
		Config:                      config,
		EngineFactory:               engineFactory,
		HistoryClient:               resource.GetHistoryClient(),
		HistoryServiceResolver:      resource.GetHistoryServiceResolver(),
		HostInfoProvider:            hostInfoProvider,
		Logger:                      resource.GetLogger(),
		MetricsHandler:              metricsTestHandler,
		NamespaceRegistry:           resource.GetNamespaceRegistry(),
		PayloadSerializer:           resource.GetPayloadSerializer(),
		PersistenceExecutionManager: resource.GetExecutionManager(),
		PersistenceShardManager:     resource.GetShardManager(),
		SaMapperProvider:            resource.GetSearchAttributesMapperProvider(),
		SaProvider:                  resource.GetSearchAttributesProvider(),
		ThrottledLogger:             resource.GetThrottledLogger(),
		TimeSource:                  resource.GetTimeSource(),
		TaskCategoryRegistry:        tasks.NewDefaultTaskCategoryRegistry(),
	})

	return ControllerProvider(
		config,
		resource.GetLogger(),
		resource.GetHistoryServiceResolver(),
		metricsTestHandler,
		resource.GetHostInfoProvider(),
		contextFactory,
	)
}

func setupControllerTest(t *testing.T) *controllerTestDeps {
	controller := gomock.NewController(t)
	mockResource := resourcetest.NewTest(controller, primitives.HistoryService)
	mockHistoryEngine := historyi.NewMockEngine(controller)
	mockEngineFactory := NewMockEngineFactory(controller)

	mockShardManager := mockResource.ShardMgr
	mockServiceResolver := mockResource.HistoryServiceResolver
	mockClusterMetadata := mockResource.ClusterMetadata
	hostInfo := mockResource.GetHostInfo()
	otherHostInfo := membership.NewHostInfoFromAddress("other")
	mockHostInfoProvider := mockResource.HostInfoProvider
	mockHostInfoProvider.EXPECT().HostInfo().Return(hostInfo).AnyTimes()

	logger := mockResource.Logger
	config := tests.NewDynamicConfig()
	metricsTestHandler, err := metricstest.NewHandler(log.NewNoopLogger(), metrics.ClientConfig{})
	require.NoError(t, err)

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()

	shardController := NewTestController(
		mockEngineFactory,
		config,
		mockResource,
		mockHostInfoProvider,
		metricsTestHandler,
	)

	t.Cleanup(func() {
		controller.Finish()
	})

	return &controllerTestDeps{
		controller:           controller,
		mockResource:         mockResource,
		mockHistoryEngine:    mockHistoryEngine,
		mockClusterMetadata:  mockClusterMetadata,
		mockServiceResolver:  mockServiceResolver,
		hostInfo:             hostInfo,
		otherHostInfo:        otherHostInfo,
		mockShardManager:     mockShardManager,
		mockEngineFactory:    mockEngineFactory,
		config:               config,
		logger:               logger,
		shardController:      shardController,
		mockHostInfoProvider: mockHostInfoProvider,
		metricsTestHandler:   metricsTestHandler,
	}
}

func (deps *controllerTestDeps) setupMocksForAcquireShard(
	shardID int32,
	mockEngine *historyi.MockEngine,
	currentRangeID, newRangeID int64,
	required bool,
) {

	queueStates := deps.queueStates()

	minTimes := 0
	if required {
		minTimes = 1
	}

	// deps.mockResource.ExecutionMgr.On("Close").Return()
	mockEngine.EXPECT().Start().MinTimes(minTimes)
	// notification step is done after engine is created, so may not be called when test finishes
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).MaxTimes(2)
	deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil).Times(2).MinTimes(minTimes)
	deps.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine).MinTimes(minTimes)
	deps.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  deps.hostInfo.Identity(),
				RangeId:                currentRangeID,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            queueStates,
			},
		}, nil).MinTimes(minTimes)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), updateShardRequestMatcher(persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  deps.hostInfo.Identity(),
			RangeId:                newRangeID,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueStates:            queueStates,
		},
		PreviousRangeID: currentRangeID,
	})).Return(nil).MinTimes(minTimes)
	deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: newRangeID,
	}).Return(nil).AnyTimes()
}

func (deps *controllerTestDeps) queueStates() map[int32]*persistencespb.QueueState {
	return map[int32]*persistencespb.QueueState{
		int32(tasks.CategoryTransfer.ID()): {
			ReaderStates: nil,
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: timestamppb.New(tasks.DefaultFireTime),
				TaskId:   rand.Int63(),
			},
		},
		int32(tasks.CategoryTimer.ID()): {
			ReaderStates: make(map[int64]*persistencespb.QueueReaderState),
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: timestamp.TimeNowPtrUtc(),
				TaskId:   rand.Int63(),
			},
		},
		int32(tasks.CategoryReplication.ID()): {
			ReaderStates: map[int64]*persistencespb.QueueReaderState{
				0: {
					Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamppb.New(tasks.DefaultFireTime),
									TaskId:   1000,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamppb.New(tasks.DefaultFireTime),
									TaskId:   2000,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes: &persistencespb.Predicate_UniversalPredicateAttributes{
									UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
								},
							},
						},
					},
				},
				1: nil,
			},
			ExclusiveReaderHighWatermark: nil,
		},
	}
}

func (deps *controllerTestDeps) readMetricsCounter(name string, nonSystemTags ...metrics.Tag) float64 {
	expectedSystemTags := []metrics.Tag{
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	}
	snapshot, err := deps.metricsTestHandler.Snapshot()
	require.NoError(deps.controller.T.(*testing.T), err)

	tags := append(nonSystemTags, expectedSystemTags...)
	value, err := snapshot.Counter(name+"_total", tags...)
	require.NoError(deps.controller.T.(*testing.T), err)
	return value
}

func (deps *controllerTestDeps) setupMockForReadiness(shardID int32, state *readinessMockState) {
	mockEngine := historyi.NewMockEngine(deps.controller)
	mockEngine.EXPECT().Start()
	mockEngine.EXPECT().Stop().AnyTimes()
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine)
	deps.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  deps.hostInfo.Identity(),
				RangeId:                5,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            deps.queueStates(),
			},
		}, nil)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), updateShardRequestMatcher(persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  deps.hostInfo.Identity(),
			RangeId:                6,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueStates:            deps.queueStates(),
		},
		PreviousRangeID: 5,
	})).Return(nil)

	// nolint:forbidigo // deliberately blocking
	deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).DoAndReturn(func(context.Context, *persistence.AssertShardOwnershipRequest) error {
		if delay, ok := state.assertDelay.Load(int(shardID)); ok {
			time.Sleep(delay.(time.Duration))
		}
		if err, ok := state.assertError.Load(int(shardID)); ok {
			return err.(error)
		}
		return nil
	}).MinTimes(1)
}

func TestAcquireShardSuccess(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(8)
	deps.config.NumberOfShards = numShards

	var myShards []int32
	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			mockEngine := historyi.NewMockEngine(deps.controller)
			historyEngines[shardID] = mockEngine
			deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(membership.NewHostInfoFromAddress(ownerHost), nil)
		}
	}

	deps.shardController.acquireShards(context.Background())
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, shardID := range myShards {
		shard, err := deps.shardController.GetShardByID(shardID)
		require.NoError(t, err)
		_, err = shard.GetEngine(ctx)
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 2, count)
}

func TestAcquireShardsConcurrently(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(10)
	deps.config.NumberOfShards = numShards
	deps.config.AcquireShardConcurrency = func() int {
		return 10
	}

	var myShards []int32
	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			mockEngine := historyi.NewMockEngine(deps.controller)
			historyEngines[shardID] = mockEngine
			deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(
				membership.NewHostInfoFromAddress(ownerHost), nil,
			)
		}
	}

	deps.shardController.acquireShards(context.Background())
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shardID := range myShards {
		shard, err := deps.shardController.GetShardByID(shardID)
		require.NoError(t, err)
		_, err = shard.GetEngine(ctx)
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 2, count)
}

func TestAcquireShardLookupFailure(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(2)
	deps.config.NumberOfShards = numShards
	for shardID := int32(1); shardID <= numShards; shardID++ {
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
	}

	deps.shardController.acquireShards(context.Background())
	for shardID := int32(1); shardID <= numShards; shardID++ {
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
		shard, err := deps.shardController.GetShardByID(shardID)
		require.Error(t, err)
		require.Nil(t, shard)
	}
}

func TestAcquireShardRenewSuccess(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(2)
	deps.config.NumberOfShards = numShards

	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(deps.controller)
		historyEngines[shardID] = mockEngine
		deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	deps.shardController.acquireShards(context.Background())

	for shardID := int32(1); shardID <= numShards; shardID++ {
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil)
	}
	deps.shardController.acquireShards(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		shard, err := deps.shardController.GetShardByID(shardID)
		require.NoError(t, err)
		require.NotNil(t, shard)
		engine, err := shard.GetEngine(ctx)
		require.NoError(t, err)
		require.NotNil(t, engine)
	}
}

func TestAcquireShardRenewLookupFailed(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(2)
	deps.config.NumberOfShards = numShards

	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(deps.controller)
		historyEngines[shardID] = mockEngine
		deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	deps.shardController.acquireShards(context.Background())

	for shardID := int32(1); shardID <= numShards; shardID++ {
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
	}
	deps.shardController.acquireShards(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		shard, err := deps.shardController.GetShardByID(shardID)
		require.NoError(t, err)
		require.NotNil(t, shard)
		engine, err := shard.GetEngine(ctx)
		require.NoError(t, err)
		require.NotNil(t, engine)
	}
}

func TestHistoryEngineClosed(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(4)
	deps.config.NumberOfShards = numShards
	deps.shardController = NewTestController(
		deps.mockEngineFactory,
		deps.config,
		deps.mockResource,
		deps.mockHostInfoProvider,
		deps.metricsTestHandler,
	)
	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(deps.controller)
		historyEngines[shardID] = mockEngine
		deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deps.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName,
		gomock.Any()).Return(nil).AnyTimes()
	deps.shardController.Start()
	deps.shardController.acquireShards(context.Background())

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := int32(1); shardID <= numShards; shardID++ {
					shard, err := deps.shardController.GetShardByID(shardID)
					require.NoError(t, err)
					require.NotNil(t, shard)
					engine, err := shard.GetEngine(ctx)
					require.NoError(t, err)
					require.NotNil(t, engine)
				}
			}
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	for shardID := int32(1); shardID <= 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return()
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.otherHostInfo, nil).AnyTimes()
		deps.shardController.CloseShardByID(shardID)
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := int32(3); shardID <= numShards; shardID++ {
					shard, err := deps.shardController.GetShardByID(shardID)
					require.NoError(t, err)
					require.NotNil(t, shard)
					engine, err := shard.GetEngine(ctx)
					require.NoError(t, err)
					require.NotNil(t, engine)
					time.Sleep(20 * time.Millisecond)
				}
			}
			workerWG.Done()
		}()
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := int32(1); shardID <= 2; shardID++ {
					_, err := deps.shardController.GetShardByID(shardID)
					if err != nil {
						deps.logger.Error("ShardLost", tag.Error(err))
						shardLost = true
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			require.True(t, shardLost)
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	deps.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()
	for shardID := int32(3); shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return()
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil).AnyTimes()
	}
	deps.shardController.Stop()
}

func TestShardControllerClosed(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(4)
	deps.config.NumberOfShards = numShards
	deps.shardController = NewTestController(
		deps.mockEngineFactory,
		deps.config,
		deps.mockResource,
		deps.mockHostInfoProvider,
		deps.metricsTestHandler,
	)

	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(deps.controller)
		historyEngines[shardID] = mockEngine
		deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	deps.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).Return(nil).AnyTimes()
	deps.shardController.Start()
	deps.shardController.acquireShards(context.Background())

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := int32(1); shardID <= numShards; shardID++ {
					_, err := deps.shardController.GetShardByID(shardID)
					if err != nil {
						deps.logger.Error("ShardLost", tag.Error(err))
						shardLost = true
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			require.True(t, shardLost)
			workerWG.Done()
		}()
	}

	deps.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop()
		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil).AnyTimes()
	}
	deps.shardController.Stop()
	workerWG.Wait()
}

func TestShardExplicitUnload(t *testing.T) {
	deps := setupControllerTest(t)

	deps.config.NumberOfShards = 1

	mockEngine := historyi.NewMockEngine(deps.controller)
	mockEngine.EXPECT().Stop().AnyTimes()
	deps.setupMocksForAcquireShard(1, mockEngine, 5, 6, false)

	shard, err := deps.shardController.getOrCreateShardContext(1)
	require.NoError(t, err)
	require.Equal(t, 1, len(deps.shardController.ShardIDs()))

	shard.UnloadForOwnershipLost()

	for tries := 0; tries < 100 && len(deps.shardController.ShardIDs()) != 0; tries++ {
		// removal from map happens asynchronously
		time.Sleep(1 * time.Millisecond)
	}
	require.Equal(t, 0, len(deps.shardController.ShardIDs()))
	require.False(t, shard.IsValid())
}

func TestShardExplicitUnloadCancelGetOrCreate(t *testing.T) {
	deps := setupControllerTest(t)

	deps.config.NumberOfShards = 1

	mockEngine := historyi.NewMockEngine(deps.controller)
	mockEngine.EXPECT().Stop().AnyTimes()

	shardID := int32(1)
	deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil)

	ready := make(chan struct{})
	wasCanceled := make(chan bool)
	// GetOrCreateShard blocks for 5s or until canceled
	deps.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).DoAndReturn(
		func(ctx context.Context, req *persistence.GetOrCreateShardRequest) (*persistence.GetOrCreateShardResponse, error) {
			ready <- struct{}{}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				wasCanceled <- false
				return nil, errors.New("timed out")
			case <-ctx.Done():
				wasCanceled <- true
				return nil, ctx.Err()
			}
		})

	// get shard, will start initializing in background
	shard, err := deps.shardController.getOrCreateShardContext(1)
	require.NoError(t, err)

	<-ready
	// now shard is blocked on GetOrCreateShard
	require.False(t, shard.(*ContextImpl).engineFuture.Ready())

	start := time.Now()
	shard.UnloadForOwnershipLost() // this cancels the context so GetOrCreateShard returns immediately
	require.True(t, <-wasCanceled)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestShardExplicitUnloadCancelAcquire(t *testing.T) {
	deps := setupControllerTest(t)

	deps.config.NumberOfShards = 1

	mockEngine := historyi.NewMockEngine(deps.controller)
	mockEngine.EXPECT().Stop().AnyTimes()

	shardID := int32(1)
	deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil)
	// return success from GetOrCreateShard
	deps.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  deps.hostInfo.Identity(),
				RangeId:                5,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            deps.queueStates(),
			},
		}, nil)

	// acquire lease (UpdateShard) blocks for 5s
	ready := make(chan struct{})
	wasCanceled := make(chan bool)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *persistence.UpdateShardRequest) error {
			ready <- struct{}{}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				wasCanceled <- false
				return errors.New("timed out")
			case <-ctx.Done():
				wasCanceled <- true
				return ctx.Err()
			}
		})

	// get shard, will start initializing in background
	shard, err := deps.shardController.getOrCreateShardContext(1)
	require.NoError(t, err)

	<-ready
	// now shard is blocked on UpdateShard
	require.False(t, shard.(*ContextImpl).engineFuture.Ready())

	start := time.Now()
	shard.UnloadForOwnershipLost() // this cancels the context so UpdateShard returns immediately
	require.True(t, <-wasCanceled)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

// Tests random concurrent sequence of shard load/acquire/unload to catch any race conditions
// that were not covered by specific tests.
func TestShardControllerFuzz(t *testing.T) {
	deps := setupControllerTest(t)

	deps.config.NumberOfShards = 10

	deps.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).Return(nil).AnyTimes()
	deps.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()

	// only for MockEngines: we just need to hook Start/Stop, not verify calls
	disconnectedMockController := gomock.NewController(nil)

	var engineStarts, engineStops atomic.Int64
	var getShards, closeContexts atomic.Int64

	for shardID := int32(1); shardID <= deps.config.NumberOfShards; shardID++ {
		shardID := shardID
		queueStates := deps.queueStates()

		deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil).AnyTimes()
		deps.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).DoAndReturn(func(shard historyi.ShardContext) historyi.Engine {
			mockEngine := historyi.NewMockEngine(disconnectedMockController)
			status := new(int32)
			// notification step is done after engine is created, so may not be called when test finishes
			mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).MaxTimes(2)
			mockEngine.EXPECT().Start().Do(func() {
				if !atomic.CompareAndSwapInt32(status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
					return
				}
				engineStarts.Add(1)
			}).AnyTimes()
			mockEngine.EXPECT().Stop().Do(func() {
				if !atomic.CompareAndSwapInt32(status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
					return
				}
				engineStops.Add(1)
			}).AnyTimes()
			return mockEngine
		}).AnyTimes()
		deps.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).DoAndReturn(
			func(ctx context.Context, req *persistence.GetOrCreateShardRequest) (*persistence.GetOrCreateShardResponse, error) {
				if ctx.Err() != nil {
					return nil, errors.New("already canceled")
				}
				// note that lifecycleCtx could be canceled right here
				getShards.Add(1)
				go func(lifecycleCtx context.Context) {
					<-lifecycleCtx.Done()
					closeContexts.Add(1)
				}(req.LifecycleContext)
				return &persistence.GetOrCreateShardResponse{
					ShardInfo: &persistencespb.ShardInfo{
						ShardId:                shardID,
						Owner:                  deps.hostInfo.Identity(),
						RangeId:                5,
						ReplicationDlqAckLevel: map[string]int64{},
						QueueStates:            queueStates,
					},
				}, nil
			}).AnyTimes()
		deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
			ShardID: shardID,
			RangeID: 6,
		}).Return(nil).AnyTimes()
	}

	randomLoadedShard := func() (int32, historyi.ShardContext) {
		deps.shardController.Lock()
		defer deps.shardController.Unlock()
		if len(deps.shardController.historyShards) == 0 {
			return -1, nil
		}
		n := rand.Intn(len(deps.shardController.historyShards))
		for id, shard := range deps.shardController.historyShards {
			if n == 0 {
				return id, shard
			}
			n--
		}
		return -1, nil
	}

	worker := func(ctx context.Context) error {
		for ctx.Err() == nil {
			shardID := int32(rand.Intn(int(deps.config.NumberOfShards))) + 1
			switch rand.Intn(5) {
			case 0:
				_, _ = deps.shardController.GetShardByID(shardID)
			case 1:
				if shard, err := deps.shardController.GetShardByID(shardID); err == nil {
					_, _ = shard.GetEngine(ctx)
				}
			case 2:
				if _, shard := randomLoadedShard(); shard != nil {
					shard.UnloadForOwnershipLost()
				}
			case 3:
				if id, _ := randomLoadedShard(); id >= 0 {
					deps.shardController.CloseShardByID(id)
				}
			case 4:
				time.Sleep(10 * time.Millisecond)
			}
		}
		return ctx.Err()
	}

	deps.shardController.Start()
	deps.shardController.acquireShards(context.Background())

	var workers goro.Group
	for i := 0; i < 10; i++ {
		workers.Go(worker)
	}

	time.Sleep(3 * time.Second)

	workers.Cancel()
	workers.Wait()
	deps.shardController.Stop()

	// check that things are good
	// wait for number of GetOrCreateShard calls to stabilize across 100ms, since there could
	// be some straggler acquireShard goroutines that call it even after shardController.Stop
	// (which will cancel all lifecycleCtxs).
	var prevGetShards int64
	require.Eventually(t, func() bool {
		thisGetShards := getShards.Load()
		ok := thisGetShards == prevGetShards && thisGetShards == closeContexts.Load()
		prevGetShards = thisGetShards
		return ok
	}, 1*time.Second, 100*time.Millisecond, "all contexts did not close")
	require.Eventually(t, func() bool {
		return engineStarts.Load() == engineStops.Load()
	}, 1*time.Second, 100*time.Millisecond, "engine start/stop")
}

func Test_GetOrCreateShard_InvalidShardID(t *testing.T) {
	deps := setupControllerTest(t)

	numShards := int32(2)
	deps.config.NumberOfShards = numShards

	_, err := deps.shardController.getOrCreateShardContext(0)
	require.ErrorIs(t, err, invalidShardIdLowerBound)

	_, err = deps.shardController.getOrCreateShardContext(3)
	require.ErrorIs(t, err, invalidShardIdUpperBound)
}

func TestShardLingerTimeout(t *testing.T) {
	deps := setupControllerTest(t)

	shardID := int32(1)
	deps.config.NumberOfShards = 1
	timeLimit := 1 * time.Second
	deps.config.ShardLingerTimeLimit = dynamicconfig.GetDurationPropertyFn(timeLimit)

	historyEngines := make(map[int32]*historyi.MockEngine)
	mockEngine := historyi.NewMockEngine(deps.controller)
	historyEngines[shardID] = mockEngine
	deps.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)

	deps.shardController.acquireShards(context.Background())

	require.Len(t, deps.shardController.ShardIDs(), 1)
	shard, err := deps.shardController.getOrCreateShardContext(shardID)
	require.NoError(t, err)

	deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress("newhost"), nil)

	mockEngine.EXPECT().Stop().Return()

	deps.shardController.acquireShards(context.Background())
	// Wait for total of timeout plus 100ms of test fudge factor.

	// Should still have a valid shard before the timeout.
	time.Sleep(timeLimit / 2)
	require.True(t, shard.IsValid())
	require.Len(t, deps.shardController.ShardIDs(), 1)

	// By now the timeout should have occurred.
	time.Sleep(timeLimit/2 + 100*time.Millisecond)
	require.Len(t, deps.shardController.ShardIDs(), 0)
	require.False(t, shard.IsValid())

	require.Equal(t, float64(1), deps.readMetricsCounter(
		metrics.ShardLingerTimeouts.Name(),
		metrics.OperationTag(metrics.HistoryShardControllerScope)))
}

func TestShardLingerSuccess(t *testing.T) {
	deps := setupControllerTest(t)

	shardID := int32(1)
	deps.config.NumberOfShards = 1
	timeLimit := 1 * time.Second
	deps.config.ShardLingerTimeLimit = dynamicconfig.GetDurationPropertyFn(timeLimit)

	checkQPS := 5
	deps.config.ShardLingerOwnershipCheckQPS = dynamicconfig.GetIntPropertyFn(checkQPS)

	historyEngines := make(map[int32]*historyi.MockEngine)
	mockEngine := historyi.NewMockEngine(deps.controller)
	historyEngines[shardID] = mockEngine

	mockEngine.EXPECT().Start().MinTimes(1)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).MaxTimes(2)
	deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(deps.hostInfo, nil).Times(2).MinTimes(1)
	deps.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine).MinTimes(1)
	deps.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  deps.hostInfo.Identity(),
				RangeId:                5,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            deps.queueStates(),
			},
		}, nil).MinTimes(1)
	deps.mockShardManager.EXPECT().UpdateShard(gomock.Any(), updateShardRequestMatcher(persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  deps.hostInfo.Identity(),
			RangeId:                6,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueStates:            deps.queueStates(),
		},
		PreviousRangeID: 5,
	})).Return(nil).MinTimes(1)
	deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).Return(nil).Times(1)

	deps.shardController.acquireShards(context.Background())
	require.Len(t, deps.shardController.ShardIDs(), 1)
	shard, err := deps.shardController.getOrCreateShardContext(shardID)
	require.NoError(t, err)

	deps.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress("newhost"), nil)

	mockEngine.EXPECT().Stop().Return().MinTimes(1)

	// We mock 2 AssertShardOwnership calls in shardLingerThenClose.
	// The second one finds that the shard is no longer owned by the host, and unloads it.
	deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).Return(nil).Times(1)
	deps.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).DoAndReturn(func(_ context.Context, _ *persistence.AssertShardOwnershipRequest) error {
		shard.UnloadForOwnershipLost()
		return nil
	}).Times(1)

	deps.shardController.acquireShards(context.Background())

	// Wait for two checks plus 100ms of test fudge factor.
	expectedWait := time.Second / time.Duration(checkQPS) * 2
	time.Sleep(expectedWait + 100*time.Millisecond)

	require.Len(t, deps.shardController.ShardIDs(), 0)
}

// TestShardCounter verifies that we can subscribe to shard count updates, receive them when shards are acquired, and
// unsubscribe from the updates when needed.
func TestShardCounter(t *testing.T) {
	deps := setupControllerTest(t)

	const totalShards = 5
	deps.config.NumberOfShards = totalShards

	var ownedShards atomic.Int32
	deps.mockServiceResolver.EXPECT().Lookup(gomock.Any()).DoAndReturn(func(key string) (membership.HostInfo, error) {
		if i, err := strconv.Atoi(key); err != nil {
			return nil, err
		} else if i <= int(ownedShards.Load()) {
			return deps.hostInfo, nil
		}
		return deps.otherHostInfo, nil
	}).AnyTimes()

	mockEngine := historyi.NewMockEngine(deps.controller)
	for i := range totalShards {
		deps.setupMocksForAcquireShard(int32(i+1), mockEngine, 5, 6, false)
	}

	// subscribe to shard count updates
	sub1 := deps.shardController.SubscribeShardCount()

	// validate that we get the initial shard count
	require.Empty(t, sub1.ShardCount(), "Should not publish shard count before acquiring shards")
	ownedShards.Store(2)
	deps.shardController.acquireShards(context.Background())
	require.Equal(t, 2, <-sub1.ShardCount(), "Should publish shard count after acquiring shards")
	require.Empty(t, sub1.ShardCount(), "Shard count channel should be drained")

	// acquire shards twice to validate that this does not block even if there's no capacity left on the channel
	ownedShards.Store(3)
	deps.shardController.acquireShards(context.Background())
	ownedShards.Store(4)
	deps.shardController.acquireShards(context.Background())
	require.Equal(t, 3, <-sub1.ShardCount(), "Shard count is buffered, so we should only get the first value")
	require.Empty(t, sub1.ShardCount(), "Shard count channel should be drained")

	// unsubscribe and validate that the channel is closed, but the other subscriber is still receiving updates
	sub2 := deps.shardController.SubscribeShardCount()
	sub1.Unsubscribe()
	ownedShards.Store(4)
	deps.shardController.acquireShards(context.Background())
	_, ok := <-sub1.ShardCount()
	require.False(t, ok, "Channel should be closed because sub1 is canceled")
	sub1.Unsubscribe() // should not panic if called twice
	require.Equal(t, 4, <-sub2.ShardCount(), "Should receive shard count updates on sub2 even if sub1 is canceled")
	sub2.Unsubscribe()
}

type readinessMockState struct {
	ownership   sync.Map
	assertError sync.Map
	assertDelay sync.Map
}

func setupMocksForReadiness(deps *controllerTestDeps) *readinessMockState {
	state := &readinessMockState{}
	state.ownership.Store(1, true)
	state.ownership.Store(3, true)
	state.ownership.Store(5, true)

	deps.config.NumberOfShards = 5

	deps.mockServiceResolver.EXPECT().Lookup(gomock.Any()).DoAndReturn(func(key string) (membership.HostInfo, error) {
		if i, err := strconv.Atoi(key); err != nil {
			return nil, err
		} else if owned, ok := state.ownership.Load(i); ok && owned.(bool) {
			return deps.hostInfo, nil
		}
		return deps.otherHostInfo, nil
	}).AnyTimes()

	state.ownership.Range(func(shardID, owned any) bool {
		if owned.(bool) {
			deps.setupMockForReadiness(int32(shardID.(int)), state)
		}
		return true
	})

	return state
}

func TestReadiness_Ready(t *testing.T) {
	deps := setupControllerTest(t)
	_ = setupMocksForReadiness(deps)

	// not ready yet
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, deps.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// acquire
	deps.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, deps.shardController.InitialShardsAcquired(ctx))
}

func TestReadiness_Error(t *testing.T) {
	deps := setupControllerTest(t)
	state := setupMocksForReadiness(deps)

	// use an error that will not cause controller to re-acquire
	state.assertError.Store(3, serviceerror.NewResourceExhausted(0, ""))

	// acquire
	deps.shardController.acquireShards(context.Background())

	// shard 3 failed in AssertShardOwnership even though it passed UpdateShard, should be not ready yet
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, deps.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// fix error and try again
	state.assertError.Delete(3)
	deps.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, deps.shardController.InitialShardsAcquired(ctx))
}

func TestReadiness_Blocked(t *testing.T) {
	deps := setupControllerTest(t)
	deps.config.ShardIOConcurrency = dynamicconfig.GetIntPropertyFn(10) // allow second assert to run while first is blocked
	state := setupMocksForReadiness(deps)

	state.assertDelay.Store(3, time.Hour)

	// acquire
	deps.shardController.acquireShards(context.Background())

	// shard 3 is blocked in AssertShardOwnership
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, deps.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// acquire again (e.g. membership changed)
	state.assertDelay.Delete(3)
	deps.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, deps.shardController.InitialShardsAcquired(ctx))
}

func TestReadiness_MembershipChanged(t *testing.T) {
	deps := setupControllerTest(t)
	state := setupMocksForReadiness(deps)

	state.assertDelay.Store(3, time.Hour)

	// acquire
	deps.shardController.acquireShards(context.Background())

	// shard 3 is blocked in AssertShardOwnership
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, deps.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// change membership, now we own 2, 4, and 5, we don't care about 3 anymore
	state.ownership.Clear()
	state.ownership.Store(2, true)
	state.ownership.Store(4, true)
	state.ownership.Store(5, true)
	deps.setupMockForReadiness(2, state)
	deps.setupMockForReadiness(4, state)

	// acquire again
	deps.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, deps.shardController.InitialShardsAcquired(ctx))
}

// This is needed to avoid race conditions when using this matcher, since
// fmt.Sprintf("%v"), used by gomock, would otherwise access private fields.
// See https://github.com/temporalio/temporal/issues/2777
var _ fmt.Stringer = (*ContextImpl)(nil)

type contextMatcher int32

func (s contextMatcher) Matches(x interface{}) bool {
	shardContext, ok := x.(historyi.ShardContext)
	return ok && shardContext.GetShardID() == int32(s)
}

func (s contextMatcher) String() string {
	return strconv.Itoa(int(s))
}

type getOrCreateShardRequestMatcher int32

func (s getOrCreateShardRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*persistence.GetOrCreateShardRequest)
	return ok && req.ShardID == int32(s)
}

func (s getOrCreateShardRequestMatcher) String() string {
	return strconv.Itoa(int(s))
}

type updateShardRequestMatcher persistence.UpdateShardRequest

func (m updateShardRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*persistence.UpdateShardRequest)
	if !ok {
		return false
	}

	// only compare the essential vars,
	// other vars like queue state / queue ack level should not be test in this util
	return m.PreviousRangeID == req.PreviousRangeID &&
		m.ShardInfo.ShardId == req.ShardInfo.ShardId &&
		strings.Contains(req.ShardInfo.Owner, m.ShardInfo.Owner) &&
		m.ShardInfo.RangeId == req.ShardInfo.RangeId &&
		m.ShardInfo.StolenSinceRenew == req.ShardInfo.StolenSinceRenew
}

func (m updateShardRequestMatcher) String() string {
	return fmt.Sprintf("%+v", (persistence.UpdateShardRequest)(m))
}
