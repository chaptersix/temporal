package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type workflowCacheTestDeps struct {
	controller *gomock.Controller
	mockShard  *shard.ContextTest
	cache      Cache
}

func setupWorkflowCacheTest(t *testing.T) *workflowCacheTestDeps {
	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &workflowCacheTestDeps{
		controller: controller,
		mockShard:  mockShard,
	}
}

func TestHistoryCacheBasic(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := historyi.NewMockMutableState(deps.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	release(nil)
	ctx, release, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)

	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	ctx, release, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.NotEqual(t, mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func TestHistoryCachePanic(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := historyi.NewMockMutableState(deps.controller)
	mockMS1.EXPECT().IsDirty().Return(true).AnyTimes()
	mockMS1.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry()).AnyTimes()
	mockMS1.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask().AnyTimes()
	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1

	defer func() {
		if recover() != nil {
			ctx, release, err = deps.cache.GetOrCreateWorkflowExecution(
				context.Background(),
				deps.mockShard,
				namespaceID,
				&execution1,
				locks.PriorityHigh,
			)
			require.NoError(t, err)
			require.Nil(t, ctx.(*workflow.ContextImpl).MutableState)
			release(nil)
		} else {
			require.Fail(t, "test should panic")
		}
	}()
	release(nil)
}

func TestHistoryCachePinning(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(1)
	namespaceID := namespace.ID("test_namespace_id")
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	require.NoError(t, err)

	we2 := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	// Cache is full because context is pinned, should get an error now
	_, _, err2 := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we2,
		locks.PriorityHigh,
	)
	require.Error(t, err2)

	// Now release the context, this should unpin it.
	release(err2)

	_, release2, err3 := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we2,
		locks.PriorityHigh,
	)
	require.NoError(t, err3)
	release2(err3)

	// Old context should be evicted.
	newContext, release, err4 := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	require.NoError(t, err4)
	require.False(t, ctx == newContext)
	release(err4)
}

func TestHistoryCacheClear(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(20)
	namespaceID := namespace.ID("test_namespace_id")
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-clear",
		RunId:      uuid.New(),
	}

	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	mock := historyi.NewMockMutableState(deps.controller)
	mock.EXPECT().IsDirty().Return(false).AnyTimes()
	mock.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask().AnyTimes()
	ctx.(*workflow.ContextImpl).MutableState = mock

	release(nil)

	// since last time, the release function receive a nil error
	// the ms will not be cleared
	ctx, release, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	require.NoError(t, err)

	require.NotNil(t, ctx.(*workflow.ContextImpl).MutableState)
	mock.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
	release(errors.New("some random error message"))

	// since last time, the release function receive a non-nil error
	// the ms will be cleared
	ctx, release, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Nil(t, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func TestHistoryCacheConcurrentAccess_Release(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	cacheMaxSize := 16
	coroutineCount := 50

	deps.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	startGroup := &sync.WaitGroup{}
	stopGroup := &sync.WaitGroup{}
	startGroup.Add(coroutineCount)
	stopGroup.Add(coroutineCount)

	namespaceID := namespace.ID("test_namespace_id")
	workflowId := "wf-cache-test-pinning"
	runID := uuid.New()

	testFn := func() {
		defer stopGroup.Done()
		startGroup.Done()

		startGroup.Wait()
		ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
			context.Background(),
			deps.mockShard,
			namespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runID,
			},
			locks.PriorityHigh,
		)
		require.NoError(t, err)
		// since each time the is reset to nil
		require.Nil(t, ctx.(*workflow.ContextImpl).MutableState)
		// since we are just testing whether the release function will clear the cache
		// all we need is a fake MutableState
		mock := historyi.NewMockMutableState(deps.controller)
		mock.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
		mock.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask()
		ctx.(*workflow.ContextImpl).MutableState = mock
		release(errors.New("some random error message"))
	}

	for i := 0; i < coroutineCount; i++ {
		go testFn()
	}
	stopGroup.Wait()

	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runID,
		},
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	require.Nil(t, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func TestHistoryCache_CacheLatencyMetricContext(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	ctx := metrics.AddMetricsContext(context.Background())
	currentRelease, err := deps.cache.GetOrCreateCurrentWorkflowExecution(
		ctx,
		deps.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	defer currentRelease(nil)

	latency1, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name())
	require.True(t, ok)
	require.NotZero(t, latency1)

	_, release, err := deps.cache.GetOrCreateWorkflowExecution(
		ctx,
		deps.mockShard,
		tests.NamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	defer release(nil)

	latency2, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name())
	require.True(t, ok)
	require.Greater(t, latency2, latency1)
}

func TestHistoryCache_CacheHoldTimeMetricContext(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()

	deps.mockShard.SetMetricsHandler(metricsHandler)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metricsHandler)

	release1, err := deps.cache.GetOrCreateCurrentWorkflowExecution(
		context.Background(),
		deps.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		release1(nil)
		snapshot := capture.Snapshot()
		require.Greater(t, snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Value, 100*time.Millisecond)
		return tests.NamespaceID.String() == snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Tags["namespace_id"]
	}, 150*time.Millisecond, 100*time.Millisecond)

	capture = metricsHandler.StartCapture()
	release2, err := deps.cache.GetOrCreateCurrentWorkflowExecution(
		context.Background(),
		deps.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		release2(nil)
		snapshot := capture.Snapshot()
		require.Greater(t, snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Value, 200*time.Millisecond)
		return tests.NamespaceID.String() == snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Tags["namespace_id"]
	}, 300*time.Millisecond, 200*time.Millisecond)
}

func TestCacheImpl_lockWorkflowExecution(t *testing.T) {
	testSets := []struct {
		name             string
		shouldLockBefore bool
		callerType       string
		withTimeout      bool
		wantErr          bool
	}{

		{
			name:       "API context without timeout without locking beforehand should not return an error",
			callerType: headers.CallerTypeAPI,
		},
		{
			name:             "API context without timeout with locking beforehand should not return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeAPI,
			wantErr:          true,
		},

		{
			name:       "API context with timeout without locking beforehand should not return an error",
			callerType: headers.CallerTypeAPI,
		},
		{
			name:             "API context with timeout and locking beforehand should return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeAPI,
			wantErr:          true,
		},
		{
			name:       "Non API context with timeout without locking beforehand should return an error",
			callerType: headers.CallerTypeBackgroundHigh,
		},
		{
			name:             "Non API context with timeout and locking beforehand should return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeBackgroundHigh,
			wantErr:          true,
		},
	}
	for _, tt := range testSets {
		t.Run(tt.name, func(t *testing.T) {
			deps := setupWorkflowCacheTest(t)
			c := NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

			namespaceID := namespace.ID("test_namespace_id")
			execution := commonpb.WorkflowExecution{
				WorkflowId: "some random workflow id",
				RunId:      uuid.New(),
			}
			cacheKey := Key{
				WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
				ShardUUID:   uuid.New(),
			}
			workflowCtx := workflow.NewContext(deps.mockShard.GetConfig(), cacheKey.WorkflowKey, deps.mockShard.GetLogger(), deps.mockShard.GetThrottledLogger(), deps.mockShard.GetMetricsHandler())
			ctx := headers.SetCallerType(context.Background(), tt.callerType)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if tt.shouldLockBefore {
				// lock the workflow to allow it to time out
				err := workflowCtx.Lock(ctx, locks.PriorityHigh)
				require.NoError(t, err)
			}

			if err := c.(*cacheImpl).lockWorkflowExecution(ctx, workflowCtx, cacheKey, locks.PriorityHigh); (err != nil) != tt.wantErr {
				t.Errorf("cacheImpl.lockWorkflowExecution() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCacheImpl_RejectsRequestWhenAtLimitSimple(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	config := tests.NewDynamicConfig()
	config.HistoryCacheLimitSizeBased = true
	config.HistoryHostLevelCacheMaxSizeBytes = dynamicconfig.GetIntPropertyFn(1000)
	mockShard := shard.NewTestContext(
		deps.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	deps.cache = NewHostLevelCache(config, deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := historyi.NewMockMutableState(deps.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release1, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	// MockMS1 should fill the entire cache. The total size of the context object will be the size of MutableState
	// plus the size of commonpb.WorkflowExecution in this case. Even though we are returning a size 900 from
	// MutableState, the size of workflow.Context object in the cache will be slightly higher (~972bytes).
	mockMS1.EXPECT().GetApproximatePersistedSize().Return(900).Times(1)
	release1(nil)
	ctx, _, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mockMS1, ctx.(*workflow.ContextImpl).MutableState)

	// Try to insert another entry before releasing previous.
	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	_, _, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, cache.ErrCacheFull)
}

func TestCacheImpl_RejectsRequestWhenAtLimitMultiple(t *testing.T) {
	// This test does the following;
	//   1. Try inserting 3 entries of size 400bytes. Last insert should fail as max size is 1000 bytes.
	//   2. Make the size of second entry 1000 and release it. This should make the cache size > max limit.
	//      Cache should evict this entry to maintain its size under limit.
	//   3. Insert another entry of size 400 bytes successfully.
	deps := setupWorkflowCacheTest(t)
	config := tests.NewDynamicConfig()
	config.HistoryCacheLimitSizeBased = true
	config.HistoryHostLevelCacheMaxSizeBytes = dynamicconfig.GetIntPropertyFn(1000)
	mockShard := shard.NewTestContext(
		deps.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	deps.cache = NewHostLevelCache(config, deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := historyi.NewMockMutableState(deps.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()

	ctx, release1, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1

	// Make mockMS1's size 400.
	mockMS1.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release1(nil)
	ctx, release1, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mockMS1, ctx.(*workflow.ContextImpl).MutableState)

	// Insert another 400byte entry.
	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS2 := historyi.NewMockMutableState(deps.controller)
	mockMS2.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release2, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS2
	mockMS2.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release2(nil)
	ctx, release2, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mockMS2, ctx.(*workflow.ContextImpl).MutableState)

	// Insert another entry. This should fail as cache has ~800bytes pinned.
	execution3 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS3 := historyi.NewMockMutableState(deps.controller)
	mockMS3.EXPECT().IsDirty().Return(false).AnyTimes()
	_, _, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		locks.PriorityHigh,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, cache.ErrCacheFull)

	// Now there are two entries pinned in the cache. Their total size is 800bytes.
	// Make mockMS1 grow to 1000 bytes. Cache should be able to handle this. Now the cache size will be more than its
	// limit. Cache will evict this entry and make more space.
	mockMS1.EXPECT().GetApproximatePersistedSize().Return(1000).Times(1)
	release1(nil)
	ctx, release1, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	// Make sure execution 3 was evicted by checking if mutable state is nil.
	require.Nil(t, ctx.(*workflow.ContextImpl).MutableState, nil)
	release1(nil)

	// Insert execution3 again with size 400bytes.
	ctx, release3, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS3

	mockMS3.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release3(nil)
	ctx, release3, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mockMS3, ctx.(*workflow.ContextImpl).MutableState)

	// Release all remaining entries.
	mockMS2.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release2(nil)
	mockMS3.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release3(nil)
}

func TestCacheImpl_CheckCacheLimitSizeBasedFlag(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	config := tests.NewDynamicConfig()
	// HistoryCacheLimitSizeBased is set to false. Cache limit should be based on entry count.
	config.HistoryCacheLimitSizeBased = false
	config.HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(1)
	mockShard := shard.NewTestContext(
		deps.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	deps.cache = NewHostLevelCache(config, deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := historyi.NewMockMutableState(deps.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release1, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	// GetApproximatePersistedSize() should not be called, since we disabled HistoryHostLevelCacheMaxSize flag.
	mockMS1.EXPECT().GetApproximatePersistedSize().Times(0)
	release1(nil)
	ctx, release1, err = deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	require.NoError(t, err)
	require.Equal(t, mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release1(nil)
}

func TestCacheImpl_GetCurrentRunID_CurrentRunExists(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      "",
	}

	currentRunID := uuid.New()

	mockExecutionManager := deps.mockShard.Resource.ExecutionMgr
	mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.New(),
		RunID:          currentRunID,
		State:          enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil).Times(1)

	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&execution,
		locks.PriorityHigh,
	)
	require.NoError(t, err)

	require.Equal(t, currentRunID, ctx.GetWorkflowKey().RunID)
	release(nil)
}

func TestCacheImpl_GetCurrentRunID_NoCurrentRun(t *testing.T) {
	deps := setupWorkflowCacheTest(t)
	deps.cache = NewHostLevelCache(deps.mockShard.GetConfig(), deps.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      "",
	}

	mockExecutionManager := deps.mockShard.Resource.ExecutionMgr
	mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
	}).Return(nil, serviceerror.NewNotFound("current worflow not found")).Times(1)

	ctx, release, err := deps.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		deps.mockShard,
		namespaceID,
		&execution,
		locks.PriorityHigh,
	)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Nil(t, ctx)
	require.Nil(t, release)
}
