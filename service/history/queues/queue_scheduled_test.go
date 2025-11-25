package queues

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func setupScheduledQueue(t *testing.T) (*gomock.Controller, *shard.ContextTest, *persistence.MockExecutionManager, *scheduledQueue) {
	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)
	mockExecutionManager := mockShard.Resource.ExecutionMgr
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	rateLimiter, _ := NewPrioritySchedulerRateLimiter(
		func(namespace string) float64 {
			return float64(mockShard.GetConfig().TaskSchedulerNamespaceMaxQPS(namespace))
		},
		func() float64 {
			return float64(mockShard.GetConfig().TaskSchedulerMaxQPS())
		},
		func(namespace string) float64 {
			return float64(mockShard.GetConfig().PersistenceNamespaceMaxQPS(namespace))
		},
		func() float64 {
			return float64(mockShard.GetConfig().PersistenceMaxQPS())
		},
	)

	logger := log.NewTestLogger()

	scheduler := NewScheduler(
		mockShard.Resource.ClusterMetadata.GetCurrentClusterName(),
		SchedulerOptions{
			WorkerCount:             mockShard.GetConfig().TimerProcessorSchedulerWorkerCount,
			ActiveNamespaceWeights:  mockShard.GetConfig().TimerProcessorSchedulerActiveRoundRobinWeights,
			StandbyNamespaceWeights: mockShard.GetConfig().TimerProcessorSchedulerStandbyRoundRobinWeights,
		},
		mockShard.GetNamespaceRegistry(),
		logger,
	)
	scheduler = NewRateLimitedScheduler(
		scheduler,
		RateLimitedSchedulerOptions{
			EnableShadowMode: mockShard.GetConfig().TaskSchedulerEnableRateLimiterShadowMode,
			StartupDelay:     mockShard.GetConfig().TaskSchedulerRateLimiterStartupDelay,
		},
		mockShard.Resource.ClusterMetadata.GetCurrentClusterName(),
		mockShard.GetNamespaceRegistry(),
		rateLimiter,
		mockShard.GetTimeSource(),
		mockShard.ChasmRegistry(),
		logger,
		metrics.NoopMetricsHandler,
	)

	rescheduler := NewRescheduler(
		scheduler,
		mockShard.GetTimeSource(),
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	factory := NewExecutableFactory(nil,
		scheduler,
		rescheduler,
		nil,
		mockShard.GetTimeSource(),
		mockShard.GetNamespaceRegistry(),
		mockShard.GetClusterMetadata(),
		mockShard.ChasmRegistry(),
		GetTaskTypeTagValue,
		logger,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
		nil,
		func() bool {
			return false
		},
		func() int {
			return math.MaxInt
		},
		func() bool {
			return false
		},
		func() string {
			return ""
		},
	)
	scheduledQueue := NewScheduledQueue(
		mockShard,
		tasks.CategoryTimer,
		scheduler,
		rescheduler,
		factory,
		&testQueueOptions,
		NewReaderPriorityRateLimiter(
			func() float64 { return 10 },
			1,
		),
		logger,
		metrics.NoopMetricsHandler,
	)

	return controller, mockShard, mockExecutionManager, scheduledQueue
}

func TestScheduledQueue_PaginationFnProvider_Success(t *testing.T) {
	t.Parallel()

	controller, mockShard, mockExecutionManager, scheduledQueue := setupScheduledQueue(t)
	defer controller.Finish()

	paginationFnProvider := scheduledQueue.paginationFnProvider

	r := NewRandomRange()

	testTaskKeys := []tasks.Key{
		tasks.NewKey(r.InclusiveMin.FireTime.Add(-time.Second), rand.Int63()),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(-time.Microsecond*10), rand.Int63()),
		tasks.NewKey(r.InclusiveMin.FireTime, r.ExclusiveMax.TaskID),
		tasks.NewKey(r.InclusiveMin.FireTime, r.ExclusiveMax.TaskID-1),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(time.Second), rand.Int63()),
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID),
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(time.Microsecond*10), rand.Int63()),
		tasks.NewKey(r.InclusiveMin.FireTime.Add(time.Second), rand.Int63()),
	}
	slices.SortFunc(testTaskKeys, func(k1, k2 tasks.Key) int {
		return k1.CompareTo(k2)
	})
	shouldHaveNextPage := true
	if testTaskKeys[len(testTaskKeys)-1].CompareTo(r.ExclusiveMax) >= 0 {
		shouldHaveNextPage = false
	}

	expectedNumTasks := 0
	mockTasks := make([]tasks.Task, 0, len(testTaskKeys))
	for _, key := range testTaskKeys {
		mockTask := tasks.NewMockTask(controller)
		mockTask.EXPECT().GetKey().Return(key).AnyTimes()
		mockTasks = append(mockTasks, mockTask)

		if r.ContainsKey(key) {
			expectedNumTasks++
		}
	}

	currentPageToken := []byte{1, 2, 3}
	nextPageToken := []byte{4, 5, 6}

	mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(r.InclusiveMin.FireTime, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(r.ExclusiveMax.FireTime.Add(common.ScheduledTaskMinPrecision), 0),
		BatchSize:           testQueueOptions.BatchSize(),
		NextPageToken:       currentPageToken,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         mockTasks,
		NextPageToken: nextPageToken,
	}, nil).Times(1)

	paginationFn := paginationFnProvider(r)
	loadedTasks, actualNextPageToken, err := paginationFn(currentPageToken)
	require.NoError(t, err)
	for _, task := range loadedTasks {
		require.True(t, r.ContainsKey(task.GetKey()))
	}
	require.Len(t, loadedTasks, expectedNumTasks)

	if shouldHaveNextPage {
		require.Equal(t, nextPageToken, actualNextPageToken)
	} else {
		require.Nil(t, actualNextPageToken)
	}
}

func TestScheduledQueue_PaginationFnProvider_ShardOwnershipLost(t *testing.T) {
	t.Parallel()

	controller, mockShard, mockExecutionManager, scheduledQueue := setupScheduledQueue(t)
	defer controller.Finish()

	paginationFnProvider := scheduledQueue.paginationFnProvider

	mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(nil, &persistence.ShardOwnershipLostError{
		ShardID: mockShard.GetShardID(),
	}).Times(1)

	paginationFn := paginationFnProvider(NewRandomRange())
	_, _, err := paginationFn(nil)
	require.True(t, shard.IsShardOwnershipLostError(err))

	// make sure shard is also marked as invalid
	require.False(t, mockShard.IsValid())
}

func TestScheduledQueue_LookAheadTask_HasLookAheadTask(t *testing.T) {
	t.Parallel()

	controller, _, mockExecutionManager, scheduledQueue := setupScheduledQueue(t)
	defer controller.Finish()

	timerGate := timer.NewRemoteGate()
	scheduledQueue.timerGate = timerGate

	lookAheadRange, lookAheadTask := setupLookAheadMock(t, controller, mockExecutionManager, scheduledQueue, true)
	scheduledQueue.lookAheadTask()

	timerGate.SetCurrentTime(lookAheadTask.GetKey().FireTime)
	select {
	case <-scheduledQueue.timerGate.FireCh():
	default:
		require.Fail(t, "timer gate should fire when look ahead task is due")
	}

	_ = lookAheadRange // silence unused variable warning
}

func TestScheduledQueue_LookAheadTask_NoLookAheadTask(t *testing.T) {
	t.Parallel()

	controller, _, mockExecutionManager, scheduledQueue := setupScheduledQueue(t)
	defer controller.Finish()

	timerGate := timer.NewRemoteGate()
	scheduledQueue.timerGate = timerGate

	lookAheadRange, _ := setupLookAheadMock(t, controller, mockExecutionManager, scheduledQueue, false)
	scheduledQueue.lookAheadTask()

	timerGate.SetCurrentTime(lookAheadRange.InclusiveMin.FireTime.Add(time.Duration(
		(1 + testQueueOptions.MaxPollIntervalJitterCoefficient()) * float64(testQueueOptions.MaxPollInterval()),
	)))
	select {
	case <-scheduledQueue.timerGate.FireCh():
	default:
		require.Fail(t, "timer gate should fire at the end of look ahead window")
	}
}

func TestScheduledQueue_LookAheadTask_ErrorLookAhead(t *testing.T) {
	t.Parallel()

	controller, _, mockExecutionManager, scheduledQueue := setupScheduledQueue(t)
	defer controller.Finish()

	timerGate := timer.NewRemoteGate()
	scheduledQueue.timerGate = timerGate

	scheduledQueue.nonReadableScope = NewScope(
		NewRandomRange(),
		predicates.Universal[tasks.Task](),
	)

	mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some random error")).Times(1)
	scheduledQueue.lookAheadTask()

	timerGate.SetCurrentTime(scheduledQueue.nonReadableScope.Range.InclusiveMin.FireTime)
	select {
	case <-scheduledQueue.timerGate.FireCh():
	default:
		require.Fail(t, "timer gate should fire when time reaches look ahead range")
	}
}

func setupLookAheadMock(
	t *testing.T,
	controller *gomock.Controller,
	mockExecutionManager *persistence.MockExecutionManager,
	scheduledQueue *scheduledQueue,
	hasLookAheadTask bool,
) (lookAheadRange Range, lookAheadTask *tasks.MockTask) {
	lookAheadMinTime := scheduledQueue.nonReadableScope.Range.InclusiveMin.FireTime
	lookAheadRange = NewRange(
		tasks.NewKey(lookAheadMinTime, 0),
		tasks.NewKey(lookAheadMinTime.Add(testQueueOptions.MaxPollInterval()), 0),
	)

	loadedTasks := []tasks.Task{}
	if hasLookAheadTask {
		lookAheadTask = tasks.NewMockTask(controller)
		lookAheadTask.EXPECT().GetKey().Return(NewRandomKeyInRange(lookAheadRange)).AnyTimes()

		loadedTasks = append(loadedTasks, lookAheadTask)
	}

	mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.GetHistoryTasksRequest) (*persistence.GetHistoryTasksResponse, error) {
		require.Equal(t, request.ShardID, request.ShardID)
		require.Equal(t, tasks.CategoryTimer, request.TaskCategory)
		require.Equal(t, lookAheadRange.InclusiveMin, request.InclusiveMinTaskKey)
		require.Equal(t, 1, request.BatchSize)
		require.Nil(t, request.NextPageToken)

		require.Equal(t, lookAheadRange.ExclusiveMax.TaskID, request.ExclusiveMaxTaskKey.TaskID)
		fireTimeDifference := request.ExclusiveMaxTaskKey.FireTime.Sub(lookAheadRange.ExclusiveMax.FireTime)
		if fireTimeDifference < 0 {
			fireTimeDifference = -fireTimeDifference
		}
		maxAllowedFireTimeDifference := time.Duration(float64(testQueueOptions.MaxPollInterval()) * testQueueOptions.MaxPollIntervalJitterCoefficient())
		require.LessOrEqual(t, fireTimeDifference, maxAllowedFireTimeDifference)

		return &persistence.GetHistoryTasksResponse{
			Tasks:         loadedTasks,
			NextPageToken: nil,
		}, nil
	}).Times(1)

	return lookAheadRange, lookAheadTask
}
