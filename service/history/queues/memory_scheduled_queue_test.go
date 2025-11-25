package queues

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type memoryScheduledQueueTestDeps struct {
	controller            *gomock.Controller
	mockTimeSource        *clock.EventTimeSource
	mockScheduler         *ctasks.MockScheduler[ctasks.Task]
	mockClusterMetadata   *cluster.MockMetadata
	mockNamespaceRegistry *namespace.MockRegistry
	scheduledQueue        *memoryScheduledQueue
}

func setupMemoryScheduledQueueTest(t *testing.T) *memoryScheduledQueueTestDeps {
	controller := gomock.NewController(t)
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(time.Now().UTC())
	mockClusterMetadata := cluster.NewMockMetadata(controller)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	mockScheduler := ctasks.NewMockScheduler[ctasks.Task](controller)
	mockScheduler.EXPECT().Start().AnyTimes()
	mockScheduler.EXPECT().Stop().AnyTimes()

	scheduledQueue := newMemoryScheduledQueue(
		mockScheduler,
		mockTimeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	scheduledQueue.Start()

	return &memoryScheduledQueueTestDeps{
		controller:            controller,
		mockTimeSource:        mockTimeSource,
		mockScheduler:         mockScheduler,
		mockClusterMetadata:   mockClusterMetadata,
		mockNamespaceRegistry: mockNamespaceRegistry,
		scheduledQueue:        scheduledQueue,
	}
}

func (d *memoryScheduledQueueTestDeps) newSpeculativeWorkflowTaskTimeoutTestExecutable(
	visibilityTimestamp time.Time,
) *speculativeWorkflowTaskTimeoutExecutable {

	wttt := &tasks.WorkflowTaskTimeoutTask{
		VisibilityTimestamp: visibilityTimestamp,
	}

	return newSpeculativeWorkflowTaskTimeoutExecutable(
		NewExecutable(
			0,
			wttt,
			nil,
			nil,
			nil,
			NewNoopPriorityAssigner(),
			d.mockTimeSource,
			d.mockNamespaceRegistry,
			d.mockClusterMetadata,
			chasm.NewRegistry(log.NewTestLogger()),
			GetTaskTypeTagValue,
			nil,
			metrics.NoopMetricsHandler,
			telemetry.NoopTracer,
		),
		wttt,
	)
}

func TestThreeInOrderTasks(t *testing.T) {
	t.Parallel()
	deps := setupMemoryScheduledQueueTest(t)
	defer deps.scheduledQueue.Stop()

	now := deps.mockTimeSource.Now()
	t1 := deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(20 * time.Millisecond))
	t2 := deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(10 * time.Millisecond))
	t3 := deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(30 * time.Millisecond))

	calls := atomic.Int32{}
	calls.Store(3)
	gomock.InOrder(
		deps.mockScheduler.EXPECT().TrySubmit(t2).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
		deps.mockScheduler.EXPECT().TrySubmit(t1).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
		deps.mockScheduler.EXPECT().TrySubmit(t3).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
	)

	deps.scheduledQueue.Add(t1)
	deps.scheduledQueue.Add(t2)
	deps.scheduledQueue.Add(t3)

	// To ensure all timers have fired.
	require.Eventually(t, func() bool { return calls.Load() == 0 }, time.Second, 100*time.Millisecond)
}

func TestThreeCancelledTasks(t *testing.T) {
	t.Parallel()
	deps := setupMemoryScheduledQueueTest(t)
	defer deps.scheduledQueue.Stop()

	deps.scheduledQueue.scheduler = deps.mockScheduler

	now := deps.mockTimeSource.Now()
	t1 := deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(20 * time.Millisecond))
	t2 := deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(10 * time.Millisecond))
	t3 := deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(30 * time.Millisecond))

	t1.Cancel()
	t2.Cancel()

	calls := atomic.Int32{}
	calls.Store(1)
	deps.mockScheduler.EXPECT().TrySubmit(t3).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) })

	deps.scheduledQueue.Add(t1)
	deps.scheduledQueue.Add(t2)
	deps.scheduledQueue.Add(t3)

	require.Eventually(t, func() bool { return calls.Load() == 0 }, time.Second, 100*time.Millisecond)
}

func Test1KRandomTasks(t *testing.T) {
	t.Parallel()
	deps := setupMemoryScheduledQueueTest(t)
	defer deps.scheduledQueue.Stop()

	deps.scheduledQueue.scheduler = deps.mockScheduler

	now := deps.mockTimeSource.Now()
	taskList := make([]*speculativeWorkflowTaskTimeoutExecutable, 1000)
	calls := atomic.Int32{}

	for i := 0; i < 1000; i++ {
		taskList[i] = deps.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(time.Duration(rand.Intn(100)) * time.Microsecond))

		// Randomly cancel some tasks.
		if rand.Intn(2) == 0 {
			taskList[i].Cancel()
		} else {
			calls.Add(1)
		}
	}

	for i := 0; i < 1000; i++ {
		if taskList[i].State() != ctasks.TaskStateCancelled {
			deps.mockScheduler.EXPECT().TrySubmit(taskList[i]).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) })
		}
	}

	for i := 0; i < 1000; i++ {
		deps.scheduledQueue.Add(taskList[i])
	}

	// To ensure all timers have fired.
	require.Eventually(t, func() bool { return calls.Load() == 0 }, 10*time.Second, 100*time.Millisecond)
}
