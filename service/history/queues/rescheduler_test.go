package queues

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type reschedulerTestDeps struct {
	controller    *gomock.Controller
	mockScheduler *MockScheduler
	timeSource    *clock.EventTimeSource
	rescheduler   *reschedulerImpl
}

func setupReschedulerTest(t *testing.T) *reschedulerTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockScheduler := NewMockScheduler(controller)
	mockScheduler.EXPECT().TaskChannelKeyFn().Return(
		func(_ Executable) TaskChannelKey { return TaskChannelKey{} },
	).AnyTimes()

	timeSource := clock.NewEventTimeSource()

	rescheduler := NewRescheduler(
		mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	return &reschedulerTestDeps{
		controller:    controller,
		mockScheduler: mockScheduler,
		timeSource:    timeSource,
		rescheduler:   rescheduler,
	}
}

func TestReschedulerStartStop(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	timeSource := clock.NewRealTimeSource()
	rescheduler := NewRescheduler(
		deps.mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	rescheduler.Start()

	numTasks := 20
	taskCh := make(chan struct{}, numTasks)
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskCh <- struct{}{}
		return true
	}).Times(numTasks)

	for i := 0; i != numTasks; i++ {
		mockExecutable := NewMockExecutable(deps.controller)
		mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
		mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		rescheduler.Add(
			mockExecutable,
			timeSource.Now().Add(time.Duration(rand.Int63n(300))*time.Millisecond),
		)
	}

	for i := 0; i != numTasks; i++ {
		<-taskCh
	}
	rescheduler.Stop()

	require.Equal(t, 0, rescheduler.Len())
}

func TestReschedulerDrain(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	timeSource := clock.NewRealTimeSource()
	rescheduler := NewRescheduler(
		deps.mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	rescheduler.Start()
	rescheduler.Stop()

	for i := 0; i != 10; i++ {
		rescheduler.Add(
			NewMockExecutable(deps.controller),
			timeSource.Now().Add(time.Duration(rand.Int63n(300))*time.Second),
		)
	}

	require.Equal(t, 0, rescheduler.Len())
}

func TestReschedulerReschedule_NoRescheduleLimit(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	now := time.Now()
	deps.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable/2; i++ {
		mockTask := NewMockExecutable(deps.controller)
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		deps.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)

		deps.rescheduler.Add(
			NewMockExecutable(deps.controller),
			now.Add(rescheduleInterval+time.Duration(rand.Int63n(time.Minute.Nanoseconds()))),
		)
	}
	require.Equal(t, numExecutable, deps.rescheduler.Len())

	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(numExecutable / 2)

	deps.timeSource.Update(now.Add(rescheduleInterval))
	deps.rescheduler.reschedule()
	require.Equal(t, numExecutable/2, deps.rescheduler.Len())
}

func TestReschedulerReschedule_TaskChanFull(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	now := time.Now()
	deps.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable; i++ {
		mockTask := NewMockExecutable(deps.controller)
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
		deps.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)
	}
	require.Equal(t, numExecutable, deps.rescheduler.Len())

	numSubmitted := 0
	numAllowed := numExecutable / 2
	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		if numSubmitted < numAllowed {
			numSubmitted++
			return true
		}

		return false
	}).Times(numAllowed + 1)

	deps.timeSource.Update(now.Add(rescheduleInterval))
	deps.rescheduler.reschedule()
	require.Equal(t, numExecutable-numSubmitted, deps.rescheduler.Len())
}

func TestReschedulerReschedule_DropCancelled(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	now := time.Now()
	deps.timeSource.Update(now)
	rescheduleInterval := time.Minute

	for i := 0; i != 10; i++ {
		mockTask := NewMockExecutable(deps.controller)
		mockTask.EXPECT().State().Return(ctasks.TaskStateCancelled).Times(1)
		deps.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)
	}

	deps.timeSource.Update(now.Add(rescheduleInterval))
	deps.rescheduler.reschedule()
	require.Equal(t, 0, deps.rescheduler.Len())
}

func TestReschedulerForceReschedule_ImmediateTask(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	now := time.Now()
	deps.timeSource.Update(now)
	namespaceID := deps.mockScheduler.TaskChannelKeyFn()(nil).NamespaceID

	deps.rescheduler.Start()
	defer deps.rescheduler.Stop()

	numTask := 10
	taskWG := &sync.WaitGroup{}
	taskWG.Add(numTask)
	for i := 0; i != numTask; i++ {
		mockTask := NewMockExecutable(deps.controller)
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().GetKey().Return(tasks.NewImmediateKey(int64(i))).AnyTimes()
		deps.rescheduler.Add(
			mockTask,
			now.Add(time.Minute+time.Duration(rand.Int63n(time.Minute.Nanoseconds()))),
		)
	}

	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskWG.Done()
		return true
	}).Times(numTask)

	deps.rescheduler.Reschedule(namespaceID)
	taskWG.Wait()
	require.Equal(t, 0, deps.rescheduler.Len())
}

func TestReschedulerForceReschedule_ScheduledTask(t *testing.T) {
	t.Parallel()

	deps := setupReschedulerTest(t)
	now := time.Now()
	deps.timeSource.Update(now)
	namespaceID := deps.mockScheduler.TaskChannelKeyFn()(nil).NamespaceID

	deps.rescheduler.Start()
	defer deps.rescheduler.Stop()

	taskWG := &sync.WaitGroup{}
	taskWG.Add(1)

	retryingTask := NewMockExecutable(deps.controller)
	retryingTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	retryingTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	retryingTask.EXPECT().GetKey().Return(tasks.NewKey(now.Add(-time.Minute), int64(1))).AnyTimes()
	deps.rescheduler.Add(
		retryingTask,
		now.Add(time.Minute),
	)

	// schedule queue pre-fetches tasks
	futureTaskTimestamp := now.Add(time.Second)
	futureTask := NewMockExecutable(deps.controller)
	futureTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	futureTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	futureTask.EXPECT().GetKey().Return(tasks.NewKey(futureTaskTimestamp, int64(2))).AnyTimes()
	deps.rescheduler.Add(
		futureTask,
		futureTaskTimestamp,
	)

	deps.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskWG.Done()
		return true
	}).Times(1)

	deps.rescheduler.Reschedule(namespaceID)
	taskWG.Wait()
	require.Equal(t, 1, deps.rescheduler.Len())
}
