package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

var (
	testSchedulerMonitorOptions = schedulerMonitorOptions{
		aggregationCount:    10,
		aggregationDuration: 200 * time.Millisecond,
	}
)

type schedulerMonitorTestDeps struct {
	controller            *gomock.Controller
	mockNamespaceRegistry *namespace.MockRegistry
	mockMetricsHandler    *metrics.MockHandler
	mockTimerMetric       *metrics.MockTimerIface
	mockTimeSource        *clock.EventTimeSource
	schedulerMonitor      *schedulerMonitor
}

func setupSchedulerMonitorTest(t *testing.T) *schedulerMonitorTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockMetricsHandler := metrics.NewMockHandler(controller)
	mockTimerMetric := metrics.NewMockTimerIface(controller)
	mockTimeSource := clock.NewEventTimeSource()

	mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	mockMetricsHandler.EXPECT().WithTags(gomock.Any()).Return(mockMetricsHandler).AnyTimes()
	mockMetricsHandler.EXPECT().Timer(metrics.QueueScheduleLatency.Name()).Return(mockTimerMetric).AnyTimes()

	schedulerMonitor := newSchedulerMonitor(
		func(e Executable) TaskChannelKey {
			return TaskChannelKey{
				NamespaceID: tests.NamespaceID.String(),
				Priority:    tasks.PriorityHigh,
			}
		},
		mockNamespaceRegistry,
		mockTimeSource,
		mockMetricsHandler,
		testSchedulerMonitorOptions,
	)

	return &schedulerMonitorTestDeps{
		controller:            controller,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockMetricsHandler:    mockMetricsHandler,
		mockTimerMetric:       mockTimerMetric,
		mockTimeSource:        mockTimeSource,
		schedulerMonitor:      schedulerMonitor,
	}
}

func TestSchedulerMonitorRecordStart_AggregationCount(t *testing.T) {
	t.Parallel()

	deps := setupSchedulerMonitorTest(t)
	deps.schedulerMonitor.Start()
	defer deps.schedulerMonitor.Stop()

	now := clock.NewRealTimeSource().Now()
	scheduledTime := now
	singleScheduleLatency := time.Millisecond * 10
	deps.mockTimeSource.Update(now)

	totalExecutables := 2*testSchedulerMonitorOptions.aggregationCount + 10

	deps.mockTimerMetric.EXPECT().Record(
		time.Duration(testSchedulerMonitorOptions.aggregationCount) * singleScheduleLatency,
	).Times(totalExecutables / testSchedulerMonitorOptions.aggregationCount)

	for numExecutables := 0; numExecutables != totalExecutables; numExecutables++ {
		mockExecutable := NewMockExecutable(deps.controller)
		mockExecutable.EXPECT().GetScheduledTime().Return(scheduledTime).Times(1)

		now = now.Add(singleScheduleLatency)
		deps.mockTimeSource.Update(now)
		deps.schedulerMonitor.RecordStart(mockExecutable)
	}
}

func TestSchedulerMonitorRecordStart_AggregationDuration(t *testing.T) {
	t.Parallel()

	deps := setupSchedulerMonitorTest(t)
	deps.schedulerMonitor.Start()
	defer deps.schedulerMonitor.Stop()

	now := clock.NewRealTimeSource().Now()
	singleScheduleLatency := time.Millisecond * 10
	deps.mockTimeSource.Update(now)

	totalExecutables := testSchedulerMonitorOptions.aggregationCount / 2

	done := make(chan struct{})
	deps.mockTimerMetric.EXPECT().Record(
		// although # of executable is less than aggregationCount
		// the value emitted should be scaled
		time.Duration(testSchedulerMonitorOptions.aggregationCount) * singleScheduleLatency,
	).Do(func(time.Duration, ...metrics.Tag) {
		close(done)
	}).Times(1)

	for numExecutables := 0; numExecutables != totalExecutables; numExecutables++ {
		mockExecutable := NewMockExecutable(deps.controller)
		mockExecutable.EXPECT().GetScheduledTime().Return(now).Times(1)

		now = now.Add(singleScheduleLatency)
		deps.mockTimeSource.Update(now)
		deps.schedulerMonitor.RecordStart(mockExecutable)

		// simulate the case where task submission is very low frequency
		now = now.Add(10 * singleScheduleLatency)
	}

	// wait for the metric emission ticker
	select {
	case <-done:
	case <-time.NewTimer(3 * testSchedulerMonitorOptions.aggregationDuration).C:
		require.Fail(t, "metric emission ticker should fire earlier")
	}
}
