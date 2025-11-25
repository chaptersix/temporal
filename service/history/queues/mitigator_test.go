package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type testMonitor struct {
	Monitor

	resolvedAlertType AlertType
}

func (m *testMonitor) ResolveAlert(alertType AlertType) {
	m.resolvedAlertType = alertType
}

func TestMitigate_ActionMatchAlert(t *testing.T) {
	t.Parallel()
	mockTimeSource := clock.NewEventTimeSource()
	monitor := &testMonitor{}

	// we use a different actionRunner implementation,
	// which doesn't require readerGroup
	mitigator := newMitigator(
		nil,
		monitor,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		dynamicconfig.GetIntPropertyFn(3),
		GrouperNamespaceID{},
	)

	testCases := []struct {
		alert          Alert
		expectedAction Action
	}{
		{
			alert: Alert{
				AlertType: AlertTypeQueuePendingTaskCount,
				AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
					CurrentPendingTaskCount:   1000,
					CiriticalPendingTaskCount: 500,
				},
			},
			expectedAction: &actionQueuePendingTask{},
		},
		{
			alert: Alert{
				AlertType: AlertTypeReaderStuck,
				AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
					ReaderID:         1,
					CurrentWatermark: NewRandomKey(),
				},
			},
			expectedAction: &actionReaderStuck{},
		},
		{
			alert: Alert{
				AlertType: AlertTypeSliceCount,
				AlertAttributesSliceCount: &AlertAttributesSlicesCount{
					CurrentSliceCount:  1000,
					CriticalSliceCount: 500,
				},
			},
			expectedAction: &actionSliceCount{},
		},
	}

	var actualAction Action
	mitigator.actionRunner = func(
		action Action,
		_ *ReaderGroup,
		_ metrics.Handler,
	) {
		actualAction = action
	}

	for _, tc := range testCases {
		mitigator.Mitigate(tc.alert)
		require.IsType(t, tc.expectedAction, actualAction)
	}

	_ = mockTimeSource
}

func TestMitigate_ResolveAlert(t *testing.T) {
	t.Parallel()
	mockTimeSource := clock.NewEventTimeSource()
	monitor := &testMonitor{}

	mitigator := newMitigator(
		nil,
		monitor,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		dynamicconfig.GetIntPropertyFn(3),
		GrouperNamespaceID{},
	)

	mitigator.actionRunner = func(
		_ Action,
		_ *ReaderGroup,
		_ metrics.Handler,
	) {
	}

	alert := Alert{
		AlertType: AlertTypeQueuePendingTaskCount,
		AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   1000,
			CiriticalPendingTaskCount: 500,
		},
	}
	mitigator.Mitigate(alert)

	require.Equal(t, alert.AlertType, monitor.resolvedAlertType)

	_ = mockTimeSource
}
