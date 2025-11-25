package queues

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/tasks"
)

func setupMonitor() (*monitorImpl, <-chan *Alert, *clock.EventTimeSource) {
	mockTimeSource := clock.NewEventTimeSource()
	monitor := newMonitor(
		tasks.CategoryTypeScheduled,
		mockTimeSource,
		&MonitorOptions{
			PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
			ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
			SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
		},
	)
	alertCh := monitor.AlertCh()
	return monitor, alertCh, mockTimeSource
}

func TestMonitor_PendingTasksStats(t *testing.T) {
	t.Parallel()

	monitor, alertCh, _ := setupMonitor()
	defer monitor.Close()

	require.Equal(t, 0, monitor.GetTotalPendingTaskCount())
	require.Equal(t, 0, monitor.GetSlicePendingTaskCount(&SliceImpl{}))

	threshold := monitor.options.PendingTasksCriticalCount()

	slice1 := &SliceImpl{}
	monitor.SetSlicePendingTaskCount(slice1, threshold/2)
	require.Equal(t, threshold/2, monitor.GetSlicePendingTaskCount(slice1))
	select {
	case <-alertCh:
		require.Fail(t, "should not trigger alert")
	default:
	}

	monitor.SetSlicePendingTaskCount(slice1, threshold*2)
	require.Equal(t, threshold*2, monitor.GetTotalPendingTaskCount())
	alert := <-alertCh
	require.Equal(t, Alert{
		AlertType: AlertTypeQueuePendingTaskCount,
		AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   threshold * 2,
			CiriticalPendingTaskCount: threshold,
		},
	}, *alert)

	slice2 := &SliceImpl{}
	monitor.SetSlicePendingTaskCount(slice2, 1)
	select {
	case <-alertCh:
		require.Fail(t, "should have only one outstanding pending task alert")
	default:
	}

	monitor.ResolveAlert(alert.AlertType)
	monitor.SetSlicePendingTaskCount(slice2, 1)
	require.Equal(t, threshold*2+1, monitor.GetTotalPendingTaskCount())
	alert = <-alertCh
	require.Equal(t, Alert{
		AlertType: AlertTypeQueuePendingTaskCount,
		AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   threshold*2 + 1,
			CiriticalPendingTaskCount: threshold,
		},
	}, *alert)

	monitor.RemoveSlice(slice1)
	require.Equal(t, 1, monitor.GetTotalPendingTaskCount())
}

func TestMonitor_ReaderWatermarkStats(t *testing.T) {
	t.Parallel()

	monitor, alertCh, _ := setupMonitor()
	defer monitor.Close()

	_, ok := monitor.GetReaderWatermark(DefaultReaderId)
	require.False(t, ok)

	now := time.Now().Truncate(monitorWatermarkPrecision)
	monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	watermark, ok := monitor.GetReaderWatermark(DefaultReaderId)
	require.True(t, ok)
	require.Equal(t, tasks.NewKey(
		now.Truncate(monitorWatermarkPrecision),
		0,
	), watermark)

	for i := 0; i != monitor.options.ReaderStuckCriticalAttempts(); i++ {
		now = now.Add(time.Millisecond * 100)
		monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	}

	alert := <-alertCh
	expectedAlert := Alert{
		AlertType: AlertTypeReaderStuck,
		AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
			ReaderID: DefaultReaderId,
			CurrentWatermark: tasks.NewKey(
				now.Truncate(monitorWatermarkPrecision),
				0,
			),
		},
	}
	require.Equal(t, expectedAlert, *alert)

	monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	select {
	case <-alertCh:
		require.Fail(t, "should have only one outstanding slice count alert")
	default:
	}

	monitor.ResolveAlert(alert.AlertType)
	monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	alert = <-alertCh
	require.Equal(t, expectedAlert, *alert)
}

func TestMonitor_SliceCount(t *testing.T) {
	t.Parallel()

	monitor, alertCh, _ := setupMonitor()
	defer monitor.Close()

	require.Equal(t, 0, monitor.GetTotalSliceCount())
	require.Equal(t, 0, monitor.GetSliceCount(DefaultReaderId))

	threshold := monitor.options.SliceCountCriticalThreshold()
	monitor.SetSliceCount(DefaultReaderId, threshold/2)
	require.Equal(t, threshold/2, monitor.GetTotalSliceCount())
	select {
	case <-alertCh:
		require.Fail(t, "should not trigger alert")
	default:
	}

	monitor.SetSliceCount(DefaultReaderId, threshold*2)
	require.Equal(t, threshold*2, monitor.GetTotalSliceCount())
	alert := <-alertCh
	require.Equal(t, Alert{
		AlertType: AlertTypeSliceCount,
		AlertAttributesSliceCount: &AlertAttributesSlicesCount{
			CurrentSliceCount:  threshold * 2,
			CriticalSliceCount: threshold,
		},
	}, *alert)

	monitor.SetSliceCount(DefaultReaderId+1, 1)
	select {
	case <-alertCh:
		require.Fail(t, "should have only one outstanding slice count alert")
	default:
	}

	monitor.ResolveAlert(alert.AlertType)
	monitor.SetSliceCount(DefaultReaderId+1, 1)
	require.Equal(t, threshold*2+1, monitor.GetTotalSliceCount())
	alert = <-alertCh
	require.Equal(t, Alert{
		AlertType: AlertTypeSliceCount,
		AlertAttributesSliceCount: &AlertAttributesSlicesCount{
			CurrentSliceCount:  threshold*2 + 1,
			CriticalSliceCount: threshold,
		},
	}, *alert)
}

func TestMonitor_ResolveAlert(t *testing.T) {
	t.Parallel()

	monitor, alertCh, _ := setupMonitor()
	defer monitor.Close()

	sliceCount := monitor.options.SliceCountCriticalThreshold() * 2

	monitor.SetSliceCount(DefaultReaderId, sliceCount) // trigger an alert

	alert := <-alertCh
	require.NotNil(t, alert)
	monitor.ResolveAlert(alert.AlertType)

	// alert should be resolved,
	// which means we can trigger the same alert type again
	monitor.SetSliceCount(DefaultReaderId, sliceCount)
	select {
	case alert := <-alertCh:
		require.NotNil(t, alert)
	default:
		require.FailNow(t, "Can't trigger new alert, previous alert likely not resolved")
	}
}

func TestMonitor_SilenceAlert(t *testing.T) {
	t.Parallel()

	monitor, alertCh, mockTimeSource := setupMonitor()
	defer monitor.Close()

	now := time.Now()
	mockTimeSource.Update(now)

	sliceCount := monitor.options.SliceCountCriticalThreshold() * 2
	monitor.SetSliceCount(DefaultReaderId, sliceCount) // trigger an alert

	alert := <-alertCh
	require.NotNil(t, alert)
	monitor.SilenceAlert(alert.AlertType)

	// alert should be silenced,
	// which means we can't trigger the same alert type again
	monitor.SetSliceCount(DefaultReaderId, sliceCount)
	select {
	case <-alertCh:
		require.FailNow(t, "Alert not silenced")
	default:
	}

	// other alert types should still be able to fire
	pendingTaskCount := monitor.options.PendingTasksCriticalCount() * 2
	monitor.SetSlicePendingTaskCount(&SliceImpl{}, pendingTaskCount)
	select {
	case alert := <-alertCh:
		require.NotNil(t, alert)
	default:
		require.FailNow(t, "Alerts with a different type should still be able to fire")
	}

	now = now.Add(defaultAlertSilenceDuration * 2)
	mockTimeSource.Update(now)

	// same alert should be able to fire after the silence duration
	monitor.SetSliceCount(DefaultReaderId, sliceCount)
	select {
	case alert := <-alertCh:
		require.NotNil(t, alert)
	default:
		require.FailNow(t, "Same alert type should fire after silence duration")
	}
}
