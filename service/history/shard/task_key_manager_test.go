package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

func setupTaskKeyManager(t *testing.T) (*taskKeyManager, *clock.EventTimeSource, *int64) {
	rangeID := int64(1)
	rangeSizeBits := uint(3) // 1 << 3 = 8 tasks per range
	config := tests.NewDynamicConfig()
	config.RangeSizeBits = rangeSizeBits
	mockTimeSource := clock.NewEventTimeSource()

	var manager *taskKeyManager
	manager = newTaskKeyManager(
		tasks.NewDefaultTaskCategoryRegistry(),
		mockTimeSource,
		config,
		log.NewTestLogger(),
		func() error {
			rangeID++
			manager.setRangeID(rangeID)
			return nil
		},
	)
	manager.setRangeID(rangeID)
	manager.setTaskMinScheduledTime(time.Now().Add(-time.Second))
	return manager, mockTimeSource, &rangeID
}

func TestSetAndTrackTaskKeys(t *testing.T) {
	manager, _, rangeID := setupTaskKeyManager(t)
	now := time.Now()

	// this tests is just for making sure task keys are set and tracked
	// the actual logic of task key generation is tested in taskKeyGeneratorTest

	numTask := 5
	transferTasks := make([]tasks.Task, 0, numTask)
	for i := 0; i < numTask; i++ {
		transferTasks = append(
			transferTasks,
			tasks.NewFakeTask(
				tests.WorkflowKey,
				tasks.CategoryTransfer,
				now,
			),
		)
	}
	// assert that task keys are not set
	for _, transferTask := range transferTasks {
		require.Zero(t, transferTask.GetKey().TaskID)
	}

	initialTaskID := *rangeID << 3
	completionFn, err := manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: transferTasks,
	})
	require.NoError(t, err)

	// assert that task keys are set after calling setAndTrackTaskKeys
	for _, transferTask := range transferTasks {
		require.NotZero(t, transferTask.GetKey().TaskID)
	}

	// assert that task keys are tracked
	highReaderWatermark := manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	require.Equal(t, initialTaskID, highReaderWatermark.TaskID)

	// assert that pending task keys are cleared after calling completionFn
	completionFn(nil)
	highReaderWatermark = manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	require.Equal(t, initialTaskID+int64(numTask), highReaderWatermark.TaskID)
}

func TestSetRangeID(t *testing.T) {
	manager, _, rangeID := setupTaskKeyManager(t)
	initialTaskID := *rangeID << 3

	_, err := manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: {
			tasks.NewFakeTask(
				tests.WorkflowKey,
				tasks.CategoryTransfer,
				time.Now(),
			),
		},
	})
	require.NoError(t, err)

	require.Equal(t,
		initialTaskID,
		manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer).TaskID,
	)

	*rangeID++
	manager.setRangeID(*rangeID)

	expectedNextTaskID := *rangeID << 3
	require.Equal(t,
		expectedNextTaskID,
		manager.peekTaskKey(tasks.CategoryTransfer).TaskID,
	)

	// setRangeID should also clear pending task requests
	require.Equal(t,
		expectedNextTaskID,
		manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer).TaskID,
	)
}

func TestGetExclusiveReaderHighWatermark_NoPendingTask(t *testing.T) {
	manager, mockTimeSource, rangeID := setupTaskKeyManager(t)
	initialTaskID := *rangeID << 3

	highReaderWatermark := manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	require.Zero(t, tasks.NewImmediateKey(initialTaskID).CompareTo(highReaderWatermark))

	now := time.Now()
	mockTimeSource.Update(now)

	highReaderWatermark = manager.getExclusiveReaderHighWatermark(tasks.CategoryTimer)
	// for scheduled category type, we only need to make sure TaskID is 0 and FireTime is moved forwarded
	require.Zero(t, highReaderWatermark.TaskID)
	require.True(t, highReaderWatermark.FireTime.After(now))
	require.False(t, highReaderWatermark.FireTime.After(now.Add(manager.config.TimerProcessorMaxTimeShift())))
}

func TestGetExclusiveReaderHighWatermark_WithPendingTask(t *testing.T) {
	manager, mockTimeSource, rangeID := setupTaskKeyManager(t)
	initialTaskID := *rangeID << 3
	now := time.Now()
	mockTimeSource.Update(now)

	transferTask := tasks.NewFakeTask(
		tests.WorkflowKey,
		tasks.CategoryTransfer,
		time.Now(),
	)
	timerTask := tasks.NewFakeTask(
		tests.WorkflowKey,
		tasks.CategoryTimer,
		now.Add(-time.Minute),
	)

	// make two calls here, otherwise the order for assgining task keys is not guaranteed
	_, err := manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: {transferTask},
	})
	require.NoError(t, err)
	_, err = manager.setAndTrackTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTimer: {timerTask},
	})
	require.NoError(t, err)

	highReaderWatermark := manager.getExclusiveReaderHighWatermark(tasks.CategoryTransfer)
	require.Zero(t, tasks.NewImmediateKey(initialTaskID).CompareTo(highReaderWatermark))

	highReaderWatermark = manager.getExclusiveReaderHighWatermark(tasks.CategoryTimer)
	require.Zero(t, tasks.NewKey(timerTask.GetVisibilityTime(), 0).CompareTo(highReaderWatermark))
}
