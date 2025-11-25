package shard

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

func setupTaskKeyGenerator(t *testing.T) (*taskKeyGenerator, *clock.EventTimeSource, *int64) {
	rangeID := int64(1)
	rangeSizeBits := uint(3) // 1 << 3 = 8 tasks per range
	mockTimeSource := clock.NewEventTimeSource()

	var generator *taskKeyGenerator
	generator = newTaskKeyGenerator(
		rangeSizeBits,
		mockTimeSource,
		log.NewTestLogger(),
		func() error {
			rangeID++
			generator.setRangeID(rangeID)
			return nil
		},
	)
	generator.setRangeID(rangeID)
	generator.setTaskMinScheduledTime(time.Now().Add(-time.Second))
	return generator, mockTimeSource, &rangeID
}

func TestSetTaskKeys_ImmediateTasks(t *testing.T) {
	generator, mockTimeSource, _ := setupTaskKeyGenerator(t)
	now := time.Now()
	mockTimeSource.Update(now)

	numTask := 5
	transferTasks := make([]tasks.Task, 0, numTask)
	for i := 0; i < numTask; i++ {
		transferTasks = append(
			transferTasks,
			tasks.NewFakeTask(
				tests.WorkflowKey,
				tasks.CategoryTransfer,
				// use some random initial timestamp for the task
				now.Add(time.Duration(time.Second*time.Duration(rand.Int63n(100)-50))),
			),
		)
	}

	err := generator.setTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: transferTasks,
	})
	require.NoError(t, err)

	expectedTaskID := int64(1 << 3) // rangeID << rangeSizeBits
	for _, transferTask := range transferTasks {
		actualKey := transferTask.GetKey()
		expectedKey := tasks.NewImmediateKey(expectedTaskID)
		require.Zero(t, expectedKey.CompareTo(actualKey))
		require.Equal(t, now, transferTask.GetVisibilityTime())

		expectedTaskID++
	}
}

func TestSetTaskKeys_ScheduledTasks(t *testing.T) {
	generator, mockTimeSource, _ := setupTaskKeyGenerator(t)
	now := time.Now().Truncate(common.ScheduledTaskMinPrecision)
	mockTimeSource.Update(now)

	timerTasks := []tasks.Task{
		tasks.NewFakeTask(tests.WorkflowKey, tasks.CategoryTimer, now.Add(-time.Minute)),
		tasks.NewFakeTask(tests.WorkflowKey, tasks.CategoryTimer, now.Add(time.Minute)),
	}
	initialTaskID := int64(1 << 3) // rangeID << rangeSizeBits
	expectedKeys := []tasks.Key{
		tasks.NewKey(now.Add(common.ScheduledTaskMinPrecision), initialTaskID),
		tasks.NewKey(now.Add(time.Minute).Add(common.ScheduledTaskMinPrecision), initialTaskID+1),
	}

	err := generator.setTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTimer: timerTasks,
	})
	require.NoError(t, err)

	for i, timerTask := range timerTasks {
		actualKey := timerTask.GetKey()
		expectedKey := expectedKeys[i]
		require.Zero(t, expectedKey.CompareTo(actualKey))
	}
}

func TestSetTaskKeys_RenewRange(t *testing.T) {
	generator, mockTimeSource, rangeID := setupTaskKeyGenerator(t)
	now := time.Now()
	mockTimeSource.Update(now)

	initialRangeID := *rangeID

	numTask := 10
	require.True(t, numTask > (1 << 3))

	transferTasks := make([]tasks.Task, 0, numTask)
	for i := 0; i < numTask; i++ {
		transferTasks = append(
			transferTasks,
			tasks.NewFakeTask(tests.WorkflowKey, tasks.CategoryTransfer, now),
		)
	}

	err := generator.setTaskKeys(map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: transferTasks,
	})
	require.NoError(t, err)

	expectedTaskID := int64(initialRangeID << 3)
	for _, transferTask := range transferTasks {
		actualKey := transferTask.GetKey()
		expectedKey := tasks.NewImmediateKey(expectedTaskID)
		require.Zero(t, expectedKey.CompareTo(actualKey))
		require.Equal(t, now, transferTask.GetVisibilityTime())

		expectedTaskID++
	}
	require.Equal(t, initialRangeID+1, *rangeID)
}

func TestPeekAndGenerateTaskKey(t *testing.T) {
	generator, mockTimeSource, rangeID := setupTaskKeyGenerator(t)
	nextTaskID := *rangeID << 3
	nextKey := generator.peekTaskKey(tasks.CategoryTransfer)
	require.Zero(t, tasks.NewImmediateKey(nextTaskID).CompareTo(nextKey))

	generatedKey, err := generator.generateTaskKey(tasks.CategoryTransfer)
	require.NoError(t, err)
	require.Zero(t, nextKey.CompareTo(generatedKey))

	nextTaskID++
	now := time.Now().Truncate(common.ScheduledTaskMinPrecision)
	mockTimeSource.Update(now)
	generator.setTaskMinScheduledTime(now)
	nextKey = generator.peekTaskKey(tasks.CategoryTimer)
	require.Zero(t, tasks.NewKey(now, nextTaskID).CompareTo(nextKey))

	generatedKey, err = generator.generateTaskKey(tasks.CategoryTimer)
	require.NoError(t, err)
	require.Zero(t, nextKey.CompareTo(generatedKey))
}
