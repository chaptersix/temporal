package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

func setupSequentialBatchQueue(t *testing.T) (*gomock.Controller, log.Logger, metrics.Handler) {
	controller := gomock.NewController(t)
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler
	return controller, logger, metricsHandler
}

func TestAdd_EmptyQueue(t *testing.T) {
	controller, logger, metricsHandler := setupSequentialBatchQueue(t)
	defer controller.Finish()

	newTask := NewMockTrackableExecutableTask(controller)
	queueId := "abc"
	newTask.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(newTask, func(_ TrackableExecutableTask) {}, logger, metricsHandler)
	require.Equal(t, 0, queue.Len())
	require.True(t, queue.IsEmpty())
}

func TestAdd_EmptyQueue_AddNewBatchedTask(t *testing.T) {
	controller, logger, metricsHandler := setupSequentialBatchQueue(t)
	defer controller.Finish()

	newTask := NewMockTrackableExecutableTask(controller)
	queueId := "abc"
	newTask.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(newTask, func(_ TrackableExecutableTask) {}, logger, metricsHandler)
	queue.Add(newTask)
	require.Equal(t, 1, queue.Len())
	require.False(t, queue.IsEmpty())

	taskRemoved := queue.Remove()
	// check queue
	require.True(t, queue.IsEmpty())
	require.Equal(t, 0, queue.Len())

	// check task
	batchTask, _ := taskRemoved.(*batchedTask)
	require.True(t, batchTask.batchedTask == newTask)
}

func TestAdd_NewTaskBatched(t *testing.T) {
	controller, logger, metricsHandler := setupSequentialBatchQueue(t)
	defer controller.Finish()

	task1 := NewMockBatchableTask(controller)
	task2 := NewMockBatchableTask(controller)
	task3 := NewMockBatchableTask(controller)
	id1 := int64(1)
	id2 := int64(2)
	id3 := int64(3)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	task3.EXPECT().TaskID().Return(id3).AnyTimes()
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, logger, metricsHandler)

	task1.EXPECT().CanBatch().Return(true).Times(1)
	task2.EXPECT().CanBatch().Return(true).Times(1)
	task3.EXPECT().CanBatch().Return(true).Times(1)
	combinedTask := NewMockBatchableTask(controller)
	task1.EXPECT().BatchWith(task2).Return(combinedTask, true).Times(1)
	combinedTask.EXPECT().CanBatch().Return(true).Times(1)
	combinedTask.EXPECT().BatchWith(task3).Return(NewMockBatchableTask(controller), true).Times(1)
	queue.Add(task1)
	queue.Add(task2)
	queue.Add(task3)
	// check queue
	require.Equal(t, 1, queue.Len())
	require.False(t, queue.IsEmpty())
	tr := queue.Remove()
	require.Equal(t, id1, tr.TaskID())
}

func TestAdd_NewTaskCannotBatched(t *testing.T) {
	controller, logger, metricsHandler := setupSequentialBatchQueue(t)
	defer controller.Finish()

	task1 := NewMockBatchableTask(controller)
	task2 := NewMockBatchableTask(controller)
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	id1 := int64(1)
	id2 := int64(2)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, logger, metricsHandler)

	task2.EXPECT().CanBatch().Return(false).Times(1)
	NewMockTrackableExecutableTask(controller)
	queue.Add(task1)
	queue.Add(task2)

	// check queue
	require.Equal(t, 2, queue.Len())
	require.False(t, queue.IsEmpty())
	r1 := queue.Remove()
	r2 := queue.Remove()
	require.Equal(t, id1, r1.TaskID())
	require.Equal(t, id2, r2.TaskID())
}

func TestAdd_NewTaskIsAddedToQueueWhenBatchFailed(t *testing.T) {
	controller, logger, metricsHandler := setupSequentialBatchQueue(t)
	defer controller.Finish()

	task1 := NewMockBatchableTask(controller)
	task2 := NewMockBatchableTask(controller)
	task3 := NewMockBatchableTask(controller)
	id1 := int64(1)
	id2 := int64(2)
	id3 := int64(3)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	task3.EXPECT().TaskID().Return(id3).AnyTimes()
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, logger, metricsHandler)

	task1.EXPECT().CanBatch().Return(false).Times(1)
	task2.EXPECT().CanBatch().Return(true).Times(2)
	task3.EXPECT().CanBatch().Return(true).Times(1)
	task2.EXPECT().BatchWith(task3).Return(NewMockBatchableTask(controller), true).Times(1)
	queue.Add(task1)
	queue.Add(task2)
	queue.Add(task3)
	// check queue
	require.Equal(t, 2, queue.Len())
	require.False(t, queue.IsEmpty())

	tr1 := queue.Remove()
	require.Equal(t, id1, tr1.TaskID())

	tr2 := queue.Remove()
	require.Equal(t, id2, tr2.TaskID())

	tr2BatchTask, _ := tr2.(*batchedTask)
	require.Len(t, tr2BatchTask.individualTasks, 2)
}

func TestAdd_NewTaskTryToBatchWithLastTask(t *testing.T) {
	controller, logger, metricsHandler := setupSequentialBatchQueue(t)
	defer controller.Finish()

	task1 := NewMockBatchableTask(controller)
	task2 := NewMockBatchableTask(controller)
	task3 := NewMockBatchableTask(controller)
	id1 := int64(1)
	id2 := int64(2)
	id3 := int64(3)
	task1.EXPECT().TaskID().Return(id1).AnyTimes()
	task2.EXPECT().TaskID().Return(id2).AnyTimes()
	task3.EXPECT().TaskID().Return(id3).AnyTimes()
	queueId := "abc"
	task1.EXPECT().QueueID().Return(queueId)
	queue := NewSequentialBatchableTaskQueue(task1, func(_ TrackableExecutableTask) {}, logger, metricsHandler)

	task1.EXPECT().CanBatch().Return(true).Times(1)
	task2.EXPECT().CanBatch().Return(true).Times(1)
	task3.EXPECT().CanBatch().Return(false).Times(2)

	queue.Add(task3)
	queue.Add(task2)
	queue.Add(task1)
	// check queue
	require.Equal(t, 3, queue.Len())

	require.Equal(t, id1, queue.Remove().TaskID())
	require.Equal(t, id2, queue.Remove().TaskID())
	require.Equal(t, id3, queue.Remove().TaskID())
}
