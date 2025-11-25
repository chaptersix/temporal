package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

func setupExecutableTaskTracker(t *testing.T) (*gomock.Controller, *ExecutableTaskTrackerImpl) {
	controller := gomock.NewController(t)
	taskTracker := NewExecutableTaskTracker(log.NewTestLogger(), metrics.NoopMetricsHandler)
	return controller, taskTracker
}

func TestTrackTasks(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	tasks := taskTracker.TrackTasks(highWatermark0, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
	require.Equal(t, highWatermark0, *taskTracker.exclusiveHighWatermarkInfo)

	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task0.TaskID() + 1).AnyTimes()
	task2 := NewMockTrackableExecutableTask(controller)
	task2.EXPECT().TaskID().Return(task1.TaskID() + 1).AnyTimes()
	highWatermark2 := WatermarkInfo{
		Watermark: task2.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	tasks = taskTracker.TrackTasks(highWatermark2, task1, task2)
	require.Equal(t, []TrackableExecutableTask{task1, task2}, tasks)

	taskIDs = []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID(), task1.TaskID(), task2.TaskID()}, taskIDs)
	require.Equal(t, highWatermark2, *taskTracker.exclusiveHighWatermarkInfo)
}

func TestTrackTasks_Duplication(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := taskTracker.TrackTasks(highWatermark0, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)
	tasks = taskTracker.TrackTasks(highWatermark0, task0)
	require.Equal(t, []TrackableExecutableTask{}, tasks)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
	require.Equal(t, highWatermark0, *taskTracker.exclusiveHighWatermarkInfo)

	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task0.TaskID() + 1).AnyTimes()
	highWatermark1 := WatermarkInfo{
		Watermark: task1.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks = taskTracker.TrackTasks(highWatermark1, task1)
	require.Equal(t, []TrackableExecutableTask{task1}, tasks)

	taskIDs = []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID(), task1.TaskID()}, taskIDs)
	require.Equal(t, highWatermark1, *taskTracker.exclusiveHighWatermarkInfo)

	task2 := NewMockTrackableExecutableTask(controller)
	task2.EXPECT().TaskID().Return(task1.TaskID() + 1).AnyTimes()
	highWatermark2 := WatermarkInfo{
		Watermark: task2.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks = taskTracker.TrackTasks(highWatermark2, task1, task2)
	require.Equal(t, []TrackableExecutableTask{task2}, tasks)

	taskIDs = []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID(), task1.TaskID(), task2.TaskID()}, taskIDs)
	require.Equal(t, highWatermark2, *taskTracker.exclusiveHighWatermarkInfo)
}

func TestTrackTasks_Cancellation(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().Cancel()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	taskTracker.Cancel()
	tasks := taskTracker.TrackTasks(highWatermark0, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
	require.Equal(t, highWatermark0, *taskTracker.exclusiveHighWatermarkInfo)
}

func TestLowWatermark_Empty(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{}, taskIDs)

	lowWatermark := taskTracker.LowWatermark()
	require.Nil(t, lowWatermark)
}

func TestLowWatermark_AckedTask_Case0(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateAcked).AnyTimes()
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := taskTracker.TrackTasks(highWatermark0, task0, task1)
	require.Equal(t, []TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task1ID,
		Timestamp: task1.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task1ID}, taskIDs)
}

func TestLowWatermark_AckedTask_Case1(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateAcked).AnyTimes()
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStateAcked).AnyTimes()
	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := taskTracker.TrackTasks(highWatermark0, task0, task1)
	require.Equal(t, []TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, highWatermark0, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{}, taskIDs)
}

func TestLowWatermark_NackedTask_Success_Case0(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()
	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(nil)
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := taskTracker.TrackTasks(highWatermark0, task0, task1)
	require.Equal(t, []TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task1ID,
		Timestamp: task1.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task1ID}, taskIDs)
}

func TestLowWatermark_NackedTask_Success_Case1(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(nil)
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task1.EXPECT().MarkPoisonPill().Return(nil)

	highWatermark0 := WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	tasks := taskTracker.TrackTasks(highWatermark0, task0, task1)
	require.Equal(t, []TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, highWatermark0, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{}, taskIDs)
}

func TestLowWatermark_NackedTask_Error_Case0(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(errors.New("random error"))
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	tasks := taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0, task1)
	require.Equal(t, []TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0ID, task1ID}, taskIDs)
}

func TestLowWatermark_NackedTask_Error_Case1(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0ID := rand.Int63()
	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(task0ID).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task0.EXPECT().MarkPoisonPill().Return(serviceerror.NewInternal("random error"))
	task1ID := task0ID + 1
	task1 := NewMockTrackableExecutableTask(controller)
	task1.EXPECT().TaskID().Return(task1ID).AnyTimes()
	task1.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task1.EXPECT().State().Return(ctasks.TaskStateNacked).AnyTimes()
	task1.EXPECT().MarkPoisonPill().Return(serviceerror.NewInternal("random error"))

	tasks := taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task1ID + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0, task1)
	require.Equal(t, []TrackableExecutableTask{task0, task1}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0ID, task1ID}, taskIDs)
}

func TestLowWatermark_AbortedTask(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateAborted).AnyTimes()

	tasks := taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
}

func TestLowWatermark_CancelledTask(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStateCancelled).AnyTimes()

	tasks := taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
}

func TestLowWatermark_PendingTask(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().TaskCreationTime().Return(time.Unix(0, rand.Int63())).AnyTimes()
	task0.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	tasks := taskTracker.TrackTasks(WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)

	lowWatermark := taskTracker.LowWatermark()
	require.Equal(t, WatermarkInfo{
		Watermark: task0.TaskID(),
		Timestamp: task0.TaskCreationTime(),
	}, *lowWatermark)

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
}

func TestCancellation(t *testing.T) {
	controller, taskTracker := setupExecutableTaskTracker(t)
	defer controller.Finish()

	task0 := NewMockTrackableExecutableTask(controller)
	task0.EXPECT().TaskID().Return(rand.Int63()).AnyTimes()
	task0.EXPECT().Cancel()
	highWatermark0 := WatermarkInfo{
		Watermark: task0.TaskID() + 1,
		Timestamp: time.Unix(0, rand.Int63()),
	}

	tasks := taskTracker.TrackTasks(highWatermark0, task0)
	require.Equal(t, []TrackableExecutableTask{task0}, tasks)
	taskTracker.Cancel()

	taskIDs := []int64{}
	for element := taskTracker.taskQueue.Front(); element != nil; element = element.Next() {
		taskIDs = append(taskIDs, element.Value.(TrackableExecutableTask).TaskID())
	}
	require.Equal(t, []int64{task0.TaskID()}, taskIDs)
	require.Equal(t, highWatermark0, *taskTracker.exclusiveHighWatermarkInfo)
}
