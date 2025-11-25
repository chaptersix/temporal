package replication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

func setupBatchedTask(t *testing.T) (*gomock.Controller, log.Logger, metrics.Handler) {
	controller := gomock.NewController(t)
	logger := log.NewTestLogger()
	metricsHandler := metrics.NoopMetricsHandler
	return controller, logger, metricsHandler
}

func TestAddTask_batchStateClose_DoNotBatch_ReturnFalse(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	incomingTask := NewMockTrackableExecutableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     incomingTask,
		individualTasks: append([]TrackableExecutableTask{}, incomingTask),
		state:           batchStateClose,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	result := batchedTestTask.AddTask(NewMockTrackableExecutableTask(controller))
	require.False(t, result)
}

func TestAddTask_ExistingTaskIsNotBatchable_DoNotBatch_ReturnFalse(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockTrackableExecutableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockTrackableExecutableTask(controller)
	result := batchedTestTask.AddTask(incoming)
	require.False(t, result)
}

func TestAddTask_IncomingTaskIsNotBatchable_DoNotBatch_ReturnFalse(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockTrackableExecutableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(controller)
	result := batchedTestTask.AddTask(incoming)
	require.False(t, result)
}

func TestAddTask_ExistingTaskDoesNotWantToBatch_DoNotBatch_ReturnFalse(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	existing.EXPECT().CanBatch().Return(false).Times(1)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(controller)
	incoming.EXPECT().CanBatch().Return(true).Times(1)

	result := batchedTestTask.AddTask(incoming)
	require.False(t, result)
}

func TestAddTask_IncomingTaskDoesNotWantToBatch_DoNotBatch_ReturnFalse(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(controller)
	incoming.EXPECT().CanBatch().Return(false).Times(1)

	result := batchedTestTask.AddTask(incoming)
	require.False(t, result)
}

func TestAddTask_TasksAreBatchableAndCanBatch_ReturnTrue(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	existing.EXPECT().CanBatch().Return(true).Times(1)

	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(controller)
	incoming.EXPECT().CanBatch().Return(true).Times(1)

	batchResult := NewMockTrackableExecutableTask(controller)
	existing.EXPECT().BatchWith(incoming).Return(batchResult, true).Times(1)
	result := batchedTestTask.AddTask(incoming)

	require.True(t, result)

	// verify individual tasks
	require.True(t, batchResult == batchedTestTask.batchedTask)
	require.Len(t, batchedTestTask.individualTasks, 2)
	require.True(t, existing == batchedTestTask.individualTasks[0])
	require.True(t, incoming == batchedTestTask.individualTasks[1])
}

func TestExecute_SetBatchStateToClose_ReturnResult(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	err := errors.New("some error")
	existing.EXPECT().Execute().Return(err).Times(1)
	result := batchedTestTask.Execute()

	require.Equal(t, batchState(batchStateClose), batchedTestTask.state)
	require.Equal(t, err, result)
}

func TestAck_AckIndividualTasks(t *testing.T) {
	controller, _, metricsHandler := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	add1 := NewMockBatchableTask(controller)
	add2 := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
		metricsHandler: metricsHandler,
	}
	existing.EXPECT().Ack().Times(1)
	add1.EXPECT().Ack().Times(1)
	add2.EXPECT().Ack().Times(1)

	batchedTestTask.Ack()
}

func TestAbort_AbortIndividualTasks(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	add1 := NewMockBatchableTask(controller)
	add2 := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Abort().Times(1)
	add1.EXPECT().Abort().Times(1)
	add2.EXPECT().Abort().Times(1)

	batchedTestTask.Abort()
}

func TestCancel_CancelIndividualTasks(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	add1 := NewMockBatchableTask(controller)
	add2 := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Cancel().Times(1)
	add1.EXPECT().Cancel().Times(1)
	add2.EXPECT().Cancel().Times(1)

	batchedTestTask.Cancel()
}

func TestNack_SingleItem_NackTheTask(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Nack(nil).Times(1)

	batchedTestTask.Nack(nil)
}

func TestNack_MultipleItems_CallIndividualHandler(t *testing.T) {
	controller, logger, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	add1 := NewMockBatchableTask(controller)
	add2 := NewMockBatchableTask(controller)
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			task.Cancel()
			task.Abort()
			task.Reschedule()
		},
		logger: logger,
	}
	existing.EXPECT().Cancel().Times(1)
	existing.EXPECT().MarkUnbatchable().Times(1)
	existing.EXPECT().Abort().Times(1)
	existing.EXPECT().Reschedule().Times(1)
	add1.EXPECT().Cancel().Times(1)
	add1.EXPECT().Abort().Times(1)
	add1.EXPECT().Reschedule().Times(1)
	add1.EXPECT().MarkUnbatchable().Times(1)

	add2.EXPECT().Cancel().Times(1)
	add2.EXPECT().Abort().Times(1)
	add2.EXPECT().Reschedule().Times(1)
	add2.EXPECT().MarkUnbatchable().Times(1)

	batchedTestTask.Nack(nil)
}

func TestMarkPoisonPill_SingleItem_MarkTheTask(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().MarkPoisonPill().Return(nil).Times(1)

	result := batchedTestTask.MarkPoisonPill()
	require.Nil(t, result)
}

func TestReschedule_SingleItem_RescheduleTheTask(t *testing.T) {
	controller, _, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Reschedule().Times(1)

	batchedTestTask.Reschedule()
	require.Equal(t, 0, handlerCallCount)
}

func TestMarkPoisonPill_MultipleItems_CallIndividualHandler(t *testing.T) {
	controller, logger, _ := setupBatchedTask(t)
	defer controller.Finish()

	existing := NewMockBatchableTask(controller)
	add1 := NewMockBatchableTask(controller)
	add2 := NewMockBatchableTask(controller)
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			task.Cancel()
			task.Abort()
			task.Reschedule()
		},
		logger: logger,
	}
	existing.EXPECT().Cancel().Times(1)
	existing.EXPECT().MarkUnbatchable().Times(1)
	existing.EXPECT().Abort().Times(1)
	existing.EXPECT().Reschedule().Times(1)
	add1.EXPECT().Cancel().Times(1)
	add1.EXPECT().Abort().Times(1)
	add1.EXPECT().Reschedule().Times(1)
	add1.EXPECT().MarkUnbatchable().Times(1)

	add2.EXPECT().Cancel().Times(1)
	add2.EXPECT().Abort().Times(1)
	add2.EXPECT().Reschedule().Times(1)
	add2.EXPECT().MarkUnbatchable().Times(1)

	result := batchedTestTask.MarkPoisonPill()
	require.Nil(t, result)
}
