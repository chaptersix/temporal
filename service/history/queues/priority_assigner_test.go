package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

func TestPriorityAssigner_Assign_SelectedTaskTypes(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	priorityAssigner := NewPriorityAssigner().(*priorityAssignerImpl)

	mockExecutable := NewMockExecutable(controller)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT).Times(1)

	require.Equal(t, tasks.PriorityPreemptable, priorityAssigner.Assign(mockExecutable))
}

func TestPriorityAssigner_Assign_UnknownTaskTypes(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	priorityAssigner := NewPriorityAssigner().(*priorityAssignerImpl)

	mockExecutable := NewMockExecutable(controller)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TaskType(1234)).Times(1)

	require.Equal(t, tasks.PriorityPreemptable, priorityAssigner.Assign(mockExecutable))
}

func TestPriorityAssigner_Assign_HighPriorityTaskTypes(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	priorityAssigner := NewPriorityAssigner().(*priorityAssignerImpl)

	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_USER_TIMER,
		enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
	} {
		mockExecutable := NewMockExecutable(controller)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		require.Equal(t, tasks.PriorityHigh, priorityAssigner.Assign(mockExecutable))
	}
}

func TestPriorityAssigner_Assign_BackgroundPriorityTaskTypes(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	priorityAssigner := NewPriorityAssigner().(*priorityAssignerImpl)

	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		enumsspb.TASK_TYPE_UNSPECIFIED,
	} {
		mockExecutable := NewMockExecutable(controller)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		require.Equal(t, tasks.PriorityPreemptable, priorityAssigner.Assign(mockExecutable))
	}
}

func TestPriorityAssigner_Assign_LowPriorityTaskTypes(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	priorityAssigner := NewPriorityAssigner().(*priorityAssignerImpl)

	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT,
	} {
		mockExecutable := NewMockExecutable(controller)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		require.Equal(t, tasks.PriorityLow, priorityAssigner.Assign(mockExecutable))
	}
}
