package isworkflowtaskvalid

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type apiTestDeps struct {
	controller      *gomock.Controller
	workflowLease   api.WorkflowLease
	workflowContext *historyi.MockWorkflowContext
	mutableState    *historyi.MockMutableState
}

func setupAPITest(t *testing.T) *apiTestDeps {
	controller := gomock.NewController(t)
	workflowContext := historyi.NewMockWorkflowContext(controller)
	mutableState := historyi.NewMockMutableState(controller)
	workflowLease := api.NewWorkflowLease(
		workflowContext,
		func(err error) {},
		mutableState,
	)

	t.Cleanup(func() {
		controller.Finish()
	})

	return &apiTestDeps{
		controller:      controller,
		workflowLease:   workflowLease,
		workflowContext: workflowContext,
		mutableState:    mutableState,
	}
}

func TestWorkflowCompleted(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	_, err := isWorkflowTaskValid(deps.workflowLease, rand.Int63(), 0)
	require.Error(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)
}

func TestWorkflowRunning_WorkflowTaskNotStarted(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	deps.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskScheduleEventID,
		StartedEventID:   common.EmptyEventID,
	})

	valid, err := isWorkflowTaskValid(deps.workflowLease, workflowTaskScheduleEventID, 0)
	require.NoError(t, err)
	require.True(t, valid)
}

func TestWorkflowRunning_WorkflowTaskStarted(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	deps.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskScheduleEventID,
		StartedEventID:   workflowTaskScheduleEventID + 10,
	})

	valid, err := isWorkflowTaskValid(deps.workflowLease, workflowTaskScheduleEventID, 0)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestWorkflowRunning_WorkflowTaskMissing(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	deps.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(nil)

	valid, err := isWorkflowTaskValid(deps.workflowLease, workflowTaskScheduleEventID, 0)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestWorkflowRunning_WorkflowTask_StampInvalid(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	deps.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskScheduleEventID,
		StartedEventID:   common.EmptyEventID,
		Stamp:            1,
	})

	valid, err := isWorkflowTaskValid(deps.workflowLease, workflowTaskScheduleEventID, 0)
	require.NoError(t, err)
	require.False(t, valid)
}
