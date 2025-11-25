package isactivitytaskvalid

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

	_, err := isActivityTaskValid(deps.workflowLease, rand.Int63(), rand.Int31())
	require.Error(t, err)
	require.IsType(t, &serviceerror.NotFound{}, err)
}

func TestWorkflowRunning_ActivityTaskNotStarted(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	activityScheduleEventID := rand.Int63()
	stamp := rand.Int31()
	deps.mutableState.EXPECT().GetActivityInfo(activityScheduleEventID).Return(&persistencespb.ActivityInfo{
		ScheduledEventId: activityScheduleEventID,
		StartedEventId:   common.EmptyEventID,
		Stamp:            stamp,
	}, true)

	valid, err := isActivityTaskValid(deps.workflowLease, activityScheduleEventID, stamp)
	require.NoError(t, err)
	require.True(t, valid)
}

func TestWorkflowRunning_ActivityTaskStarted(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	activityScheduleEventID := rand.Int63()
	stamp := rand.Int31()
	deps.mutableState.EXPECT().GetActivityInfo(activityScheduleEventID).Return(&persistencespb.ActivityInfo{
		ScheduledEventId: activityScheduleEventID,
		StartedEventId:   activityScheduleEventID + 1,
		Stamp:            stamp,
	}, true)

	valid, err := isActivityTaskValid(deps.workflowLease, activityScheduleEventID, stamp)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestWorkflowRunning_ActivityTaskStampMismatch(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	activityScheduleEventID := rand.Int63()
	const storedStamp = int32(456)
	deps.mutableState.EXPECT().GetActivityInfo(activityScheduleEventID).Return(&persistencespb.ActivityInfo{
		ScheduledEventId: activityScheduleEventID,
		StartedEventId:   common.EmptyEventID,
		Stamp:            storedStamp,
	}, true)

	valid, err := isActivityTaskValid(deps.workflowLease, activityScheduleEventID, storedStamp+1)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestWorkflowRunning_ActivityTaskStampLegacy(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	activityScheduleEventID := rand.Int63()
	deps.mutableState.EXPECT().GetActivityInfo(activityScheduleEventID).Return(&persistencespb.ActivityInfo{
		ScheduledEventId: activityScheduleEventID,
		StartedEventId:   common.EmptyEventID,
		Stamp:            0,
	}, true)

	valid, err := isActivityTaskValid(deps.workflowLease, activityScheduleEventID, 0)
	require.NoError(t, err)
	require.True(t, valid)
}

func TestWorkflowRunning_ActivityTaskMissing(t *testing.T) {
	deps := setupAPITest(t)

	deps.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	activityScheduleEventID := rand.Int63()
	deps.mutableState.EXPECT().GetActivityInfo(activityScheduleEventID).Return(nil, false)

	valid, err := isActivityTaskValid(deps.workflowLease, activityScheduleEventID, rand.Int31())
	require.NoError(t, err)
	require.False(t, valid)
}
