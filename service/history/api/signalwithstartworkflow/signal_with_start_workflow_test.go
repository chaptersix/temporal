package signalwithstartworkflow

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type testDeps struct {
	controller          *gomock.Controller
	shardContext        *historyi.MockShardContext
	namespaceID         string
	workflowID          string
	currentContext      *historyi.MockWorkflowContext
	currentMutableState *historyi.MockMutableState
	currentRunID        string
}

func setupTest(t *testing.T) *testDeps {
	t.Helper()

	controller := gomock.NewController(t)
	shardContext := historyi.NewMockShardContext(controller)

	namespaceID := uuid.New().String()
	workflowID := uuid.New().String()

	currentContext := historyi.NewMockWorkflowContext(controller)
	currentMutableState := historyi.NewMockMutableState(controller)
	currentRunID := uuid.New().String()

	shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()
	shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()

	currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: workflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: currentRunID,
	}).AnyTimes()

	t.Cleanup(func() {
		controller.Finish()
	})

	return &testDeps{
		controller:          controller,
		shardContext:        shardContext,
		namespaceID:         namespaceID,
		workflowID:          workflowID,
		currentContext:      currentContext,
		currentMutableState: currentMutableState,
		currentRunID:        currentRunID,
	}
}

func randomRequest() *workflowservice.SignalWithStartWorkflowExecutionRequest {
	var request workflowservice.SignalWithStartWorkflowExecutionRequest
	_ = fakedata.FakeStruct(&request)
	return &request
}

func TestSignalWorkflow_WorkflowCloseAttempted(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		deps.currentContext,
		wcache.NoopReleaseFn,
		deps.currentMutableState,
	)
	request := randomRequest()

	deps.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(true)
	deps.currentMutableState.EXPECT().HasStartedWorkflowTask().Return(true)

	err := signalWorkflow(
		ctx,
		deps.shardContext,
		currentWorkflowLease,
		request,
	)
	require.Error(t, err)
	require.Equal(t, consts.ErrWorkflowClosing, err)
}

func TestSignalWorkflow_Dedup(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		deps.currentContext,
		wcache.NoopReleaseFn,
		deps.currentMutableState,
	)
	request := randomRequest()

	deps.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	deps.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(true)

	err := signalWorkflow(
		ctx,
		deps.shardContext,
		currentWorkflowLease,
		request,
	)
	require.NoError(t, err)
}

func TestSignalWorkflow_NewWorkflowTask(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		deps.currentContext,
		wcache.NoopReleaseFn,
		deps.currentMutableState,
	)
	request := randomRequest()

	deps.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	deps.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	deps.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	deps.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	deps.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	deps.currentMutableState.EXPECT().HadOrHasWorkflowTask().Return(true)
	deps.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL).Return(&historyi.WorkflowTaskInfo{}, nil)
	deps.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, deps.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		deps.shardContext,
		currentWorkflowLease,
		request,
	)
	require.NoError(t, err)
}

func TestSignalWorkflow_NoNewWorkflowTask(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		deps.currentContext,
		wcache.NoopReleaseFn,
		deps.currentMutableState,
	)
	request := randomRequest()

	deps.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	deps.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	deps.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	deps.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	deps.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(true)
	deps.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, deps.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		deps.shardContext,
		currentWorkflowLease,
		request,
	)
	require.NoError(t, err)
}
