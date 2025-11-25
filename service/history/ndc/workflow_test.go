package ndc

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type workflowTestDeps struct {
	controller          *gomock.Controller
	mockContext         *historyi.MockWorkflowContext
	mockMutableState    *historyi.MockMutableState
	mockClusterMetadata *cluster.MockMetadata
	namespaceID         string
	workflowID          string
	runID               string
}

func setupWorkflowTest(t *testing.T) *workflowTestDeps {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockContext := historyi.NewMockWorkflowContext(ctrl)
	mockMutableState := historyi.NewMockMutableState(ctrl)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	return &workflowTestDeps{
		controller:          ctrl,
		mockContext:         mockContext,
		mockMutableState:    mockMutableState,
		mockClusterMetadata: mockClusterMetadata,
		namespaceID:         uuid.New(),
		workflowID:          "some random workflow ID",
		runID:               uuid.New(),
	}
}

func TestGetMethods(t *testing.T) {
	deps := setupWorkflowTest(t)

	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: lastRunningClock,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	nDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		deps.mockContext,
		deps.mockMutableState,
		wcache.NoopReleaseFn,
	)

	require.Equal(t, deps.mockContext, nDCWorkflow.GetContext())
	require.Equal(t, deps.mockMutableState, nDCWorkflow.GetMutableState())
	// NOTE golang does not seem to let people compare functions, easily
	//  link: https://github.com/stretchr/testify/issues/182
	// this is a hack to compare 2 functions, being the same
	expectedReleaseFn := runtime.FuncForPC(reflect.ValueOf(wcache.NoopReleaseFn).Pointer()).Name()
	actualReleaseFn := runtime.FuncForPC(reflect.ValueOf(nDCWorkflow.GetReleaseFn()).Pointer()).Name()
	require.Equal(t, expectedReleaseFn, actualReleaseFn)
	version, clock, err := nDCWorkflow.GetVectorClock()
	require.NoError(t, err)
	require.Equal(t, lastEventVersion, version)
	require.Equal(t, lastRunningClock, clock)
}

func TestHappensAfter_LargerVersion(t *testing.T) {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion - 1
	thatLastRunningClock := int64(123)

	require.True(t, WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func TestHappensAfter_SmallerVersion(t *testing.T) {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion + 1
	thatLastRunningClock := int64(23)

	require.False(t, WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func TestHappensAfter_SameVersion_SmallerTaskID(t *testing.T) {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastRunningClock := thisLastRunningClock + 1

	require.False(t, WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func TestHappensAfter_SameVersion_LatrgerTaskID(t *testing.T) {
	thisLastWriteVersion := int64(0)
	thisLastRunningClock := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastRunningClock := thisLastRunningClock - 1

	require.True(t, WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastRunningClock,
		thatLastWriteVersion,
		thatLastRunningClock,
	))
}

func TestSuppressWorkflowBy_Error(t *testing.T) {
	deps := setupWorkflowTest(t)

	nDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		deps.mockContext,
		deps.mockMutableState,
		wcache.NoopReleaseFn,
	)

	incomingMockContext := historyi.NewMockWorkflowContext(deps.controller)
	incomingMockMutableState := historyi.NewMockMutableState(deps.controller)
	incomingNDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		wcache.NoopReleaseFn,
	)

	// cannot suppress by older workflow
	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: lastRunningClock,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	incomingRunID := uuid.New()
	incomingLastRunningClock := int64(144)
	incomingLastEventVersion := lastEventVersion - 1
	incomingMockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: incomingLastRunningClock,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()

	_, err := nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	require.Error(t, err)
}

func TestSuppressWorkflowBy_Terminate(t *testing.T) {
	deps := setupWorkflowTest(t)

	randomEventID := int64(2208)
	wtFailedEventID := int64(2)
	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	deps.mockMutableState.EXPECT().GetNextEventID().Return(randomEventID).AnyTimes() // This doesn't matter, GetNextEventID is not used if there is started WT.
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: lastRunningClock,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	nDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		deps.mockContext,
		deps.mockMutableState,
		wcache.NoopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingLastRunningClock := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := historyi.NewMockWorkflowContext(deps.controller)
	incomingMockMutableState := historyi.NewMockMutableState(deps.controller)
	incomingNDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		wcache.NoopReleaseFn,
	)
	incomingMockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: incomingLastRunningClock,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastEventVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	deps.mockMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().UpdateCurrentVersion(lastEventVersion, true).Return(nil).AnyTimes()
	startedWorkflowTask := &historyi.WorkflowTaskInfo{
		Version:          1234,
		ScheduledEventID: 5678,
		StartedEventID:   9012,
	}
	deps.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(startedWorkflowTask)
	deps.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		startedWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{EventId: wtFailedEventID}, nil)
	deps.mockMutableState.EXPECT().FlushBufferedEvents()

	deps.mockMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		wtFailedEventID, common.FailureReasonWorkflowTerminationDueToVersionConflict, gomock.Any(), consts.IdentityHistoryService, false, nil,
	).Return(&historypb.HistoryEvent{}, nil)

	// if workflow is in zombie or finished state, keep as is
	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(2)
	deps.mockMutableState.EXPECT().GetCloseVersion().Return(lastEventVersion, nil)
	policy, err := nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	require.NoError(t, err)
	require.Equal(t, historyi.TransactionPolicyPassive, policy)

	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	deps.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil)
	policy, err = nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	require.NoError(t, err)
	require.Equal(t, historyi.TransactionPolicyActive, policy)
}

func TestSuppressWorkflowBy_Zombiefy(t *testing.T) {
	deps := setupWorkflowTest(t)

	lastRunningClock := int64(144)
	lastEventVersion := int64(12)
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: lastRunningClock,
	}
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	executionState := &persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}
	deps.mockMutableState.EXPECT().UpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).
		DoAndReturn(func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
			executionState.State, executionState.Status = state, status
			return true, nil
		}).AnyTimes()

	nDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		deps.mockContext,
		deps.mockMutableState,
		wcache.NoopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingLastRunningClock := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := historyi.NewMockWorkflowContext(deps.controller)
	incomingMockMutableState := historyi.NewMockMutableState(deps.controller)
	incomingNDCWorkflow := NewWorkflow(
		deps.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		wcache.NoopReleaseFn,
	)
	incomingMockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		LastRunningClock: incomingLastRunningClock,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastEventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	// if workflow is in zombie or finished state, keep as is
	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(2)
	deps.mockMutableState.EXPECT().GetCloseVersion().Return(lastEventVersion, nil).AnyTimes()
	policy, err := nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	require.NoError(t, err)
	require.Equal(t, historyi.TransactionPolicyPassive, policy)

	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(2)
	deps.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	policy, err = nDCWorkflow.SuppressBy(incomingNDCWorkflow)
	require.NoError(t, err)
	require.Equal(t, historyi.TransactionPolicyPassive, policy)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, executionState.State)
	require.EqualValues(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, executionState.Status)
}
