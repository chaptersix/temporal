package ndc

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type transactionMgrForNewWorkflowTestDeps struct {
	controller         *gomock.Controller
	mockTransactionMgr *MockTransactionManager
	mockShard          *historyi.MockShardContext
	createMgr          *nDCTransactionMgrForNewWorkflowImpl
}

func setupTransactionMgrForNewWorkflowTest(t *testing.T) *transactionMgrForNewWorkflowTestDeps {
	t.Helper()
	controller := gomock.NewController(t)
	mockTransactionMgr := NewMockTransactionManager(controller)
	mockShard := historyi.NewMockShardContext(controller)

	createMgr := newTransactionMgrForNewWorkflow(mockShard, mockTransactionMgr, false)

	return &transactionMgrForNewWorkflowTestDeps{
		controller:         controller,
		mockTransactionMgr: mockTransactionMgr,
		mockShard:          mockShard,
		createMgr:          createMgr,
	}
}

func TestDispatchForNewWorkflow_Dup(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	newWorkflow := NewMockWorkflow(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	newWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()

	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(runID, nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, newWorkflow)
	require.ErrorIs(t, err, consts.ErrDuplicate)
}

func TestDispatchForNewWorkflow_BrandNew(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	newWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	workflowSnapshot := &persistence.WorkflowSnapshot{}
	workflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().CloseTransactionAsSnapshot(historyi.TransactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(
		ctx, namespaceID, workflowID,
	).Return("", nil)

	weContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
		mutableState,
		workflowSnapshot,
		workflowEventsSeq,
	).Return(nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, newWorkflow)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestDispatchForNewWorkflow_CreateAsCurrent(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"
	currentLastWriteVersion := int64(4321)

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	currentMutableState := historyi.NewMockMutableState(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: currentRunID,
	}).AnyTimes()
	currentWorkflow.EXPECT().GetVectorClock().Return(currentLastWriteVersion, int64(0), nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentRunID,
		currentLastWriteVersion,
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	).Return(nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForNewWorkflow_CreateAsZombie(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID.String(),
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	).Return(nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, targetWorkflowEventsSeq).Return(nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForNewWorkflow_CreateAsZombie_ReapplyCandidates(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID.String(),
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{}

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	eventReapplyCandidates := []*historypb.HistoryEvent{{
		EventId: common.FirstEventID + rand.Int63(),
	}}
	eventsToApply := []*persistence.WorkflowEvents{
		{
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
			RunID:       targetRunID,
			Events:      eventReapplyCandidates,
		},
	}
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(eventReapplyCandidates)

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	).Return(nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, eventsToApply).Return(nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForNewWorkflow_CreateAsZombie_Dedup(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID.String(),
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	).Return(&persistence.WorkflowConditionFailedError{})
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, targetWorkflowEventsSeq).Return(nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForNewWorkflow_SuppressCurrentAndCreateAsCurrent(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForNewWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	currentContext := historyi.NewMockWorkflowContext(deps.controller)
	currentMutableState := historyi.NewMockMutableState(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()

	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflowPolicy := historyi.TransactionPolicyActive
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil)
	targetWorkflow.EXPECT().Revive().Return(nil)

	currentContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		deps.mockShard,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetContext,
		targetMutableState,
		currentWorkflowPolicy,
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := deps.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, currentReleaseCalled)
}
