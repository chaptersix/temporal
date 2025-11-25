package ndc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type transactionMgrForExistingWorkflowTestDeps struct {
	controller         *gomock.Controller
	mockTransactionMgr *MockTransactionManager
	mockShard          *historyi.MockShardContext
	updateMgr          *nDCTransactionMgrForExistingWorkflowImpl
}

func setupTransactionMgrForExistingWorkflowTest(t *testing.T) *transactionMgrForExistingWorkflowTestDeps {
	t.Helper()
	controller := gomock.NewController(t)
	mockTransactionMgr := NewMockTransactionManager(controller)
	mockShard := historyi.NewMockShardContext(controller)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.EXPECT().StateMachineRegistry().Return(reg).AnyTimes()

	updateMgr := newNDCTransactionMgrForExistingWorkflow(mockShard, mockTransactionMgr, false)

	return &transactionMgrForExistingWorkflowTestDeps{
		controller:         controller,
		mockTransactionMgr: mockTransactionMgr,
		mockShard:          mockShard,
		updateMgr:          updateMgr,
	}
}

func TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowGuaranteed(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()

	targetContext.EXPECT().UpdateWorkflowExecutionWithNewAsPassive(
		gomock.Any(),
		deps.mockShard,
		newContext,
		newMutableState,
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
}

func TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentRunning_UpdateAsCurrent(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive().Return(nil)

	currentWorkflow := NewMockWorkflow(deps.controller)
	currentContext := historyi.NewMockWorkflowContext(deps.controller)
	currentMutableState := historyi.NewMockMutableState(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
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
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	targetWorkflow.EXPECT().Revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentComplete_UpdateAsCurrent(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive().Return(nil)

	currentWorkflow := NewMockWorkflow(deps.controller)
	currentContext := historyi.NewMockWorkflowContext(deps.controller)
	currentMutableState := historyi.NewMockMutableState(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
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
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil).Times(0)
	targetWorkflow.EXPECT().Revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesNotExists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)
	deps.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		deps.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesExists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)
	deps.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		deps.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForExistingWorkflow_Rebuild_IsCurrent(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
}

func TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsCurrent(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive().Return(nil)

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
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyActive, nil)
	targetWorkflow.EXPECT().Revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		historyi.TransactionPolicyActive.Ptr(),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesNotExists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)
	deps.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}

func TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesExists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrForExistingWorkflowTest(t)
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	targetContext := historyi.NewMockWorkflowContext(deps.controller)
	targetMutableState := historyi.NewMockMutableState(deps.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(deps.controller)
	newContext := historyi.NewMockWorkflowContext(deps.controller)
	newMutableState := historyi.NewMockMutableState(deps.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(deps.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	deps.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	deps.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.ArchetypeAny).Return(currentWorkflow, nil)
	deps.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		deps.mockShard,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
	require.True(t, targetReleaseCalled)
	require.True(t, newReleaseCalled)
	require.True(t, currentReleaseCalled)
}
