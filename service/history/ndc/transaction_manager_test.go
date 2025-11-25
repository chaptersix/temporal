package ndc

import (
	"context"
	"math/rand"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

type transactionMgrTestDeps struct {
	controller           *gomock.Controller
	mockShard            *shard.ContextTest
	mockCreateMgr        *MocktransactionMgrForNewWorkflow
	mockUpdateMgr        *MocktransactionMgrForExistingWorkflow
	mockEventsReapplier  *MockEventsReapplier
	mockWorkflowResetter *MockWorkflowResetter
	mockClusterMetadata  *cluster.MockMetadata
	mockExecutionMgr     *persistence.MockExecutionManager
	logger               log.Logger
	namespaceEntry       *namespace.Namespace
	transactionMgr       *transactionMgrImpl
}

func setupTransactionMgrTest(t *testing.T) *transactionMgrTestDeps {
	t.Helper()
	controller := gomock.NewController(t)
	mockCreateMgr := NewMocktransactionMgrForNewWorkflow(controller)
	mockUpdateMgr := NewMocktransactionMgrForExistingWorkflow(controller)
	mockEventsReapplier := NewMockEventsReapplier(controller)
	mockWorkflowResetter := NewMockWorkflowResetter(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockExecutionMgr := mockShard.Resource.ExecutionMgr

	logger := mockShard.GetLogger()
	namespaceEntry := tests.GlobalNamespaceEntry

	transactionMgr := NewTransactionManager(
		mockShard,
		wcache.NewHostLevelCache(mockShard.GetConfig(), mockShard.GetLogger(), metrics.NoopMetricsHandler),
		mockEventsReapplier,
		logger,
		false,
	)
	transactionMgr.createMgr = mockCreateMgr
	transactionMgr.updateMgr = mockUpdateMgr
	transactionMgr.workflowResetter = mockWorkflowResetter

	return &transactionMgrTestDeps{
		controller:           controller,
		mockShard:            mockShard,
		mockCreateMgr:        mockCreateMgr,
		mockUpdateMgr:        mockUpdateMgr,
		mockEventsReapplier:  mockEventsReapplier,
		mockWorkflowResetter: mockWorkflowResetter,
		mockClusterMetadata:  mockClusterMetadata,
		mockExecutionMgr:     mockExecutionMgr,
		logger:               logger,
		namespaceEntry:       namespaceEntry,
		transactionMgr:       transactionMgr,
	}
}

func TestCreateWorkflow(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	targetWorkflow := NewMockWorkflow(deps.controller)

	deps.mockCreateMgr.EXPECT().dispatchForNewWorkflow(
		ctx, targetWorkflow,
	).Return(nil)

	err := deps.transactionMgr.CreateWorkflow(ctx, targetWorkflow)
	require.NoError(t, err)
}

func TestUpdateWorkflow(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	isWorkflowRebuilt := true
	targetWorkflow := NewMockWorkflow(deps.controller)
	newWorkflow := NewMockWorkflow(deps.controller)

	deps.mockUpdateMgr.EXPECT().dispatchForExistingWorkflow(
		ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow,
	).Return(nil)

	err := deps.transactionMgr.UpdateWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	require.NoError(t, err)
}

func TestBackfillWorkflow_CurrentWorkflow_Active_Open(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	releaseCalled := false
	runID := uuid.New()

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	mutableState.EXPECT().VisitUpdates(gomock.Any()).Return()
	mutableState.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(mutableState)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	deps.mockEventsReapplier.EXPECT().ReapplyEvents(ctx, mutableState, updateRegistry, workflowEvents.Events, runID).Return(workflowEvents.Events, nil)

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: runID})
	mutableState.EXPECT().AddHistorySize(historySize)
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyActive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	weContext.EXPECT().UpdateRegistry(ctx).Return(updateRegistry)
	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestBackfillWorkflow_CurrentWorkflow_Active_Closed(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventId := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventId * 2
	lastWorkflowTaskStartedVersion := deps.namespaceEntry.FailoverVersion()
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventId, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	histroySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventId)
	mutableState.EXPECT().AddHistorySize(histroySize)

	deps.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventId,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Any(),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false,
		nil,
	).Return(nil)

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(histroySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestBackfillWorkflow_CurrentWorkflow_Closed_ResetFailed(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventId := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventId * 2
	lastWorkflowTaskStartedVersion := deps.namespaceEntry.FailoverVersion()
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventId, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventId)
	mutableState.EXPECT().AddHistorySize(historySize)

	deps.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventId,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Any(),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false,
		nil,
	).Return(serviceerror.NewInvalidArgument("reset fail"))

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestBackfillWorkflow_CurrentWorkflow_Passive_Open(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestBackfillWorkflow_CurrentWorkflow_Passive_Closed(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestBackfillWorkflow_NotCurrentWorkflow_Active(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestBackfillWorkflow_NotCurrentWorkflow_Passive(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(deps.controller)
	weContext := historyi.NewMockWorkflowContext(deps.controller)
	mutableState := historyi.NewMockMutableState(deps.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(deps.namespaceEntry.IsGlobalNamespace(), deps.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(deps.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), deps.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), deps.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), deps.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	err := deps.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	require.NoError(t, err)
	require.True(t, releaseCalled)
}

func TestCheckWorkflowExists_DoesNotExists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))

	exists, err := deps.transactionMgr.CheckWorkflowExists(ctx, namespaceID, workflowID, runID)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestCheckWorkflowExists_DoesExists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil)

	exists, err := deps.transactionMgr.CheckWorkflowExists(ctx, namespaceID, workflowID, runID)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestGetWorkflowCurrentRunID_Missing(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(nil, serviceerror.NewNotFound(""))

	currentRunID, err := deps.transactionMgr.GetCurrentWorkflowRunID(ctx, namespaceID, workflowID)
	require.NoError(t, err)
	require.Equal(t, "", currentRunID)
}

func TestGetWorkflowCurrentRunID_Exists(t *testing.T) {
	t.Parallel()
	deps := setupTransactionMgrTest(t)
	defer deps.mockShard.StopForTest()

	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	deps.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     deps.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	currentRunID, err := deps.transactionMgr.GetCurrentWorkflowRunID(ctx, namespaceID, workflowID)
	require.NoError(t, err)
	require.Equal(t, runID, currentRunID)
}
