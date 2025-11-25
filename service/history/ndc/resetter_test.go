package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type resetterTestDeps struct {
	controller              *gomock.Controller
	mockShard               *shard.ContextTest
	mockBaseMutableState    *historyi.MockMutableState
	mockRebuiltMutableState *historyi.MockMutableState
	mockTransactionMgr      *MockTransactionManager
	mockStateBuilder        *MockStateRebuilder
	logger                  log.Logger
	mockExecManager         *persistence.MockExecutionManager
	namespaceID             namespace.ID
	namespace               namespace.Name
	workflowID              string
	baseRunID               string
	newContext              historyi.WorkflowContext
	newRunID                string
	workflowResetter        *resetterImpl
}

func setupResetterTest(t *testing.T) *resetterTestDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockBaseMutableState := historyi.NewMockMutableState(controller)
	mockRebuiltMutableState := historyi.NewMockMutableState(controller)
	mockTransactionMgr := NewMockTransactionManager(controller)
	mockStateBuilder := NewMockStateRebuilder(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	mockExecManager := mockShard.Resource.ExecutionMgr

	logger := mockShard.GetLogger()

	namespaceID := namespace.ID(uuid.New())
	namespaceName := namespace.Name("some random namespace name")
	workflowID := "some random workflow ID"
	baseRunID := uuid.New()
	newRunID := uuid.New()
	newContext := workflow.NewContext(
		mockShard.GetConfig(),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			newRunID,
		),
		logger,
		mockShard.GetThrottledLogger(),
		mockShard.GetMetricsHandler(),
	)

	workflowResetter := NewResetter(
		mockShard, mockTransactionMgr, namespaceID, workflowID, baseRunID, newContext, newRunID, logger,
	)
	workflowResetter.stateRebuilder = mockStateBuilder

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &resetterTestDeps{
		controller:              controller,
		mockShard:               mockShard,
		mockBaseMutableState:    mockBaseMutableState,
		mockRebuiltMutableState: mockRebuiltMutableState,
		mockTransactionMgr:      mockTransactionMgr,
		mockStateBuilder:        mockStateBuilder,
		logger:                  logger,
		mockExecManager:         mockExecManager,
		namespaceID:             namespaceID,
		namespace:               namespaceName,
		workflowID:              workflowID,
		baseRunID:               baseRunID,
		newContext:              newContext,
		newRunID:                newRunID,
		workflowResetter:        workflowResetter,
	}
}

func TestResetWorkflow_NoError(t *testing.T) {
	t.Parallel()
	deps := setupResetterTest(t)

	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	baseEventID := lastEventID - 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingVersion := baseVersion + 3

	rebuiltHistorySize := int64(9999)
	newBranchToken := []byte("other random branch token")

	deps.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFnCalled := false
	mockBaseWorkflowReleaseFn := func(err error) {
		mockBaseWorkflowReleaseFnCalled = true
	}
	mockBaseWorkflow := NewMockWorkflow(deps.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(deps.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	deps.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		deps.namespaceID,
		deps.workflowID,
		deps.baseRunID,
		chasm.WorkflowArchetype,
	).Return(mockBaseWorkflow, nil)

	deps.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		now,
		definition.NewWorkflowKey(
			deps.namespaceID.String(),
			deps.workflowID,
			deps.baseRunID,
		),
		branchToken,
		baseEventID,
		util.Ptr(baseVersion),
		definition.NewWorkflowKey(
			deps.namespaceID.String(),
			deps.workflowID,
			deps.newRunID,
		),
		newBranchToken,
		gomock.Any(),
	).Return(deps.mockRebuiltMutableState, rebuiltHistorySize, nil)
	deps.mockRebuiltMutableState.EXPECT().AddHistorySize(rebuiltHistorySize)

	shardID := deps.mockShard.GetShardID()
	deps.mockExecManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: branchToken,
		ForkNodeID:      baseEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(deps.namespaceID.String(), deps.workflowID, deps.newRunID),
		ShardID:         shardID,
		NamespaceID:     deps.namespaceID.String(),
		NewRunID:        deps.newRunID,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: newBranchToken}, nil)

	deps.mockRebuiltMutableState.EXPECT().RefreshExpirationTimeoutTask(gomock.Any()).Return(nil)

	rebuiltMutableState, err := deps.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingVersion,
	)
	require.NoError(t, err)
	require.Equal(t, deps.mockRebuiltMutableState, rebuiltMutableState)
	require.True(t, mockBaseWorkflowReleaseFnCalled)
}

func TestResetWorkflow_Error(t *testing.T) {
	t.Parallel()
	deps := setupResetterTest(t)

	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	baseEventID := lastEventID + 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingFirstEventVersion := baseVersion + 3

	deps.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFn := func(err error) {
	}
	mockBaseWorkflow := NewMockWorkflow(deps.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(deps.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	deps.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		deps.namespaceID,
		deps.workflowID,
		deps.baseRunID,
		chasm.WorkflowArchetype,
	).Return(mockBaseWorkflow, nil)

	rebuiltMutableState, err := deps.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	require.Error(t, err)
	require.IsType(t, &serviceerrors.RetryReplication{}, err)
	require.Nil(t, rebuiltMutableState)

	retryErr, isRetryError := err.(*serviceerrors.RetryReplication)
	require.True(t, isRetryError)
	expectedErr := serviceerrors.NewRetryReplication(
		resendOnResetWorkflowMessage,
		deps.namespaceID.String(),
		deps.workflowID,
		deps.newRunID,
		common.EmptyEventID,
		common.EmptyVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	require.Equal(t, expectedErr, retryErr)
}
