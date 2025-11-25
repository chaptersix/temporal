package ndc

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type branchMgrTestDeps struct {
	controller           *gomock.Controller
	mockShard            *shard.ContextTest
	mockContext          *historyi.MockWorkflowContext
	mockMutableState     *historyi.MockMutableState
	mockClusterMetadata  *cluster.MockMetadata
	mockExecutionManager *persistence.MockExecutionManager
	logger               log.Logger
	branchIndex          int
	namespaceID          string
	workflowID           string
	runID                string
	nDCBranchMgr         *BranchMgrImpl
}

func setupBranchMgrTest(t *testing.T) *branchMgrTestDeps {
	controller := gomock.NewController(t)
	mockContext := historyi.NewMockWorkflowContext(controller)
	mockMutableState := historyi.NewMockMutableState(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	mockExecutionManager := mockShard.Resource.ExecutionMgr
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	logger := mockShard.GetLogger()

	namespaceID := uuid.New()
	workflowID := "some random workflow ID"
	runID := uuid.New()
	branchIndex := 0
	nDCBranchMgr := NewBranchMgr(
		mockShard, mockContext, mockMutableState, logger,
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &branchMgrTestDeps{
		controller:           controller,
		mockShard:            mockShard,
		mockContext:          mockContext,
		mockMutableState:     mockMutableState,
		mockClusterMetadata:  mockClusterMetadata,
		mockExecutionManager: mockExecutionManager,
		logger:               logger,
		branchIndex:          branchIndex,
		namespaceID:          namespaceID,
		workflowID:           workflowID,
		runID:                runID,
		nDCBranchMgr:         nDCBranchMgr,
	}
}

func TestCreateNewBranch(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventVersion := int64(200)
	baseBranchLCAEventID := int64(1394)
	baseBranchLastEventVersion := int64(400)
	baseBranchLastEventID := int64(2333)
	versionHistory := versionhistory.NewVersionHistory(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	newBranchToken := []byte("some random new branch token")
	newVersionHistory, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory,
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	require.NoError(t, err)

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	shardID := deps.mockShard.GetShardID()
	deps.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
			require.Equal(t, &persistence.ForkHistoryBranchRequest{
				ForkBranchToken: baseBranchToken,
				ForkNodeID:      baseBranchLCAEventID + 1,
				Info:            input.Info,
				ShardID:         shardID,
				NamespaceID:     deps.namespaceID,
				NewRunID:        input.NewRunID,
			}, input)
			return &persistence.ForkHistoryBranchResponse{
				NewBranchToken: newBranchToken,
			}, nil
		})

	newIndex, err := deps.nDCBranchMgr.createNewBranch(context.Background(), baseBranchToken, baseBranchLCAEventID, newVersionHistory)
	require.Nil(t, err)
	require.Equal(t, int32(1), newIndex)

	compareVersionHistory, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(
		versionHistory,
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	require.NoError(t, err)
	versionhistory.SetVersionHistoryBranchToken(compareVersionHistory, newBranchToken)
	newVersionHistory, err = versionhistory.GetVersionHistory(versionHistories, newIndex)
	require.NoError(t, err)
	require.True(t, compareVersionHistory.Equal(newVersionHistory))
}

func TestGetOrCreate_BranchAppendable_NoMissingEventInBetween(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		versionhistory.NewVersionHistoryItem(200, 300),
	)
	require.NoError(t, err)

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()

	doContinue, index, err := deps.nDCBranchMgr.GetOrCreate(
		context.Background(),
		incomingVersionHistory,
		150+1,
		300)
	require.NoError(t, err)
	require.True(t, doContinue)
	require.Equal(t, int32(0), index)
}

func TestGetOrCreate_BranchAppendable_MissingEventInBetween(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	incomingFirstEventVersionHistoryItem := versionhistory.NewVersionHistoryItem(200, 300)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		incomingFirstEventVersionHistoryItem,
	)
	require.NoError(t, err)

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	_, _, err = deps.nDCBranchMgr.GetOrCreate(
		context.Background(),
		incomingVersionHistory,
		150+2,
		300)
	require.IsType(t, &serviceerrors.RetryReplication{}, err)
}

func TestGetOrCreate_BranchNotAppendable_NoMissingEventInBetween(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)

	versionHistory := versionhistory.NewVersionHistory(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(200, 400),
	})

	newBranchToken := []byte("some random new branch token")
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	shardID := deps.mockShard.GetShardID()
	deps.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
			require.Equal(t, &persistence.ForkHistoryBranchRequest{
				ForkBranchToken: baseBranchToken,
				ForkNodeID:      baseBranchLCAEventID + 1,
				Info:            input.Info,
				ShardID:         shardID,
				NamespaceID:     deps.namespaceID,
				NewRunID:        input.NewRunID,
			}, input)
			return &persistence.ForkHistoryBranchResponse{
				NewBranchToken: newBranchToken,
			}, nil
		})

	doContinue, index, err := deps.nDCBranchMgr.GetOrCreate(
		context.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+1,
		baseBranchLCAEventVersion,
	)
	require.NoError(t, err)
	require.True(t, doContinue)
	require.Equal(t, int32(1), index)
}

func TestGetOrCreate_BranchNotAppendable_MissingEventInBetween(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)
	baseBranchLastEventID := int64(150)
	baseBranchLastEventVersion := int64(300)

	versionHistory := versionhistory.NewVersionHistory(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(200, 400),
	})

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	_, _, err := deps.nDCBranchMgr.GetOrCreate(
		context.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+2,
		baseBranchLCAEventVersion,
	)
	require.IsType(t, &serviceerrors.RetryReplication{}, err)
}

func TestCreate_NoMissingEventInBetween(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(150)
	baseBranchLCAEventVersion := int64(300)
	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID+50, baseBranchLCAEventVersion),
	)
	require.NoError(t, err)

	newBranchToken := []byte("some random new branch token")
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	shardID := deps.mockShard.GetShardID()
	deps.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
			require.Equal(t, &persistence.ForkHistoryBranchRequest{
				ForkBranchToken: baseBranchToken,
				ForkNodeID:      baseBranchLCAEventID + 1,
				Info:            input.Info,
				ShardID:         shardID,
				NamespaceID:     deps.namespaceID,
				NewRunID:        input.NewRunID,
			}, input)
			return &persistence.ForkHistoryBranchResponse{
				NewBranchToken: newBranchToken,
			}, nil
		})

	doContinue, index, err := deps.nDCBranchMgr.Create(
		context.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+1,
		baseBranchLCAEventVersion,
	)
	require.NoError(t, err)
	require.True(t, doContinue)
	require.Equal(t, int32(1), index)
}

func TestCreate_MissingEventInBetween(t *testing.T) {
	t.Parallel()
	deps := setupBranchMgrTest(t)

	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	incomingFirstEventVersionHistoryItem := versionhistory.NewVersionHistoryItem(200, 300)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		incomingFirstEventVersionHistoryItem,
	)
	require.NoError(t, err)

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	_, _, err = deps.nDCBranchMgr.Create(
		context.Background(),
		incomingVersionHistory,
		150+2,
		300)
	require.IsType(t, &serviceerrors.RetryReplication{}, err)
}
