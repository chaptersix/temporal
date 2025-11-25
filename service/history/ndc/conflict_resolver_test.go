package ndc

import (
	"context"
	"math/rand"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/util"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type conflictResolverTestDeps struct {
	controller           *gomock.Controller
	mockShard            *shard.ContextTest
	mockContext          *historyi.MockWorkflowContext
	mockMutableState     *historyi.MockMutableState
	mockStateBuilder     *MockStateRebuilder
	logger               log.Logger
	namespaceID          string
	namespace            string
	workflowID           string
	runID                string
	nDCConflictResolver  *ConflictResolverImpl
}

func setupConflictResolverTest(t *testing.T) *conflictResolverTestDeps {
	controller := gomock.NewController(t)
	mockContext := historyi.NewMockWorkflowContext(controller)
	mockMutableState := historyi.NewMockMutableState(controller)
	mockStateBuilder := NewMockStateRebuilder(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	logger := mockShard.GetLogger()

	namespaceID := uuid.New()
	namespace := "some random namespace name"
	workflowID := "some random workflow ID"
	runID := uuid.New()

	nDCConflictResolver := NewConflictResolver(
		mockShard, mockContext, mockMutableState, logger,
	)
	nDCConflictResolver.stateRebuilder = mockStateBuilder

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &conflictResolverTestDeps{
		controller:          controller,
		mockShard:           mockShard,
		mockContext:         mockContext,
		mockMutableState:    mockMutableState,
		mockStateBuilder:    mockStateBuilder,
		logger:              logger,
		namespaceID:         namespaceID,
		namespace:           namespace,
		workflowID:          workflowID,
		runID:               runID,
		nDCConflictResolver: nDCConflictResolver,
	}
}

func TestRebuild(t *testing.T) {
	t.Parallel()
	deps := setupConflictResolverTest(t)

	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	requestID := uuid.New()
	version := int64(12)
	historySize := int64(12345)

	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(5)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID0, version)},
	)
	branchToken1 := []byte("other random branch token")
	lastEventID1 := int64(2)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	require.NoError(t, err)

	deps.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetHistorySize().Return(historySize).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		deps.namespaceID,
		deps.workflowID,
		deps.runID,
	)
	mockRebuildMutableState := historyi.NewMockMutableState(deps.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).AnyTimes()
	mockRebuildMutableState.EXPECT().AddHistorySize(historySize)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	deps.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		util.Ptr(version),
		workflowKey,
		branchToken1,
		requestID,
	).Return(mockRebuildMutableState, rand.Int63(), nil)

	deps.mockContext.EXPECT().Clear()
	rebuiltMutableState, err := deps.nDCConflictResolver.rebuild(ctx, 1, requestID)
	require.NoError(t, err)
	require.NotNil(t, rebuiltMutableState)
	require.Equal(t, int32(1), versionHistories.GetCurrentVersionHistoryIndex())
}

func TestGetOrRebuildCurrentMutableState_NoRebuild_NotCurrent(t *testing.T) {
	t.Parallel()
	deps := setupConflictResolverTest(t)

	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)
	version0 := int64(12)
	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version0)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)
	branchToken1 := []byte("another random branch token")
	lastEventID1 := lastEventID0 + 1
	version1 := version0 + 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version1)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0, versionHistoryItem1},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	require.Nil(t, err)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := deps.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 0, version0)
	require.NoError(t, err)
	require.False(t, isRebuilt)
	require.Equal(t, deps.mockMutableState, rebuiltMutableState)
}

func TestGetOrRebuildCurrentMutableState_NoRebuild_SameIndex(t *testing.T) {
	t.Parallel()
	deps := setupConflictResolverTest(t)

	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(lastEventID, version)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := deps.nDCConflictResolver.GetOrRebuildCurrentMutableState(context.Background(), 0, version)
	require.NoError(t, err)
	require.False(t, isRebuilt)
	require.Equal(t, deps.mockMutableState, rebuiltMutableState)
}

func TestGetOrRebuildCurrentMutableState_Rebuild(t *testing.T) {
	t.Parallel()
	deps := setupConflictResolverTest(t)

	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	version := int64(12)
	incomingVersion := version + 1
	historySize := int64(12345)

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for Rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	require.Nil(t, err)

	deps.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetHistorySize().Return(historySize).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		deps.namespaceID,
		deps.workflowID,
		deps.runID,
	)
	mockRebuildMutableState := historyi.NewMockMutableState(deps.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).AnyTimes()
	mockRebuildMutableState.EXPECT().AddHistorySize(historySize)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	deps.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		util.Ptr(version),
		workflowKey,
		branchToken1,
		gomock.Any(),
	).Return(mockRebuildMutableState, rand.Int63(), nil)

	deps.mockContext.EXPECT().Clear()
	rebuiltMutableState, isRebuilt, err := deps.nDCConflictResolver.GetOrRebuildCurrentMutableState(ctx, 1, incomingVersion)
	require.NoError(t, err)
	require.NotNil(t, rebuiltMutableState)
	require.True(t, isRebuilt)
}

func TestGetOrRebuildMutableState_NoRebuild_SameIndex(t *testing.T) {
	t.Parallel()
	deps := setupConflictResolverTest(t)

	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(lastEventID, version)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := deps.nDCConflictResolver.GetOrRebuildMutableState(context.Background(), 0)
	require.NoError(t, err)
	require.False(t, isRebuilt)
	require.Equal(t, deps.mockMutableState, rebuiltMutableState)
}

func TestGetOrRebuildMutableState_Rebuild(t *testing.T) {
	t.Parallel()
	deps := setupConflictResolverTest(t)

	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	version := int64(12)
	historySize := int64(12345)

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for Rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, versionHistory1)
	require.Nil(t, err)

	deps.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetHistorySize().Return(historySize).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		deps.namespaceID,
		deps.workflowID,
		deps.runID,
	)
	mockRebuildMutableState := historyi.NewMockMutableState(deps.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).AnyTimes()
	mockRebuildMutableState.EXPECT().AddHistorySize(historySize)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	deps.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		util.Ptr(version),
		workflowKey,
		branchToken1,
		gomock.Any(),
	).Return(mockRebuildMutableState, rand.Int63(), nil)

	deps.mockContext.EXPECT().Clear()
	rebuiltMutableState, isRebuilt, err := deps.nDCConflictResolver.GetOrRebuildMutableState(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, rebuiltMutableState)
	require.True(t, isRebuilt)
}
