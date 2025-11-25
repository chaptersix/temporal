package ndc

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type bufferEventFlusherTestDeps struct {
	controller              *gomock.Controller
	mockShard               *shard.ContextTest
	mockContext             *historyi.MockWorkflowContext
	mockMutableState        *historyi.MockMutableState
	mockClusterMetadata     *cluster.MockMetadata
	logger                  log.Logger
	namespaceID             string
	workflowID              string
	runID                   string
	nDCBufferEventFlusher   *BufferEventFlusherImpl
}

func setupBufferEventFlusherTest(t *testing.T) *bufferEventFlusherTestDeps {
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
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	logger := mockShard.GetLogger()

	namespaceID := uuid.New()
	workflowID := "some random workflow ID"
	runID := uuid.New()
	nDCBufferEventFlusher := NewBufferEventFlusher(
		mockShard, mockContext, mockMutableState, logger,
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &bufferEventFlusherTestDeps{
		controller:            controller,
		mockShard:             mockShard,
		mockContext:           mockContext,
		mockMutableState:      mockMutableState,
		mockClusterMetadata:   mockClusterMetadata,
		logger:                logger,
		namespaceID:           namespaceID,
		workflowID:            workflowID,
		runID:                 runID,
		nDCBufferEventFlusher: nDCBufferEventFlusher,
	}
}

func TestClearTransientWorkflowTask(t *testing.T) {
	t.Parallel()
	deps := setupBufferEventFlusherTest(t)

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

	deps.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	deps.mockMutableState.EXPECT().HasStartedWorkflowTask().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().IsTransientWorkflowTask().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().ClearTransientWorkflowTask().Return(nil).AnyTimes()

	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      deps.namespaceID,
		WorkflowId:       deps.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: deps.runID,
	}).AnyTimes()

	_, _, err = deps.nDCBufferEventFlusher.flush(context.Background())
	require.NoError(t, err)
}

func TestFlushBufferedEvents(t *testing.T) {
	t.Parallel()
	deps := setupBufferEventFlusherTest(t)

	lastWriteVersion := int64(300)
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

	deps.mockMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	deps.mockMutableState.EXPECT().HasBufferedEvents().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	deps.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	workflowTask := &historyi.WorkflowTaskInfo{
		ScheduledEventID: 1234,
		StartedEventID:   2345,
	}
	deps.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(workflowTask)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	}).AnyTimes()
	deps.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil)
	deps.mockMutableState.EXPECT().AddWorkflowTaskScheduledEvent(
		false,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(&historyi.WorkflowTaskInfo{}, nil)
	deps.mockMutableState.EXPECT().FlushBufferedEvents()
	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	deps.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	deps.mockContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), deps.mockShard).Return(nil)

	_, _, err = deps.nDCBufferEventFlusher.flush(context.Background())
	require.NoError(t, err)
}
