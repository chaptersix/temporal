package pauseactivity

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type testDeps struct {
	controller          *gomock.Controller
	mockShard           *shard.ContextTest
	mockEventsCache     *events.MockCache
	mockNamespaceCache  *namespace.MockRegistry
	mockTaskGenerator   *workflow.MockTaskGenerator
	mockMutableState    *historyi.MockMutableState
	mockClusterMetadata *cluster.MockMetadata
	executionInfo       *persistencespb.WorkflowExecutionInfo
	validator           *api.CommandAttrValidator
	logger              log.Logger
}

func setupTest(t *testing.T) *testDeps {
	t.Helper()

	controller := gomock.NewController(t)
	mockTaskGenerator := workflow.NewMockTaskGenerator(controller)
	mockMutableState := historyi.NewMockMutableState(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	mockNamespaceCache := mockShard.Resource.NamespaceCache
	mockClusterMetadata := mockShard.Resource.ClusterMetadata
	mockEventsCache := mockShard.MockEventsCache
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	logger := mockShard.GetLogger()
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.New(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
	mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()

	validator := api.NewCommandAttrValidator(
		mockShard.GetNamespaceRegistry(),
		mockShard.GetConfig(),
		nil,
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &testDeps{
		controller:          controller,
		mockShard:           mockShard,
		mockEventsCache:     mockEventsCache,
		mockNamespaceCache:  mockNamespaceCache,
		mockTaskGenerator:   mockTaskGenerator,
		mockMutableState:    mockMutableState,
		mockClusterMetadata: mockClusterMetadata,
		executionInfo:       executionInfo,
		validator:           validator,
		logger:              logger,
	}
}

func TestPauseActivityAcceptance(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)

	activityId := "activity_id"
	activityInfo := &persistencespb.ActivityInfo{
		TaskQueue:  "task_queue_name",
		ActivityId: activityId,
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	deps.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(activityInfo, true)
	deps.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any())

	err := workflow.PauseActivity(deps.mockMutableState, activityId, nil)
	require.NoError(t, err)
}
