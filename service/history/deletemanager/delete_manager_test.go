package deletemanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type deleteManagerTestDeps struct {
	controller            *gomock.Controller
	mockCache             *wcache.MockCache
	mockShardContext      *historyi.MockShardContext
	mockClock             *clock.EventTimeSource
	mockNamespaceRegistry *namespace.MockRegistry
	mockMetadata          *cluster.MockMetadata
	mockVisibilityManager *manager.MockVisibilityManager
	deleteManager         DeleteManager
}

func setupDeleteManagerTest(t *testing.T) *deleteManagerTestDeps {
	controller := gomock.NewController(t)
	mockCache := wcache.NewMockCache(controller)
	mockClock := clock.NewEventTimeSource()
	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockMetadata := cluster.NewMockMetadata(controller)
	mockVisibilityManager := manager.NewMockVisibilityManager(controller)
	mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()

	config := tests.NewDynamicConfig()
	mockShardContext := historyi.NewMockShardContext(controller)
	mockShardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	mockShardContext.EXPECT().GetNamespaceRegistry().Return(mockNamespaceRegistry).AnyTimes()
	mockShardContext.EXPECT().GetClusterMetadata().Return(mockMetadata).AnyTimes()

	deleteManager := NewDeleteManager(
		mockShardContext,
		mockCache,
		config,
		mockClock,
		mockVisibilityManager,
	)

	return &deleteManagerTestDeps{
		controller:            controller,
		mockCache:             mockCache,
		mockShardContext:      mockShardContext,
		mockClock:             mockClock,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockMetadata:          mockMetadata,
		mockVisibilityManager: mockVisibilityManager,
		deleteManager:         deleteManager,
	}
}

func TestDeleteDeletedWorkflowExecution(t *testing.T) {
	deps := setupDeleteManagerTest(t)

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	closeExecutionVisibilityTaskID := int64(39)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseVisibilityTaskId: closeExecutionVisibilityTaskID,
	})
	stage := tasks.DeleteWorkflowExecutionStageNone

	deps.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		[]byte{22, 8, 78},
		closeExecutionVisibilityTaskID,
		time.Unix(0, 0).UTC(),
		&stage,
	).Return(nil)
	mockWeCtx.EXPECT().Clear()

	err := deps.deleteManager.DeleteWorkflowExecution(
		context.Background(),
		tests.NamespaceID,
		&we,
		mockWeCtx,
		mockMutableState,
		&stage,
	)
	require.NoError(t, err)
}

func TestDeleteDeletedWorkflowExecution_Error(t *testing.T) {
	deps := setupDeleteManagerTest(t)

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	closeExecutionVisibilityTaskID := int64(39)
	mockMutableState.EXPECT().GetExecutionInfo().MinTimes(1).Return(&persistencespb.WorkflowExecutionInfo{
		CloseVisibilityTaskId: closeExecutionVisibilityTaskID,
	})
	stage := tasks.DeleteWorkflowExecutionStageNone

	deps.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		[]byte{22, 8, 78},
		closeExecutionVisibilityTaskID,
		time.Unix(0, 0).UTC(),
		&stage,
	).Return(serviceerror.NewInternal("test error"))

	err := deps.deleteManager.DeleteWorkflowExecution(
		context.Background(),
		tests.NamespaceID,
		&we,
		mockWeCtx,
		mockMutableState,
		&stage,
	)
	require.Error(t, err)
}

func TestDeleteWorkflowExecution_OpenWorkflow(t *testing.T) {
	deps := setupDeleteManagerTest(t)

	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := historyi.NewMockWorkflowContext(deps.controller)
	mockMutableState := historyi.NewMockMutableState(deps.controller)
	closeExecutionVisibilityTaskID := int64(39)
	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	mockMutableState.EXPECT().GetExecutionInfo().MinTimes(1).Return(&persistencespb.WorkflowExecutionInfo{
		CloseVisibilityTaskId: closeExecutionVisibilityTaskID,
	})
	stage := tasks.DeleteWorkflowExecutionStageNone

	deps.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		[]byte{22, 8, 78},
		closeExecutionVisibilityTaskID,
		time.Unix(0, 0).UTC(),
		&stage,
	).Return(nil)
	mockWeCtx.EXPECT().Clear()

	err := deps.deleteManager.DeleteWorkflowExecution(
		context.Background(),
		tests.NamespaceID,
		&we,
		mockWeCtx,
		mockMutableState,
		&stage,
	)
	require.NoError(t, err)
}
