package history

import (
	"context"
	"errors"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/api/forcedeleteworkflowexecution"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type historyAPITestFixture struct {
	controller         *gomock.Controller
	logger             log.Logger
	mockExecutionMgr   *persistence.MockExecutionManager
	mockVisibilityMgr  *manager.MockVisibilityManager
	mockNamespaceCache *namespace.MockRegistry
}

func setupHistoryAPITest(t *testing.T) *historyAPITestFixture {
	controller := gomock.NewController(t)
	logger := log.NewTestLogger()
	mockExecutionMgr := persistence.NewMockExecutionManager(controller)
	mockVisibilityMgr := manager.NewMockVisibilityManager(controller)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	return &historyAPITestFixture{
		controller:         controller,
		logger:             logger,
		mockExecutionMgr:   mockExecutionMgr,
		mockVisibilityMgr:  mockVisibilityMgr,
		mockNamespaceCache: mockNamespaceCache,
	}
}

func TestDeleteWorkflowExecution_DeleteCurrentExecution(t *testing.T) {
	f := setupHistoryAPITest(t)
	defer f.controller.Finish()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
	}

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
		},
	}

	f.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	f.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	f.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	resp, err := forcedeleteworkflowexecution.Invoke(context.Background(), request, shardID, f.mockExecutionMgr, f.mockVisibilityMgr, f.logger)
	require.Nil(t, resp)
	require.Error(t, err)

	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{BranchToken: []byte("branch1")},
					{BranchToken: []byte("branch2")},
					{BranchToken: []byte("branch3")},
				},
			},
		},
	}

	runID := uuid.New()
	f.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.New(),
		RunID:          runID,
		State:          enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil)
	f.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	f.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	f.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	f.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err = forcedeleteworkflowexecution.Invoke(context.Background(), request, shardID, f.mockExecutionMgr, f.mockVisibilityMgr, f.logger)
	require.NoError(t, err)
}

func TestDeleteWorkflowExecution_LoadMutableStateFailed(t *testing.T) {
	f := setupHistoryAPITest(t)
	defer f.controller.Finish()

	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.New(),
	}

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: &execution,
		},
	}

	f.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	f.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	f.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	f.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	f.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := forcedeleteworkflowexecution.Invoke(context.Background(), request, shardID, f.mockExecutionMgr, f.mockVisibilityMgr, f.logger)
	require.NoError(t, err)
}
