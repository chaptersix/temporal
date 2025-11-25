package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type transactionTestDeps struct {
	controller         *gomock.Controller
	mockShard          *historyi.MockShardContext
	mockEngine         *historyi.MockEngine
	mockNamespaceCache *namespace.MockRegistry
	logger             log.Logger
	transaction        *TransactionImpl
}

func setupTransactionTest(t *testing.T) *transactionTestDeps {
	controller := gomock.NewController(t)
	mockShard := historyi.NewMockShardContext(controller)
	mockEngine := historyi.NewMockEngine(controller)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	logger := log.NewTestLogger()

	mockShard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	mockShard.EXPECT().GetEngine(gomock.Any()).Return(mockEngine, nil).AnyTimes()
	mockShard.EXPECT().GetNamespaceRegistry().Return(mockNamespaceCache).AnyTimes()
	mockShard.EXPECT().GetLogger().Return(logger).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	transaction := NewTransaction(mockShard)

	return &transactionTestDeps{
		controller:         controller,
		mockShard:          mockShard,
		mockEngine:         mockEngine,
		mockNamespaceCache: mockNamespaceCache,
		logger:             logger,
		transaction:        transaction,
	}
}

func setupMockForTaskNotification(mockEngine *historyi.MockEngine) {
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)
}

func TestOperationMayApplied(t *testing.T) {
	testCases := []struct {
		err        error
		mayApplied bool
	}{
		{err: &persistence.CurrentWorkflowConditionFailedError{}, mayApplied: false},
		{err: &persistence.WorkflowConditionFailedError{}, mayApplied: false},
		{err: &persistence.ConditionFailedError{}, mayApplied: false},
		{err: &persistence.ShardOwnershipLostError{}, mayApplied: false},
		{err: &persistence.InvalidPersistenceRequestError{}, mayApplied: false},
		{err: &persistence.TransactionSizeLimitError{}, mayApplied: false},
		{err: &serviceerror.ResourceExhausted{}, mayApplied: false},
		{err: &serviceerror.NotFound{}, mayApplied: false},
		{err: &serviceerror.NamespaceNotFound{}, mayApplied: false},
		{err: nil, mayApplied: true},
		{err: &persistence.TimeoutError{}, mayApplied: true},
		{err: &serviceerror.Unavailable{}, mayApplied: true},
		{err: errors.New("some unknown error"), mayApplied: true},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.mayApplied, persistence.OperationPossiblySucceeded(tc.err))
	}
}

func TestCreateWorkflowExecution_NotifyTaskWhenFailed(t *testing.T) {
	deps := setupTransactionTest(t)
	defer deps.controller.Finish()

	timeoutErr := &persistence.TimeoutError{}
	require.True(t, persistence.OperationPossiblySucceeded(timeoutErr))

	deps.mockShard.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	setupMockForTaskNotification(deps.mockEngine)

	_, err := deps.transaction.CreateWorkflowExecution(
		context.Background(),
		persistence.CreateWorkflowModeBrandNew,
		0,
		&persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	require.Equal(t, timeoutErr, err)
}

func TestUpdateWorkflowExecution_NotifyTaskWhenFailed(t *testing.T) {
	deps := setupTransactionTest(t)
	defer deps.controller.Finish()

	timeoutErr := &persistence.TimeoutError{}
	require.True(t, persistence.OperationPossiblySucceeded(timeoutErr))

	deps.mockShard.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	setupMockForTaskNotification(deps.mockEngine) // for current workflow mutation
	setupMockForTaskNotification(deps.mockEngine) // for new workflow snapshot

	_, _, err := deps.transaction.UpdateWorkflowExecution(
		context.Background(),
		persistence.UpdateWorkflowModeUpdateCurrent,
		0,
		&persistence.WorkflowMutation{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		util.Ptr(int64(0)),
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	require.Equal(t, timeoutErr, err)
}

func TestUpdateWorkflowExecution_CompletionMetrics(t *testing.T) {
	deps := setupTransactionTest(t)
	defer deps.controller.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	deps.mockShard.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
	deps.mockShard.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	deps.mockShard.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()

	deps.mockShard.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil).AnyTimes()
	deps.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	deps.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	cases := []struct {
		name                   string
		updateMode             persistence.UpdateWorkflowMode
		expectCompletionMetric bool
	}{
		{
			name:                   "UpdateCurrent",
			updateMode:             persistence.UpdateWorkflowModeUpdateCurrent,
			expectCompletionMetric: true,
		},
		{
			name:                   "BypassCurrent",
			updateMode:             persistence.UpdateWorkflowModeBypassCurrent,
			expectCompletionMetric: true,
		},
		{
			name:                   "IgnoreCurrent",
			updateMode:             persistence.UpdateWorkflowModeIgnoreCurrent,
			expectCompletionMetric: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			capture := metricsHandler.StartCapture()

			_, _, err := deps.transaction.UpdateWorkflowExecution(
				context.Background(),
				tc.updateMode,
				0,
				&persistence.WorkflowMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId:      tests.NamespaceID.String(),
						WorkflowId:       tests.WorkflowID,
						VersionHistories: &historyspb.VersionHistories{},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:  tests.RunID,
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
					},
				},
				[]*persistence.WorkflowEvents{},
				nil,
				nil,
				nil,
				true, // isWorkflow
			)
			require.NoError(t, err)

			snapshot := capture.Snapshot()
			completionMetric := snapshot[metrics.WorkflowSuccessCount.Name()]

			if tc.expectCompletionMetric {
				require.Len(t, completionMetric, 1)
			} else {
				require.Empty(t, completionMetric)
			}

			metricsHandler.StopCapture(capture)
		})
	}

}

func TestConflictResolveWorkflowExecution_NotifyTaskWhenFailed(t *testing.T) {
	deps := setupTransactionTest(t)
	defer deps.controller.Finish()

	timeoutErr := &persistence.TimeoutError{}
	require.True(t, persistence.OperationPossiblySucceeded(timeoutErr))

	deps.mockShard.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, timeoutErr)
	setupMockForTaskNotification(deps.mockEngine) // for reset workflow snapshot
	setupMockForTaskNotification(deps.mockEngine) // for new workflow snapshot
	setupMockForTaskNotification(deps.mockEngine) // for current workflow mutation

	_, _, _, err := deps.transaction.ConflictResolveWorkflowExecution(
		context.Background(),
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		0,
		&persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		util.Ptr(int64(0)),
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		util.Ptr(int64(0)),
		&persistence.WorkflowMutation{},
		[]*persistence.WorkflowEvents{},
		true, // isWorkflow
	)
	require.Equal(t, timeoutErr, err)
}
