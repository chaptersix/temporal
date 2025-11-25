package signalworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type signalWorkflowTestDeps struct {
	controller                 *gomock.Controller
	shardContext               *historyi.MockShardContext
	namespaceRegistry          *namespace.MockRegistry
	workflowCache              *wcache.MockCache
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	currentContext             *historyi.MockWorkflowContext
	currentMutableState        *historyi.MockMutableState
}

func setupSignalWorkflowTest(t *testing.T) *signalWorkflowTestDeps {
	controller := gomock.NewController(t)

	namespaceRegistry := namespace.NewMockRegistry(controller)
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	shardContext := historyi.NewMockShardContext(controller)
	shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()
	shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()

	currentMutableState := historyi.NewMockMutableState(controller)
	currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()

	currentContext := historyi.NewMockWorkflowContext(controller)
	currentContext.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(currentMutableState, nil).AnyTimes()

	workflowCache := wcache.NewMockCache(controller)
	workflowCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.WorkflowArchetype, locks.PriorityHigh).
		Return(currentContext, wcache.NoopReleaseFn, nil).AnyTimes()

	workflowConsistencyChecker := api.NewWorkflowConsistencyChecker(
		shardContext,
		workflowCache,
	)

	return &signalWorkflowTestDeps{
		controller:                 controller,
		shardContext:               shardContext,
		namespaceRegistry:          namespaceRegistry,
		workflowCache:              workflowCache,
		workflowConsistencyChecker: workflowConsistencyChecker,
		currentContext:             currentContext,
		currentMutableState:        currentMutableState,
	}
}

func TestSignalWorkflow_WorkflowCloseAttempted(t *testing.T) {
	deps := setupSignalWorkflowTest(t)
	defer deps.controller.Finish()

	deps.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	deps.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(true)
	deps.currentMutableState.EXPECT().HasStartedWorkflowTask().Return(true)

	resp, err := Invoke(
		context.Background(),
		&historyservice.SignalWorkflowExecutionRequest{
			NamespaceId: tests.NamespaceID.String(),
			SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: tests.Namespace.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      tests.RunID,
				},
				SignalName: "signal-name",
				Input:      nil,
			},
		},
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.Nil(t, resp)
	require.Error(t, err, consts.ErrWorkflowClosing)
}
