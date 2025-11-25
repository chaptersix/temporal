package verifyfirstworkflowtaskscheduled

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

var defaultWorkflowTaskCompletionLimits = historyi.WorkflowTaskCompletionLimits{MaxResetPoints: primitives.DefaultHistoryMaxAutoResetPoints, MaxSearchAttributeValueSize: 2048}

type verifyFirstWorkflowTaskScheduledTestDeps struct {
	controller                 *gomock.Controller
	mockEventsCache            *events.MockCache
	mockExecutionMgr           *persistence.MockExecutionManager
	shardContext               *shard.ContextTest
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	logger                     log.Logger
}

func setupVerifyFirstWorkflowTaskScheduledTest(t *testing.T) *verifyFirstWorkflowTaskScheduledTestDeps {
	controller := gomock.NewController(t)

	config := tests.NewDynamicConfig()
	shardContext := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	shardContext.SetStateMachineRegistry(reg)

	mockNamespaceCache := shardContext.Resource.NamespaceCache
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	mockExecutionMgr := shardContext.Resource.ExecutionMgr
	mockClusterMetadata := shardContext.Resource.ClusterMetadata
	mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.Version).Return(cluster.TestCurrentClusterName).AnyTimes()

	workflowConsistencyChecker := api.NewWorkflowConsistencyChecker(
		shardContext,
		wcache.NewHostLevelCache(shardContext.GetConfig(), shardContext.GetLogger(), metrics.NoopMetricsHandler))
	mockEventsCache := shardContext.MockEventsCache
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	logger := shardContext.GetLogger()

	return &verifyFirstWorkflowTaskScheduledTestDeps{
		controller:                 controller,
		mockEventsCache:            mockEventsCache,
		mockExecutionMgr:           mockExecutionMgr,
		shardContext:               shardContext,
		workflowConsistencyChecker: workflowConsistencyChecker,
		logger:                     logger,
	}
}

func TestVerifyFirstWorkflowTaskScheduled_WorkflowNotFound(t *testing.T) {
	deps := setupVerifyFirstWorkflowTaskScheduledTest(t)
	defer deps.controller.Finish()

	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NotFound{})

	err := Invoke(context.Background(), request, deps.workflowConsistencyChecker)
	require.IsType(t, &serviceerror.NotFound{}, err)
}

func TestVerifyFirstWorkflowTaskScheduled_WorkflowCompleted(t *testing.T) {
	deps := setupVerifyFirstWorkflowTaskScheduledTest(t)
	defer deps.controller.Finish()

	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	ms := workflow.TestGlobalMutableState(deps.shardContext, deps.mockEventsCache, deps.logger, tests.Version, tests.WorkflowID, tests.RunID)

	addWorkflowExecutionStartedEventWithParent(ms,
		&commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}, "wType", "testTaskQueue", payloads.EncodeString("input"),
		25*time.Second, 20*time.Second, 200*time.Second, nil, "identity")

	_, err := ms.AddTimeoutWorkflowEvent(
		ms.GetNextEventID(),
		enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET,
		uuid.New(),
	)
	require.NoError(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err = Invoke(context.Background(), request, deps.workflowConsistencyChecker)
	require.NoError(t, err)
}

func TestVerifyFirstWorkflowTaskScheduled_WorkflowZombie(t *testing.T) {
	deps := setupVerifyFirstWorkflowTaskScheduledTest(t)
	defer deps.controller.Finish()

	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	ms := workflow.TestGlobalMutableState(deps.shardContext, deps.mockEventsCache, deps.logger, tests.Version, tests.WorkflowID, tests.RunID)

	addWorkflowExecutionStartedEventWithParent(ms,
		&commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}, "wType", "testTaskQueue", payloads.EncodeString("input"),
		25*time.Second, 20*time.Second, 200*time.Second, nil, "identity")

	// zombie state should be treated as open
	_, err := ms.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)
	require.NoError(t, err)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err = Invoke(context.Background(), request, deps.workflowConsistencyChecker)
	require.IsType(t, &serviceerror.WorkflowNotReady{}, err)
}

func TestVerifyFirstWorkflowTaskScheduled_WorkflowRunning_TaskPending(t *testing.T) {
	deps := setupVerifyFirstWorkflowTaskScheduledTest(t)
	defer deps.controller.Finish()

	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	ms := workflow.TestGlobalMutableState(deps.shardContext, deps.mockEventsCache, deps.logger, tests.Version, tests.WorkflowID, tests.RunID)

	addWorkflowExecutionStartedEventWithParent(ms,
		&commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}, "wType", "testTaskQueue", payloads.EncodeString("input"),
		25*time.Second, 20*time.Second, 200*time.Second, nil, "identity")
	_, _ = ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := Invoke(context.Background(), request, deps.workflowConsistencyChecker)
	require.NoError(t, err)
}

func TestVerifyFirstWorkflowTaskScheduled_WorkflowRunning_TaskProcessed(t *testing.T) {
	deps := setupVerifyFirstWorkflowTaskScheduledTest(t)
	defer deps.controller.Finish()

	request := &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
	}

	ms := workflow.TestGlobalMutableState(deps.shardContext, deps.mockEventsCache, deps.logger, tests.Version, tests.WorkflowID, tests.RunID)

	addWorkflowExecutionStartedEventWithParent(ms,
		&commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}, "wType", "testTaskQueue", payloads.EncodeString("input"),
		25*time.Second, 20*time.Second, 200*time.Second, nil, "identity")

	// Schedule WFT
	wt, _ := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)

	// Start WFT
	workflowTasksStartEvent, _, _ := ms.AddWorkflowTaskStartedEvent(
		wt.ScheduledEventID,
		tests.RunID,
		&taskqueuepb.TaskQueue{Name: "testTaskQueue"},
		uuid.New(),
		nil,
		nil,
		nil,
		false,
	)
	wt.StartedEventID = workflowTasksStartEvent.GetEventId()

	// Complete WFT
	workflowTask := ms.GetWorkflowTaskByID(wt.ScheduledEventID)
	require.NotNil(t, workflowTask)
	require.Equal(t, wt.StartedEventID, workflowTask.StartedEventID)
	_, _ = ms.AddWorkflowTaskCompletedEvent(workflowTask,
		&workflowservice.RespondWorkflowTaskCompletedRequest{Identity: "some random identity"}, defaultWorkflowTaskCompletionLimits)
	ms.FlushBufferedEvents()

	wfMs := workflow.TestCloneToProto(ms)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: wfMs}
	deps.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(gwmsResponse, nil)

	err := Invoke(context.Background(), request, deps.workflowConsistencyChecker)
	require.NoError(t, err)
}

func addWorkflowExecutionStartedEventWithParent(
	ms historyi.MutableState,
	workflowExecution *commonpb.WorkflowExecution,
	workflowType, taskQueue string,
	input *commonpb.Payloads,
	executionTimeout, runTimeout, taskTimeout time.Duration,
	parentInfo *workflowspb.ParentExecutionInfo,
	identity string,
) *historypb.HistoryEvent {
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               workflowExecution.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:                    input,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
		WorkflowRunTimeout:       durationpb.New(runTimeout),
		WorkflowTaskTimeout:      durationpb.New(taskTimeout),
		Identity:                 identity,
	}

	event, _ := ms.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:             1,
			NamespaceId:         tests.NamespaceID.String(),
			StartRequest:        startRequest,
			ParentExecutionInfo: parentInfo,
		},
	)

	return event
}
