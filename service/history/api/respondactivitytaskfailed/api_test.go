package respondactivitytaskfailed

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type testDeps struct {
	controller                 *gomock.Controller
	shardContext               *historyi.MockShardContext
	namespaceRegistry          *namespace.MockRegistry
	workflowCache              *wcache.MockCache
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	workflowContext            *historyi.MockWorkflowContext
	currentMutableState        *historyi.MockMutableState
	activityInfo               *persistencespb.ActivityInfo
}

type UsecaseConfig struct {
	request             *historyservice.RespondActivityTaskFailedRequest
	attempt             int32
	activityId          string
	activityType        string
	startedEventId      int64
	scheduledEventId    int64
	taskQueueId         string
	isActivityActive    bool
	isExecutionRunning  bool
	expectRetryActivity bool
	retryActivityError  error
	retryActivityState  enumspb.RetryState
	namespaceId         namespace.ID
	namespaceName       namespace.Name
	wfType              *commonpb.WorkflowType
	tokenVersion        int64
	tokenAttempt        int32
	isCacheStale        bool
	includeHeartbeat    bool
}

func setupTest(t *testing.T) *testDeps {
	t.Helper()

	controller := gomock.NewController(t)

	t.Cleanup(func() {
		controller.Finish()
	})

	return &testDeps{
		controller: controller,
	}
}

func Test_NormalFlowShouldRescheduleActivity_UpdatesWorkflowExecutionAsActive(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	ctx := context.Background()
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		isCacheStale:        false,
		retryActivityState:  enumspb.RETRY_STATE_IN_PROGRESS,
	})
	request := newRespondActivityTaskFailedRequest(t, uc)
	setupStubs(t, deps, uc)

	expectTransientFailureMetricsRecorded(deps.controller, uc, deps.shardContext)
	deps.workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, deps.shardContext).Return(nil)
	deps.currentMutableState.EXPECT().GetEffectiveVersioningBehavior().Return(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

	_, err := Invoke(ctx, request, deps.shardContext, deps.workflowConsistencyChecker)
	require.NoError(t, err)
}

func Test_WorkflowExecutionIsNotRunning_ReturnWorkflowNotRunningError(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isExecutionRunning: false,
	})
	request := newRespondActivityTaskFailedRequest(t, uc)
	setupStubs(t, deps, uc)
	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.Error(t, err)
	require.Equal(t, consts.ErrWorkflowCompleted, err)
}

func Test_CacheRefreshRequired_ReturnCacheStaleError(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   false,
		isExecutionRunning: true,
		isCacheStale:       true,
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)
	expectCounterRecorded(deps.controller, deps.shardContext)

	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.Error(t, err)
	require.Equal(t, consts.ErrStaleState, err)
}

func Test_ActivityTaskDoesNotExist_ActivityNotRunning(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   false,
		isExecutionRunning: true,
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)

	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.Error(t, err)
	require.Equal(t, consts.ErrActivityTaskNotFound, err)
}

func Test_ActivityTaskDoesNotExist_TokenVersionDoesNotMatchActivityVersion(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   true,
		isExecutionRunning: true,
		tokenVersion:       int64(72),
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)

	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.Error(t, err)
	require.Equal(t, consts.ErrActivityTaskNotFound, err)
}

func Test_ActivityTaskDoesNotExist_TokenVersionNonZeroAndAttemptDoesNotMatchActivityAttempt(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   true,
		isExecutionRunning: true,
		tokenVersion:       int64(2),
		tokenAttempt:       int32(5),
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)

	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)
	require.Error(t, err)
	require.Equal(t, consts.ErrActivityTaskNotFound, err)
}

func Test_LastHeartBeatDetailsExist_UpdatesMutableState(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityState:  enumspb.RETRY_STATE_IN_PROGRESS,
		isCacheStale:        false,
		includeHeartbeat:    true,
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)

	ctx := context.Background()
	deps.workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, deps.shardContext).Return(nil)
	deps.currentMutableState.EXPECT().UpdateActivityProgress(deps.activityInfo, &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: request.FailedRequest.GetTaskToken(),
		Details:   request.FailedRequest.GetLastHeartbeatDetails(),
		Identity:  request.FailedRequest.GetIdentity(),
		Namespace: request.FailedRequest.GetNamespace(),
	})
	deps.currentMutableState.EXPECT().GetEffectiveVersioningBehavior().Return(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

	expectTransientFailureMetricsRecorded(deps.controller, uc, deps.shardContext)

	_, err := Invoke(
		ctx,
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)

	require.NoError(t, err)
}

func Test_RetryActivityFailsWithAnError_WillReturnTheError(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	retryError := fmt.Errorf("bizzare error")
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityError:  retryError,
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)

	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)

	require.Error(t, err)
	require.Equal(t, retryError, err, "error from RetryActivity was not propagated expected %v got %v", retryError, err)
}

func Test_NoMoreRetriesAndMutableStateHasNoPendingTasks_WillRecordFailedEventAndAddWorkflowTaskScheduledEvent(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	ctx := context.Background()
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)
	expectTerminalFailureMetricsRecorded(deps.controller, uc, deps.shardContext)
	deps.currentMutableState.EXPECT().AddActivityTaskFailedEvent(
		uc.scheduledEventId,
		uc.startedEventId,
		request.FailedRequest.GetFailure(),
		uc.retryActivityState,
		request.FailedRequest.GetIdentity(),
		request.FailedRequest.WorkerVersion,
	).Return(nil, nil)
	deps.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	deps.currentMutableState.EXPECT().GetEffectiveVersioningBehavior().Return(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)
	deps.workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, deps.shardContext).Return(nil)

	_, err := Invoke(ctx, request, deps.shardContext, deps.workflowConsistencyChecker)

	require.NoError(t, err)
}

func Test_AttemptToAddActivityTaskFailedEventFails_ReturnError(t *testing.T) {
	t.Parallel()

	deps := setupTest(t)
	addTaskError := fmt.Errorf("can't add task")
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	})
	setupStubs(t, deps, uc)
	request := newRespondActivityTaskFailedRequest(t, uc)
	deps.currentMutableState.EXPECT().AddActivityTaskFailedEvent(
		uc.scheduledEventId,
		uc.startedEventId,
		request.FailedRequest.GetFailure(),
		uc.retryActivityState,
		request.FailedRequest.GetIdentity(),
		request.FailedRequest.WorkerVersion,
	).Return(nil, addTaskError)

	_, err := Invoke(
		context.Background(),
		request,
		deps.shardContext,
		deps.workflowConsistencyChecker,
	)

	require.Error(t, err)
	require.Equal(t, addTaskError, err)
}

func newUseCase(uconfig UsecaseConfig) UsecaseConfig {
	if uconfig.activityId == "" {
		uconfig.activityId = "activity-1"
	}
	if uconfig.wfType == nil {
		uconfig.wfType = &commonpb.WorkflowType{Name: "workflow-type"}
	}
	if uconfig.taskQueueId == "" {
		uconfig.taskQueueId = "some-task-queue"
	}
	if uconfig.namespaceId == "" {
		uconfig.namespaceId = namespace.ID("066935ba-910d-4656-bb56-85488e90b151")
	}
	if uconfig.expectRetryActivity {
		uconfig.isActivityActive = true
		uconfig.isExecutionRunning = true
	}
	return uconfig
}

func setupStubs(t *testing.T, deps *testDeps, uc UsecaseConfig) {
	t.Helper()
	require.False(t, uc.isActivityActive && uc.isCacheStale, "either activity can be active or cache is stale not both")
	deps.activityInfo = setupActivityInfo(uc)
	deps.currentMutableState = setupMutableState(t, deps.controller, uc, deps.activityInfo)
	deps.namespaceRegistry = setupNamespaceRegistry(deps.controller, uc)
	deps.shardContext = setupShardContext(deps.controller, deps.namespaceRegistry)
	deps.workflowContext = setupWorkflowContext(deps.controller, deps.shardContext, deps.currentMutableState)
	deps.workflowCache = setupCache(deps.controller, deps.workflowContext)

	deps.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(deps.shardContext, deps.workflowCache)
}

func newRespondActivityTaskFailedRequest(t *testing.T, uc UsecaseConfig) *historyservice.RespondActivityTaskFailedRequest {
	t.Helper()
	tt := &tokenspb.Task{
		Attempt:          uc.tokenAttempt,
		NamespaceId:      uc.namespaceId.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       uc.activityId,
		ActivityType:     uc.activityType,
		Version:          uc.tokenVersion,
	}
	taskToken, err := tt.Marshal()
	require.NoError(t, err)
	var hbDetails *commonpb.Payloads
	if uc.includeHeartbeat {
		hbDetails = &commonpb.Payloads{}
	}
	request := &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: uc.namespaceId.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			Identity:             "ID1",
			Namespace:            uc.namespaceId.String(),
			TaskToken:            taskToken,
			LastHeartbeatDetails: hbDetails,
		},
	}
	return request
}

func setupWorkflowContext(controller *gomock.Controller, shardContext *historyi.MockShardContext, mutableState *historyi.MockMutableState) *historyi.MockWorkflowContext {
	workflowContext := historyi.NewMockWorkflowContext(controller)
	workflowContext.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(mutableState, nil).AnyTimes()
	return workflowContext
}

func setupCache(controller *gomock.Controller, workflowContext *historyi.MockWorkflowContext) *wcache.MockCache {
	workflowCache := wcache.NewMockCache(controller)
	workflowCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), chasm.WorkflowArchetype, locks.PriorityHigh).
		Return(workflowContext, wcache.NoopReleaseFn, nil).AnyTimes()
	workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), locks.PriorityHigh).Return(wcache.NoopReleaseFn, nil).AnyTimes()
	return workflowCache
}

func setupShardContext(controller *gomock.Controller, registry namespace.Registry) *historyi.MockShardContext {
	shardContext := historyi.NewMockShardContext(controller)
	shardContext.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
	shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()

	shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	response := &persistence.GetCurrentExecutionResponse{
		RunID: tests.RunID,
	}
	shardContext.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(response, nil).AnyTimes()
	return shardContext
}

func expectTransientFailureMetricsRecorded(ctrl *gomock.Controller, uc UsecaseConfig, shardContext *historyi.MockShardContext) {
	timer := metrics.NewMockTimerIface(ctrl)
	counter := metrics.NewMockCounterIface(ctrl)
	tags := []metrics.Tag{
		metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
		metrics.WorkflowTypeTag(uc.wfType.Name),
		metrics.ActivityTypeTag(uc.activityType),
		metrics.VersioningBehaviorTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
		metrics.NamespaceTag(uc.namespaceName.String()),
		metrics.UnsafeTaskQueueTag(uc.taskQueueId),
	}

	metricsHandler := metrics.NewMockHandler(ctrl)
	metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler)

	timer.EXPECT().Record(gomock.Any()).Times(2) // ActivityE2ELatency and ActivityStartToCloseLatency
	metricsHandler.EXPECT().Timer(metrics.ActivityE2ELatency.Name()).Return(timer)
	metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timer)
	// ActivityScheduleToCloseLatency is NOT recorded for retries
	counter.EXPECT().Record(int64(1))
	metricsHandler.EXPECT().Counter(metrics.ActivityTaskFail.Name()).Return(counter)

	shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
}

func expectTerminalFailureMetricsRecorded(ctrl *gomock.Controller, uc UsecaseConfig, shardContext *historyi.MockShardContext) {
	timer := metrics.NewMockTimerIface(ctrl)
	counter := metrics.NewMockCounterIface(ctrl)
	tags := []metrics.Tag{
		metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
		metrics.WorkflowTypeTag(uc.wfType.Name),
		metrics.ActivityTypeTag(uc.activityType),
		metrics.VersioningBehaviorTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
		metrics.NamespaceTag(uc.namespaceName.String()),
		metrics.UnsafeTaskQueueTag(uc.taskQueueId),
	}

	metricsHandler := metrics.NewMockHandler(ctrl)
	metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler)

	timer.EXPECT().Record(gomock.Any()).Times(3) // ActivityE2ELatency, ActivityStartToCloseLatency, and ActivityScheduleToCloseLatency
	metricsHandler.EXPECT().Timer(metrics.ActivityE2ELatency.Name()).Return(timer)
	metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timer)
	metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timer) // Recorded for terminal failures
	counter.EXPECT().Record(int64(1)).Times(2)                                                 // ActivityFail and ActivityTaskFail
	metricsHandler.EXPECT().Counter(metrics.ActivityFail.Name()).Return(counter)
	metricsHandler.EXPECT().Counter(metrics.ActivityTaskFail.Name()).Return(counter)

	shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
}

func expectCounterRecorded(ctrl *gomock.Controller, shardContext *historyi.MockShardContext) {
	counter := metrics.NewMockCounterIface(ctrl)
	counter.EXPECT().Record(int64(1), metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope))

	counterHandler := metrics.NewMockHandler(ctrl)
	counterHandler.EXPECT().Counter(gomock.Any()).Return(counter)
	shardContext.EXPECT().GetMetricsHandler().Return(counterHandler).AnyTimes()
}

func setupNamespaceRegistry(controller *gomock.Controller, uc UsecaseConfig) *namespace.MockRegistry {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   uc.namespaceId.String(),
			Name: uc.namespaceName.String(),
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(1),
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "test:///visibility/archival",
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)
	namespaceRegistry := namespace.NewMockRegistry(controller)
	namespaceRegistry.EXPECT().GetNamespaceByID(uc.namespaceId).Return(namespaceEntry, nil).AnyTimes()
	return namespaceRegistry
}

func setupMutableState(t *testing.T, controller *gomock.Controller, uc UsecaseConfig, ai *persistencespb.ActivityInfo) *historyi.MockMutableState {
	currentMutableState := historyi.NewMockMutableState(controller)
	currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(uc.isExecutionRunning).AnyTimes()

	currentMutableState.EXPECT().GetActivityByActivityID(uc.activityId).Return(ai, true).AnyTimes()
	currentMutableState.EXPECT().GetActivityInfo(uc.scheduledEventId).Return(ai, uc.isActivityActive).AnyTimes()
	if uc.isExecutionRunning == true && uc.isActivityActive == false {
		if uc.isCacheStale {
			currentMutableState.EXPECT().GetNextEventID().Return(uc.scheduledEventId - 4).AnyTimes()
		} else {
			currentMutableState.EXPECT().GetNextEventID().Return(uc.scheduledEventId + 4).AnyTimes()
		}
	}

	currentMutableState.EXPECT().GetWorkflowType().Return(uc.wfType).AnyTimes()
	if uc.expectRetryActivity {
		currentMutableState.EXPECT().RecordLastActivityCompleteTime(gomock.Any())
		currentMutableState.EXPECT().RetryActivity(ai, gomock.Any()).Return(uc.retryActivityState, uc.retryActivityError)
		currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false).AnyTimes()
	}
	return currentMutableState
}

func setupActivityInfo(uc UsecaseConfig) *persistencespb.ActivityInfo {
	return &persistencespb.ActivityInfo{
		ScheduledEventId: uc.scheduledEventId,
		Attempt:          uc.attempt,
		StartedEventId:   uc.startedEventId,
		TaskQueue:        uc.taskQueueId,
	}
}
