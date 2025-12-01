package history

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/parentclosepolicy"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type transferQueueActiveTaskExecutorTestDeps struct {
	controller                      *gomock.Controller
	mockShard                       *shard.ContextTest
	mockTxProcessor                 *queues.MockQueue
	mockTimerProcessor              *queues.MockQueue
	mockNamespaceCache              *namespace.MockRegistry
	mockMatchingClient              *matchingservicemock.MockMatchingServiceClient
	mockHistoryClient               *historyservicemock.MockHistoryServiceClient
	mockClusterMetadata             *cluster.MockMetadata
	mockSearchAttributesProvider    *searchattribute.MockProvider
	mockVisibilityManager           *manager.MockVisibilityManager
	mockChasmEngine                 chasm.Engine
	mockExecutionMgr                *persistence.MockExecutionManager
	mockArchivalMetadata            archiver.MetadataMock
	mockArchiverProvider            *provider.MockArchiverProvider
	mockParentClosePolicyClient     *parentclosepolicy.MockClient
	workflowCache                   wcache.Cache
	logger                          log.Logger
	namespaceID                     namespace.ID
	namespace                       namespace.Name
	namespaceEntry                  *namespace.Namespace
	targetNamespaceID               namespace.ID
	targetNamespace                 namespace.Name
	targetNamespaceEntry            *namespace.Namespace
	childNamespaceID                namespace.ID
	childNamespace                  namespace.Name
	childNamespaceEntry             *namespace.Namespace
	version                         int64
	now                             time.Time
	timeSource                      *clock.EventTimeSource
	transferQueueActiveTaskExecutor *transferQueueActiveTaskExecutor
}

var defaultWorkflowTaskCompletionLimits = historyi.WorkflowTaskCompletionLimits{MaxResetPoints: primitives.DefaultHistoryMaxAutoResetPoints, MaxSearchAttributeValueSize: 2048}

func setupTransferQueueActiveTaskExecutorTest(t *testing.T) *transferQueueActiveTaskExecutorTestDeps {
	d := &transferQueueActiveTaskExecutorTestDeps{}

	d.namespaceID = tests.NamespaceID
	d.namespace = tests.Namespace
	d.namespaceEntry = tests.GlobalNamespaceEntry
	d.targetNamespaceID = tests.TargetNamespaceID
	d.targetNamespace = tests.TargetNamespace
	d.targetNamespaceEntry = tests.GlobalTargetNamespaceEntry
	d.childNamespaceID = tests.ChildNamespaceID
	d.childNamespace = tests.ChildNamespace
	d.childNamespaceEntry = tests.GlobalChildNamespaceEntry
	d.version = d.namespaceEntry.FailoverVersion()
	d.now = time.Now().UTC()
	d.timeSource = clock.NewEventTimeSource().Update(d.now)

	d.controller = gomock.NewController(t)
	d.mockTxProcessor = queues.NewMockQueue(d.controller)
	d.mockTimerProcessor = queues.NewMockQueue(d.controller)
	d.mockTxProcessor.EXPECT().Category().Return(tasks.CategoryTransfer).AnyTimes()
	d.mockTimerProcessor.EXPECT().Category().Return(tasks.CategoryTimer).AnyTimes()
	d.mockTxProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	d.mockTimerProcessor.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()

	config := tests.NewDynamicConfig()
	d.mockShard = shard.NewTestContextWithTimeSource(
		d.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		config,
		d.timeSource,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	require.NoError(t, err)
	d.mockShard.SetStateMachineRegistry(reg)

	d.mockShard.SetEventsCacheForTesting(events.NewHostLevelEventsCache(
		d.mockShard.GetExecutionManager(),
		d.mockShard.GetConfig(),
		d.mockShard.GetMetricsHandler(),
		d.mockShard.GetLogger(),
		false,
	))

	d.mockParentClosePolicyClient = parentclosepolicy.NewMockClient(d.controller)
	d.mockMatchingClient = d.mockShard.Resource.MatchingClient
	d.mockHistoryClient = d.mockShard.Resource.HistoryClient
	d.mockExecutionMgr = d.mockShard.Resource.ExecutionMgr
	d.mockClusterMetadata = d.mockShard.Resource.ClusterMetadata
	d.mockSearchAttributesProvider = d.mockShard.Resource.SearchAttributesProvider
	d.mockVisibilityManager = d.mockShard.Resource.VisibilityManager
	d.mockArchivalMetadata = d.mockShard.Resource.ArchivalMetadata
	d.mockArchiverProvider = d.mockShard.Resource.ArchiverProvider
	d.mockNamespaceCache = d.mockShard.Resource.NamespaceCache
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.TargetNamespaceID).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespace(tests.TargetNamespace).Return(tests.GlobalTargetNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ParentNamespaceID).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.ChildNamespaceID).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespace(tests.ChildNamespace).Return(tests.GlobalChildNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.MissedNamespaceID).Return(nil, serviceerror.NewNamespaceNotFound(tests.MissedNamespaceID.String())).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetClusterID().Return(tests.Version).AnyTimes()
	d.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(tests.Version, tests.Version).Return(true).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	d.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	d.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(d.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()
	d.mockArchivalMetadata.SetHistoryEnabledByDefault()
	d.mockArchivalMetadata.SetVisibilityEnabledByDefault()
	d.mockChasmEngine = chasm.NewMockEngine(d.controller)

	d.workflowCache = wcache.NewHostLevelCache(d.mockShard.GetConfig(), d.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	d.logger = d.mockShard.GetLogger()

	h := &historyEngineImpl{
		currentClusterName: d.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName(),
		shardContext:       d.mockShard,
		clusterMetadata:    d.mockClusterMetadata,
		executionManager:   d.mockExecutionMgr,
		logger:             d.logger,
		tokenSerializer:    tasktoken.NewSerializer(),
		metricsHandler:     d.mockShard.GetMetricsHandler(),
		eventNotifier:      events.NewNotifier(clock.NewRealTimeSource(), metrics.NoopMetricsHandler, func(namespace.ID, string) int32 { return 1 }),
		queueProcessors: map[tasks.Category]queues.Queue{
			d.mockTxProcessor.Category():    d.mockTxProcessor,
			d.mockTimerProcessor.Category(): d.mockTimerProcessor,
		},
	}
	d.mockShard.SetEngineForTesting(h)

	d.transferQueueActiveTaskExecutor = newTransferQueueActiveTaskExecutor(
		d.mockShard,
		d.workflowCache,
		h.sdkClientFactory,
		d.logger,
		metrics.NoopMetricsHandler,
		config,
		d.mockShard.Resource.HistoryClient,
		d.mockShard.Resource.MatchingClient,
		d.mockVisibilityManager,
		d.mockChasmEngine,
	).(*transferQueueActiveTaskExecutor)
	d.transferQueueActiveTaskExecutor.parentClosePolicyClient = d.mockParentClosePolicyClient

	t.Cleanup(func() {
		d.controller.Finish()
		d.mockShard.StopForTest()
	})

	return d
}

func (d *transferQueueActiveTaskExecutorTestDeps) createAddActivityTaskRequest(
	task *tasks.ActivityTask,
	ai *persistencespb.ActivityInfo,
) *matchingservice.AddActivityTaskRequest {
	return &matchingservice.AddActivityTaskRequest{
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: ai.ScheduleToStartTimeout,
		Clock:                  vclock.NewVectorClock(d.mockClusterMetadata.GetClusterID(), d.mockShard.GetShardID(), task.TaskID),
		VersionDirective:       worker_versioning.MakeUseAssignmentRulesDirective(),
		Stamp:                  ai.Stamp,
	}
}

func (d *transferQueueActiveTaskExecutorTestDeps) createAddWorkflowTaskRequest(
	task *tasks.WorkflowTask,
	mutableState historyi.MutableState,
) gomock.Matcher {
	taskQueue := &taskqueuepb.TaskQueue{
		Name: task.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	executionInfo := mutableState.GetExecutionInfo()
	timeout := executionInfo.WorkflowRunTimeout
	if executionInfo.TaskQueue != task.TaskQueue {
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
		taskQueue.NormalName = executionInfo.TaskQueue
		timeout = executionInfo.StickyScheduleToStartTimeout
	}

	directive := MakeDirectiveForWorkflowTask(mutableState)

	return protomock.Eq(&matchingservice.AddWorkflowTaskRequest{
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue:              taskQueue,
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: timeout,
		Clock:                  vclock.NewVectorClock(d.mockClusterMetadata.GetClusterID(), d.mockShard.GetShardID(), task.TaskID),
		VersionDirective:       directive,
	})
}

func (d *transferQueueActiveTaskExecutorTestDeps) createRequestCancelWorkflowExecutionRequest(
	targetNamespace namespace.Name,
	task *tasks.CancelExecutionTask,
	rci *persistencespb.RequestCancelInfo,
	attributes *historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
) *historyservice.RequestCancelWorkflowExecutionRequest {
	sourceExecution := &commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	targetExecution := attributes.GetWorkflowExecution()

	return &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: attributes.GetNamespaceId(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         targetNamespace.String(),
			WorkflowExecution: targetExecution,
			Identity:          consts.IdentityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: rci.GetCancelRequestId(),
			Reason:    attributes.Reason,
		},
		ExternalInitiatedEventId:  task.InitiatedEventID,
		ExternalWorkflowExecution: sourceExecution,
		ChildWorkflowOnly:         attributes.GetChildWorkflowOnly(),
	}
}

func (d *transferQueueActiveTaskExecutorTestDeps) createSignalWorkflowExecutionRequest(
	targetNamespace namespace.Name,
	task *tasks.SignalExecutionTask,
	si *persistencespb.SignalInfo,
	attributes *historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes,
) *historyservice.SignalWorkflowExecutionRequest {
	sourceExecution := &commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}

	return &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: attributes.GetNamespaceId(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         targetNamespace.String(),
			WorkflowExecution: attributes.GetWorkflowExecution(),
			Identity:          consts.IdentityHistoryService,
			SignalName:        attributes.SignalName,
			Input:             attributes.Input,
			RequestId:         si.GetRequestId(),
			Control:           attributes.Control,
			Header:            attributes.Header,
		},
		ExternalWorkflowExecution: sourceExecution,
		ChildWorkflowOnly:         attributes.GetChildWorkflowOnly(),
	}
}

func (d *transferQueueActiveTaskExecutorTestDeps) createChildWorkflowExecutionRequest(
	t *testing.T,
	childNamespace namespace.Name,
	task *tasks.StartChildExecutionTask,
	mutableState historyi.MutableState,
	ci *persistencespb.ChildExecutionInfo,
	rootExecutionInfo *workflowspb.RootExecutionInfo,
	userMetadata *sdkpb.UserMetadata,
) *historyservice.StartWorkflowExecutionRequest {
	event, err := mutableState.GetChildExecutionInitiatedEvent(context.Background(), task.InitiatedEventID)
	require.NoError(t, err)
	attributes := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      task.RunID,
	}
	now := d.timeSource.Now().UTC()
	return &historyservice.StartWorkflowExecutionRequest{
		Attempt:     1,
		NamespaceId: ci.NamespaceId,
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                childNamespace.String(),
			WorkflowId:               attributes.WorkflowId,
			WorkflowType:             attributes.WorkflowType,
			TaskQueue:                attributes.TaskQueue,
			Input:                    attributes.Input,
			WorkflowExecutionTimeout: attributes.WorkflowExecutionTimeout,
			WorkflowRunTimeout:       attributes.WorkflowRunTimeout,
			WorkflowTaskTimeout:      attributes.WorkflowTaskTimeout,
			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             ci.CreateRequestId,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
			UserMetadata:          userMetadata,
		},
		ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
			NamespaceId:      task.NamespaceID,
			Namespace:        tests.Namespace.String(),
			Execution:        execution,
			InitiatedId:      task.InitiatedEventID,
			InitiatedVersion: task.Version,
			Clock:            vclock.NewVectorClock(d.mockClusterMetadata.GetClusterID(), d.mockShard.GetShardID(), task.TaskID),
		},
		FirstWorkflowTaskBackoff:        durationpb.New(backoff.GetBackoffForNextScheduleNonNegative(attributes.GetCronSchedule(), now, now)),
		ContinueAsNewInitiator:          enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		WorkflowExecutionExpirationTime: timestamppb.New(now.Add(attributes.WorkflowExecutionTimeout.AsDuration()).Round(time.Millisecond)),
		RootExecutionInfo:               rootExecutionInfo,
	}
}

func (d *transferQueueActiveTaskExecutorTestDeps) createPersistenceMutableState(
	t *testing.T,
	ms historyi.MutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistencespb.WorkflowMutableState {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
	require.NoError(t, err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEventID, lastEventVersion,
	))
	require.NoError(t, err)
	return workflow.TestCloneToProto(ms)
}

func (d *transferQueueActiveTaskExecutorTestDeps) newTaskExecutable(
	task tasks.Task,
) queues.Executable {
	return queues.NewExecutable(
		queues.DefaultReaderId,
		task,
		d.transferQueueActiveTaskExecutor,
		nil,
		nil,
		queues.NewNoopPriorityAssigner(),
		d.mockShard.GetTimeSource(),
		d.mockNamespaceCache,
		d.mockClusterMetadata,
		d.mockShard.ChasmRegistry(),
		queues.GetTaskTypeTagValue,
		nil,
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
	)
}

func (d *transferQueueActiveTaskExecutorTestDeps) mustGenerateTaskID(t *testing.T) int64 {
	taskID, err := d.mockShard.GenerateTaskID()
	require.NoError(t, err)
	return taskID
}

func TestProcessActivityTask_Success(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockMatchingClient.EXPECT().AddActivityTask(gomock.Any(), protomock.Eq(d.createAddActivityTaskRequest(transferTask, ai)), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestExecuteChasmSideEffectTransferTask_ExecutesTask(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowKey.WorkflowID,
		RunId:      tests.WorkflowKey.RunID,
	}

	// Mock the CHASM tree.
	chasmTree := historyi.NewMockChasmTree(d.controller)
	chasmTree.EXPECT().ExecuteSideEffectTask(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Times(1).Return(nil)

	// Mock mutable state.
	ms := historyi.NewMockMutableState(d.controller)
	info := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetCurrentVersion().Return(int64(2)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes() // emulate transition history disabled.
	ms.EXPECT().GetNextEventID().Return(int64(2)).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(info).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms.EXPECT().GetExecutionState().Return(
		&persistencespb.WorkflowExecutionState{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	).AnyTimes()
	ms.EXPECT().ChasmTree().Return(chasmTree).AnyTimes()

	// Add a valid transfer task.
	transferTask := &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		VisibilityTimestamp: d.now,
		TaskID:              d.mustGenerateTaskID(t),
		Info: &persistencespb.ChasmTaskInfo{
			TypeId: 1234,
			Data: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			},
		},
	}

	wfCtx := historyi.NewMockWorkflowContext(d.controller)
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), d.mockShard).Return(ms, nil)

	mockCache := wcache.NewMockCache(d.controller)
	mockCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), d.mockShard, gomock.Any(), execution, chasm.ArchetypeAny, gomock.Any(),
	).Return(wfCtx, wcache.NoopReleaseFn, nil)

	//nolint:revive // unchecked-type-assertion
	transferQueueActiveTaskExecutor := newTransferQueueActiveTaskExecutor(
		d.mockShard,
		mockCache,
		nil,
		d.logger,
		metrics.NoopMetricsHandler,
		tests.NewDynamicConfig(),
		d.mockShard.Resource.HistoryClient,
		d.mockShard.Resource.MatchingClient,
		d.mockVisibilityManager,
		d.mockChasmEngine,
	).(*transferQueueActiveTaskExecutor)

	// Execution should succeed.
	resp := transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NotNil(t, resp)
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessActivityTask_Duplication(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	event = addActivityTaskStartedEvent(mutableState, event.GetEventId(), "")
	ai.StartedEventId = event.GetEventId()
	event = addActivityTaskCompletedEvent(mutableState, ai.ScheduledEventId, ai.StartedEventId, nil, "")
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrActivityTaskNotFound)
}

func TestProcessActivityTask_Paused(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	activityID := "activity-1"
	activityType := "some random activity type"
	event, ai := addActivityTaskScheduledEvent(mutableState, event.GetEventId(), activityID, activityType, taskQueueName, &commonpb.Payloads{}, 1*time.Second, 1*time.Second, 1*time.Second, 1*time.Second)

	// Set the activity as paused
	ai.Paused = true

	transferTask := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
		Stamp:               ai.Stamp, // Ensure stamp matches
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrStaleReference)
}

func TestProcessWorkflowTask_FirstWorkflowTask(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	taskID := d.mustGenerateTaskID(t)
	wt := addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), d.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessWorkflowTask_NonFirstWorkflowTask(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	require.NotNil(t, event)

	// make another round of workflow task
	taskID := d.mustGenerateTaskID(t)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), d.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessWorkflowTask_Sticky_NonFirstWorkflowTask(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	stickyTaskQueueName := "some random sticky task queue"
	stickyTaskQueueTimeout := timestamp.DurationFromSeconds(233)

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	require.NotNil(t, event)
	// set the sticky taskqueue attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskQueue = stickyTaskQueueName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskQueueTimeout

	// make another round of workflow task
	taskID := d.mustGenerateTaskID(t)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           stickyTaskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), d.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessWorkflowTask_WorkflowTaskNotSticky_MutableStateSticky(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"
	stickyTaskQueueName := "some random sticky task queue"
	stickyTaskQueueTimeout := timestamp.DurationFromSeconds(233)

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: workflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueueName,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	require.NotNil(t, event)
	// set the sticky taskqueue attr
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StickyTaskQueue = stickyTaskQueueName
	executionInfo.StickyScheduleToStartTimeout = stickyTaskQueueTimeout

	// make another round of workflow task
	taskID := d.mustGenerateTaskID(t)
	wt = addWorkflowTaskScheduledEvent(mutableState)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, wt.Version)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockMatchingClient.EXPECT().AddWorkflowTask(gomock.Any(), d.createAddWorkflowTaskRequest(transferTask, mutableState), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessWorkflowTask_Duplication(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	taskID := d.mustGenerateTaskID(t)
	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessWorkflowTask_StampMismatch(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	taskID := d.mustGenerateTaskID(t)

	transferTask := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		TaskQueue:           taskQueueName,
		ScheduledEventID:    wt.ScheduledEventID,
		VisibilityTimestamp: time.Now().UTC(),
		Stamp:               wt.Stamp,
	}

	// Modify the workflow task stamp in mutable state to create mismatch
	mutableState.GetExecutionInfo().WorkflowTaskStamp = wt.Stamp + 1

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, wt.ScheduledEventID, d.version)
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	// Execute the task - should return stale reference error
	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrStaleReference)
}
func TestProcessCloseExecution_HasParent(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	parentNamespaceID := "some random parent namespace ID"
	parentInitiatedID := int64(3222)
	parentInitiatedVersion := int64(1234)
	parentNamespace := "some random parent namespace Name"
	parentExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random parent workflow ID",
		RunId:      uuid.New(),
	}
	parentClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
			ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
				NamespaceId:      parentNamespaceID,
				Namespace:        parentNamespace,
				Execution:        parentExecution,
				InitiatedId:      parentInitiatedID,
				InitiatedVersion: parentInitiatedVersion,
				Clock:            parentClock,
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().RecordChildExecutionCompleted(gomock.Any(), protomock.Eq(&historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId:              parentNamespaceID,
		ParentExecution:          parentExecution,
		ParentInitiatedId:        parentInitiatedID,
		ParentInitiatedVersion:   parentInitiatedVersion,
		ChildFirstExecutionRunId: execution.GetRunId(),
		Clock:                    parentClock,
		ChildExecution:           execution,
		CompletionEvent:          event,
	})).Return(nil, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution_NoParent(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution_NoParent_HasFewChildren(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	d.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace1")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace2")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	d.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace3")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy1 := enumspb.PARENT_CLOSE_POLICY_ABANDON
	parentClosePolicy2 := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	parentClosePolicy3 := enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: []*commandpb.Command{
			{
				CommandType: commandType,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace1",
					WorkflowId: "child workflow1",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy1,
				}},
			},
			{
				CommandType: commandType,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace2",
					WorkflowId: "child workflow2",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy2,
				}},
			},
			{
				CommandType: commandType,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace3",
					WorkflowId: "child workflow3",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: parentClosePolicy3,
				}},
			},
		},
	}, defaultWorkflowTaskCompletionLimits)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace1",
		WorkflowId: "child workflow1",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy1,
	}, "child namespace1-ID")
	require.NoError(t, err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace2",
		WorkflowId: "child workflow2",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy2,
	}, "child namespace2-ID")
	require.NoError(t, err)
	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace3",
		WorkflowId: "child workflow3",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: parentClosePolicy3,
	}, "child namespace3-ID")
	require.NoError(t, err)

	mutableState.FlushBufferedEvents()

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			require.True(t, request.GetChildWorkflowOnly())
			require.Equal(t, execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			require.Equal(t, execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, nil
		},
	)
	d.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			require.True(t, request.GetChildWorkflowOnly())
			require.Equal(t, execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			require.Equal(t, execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, nil
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution_NoParent_HasManyChildren(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: commandType,
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: "child workflow" + convert.IntToString(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
				Input:             payloads.EncodeString("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: commands,
	}, defaultWorkflowTaskCompletionLimits)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
			WorkflowId: "child workflow" + convert.IntToString(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
			Input:             payloads.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		}, "child namespace1-ID")
		require.NoError(t, err)
	}

	mutableState.FlushBufferedEvents()

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockParentClosePolicyClient.EXPECT().SendParentClosePolicyRequest(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request parentclosepolicy.Request) error {
			require.Equal(t, execution, request.ParentExecution)
			return nil
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}
func TestProcessCloseExecution_ParentWasReset_HasManyChildren(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	mutableState.GetExecutionInfo().ResetRunId = uuid.New() // indicate that the execution was reset.
	d.mockShard.GetConfig().AllowResetWithPendingChildren = func(namespace string) bool {
		return true // force the dynamic config to allow reset with pending children.
	}

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: commandType,
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: "child workflow" + convert.IntToString(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
				Input:             payloads.EncodeString("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: consts.IdentityResetter,
		Commands: commands,
	}, defaultWorkflowTaskCompletionLimits)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
			WorkflowId: "child workflow" + convert.IntToString(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
			Input:             payloads.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		}, "child namespace1-ID")
		require.NoError(t, err)
	}

	mutableState.FlushBufferedEvents()

	taskID := d.mustGenerateTaskID(t)
	// Simulate termination due to reset.
	event, err = mutableState.AddWorkflowExecutionTerminatedEvent(event.GetEventId(), "some reason", nil, consts.IdentityResetter, false, nil)
	require.NoError(t, err)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockParentClosePolicyClient.EXPECT().SendParentClosePolicyRequest(gomock.Any(), gomock.Any()).Times(0) // parent close policies should not be processed.

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution_NoParent_HasManyAbandonedChildren(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	commandType := enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
	parentClosePolicy := enumspb.PARENT_CLOSE_POLICY_ABANDON
	var commands []*commandpb.Command
	for i := 0; i < 10; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: commandType,
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: "child workflow" + convert.IntToString(i),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child workflow type",
				},
				TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
				Input:             payloads.EncodeString("random input"),
				ParentClosePolicy: parentClosePolicy,
			}},
		})
	}

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: consts.IdentityResetter,
		Commands: commands,
	}, defaultWorkflowTaskCompletionLimits)

	for i := 0; i < 10; i++ {
		_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
			WorkflowId: "child workflow" + convert.IntToString(i),
			WorkflowType: &commonpb.WorkflowType{
				Name: "child workflow type",
			},
			TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
			Input:             payloads.EncodeString("random input"),
			ParentClosePolicy: parentClosePolicy,
		}, "child namespace1-ID")
		require.NoError(t, err)
	}

	mutableState.FlushBufferedEvents()

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution_NoParent_ChildInDeletedNamespace(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	d.mockNamespaceCache.EXPECT().GetNamespace(namespace.Name("child namespace1")).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()

	event, _ = mutableState.AddWorkflowTaskCompletedEvent(wt, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "some random identity",
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace1",
					WorkflowId: "child workflow1",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
				}},
			},
			{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace:  "child namespace1",
					WorkflowId: "child workflow2",
					WorkflowType: &commonpb.WorkflowType{
						Name: "child workflow type",
					},
					TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
					Input:             payloads.EncodeString("random input"),
					ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
				}},
			},
		},
	}, defaultWorkflowTaskCompletionLimits)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace1",
		WorkflowId: "child workflow1",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	}, "child namespace1-ID")
	require.NoError(t, err)

	_, _, err = mutableState.AddStartChildWorkflowExecutionInitiatedEvent(event.GetEventId(), &commandpb.StartChildWorkflowExecutionCommandAttributes{
		Namespace:  "child namespace1",
		WorkflowId: "child workflow2",
		WorkflowType: &commonpb.WorkflowType{
			Name: "child workflow type",
		},
		TaskQueue:         &taskqueuepb.TaskQueue{Name: taskQueueName},
		Input:             payloads.EncodeString("random input"),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}, "child namespace2-ID")
	require.NoError(t, err)

	mutableState.FlushBufferedEvents()

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	d.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			require.True(t, request.GetChildWorkflowOnly())
			require.Equal(t, execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			require.Equal(t, execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, serviceerror.NewNamespaceNotFound("child namespace1")
		},
	)

	d.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			require.True(t, request.GetChildWorkflowOnly())
			require.Equal(t, execution.GetWorkflowId(), request.GetExternalWorkflowExecution().GetWorkflowId())
			require.Equal(t, execution.GetRunId(), request.GetExternalWorkflowExecution().GetRunId())
			return nil, serviceerror.NewNamespaceNotFound("child namespace1")
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCloseExecution_DeleteAfterClose(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event = addCompleteWorkflowEvent(mutableState, event.GetEventId(), nil)

	transferTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
		DeleteAfterClose:    true,
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	mockDeleteMgr := deletemanager.NewMockDeleteManager(d.controller)
	mockDeleteMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	d.transferQueueActiveTaskExecutor.workflowDeleteManager = mockDeleteMgr
	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)

	transferTask.DeleteAfterClose = false
	resp = d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCancelExecution_Success(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), d.targetNamespace, d.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)
	attributes := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:          d.version,
		TaskID:           taskID,
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), d.createRequestCancelWorkflowExecutionRequest(d.targetNamespace, transferTask, rci, attributes)).Return(nil, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCancelExecution_Failure(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event, rci := addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), d.targetNamespace, d.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)
	attributes := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:          d.version,
		TaskID:           taskID,
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), d.createRequestCancelWorkflowExecutionRequest(d.targetNamespace, transferTask, rci, attributes)).Return(nil, serviceerror.NewNotFound(""))
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCancelExecution_Failure_TargetNamespaceNotFound(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), tests.MissedNamespace, tests.MissedNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:          d.version,
		TaskID:           taskID,
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessCancelExecution_Duplication(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)
	event, _ = addRequestCancelInitiatedEvent(mutableState, event.GetEventId(), uuid.New(), d.targetNamespace, d.targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true)

	transferTask := &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:          d.version,
		TaskID:           taskID,
		InitiatedEventID: event.GetEventId(),
	}

	event = addCancelRequestedEvent(mutableState, event.GetEventId(), tests.TargetNamespace, tests.TargetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId())
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Success(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	mutableState, event, si := setupSignalExternalWorkflowInitiated(t, d, d.targetNamespace, d.targetNamespaceID)
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			mutableState.GetExecutionInfo().NamespaceId,
			mutableState.GetExecutionInfo().WorkflowId,
			mutableState.GetExecutionState().RunId,
		),
		Version:          d.version,
		TaskID:           d.mustGenerateTaskID(t),
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), d.createSignalWorkflowExecutionRequest(namespace.Name(attributes.Namespace), transferTask, si, attributes)).Return(nil, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	d.mockHistoryClient.EXPECT().RemoveSignalMutableState(gomock.Any(), &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId:       attributes.GetNamespaceId(),
		WorkflowExecution: attributes.GetWorkflowExecution(),
		RequestId:         si.GetRequestId(),
	}).Return(nil, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Failure_TargetWorkflowNotFound(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	mutableState, event, si := setupSignalExternalWorkflowInitiated(t, d, d.targetNamespace, d.targetNamespaceID)
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			mutableState.GetExecutionInfo().NamespaceId,
			mutableState.GetExecutionInfo().WorkflowId,
			mutableState.GetExecutionState().RunId,
		),
		Version:          d.version,
		TaskID:           d.mustGenerateTaskID(t),
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), d.createSignalWorkflowExecutionRequest(namespace.Name(attributes.Namespace), transferTask, si, attributes)).Return(nil, serviceerror.NewNotFound(""))
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			validateUpdateExecutionRequestWithSignalExternalFailedEvent(t,
				si.InitiatedEventId,
				enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
				request,
			)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Failure_TargetNamespaceNotFound(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	mutableState, event, si := setupSignalExternalWorkflowInitiated(t, d, tests.MissedNamespace, tests.MissedNamespaceID)

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			mutableState.GetExecutionInfo().NamespaceId,
			mutableState.GetExecutionInfo().WorkflowId,
			mutableState.GetExecutionState().RunId,
		),
		Version:          d.version,
		TaskID:           d.mustGenerateTaskID(t),
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			validateUpdateExecutionRequestWithSignalExternalFailedEvent(t,
				si.InitiatedEventId,
				enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
				request,
			)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Failure_SignalCountLimitExceeded(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	mutableState, event, si := setupSignalExternalWorkflowInitiated(t, d, d.targetNamespace, d.targetNamespaceID)
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			mutableState.GetExecutionInfo().NamespaceId,
			mutableState.GetExecutionInfo().WorkflowId,
			mutableState.GetExecutionState().RunId,
		),
		Version:          d.version,
		TaskID:           d.mustGenerateTaskID(t),
		InitiatedEventID: event.GetEventId(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().SignalWorkflowExecution(gomock.Any(), d.createSignalWorkflowExecutionRequest(namespace.Name(attributes.Namespace), transferTask, si, attributes)).Return(nil, consts.ErrSignalsLimitExceeded)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			validateUpdateExecutionRequestWithSignalExternalFailedEvent(t,
				si.InitiatedEventId,
				enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED,
				request,
			)
			return tests.UpdateWorkflowExecutionResponse, nil
		},
	)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessSignalExecution_Duplication(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	mutableState, event, _ := setupSignalExternalWorkflowInitiated(t, d, d.targetNamespace, d.targetNamespaceID)
	attributes := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	transferTask := &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			mutableState.GetExecutionInfo().NamespaceId,
			mutableState.GetExecutionInfo().WorkflowId,
			mutableState.GetExecutionState().RunId,
		),
		Version:          d.version,
		TaskID:           d.mustGenerateTaskID(t),
		InitiatedEventID: event.GetEventId(),
	}

	event = addSignaledEvent(
		mutableState,
		event.GetEventId(),
		tests.TargetNamespace,
		namespace.ID(attributes.GetNamespaceId()),
		attributes.WorkflowExecution.GetWorkflowId(),
		attributes.WorkflowExecution.GetRunId(),
		"",
	)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func setupSignalExternalWorkflowInitiated(
	t *testing.T,
	d *transferQueueActiveTaskExecutorTestDeps,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
) (
	*workflow.MutableStateImpl,
	*historypb.HistoryEvent,
	*persistencespb.SignalInfo,
) {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	targetExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      uuid.New(),
	}
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalControl := "some random signal control"
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	event, signalInfo := addRequestSignalInitiatedEvent(mutableState, event.GetEventId(), uuid.New(),
		targetNamespace, targetNamespaceID, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), true, signalName, signalInput,
		signalControl, signalHeader)

	return mutableState, event, signalInfo
}

func validateUpdateExecutionRequestWithSignalExternalFailedEvent(
	t *testing.T,
	signalInitiatedEventId int64,
	expectedFailedCause enumspb.SignalExternalWorkflowExecutionFailedCause,
	request *persistence.UpdateWorkflowExecutionRequest,
) {
	require.Len(t, request.UpdateWorkflowMutation.DeleteSignalInfos, 1)
	_, ok := request.UpdateWorkflowMutation.DeleteSignalInfos[signalInitiatedEventId]
	require.True(t, ok)

	numFailedEvent := 0
	require.Len(t, request.UpdateWorkflowEvents, 1)
	for _, event := range request.UpdateWorkflowEvents[0].Events {
		if event.EventType != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			continue
		}
		attr := event.GetSignalExternalWorkflowExecutionFailedEventAttributes()
		require.Equal(t, expectedFailedCause, attr.GetCause())
		numFailedEvent++
	}
	require.Equal(t, 1, numFailedEvent)
}

func TestProcessStartChildExecution_Success(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"
	userMetadata := &sdkpb.UserMetadata{
		Summary: &commonpb.Payload{
			Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
			Data:     []byte(`Test summary Data`),
		},
		Details: &commonpb.Payload{
			Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
			Data:     []byte(`Test Details Data`),
		},
	}

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:               execution.WorkflowId,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		d.childNamespace,
		d.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)
	event.UserMetadata = userMetadata

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	rootExecutionInfo := &workflowspb.RootExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
	}

	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), d.createChildWorkflowExecutionRequest(
		d.childNamespace,
		transferTask,
		mutableState,
		ci,
		rootExecutionInfo,
		userMetadata,
	)).Return(&historyservice.StartWorkflowExecutionResponse{RunId: childRunID, Clock: childClock}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	currentShardClock := d.mockShard.CurrentVectorClock()
	d.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.ScheduleWorkflowTaskRequest, _ ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
			parentClock := request.ParentClock
			request.ParentClock = nil
			require.Equal(t, &historyservice.ScheduleWorkflowTaskRequest{
				NamespaceId: tests.ChildNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childWorkflowID,
					RunId:      childRunID,
				},
				IsFirstWorkflowTask: true,
				ParentClock:         nil,
				ChildClock:          childClock,
			}, request)
			cmpResult, err := vclock.Compare(currentShardClock, parentClock)
			if err != nil {
				return nil, err
			}
			require.NoError(t, err)
			require.True(t, cmpResult <= 0)
			return &historyservice.ScheduleWorkflowTaskResponse{}, nil
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

// TestProcessStartChildExecution_ResetSuccess tests that processStartChildExecution() in a reset run actually describes the child to assert parent-child relationship before 'reconnecting' and unpausing the child.
func TestProcessStartChildExecution_ResetSuccess(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	workflowID := "TEST_WORKFLOW_ID"
	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	workflowType := "TEST_WORKFLOW_TYPE"
	taskQueueName := "TEST_TASK_QUEUE"

	childWorkflowID := "TEST_CHILD_WORKFLOW_ID"
	childRunID := uuid.New()
	childWorkflowType := "TEST_CHILD_WORKFLOW_TYPE"
	childTaskQueueName := "TEST_CHILD_TASK_QUEUE"

	originalExecutionRunID := "TEST_ORIGINAL_EXECUTION_RUN_ID"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:               execution.WorkflowId,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)
	mutableState.GetExecutionInfo().OriginalExecutionRunId = originalExecutionRunID

	childInitEvent, _ := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		1111,
		d.childNamespace,
		d.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)
	// Set the base workflow for the reset run and simulate a reset point that is after the childInitEvent.EventId
	mutableState.SetBaseWorkflow("baseRunID", childInitEvent.EventId+1, 123)

	taskID := d.mustGenerateTaskID(t)
	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    childInitEvent.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, childInitEvent.GetEventId(), childInitEvent.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// Assert that child workflow describe is called.
	// The child describe returns a mock parent whose originalExecutionRunID points to the same as the current reset run's originalExecutionRunID
	d.mockHistoryClient.EXPECT().DescribeWorkflowExecution(
		gomock.Any(),
		&historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: d.childNamespaceEntry.ID().String(),
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: d.childNamespaceEntry.Name().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: childWorkflowID,
				},
			},
		},
		gomock.Any(),
	).Return(&historyservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: childWorkflowID, RunId: childRunID},
			ParentExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      originalExecutionRunID,
			},
		},
	}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()
	currentShardClock := d.mockShard.CurrentVectorClock()
	d.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.ScheduleWorkflowTaskRequest, _ ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
			parentClock := request.ParentClock
			request.ParentClock = nil
			require.Equal(t, &historyservice.ScheduleWorkflowTaskRequest{
				NamespaceId: tests.ChildNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childWorkflowID,
					RunId:      childRunID,
				},
				IsFirstWorkflowTask: true,
			}, request)
			cmpResult, err := vclock.Compare(currentShardClock, parentClock)
			if err != nil {
				return nil, err
			}
			require.NoError(t, err)
			require.True(t, cmpResult <= 0)
			return &historyservice.ScheduleWorkflowTaskResponse{}, nil
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessStartChildExecution_Failure(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:               execution.WorkflowId,
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
			ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		d.childNamespace,
		d.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	rootExecutionInfo := &workflowspb.RootExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), d.createChildWorkflowExecutionRequest(
		d.childNamespace,
		transferTask,
		mutableState,
		ci,
		rootExecutionInfo,
		nil,
	)).Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("msg", "", ""))
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessStartChildExecution_Failure_TargetNamespaceNotFound(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
			ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED,
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)

	event, _ = addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		tests.MissedNamespace,
		tests.MissedNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	d.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(tests.UpdateWorkflowExecutionResponse, nil)
	d.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(d.namespaceEntry.IsGlobalNamespace(), d.version).Return(cluster.TestCurrentClusterName).AnyTimes()

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessStartChildExecution_Success_Dup(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childWorkflowID := "some random child workflow ID"
	childRunID := uuid.New()
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		d.childNamespace,
		d.childNamespaceID,
		childWorkflowID,
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}
	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childWorkflowID, childRunID, childWorkflowType, childClock)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()
	ci.StartedEventId = event.GetEventId()

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	currentShardClock := d.mockShard.CurrentVectorClock()
	d.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.ScheduleWorkflowTaskRequest, _ ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
			parentClock := request.ParentClock
			request.ParentClock = nil
			require.Equal(t, &historyservice.ScheduleWorkflowTaskRequest{
				NamespaceId: tests.ChildNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childWorkflowID,
					RunId:      childRunID,
				},
				IsFirstWorkflowTask: true,
				ParentClock:         nil,
				ChildClock:          childClock,
			}, request)
			cmpResult, err := vclock.Compare(currentShardClock, parentClock)
			if err != nil {
				return nil, err
			}
			require.NoError(t, err)
			require.True(t, cmpResult <= 0)
			return &historyservice.ScheduleWorkflowTaskResponse{}, nil
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}

func TestProcessStartChildExecution_Duplication(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random child workflow ID",
		RunId:      uuid.New(),
	}
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		d.childNamespace,
		d.childNamespaceID,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}
	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType, childClock)
	ci.StartedEventId = event.GetEventId()
	event = addChildWorkflowExecutionCompletedEvent(mutableState, ci.InitiatedEventId, childExecution, &historypb.WorkflowExecutionCompletedEventAttributes{
		Result:                       payloads.EncodeString("some random child workflow execution result"),
		WorkflowTaskCompletedEventId: transferTask.InitiatedEventID,
	})
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.ErrorIs(t, resp.ExecutionErr, consts.ErrChildExecutionNotFound)
}

func TestProcessorStartChildExecution_ChildStarted_ParentClosed(t *testing.T) {
	d := setupTransferQueueActiveTaskExecutorTest(t)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	workflowType := "some random workflow type"
	taskQueueName := "some random task queue"

	childExecution := &commonpb.WorkflowExecution{
		WorkflowId: "some random child workflow ID",
		RunId:      uuid.New(),
	}
	childWorkflowType := "some random child workflow type"
	childTaskQueueName := "some random child task queue"

	mutableState := workflow.TestGlobalMutableState(d.mockShard, d.mockShard.GetEventsCache(), d.logger, d.version, execution.GetWorkflowId(), execution.GetRunId())
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: d.namespaceID.String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueueName},
				WorkflowExecutionTimeout: durationpb.New(2 * time.Second),
				WorkflowTaskTimeout:      durationpb.New(1 * time.Second),
			},
		},
	)
	require.NoError(t, err)

	wt := addWorkflowTaskScheduledEvent(mutableState)
	event := addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, uuid.New())
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")

	taskID := d.mustGenerateTaskID(t)

	event, ci := addStartChildWorkflowExecutionInitiatedEvent(
		mutableState,
		event.GetEventId(),
		d.childNamespace,
		d.childNamespaceID,
		childExecution.GetWorkflowId(),
		childWorkflowType,
		childTaskQueueName,
		nil,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		enumspb.PARENT_CLOSE_POLICY_ABANDON,
	)

	transferTask := &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			d.namespaceID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		Version:             d.version,
		TaskID:              taskID,
		InitiatedEventID:    event.GetEventId(),
		VisibilityTimestamp: time.Now().UTC(),
	}
	childClock := vclock.NewVectorClock(rand.Int63(), rand.Int31(), rand.Int63())
	event = addChildWorkflowExecutionStartedEvent(mutableState, event.GetEventId(), childExecution.GetWorkflowId(), childExecution.GetRunId(), childWorkflowType, childClock)
	ci.StartedEventId = event.GetEventId()
	wt = addWorkflowTaskScheduledEvent(mutableState)
	event = addWorkflowTaskStartedEvent(mutableState, wt.ScheduledEventID, taskQueueName, "some random identity")
	wt.StartedEventID = event.GetEventId()
	event = addWorkflowTaskCompletedEvent(t, mutableState, wt.ScheduledEventID, wt.StartedEventID, "some random identity")
	event = addCompleteWorkflowEvent(mutableState, event.EventId, nil)
	// Flush buffered events so real IDs get assigned
	mutableState.FlushBufferedEvents()

	persistenceMutableState := d.createPersistenceMutableState(t, mutableState, event.GetEventId(), event.GetVersion())
	d.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	currentShardClock := d.mockShard.CurrentVectorClock()
	d.mockHistoryClient.EXPECT().ScheduleWorkflowTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *historyservice.ScheduleWorkflowTaskRequest, _ ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
			parentClock := request.ParentClock
			request.ParentClock = nil
			require.Equal(t, &historyservice.ScheduleWorkflowTaskRequest{
				NamespaceId: d.childNamespaceID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childExecution.WorkflowId,
					RunId:      childExecution.RunId,
				},
				IsFirstWorkflowTask: true,
				ParentClock:         nil,
				ChildClock:          childClock,
			}, request)
			cmpResult, err := vclock.Compare(currentShardClock, parentClock)
			if err != nil {
				return nil, err
			}
			require.NoError(t, err)
			require.True(t, cmpResult <= 0)
			return &historyservice.ScheduleWorkflowTaskResponse{}, nil
		},
	)

	resp := d.transferQueueActiveTaskExecutor.Execute(context.Background(), d.newTaskExecutable(transferTask))
	require.NoError(t, resp.ExecutionErr)
}



func TestPendingCloseExecutionTasks(t *testing.T) {
	testCases := []struct {
		Name                    string
		EnsureCloseBeforeDelete bool
		CloseTransferTaskIdSet  bool
		CloseTaskIsAcked        bool
		ShouldDelete            bool
	}{
		{
			Name:                    "skip the check",
			EnsureCloseBeforeDelete: false,
			ShouldDelete:            true,
		},
		{
			Name:                    "no task id",
			EnsureCloseBeforeDelete: true,
			CloseTransferTaskIdSet:  false,
			ShouldDelete:            true,
		},
		{
			Name:                    "multicursor queue unacked",
			EnsureCloseBeforeDelete: true,
			CloseTransferTaskIdSet:  true,
			CloseTaskIsAcked:        false,
			ShouldDelete:            false,
		},
		{
			Name:                    "multicursor queue acked",
			EnsureCloseBeforeDelete: true,
			CloseTransferTaskIdSet:  true,
			CloseTaskIsAcked:        true,
			ShouldDelete:            true,
		},
	}
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockMutableState := historyi.NewMockMutableState(ctrl)
			var closeTransferTaskId int64
			if c.CloseTransferTaskIdSet {
				closeTransferTaskId = 10
			}
			workflowKey := definition.NewWorkflowKey(uuid.New(), uuid.New(), uuid.New())
			mockMutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()
			mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				NamespaceId:         workflowKey.NamespaceID,
				WorkflowId:          workflowKey.WorkflowID,
				CloseTransferTaskId: closeTransferTaskId,
			}).AnyTimes()
			var deleteExecutionTaskId int64 = 1
			mockMutableState.EXPECT().GetNextEventID().Return(deleteExecutionTaskId + 1).AnyTimes()
			namespaceEntry := tests.GlobalNamespaceEntry
			mockMutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry).AnyTimes()

			mockWorkflowContext := historyi.NewMockWorkflowContext(ctrl)
			mockShard := historyi.NewMockShardContext(ctrl)
			mockWorkflowContext.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()
			mockWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), mockShard).Return(mockMutableState, nil)

			mockWorkflowCache := wcache.NewMockCache(ctrl)

			mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(gomock.Any(), mockShard, gomock.Any(), gomock.Any(),
				chasm.ArchetypeAny, gomock.Any(),
			).Return(mockWorkflowContext, historyi.ReleaseWorkflowContextFunc(func(err error) {
			}), nil)

			mockClusterMetadata := cluster.NewMockMetadata(ctrl)
			mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
			mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()

			mockShard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
			mockShard.EXPECT().CurrentVectorClock().Return(
				vclock.NewVectorClock(mockClusterMetadata.GetClusterID(), mockShard.GetShardID(), deleteExecutionTaskId+1),
			).Times(1)
			mockShard.EXPECT().GetConfig().Return(&configs.Config{
				TransferProcessorEnsureCloseBeforeDelete: func() bool {
					return c.EnsureCloseBeforeDelete
				},
			}).AnyTimes()
			mockShard.EXPECT().GetClusterMetadata().Return(mockClusterMetadata).AnyTimes()
			mockMutableState.EXPECT().GetCloseVersion().Return(tests.Version, nil).AnyTimes()
			mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
			mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespaceEntry, nil)
			mockShard.EXPECT().GetNamespaceRegistry().Return(mockNamespaceRegistry)
			mockShard.EXPECT().ChasmRegistry().Return(chasm.NewRegistry(log.NewTestLogger())).AnyTimes()

			var highWatermarkTaskId int64
			if c.CloseTaskIsAcked {
				highWatermarkTaskId = closeTransferTaskId + 1
			} else {
				highWatermarkTaskId = closeTransferTaskId
			}
			mockShard.EXPECT().GetQueueState(tasks.CategoryTransfer).Return(&persistencespb.QueueState{
				ReaderStates: nil,
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamppb.New(tasks.DefaultFireTime),
					TaskId:   highWatermarkTaskId,
				},
			}, true).AnyTimes()

			mockWorkflowDeleteManager := deletemanager.NewMockDeleteManager(ctrl)
			if c.ShouldDelete {
				mockWorkflowDeleteManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(),
					gomock.Any(), gomock.Any(), gomock.Any())
			}

			executor := &transferQueueActiveTaskExecutor{
				transferQueueTaskExecutorBase: &transferQueueTaskExecutorBase{
					cache:                 mockWorkflowCache,
					config:                mockShard.GetConfig(),
					metricHandler:         metrics.NoopMetricsHandler,
					shardContext:          mockShard,
					workflowDeleteManager: mockWorkflowDeleteManager,
				},
			}

			task := &tasks.DeleteExecutionTask{
				WorkflowKey: workflowKey,
				TaskID:      deleteExecutionTaskId,
			}
			executable := queues.NewMockExecutable(ctrl)
			executable.EXPECT().GetTask().Return(task)
			resp := executor.Execute(context.Background(), executable)
			if c.ShouldDelete {
				require.NoError(t, resp.ExecutionErr)
			} else {
				require.Error(t, resp.ExecutionErr)
				require.ErrorIs(t, resp.ExecutionErr, consts.ErrDependencyTaskNotCompleted)
			}
		})
	}
}
