package workflow

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	stateBuilderTestDeps struct {
		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockEventsCache      *events.MockCache
		mockNamespaceCache   *namespace.MockRegistry
		mockTaskGenerator    *MockTaskGenerator
		mockMutableState     *historyi.MockMutableState
		mockClusterMetadata  *cluster.MockMetadata
		stateMachineRegistry *hsm.Registry

		logger log.Logger

		sourceCluster  string
		executionInfo  *persistencespb.WorkflowExecutionInfo
		stateRebuilder *MutableStateRebuilderImpl
	}

	testTaskGeneratorProvider struct {
		mockMutableState  *historyi.MockMutableState
		mockTaskGenerator *MockTaskGenerator
	}
)

func setupStateBuilderTest(t *testing.T) *stateBuilderTestDeps {
	controller := gomock.NewController(t)
	mockTaskGenerator := NewMockTaskGenerator(controller)
	mockMutableState := historyi.NewMockMutableState(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	require.NoError(t, RegisterStateMachine(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	require.NoError(t, nexusoperations.RegisterEventDefinitions(reg))
	require.NoError(t, nexusoperations.RegisterTaskSerializers(reg))
	mockShard.SetStateMachineRegistry(reg)

	root, err := hsm.NewRoot(reg, StateMachineType, mockMutableState, make(map[string]*persistencespb.StateMachineMap), mockMutableState)
	require.NoError(t, err)
	mockMutableState.EXPECT().HSM().Return(root).AnyTimes()
	mockMutableState.EXPECT().IsTransitionHistoryEnabled().Return(false).AnyTimes()

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
		WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
	}
	mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	mockMutableState.EXPECT().NextTransitionCount().Return(int64(2)).AnyTimes()

	taskGeneratorProvider = &testTaskGeneratorProvider{
		mockMutableState:  mockMutableState,
		mockTaskGenerator: mockTaskGenerator,
	}
	stateRebuilder := NewMutableStateRebuilder(
		mockShard,
		logger,
		mockMutableState,
	)
	sourceCluster := "some random source cluster"

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &stateBuilderTestDeps{
		controller:           controller,
		mockShard:            mockShard,
		mockEventsCache:      mockEventsCache,
		mockNamespaceCache:   mockNamespaceCache,
		mockTaskGenerator:    mockTaskGenerator,
		mockMutableState:     mockMutableState,
		mockClusterMetadata:  mockClusterMetadata,
		stateMachineRegistry: reg,
		logger:               logger,
		sourceCluster:        sourceCluster,
		executionInfo:        executionInfo,
		stateRebuilder:       stateRebuilder,
	}
}

func mockUpdateVersion(
	deps *stateBuilderTestDeps,
	events ...*historypb.HistoryEvent,
) {
	for _, event := range events {
		deps.mockMutableState.EXPECT().UpdateCurrentVersion(event.GetVersion(), true)
	}
	deps.mockTaskGenerator.EXPECT().GenerateActivityTimerTasks().Return(nil)
	deps.mockTaskGenerator.EXPECT().GenerateUserTimerTasks().Return(nil)
	deps.mockTaskGenerator.EXPECT().GenerateDirtySubStateMachineTasks(deps.stateMachineRegistry).Return(nil).AnyTimes()
	deps.mockMutableState.EXPECT().SetHistoryBuilder(historybuilder.NewImmutable(events))
}

func toHistory(eventss ...*historypb.HistoryEvent) [][]*historypb.HistoryEvent {
	return [][]*historypb.HistoryEvent{eventss}
}

// workflow operations

func TestApplyEvents_EventTypeWorkflowExecutionStarted_NoCronSchedule(t *testing.T) {
	deps := setupStateBuilderTest(t)

	cronSchedule := ""
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	deps.executionInfo.WorkflowRunTimeout = timestamp.DurationFromSeconds(100)
	deps.executionInfo.CronSchedule = cronSchedule

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	startWorkflowAttribute := &historypb.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowNamespace:   tests.ParentNamespace.String(),
		ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
	}

	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    1,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionStartedEvent(nil, execution, requestID, protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(
		protomock.Eq(event),
	).Return(nil)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(
		protomock.Eq(event),
	).Return(int32(TimerTaskStatusCreated), nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()
	deps.mockMutableState.EXPECT().SetHistoryTree(nil, timestamp.DurationFromSeconds(100), tests.RunID).Return(nil)

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionStarted_WithCronSchedule(t *testing.T) {
	deps := setupStateBuilderTest(t)

	cronSchedule := "* * * * *"
	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	deps.executionInfo.WorkflowRunTimeout = timestamp.DurationFromSeconds(100)
	deps.executionInfo.CronSchedule = cronSchedule

	now := time.Now().UTC()
	eventType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	startWorkflowAttribute := &historypb.WorkflowExecutionStartedEventAttributes{
		ParentWorkflowNamespace:   tests.ParentNamespace.String(),
		ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
		Initiator:                 enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
		FirstWorkflowTaskBackoff:  durationpb.New(backoff.GetBackoffForNextSchedule(cronSchedule, now, now)),
	}

	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    1,
		EventTime:  timestamppb.New(now),
		EventType:  eventType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: startWorkflowAttribute},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionStartedEvent(nil, protomock.Eq(execution), requestID, protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(
		protomock.Eq(event),
	).Return(nil)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(
		protomock.Eq(event),
	).Return(int32(TimerTaskStatusCreated), nil)
	deps.mockTaskGenerator.EXPECT().GenerateDelayedWorkflowTasks(
		protomock.Eq(event),
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()
	deps.mockMutableState.EXPECT().SetHistoryTree(nil, timestamp.DurationFromSeconds(100), tests.RunID).Return(nil)

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionTimedOut(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionTimedoutEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionTimedOut_WithNewRunHistory(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	newRunID := uuid.New()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
				NewExecutionRunId: newRunID,
			},
		},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             deps.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionTimedoutEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	deps.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	})
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), newRunEvents, newRunID)
	require.Nil(t, err)
	require.NotNil(t, newRunStateBuilder)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)

	newRunTasks := newRunStateBuilder.PopTasks()
	require.Len(t, newRunTasks[tasks.CategoryTimer], 1)      // backoffTimer
	require.Len(t, newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func TestApplyEvents_EventTypeWorkflowExecutionTerminated(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionTerminatedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()
	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionTerminated_WithNewRunHistory(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{}},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             uuid.New(),
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionTerminatedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	deps.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	})
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), newRunEvents, uuid.New())
	require.Nil(t, err)
	require.NotNil(t, newRunStateBuilder)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)

	newRunTasks := newRunStateBuilder.PopTasks()
	require.Len(t, newRunTasks[tasks.CategoryTimer], 3)      // backoff timer, runTimeout timer, executionTimeout timer
	require.Len(t, newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func TestApplyEvents_EventTypeWorkflowExecutionFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionFailedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionFailed_WithNewRunHistory(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	newRunID := uuid.New()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				NewExecutionRunId: newRunID,
			},
		},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(10 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstWorkflowTaskBackoff:        durationpb.New(10 * time.Second),
			FirstExecutionRunId:             deps.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionFailedEvent(event.GetEventId(), protomock.Eq(event)).Return(nil)
	deps.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	})
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), newRunEvents, newRunID)
	require.Nil(t, err)
	require.NotNil(t, newRunStateBuilder)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)

	newRunTasks := newRunStateBuilder.PopTasks()
	require.Len(t, newRunTasks[tasks.CategoryTimer], 2)      // backoffTimer, runTimeout timer
	require.Len(t, newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func TestApplyEvents_EventTypeWorkflowExecutionCompleted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionCompleted_WithNewRunHistory(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	newRunID := uuid.New()

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				NewExecutionRunId: newRunID,
			},
		},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionTimeout:        durationpb.New(100 * time.Second),
			WorkflowRunTimeout:              durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:             durationpb.New(10 * time.Second),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: "some random taskqueue"},
			WorkflowType:                    &commonpb.WorkflowType{Name: "some random workflow type"},
			FirstExecutionRunId:             deps.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(100 * time.Second)),
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{newRunStartedEvent}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionCompletedEvent(event.GetEventId(), event).Return(nil)
	deps.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	})
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	newRunStateBuilder, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), newRunEvents, newRunID)
	require.Nil(t, err)
	require.NotNil(t, newRunStateBuilder)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)

	newRunTasks := newRunStateBuilder.PopTasks()
	require.Len(t, newRunTasks[tasks.CategoryTimer], 0)
	require.Len(t, newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
}

func TestApplyEvents_EventTypeWorkflowExecutionCanceled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionCanceledEvent(event.GetEventId(), event).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	parentWorkflowID := "some random parent workflow ID"
	parentRunID := uuid.New()
	parentInitiatedEventID := int64(144)

	now := time.Now().UTC()
	taskqueue := "some random taskqueue"
	workflowType := "some random workflow type"
	workflowTimeoutSecond := time.Duration(110) * time.Second
	taskTimeout := time.Duration(11) * time.Second
	newRunID := uuid.New()

	continueAsNewEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	newRunStartedEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   1,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowNamespace:   tests.ParentNamespace.String(),
			ParentWorkflowNamespaceId: tests.ParentNamespaceID.String(),
			ParentWorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedEventId:          parentInitiatedEventID,
			WorkflowExecutionTimeout:        durationpb.New(workflowTimeoutSecond),
			WorkflowTaskTimeout:             durationpb.New(taskTimeout),
			TaskQueue:                       &taskqueuepb.TaskQueue{Name: taskqueue},
			WorkflowType:                    &commonpb.WorkflowType{Name: workflowType},
			WorkflowExecutionExpirationTime: timestamppb.New(now.Add(workflowTimeoutSecond)),
			FirstExecutionRunId:             deps.mockMutableState.GetExecutionInfo().FirstExecutionRunId,
			ContinuedExecutionRunId:         execution.RunId,
		}},
	}

	newRunSignalEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   2,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}},
	}

	newRunWorkflowTaskAttempt := int32(123)
	newRunWorkflowTaskEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   3,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: durationpb.New(taskTimeout),
			Attempt:             newRunWorkflowTaskAttempt,
		}},
	}
	newRunEvents := []*historypb.HistoryEvent{
		newRunStartedEvent, newRunSignalEvent, newRunWorkflowTaskEvent,
	}

	deps.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, continueAsNewEvent.GetVersion()).Return(deps.sourceCluster).AnyTimes()
	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		protomock.Eq(continueAsNewEvent),
	).Return(nil)
	deps.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	deps.mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(tests.GlobalNamespaceEntry.ID().String(), execution.WorkflowId, execution.RunId)).AnyTimes()
	deps.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New(),
		State:           enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:          enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
	})
	mockUpdateVersion(deps, continueAsNewEvent)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	// new workflow namespace
	deps.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()

	newRunStateBuilder, err := deps.stateRebuilder.ApplyEvents(
		context.Background(), tests.NamespaceID, requestID, execution, toHistory(continueAsNewEvent), newRunEvents, "",
	)
	require.Nil(t, err)
	require.NotNil(t, newRunStateBuilder)
	require.Equal(t, continueAsNewEvent.TaskId, deps.executionInfo.LastRunningClock)

	newRunTasks := newRunStateBuilder.PopTasks()
	require.Empty(t, newRunTasks[tasks.CategoryTimer])
	require.Len(t, newRunTasks[tasks.CategoryVisibility], 1) // recordWorkflowStarted
	require.Len(t, newRunTasks[tasks.CategoryTransfer], 1)   // workflow task
}

func TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew_EmptyNewRunHistory(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	newRunID := uuid.New()

	continueAsNewEvent := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: newRunID,
		}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionContinuedAsNewEvent(
		continueAsNewEvent.GetEventId(),
		protomock.Eq(continueAsNewEvent),
	).Return(nil)
	deps.mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	mockUpdateVersion(deps, continueAsNewEvent)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(
		now,
		false,
		false, // skipCloseTransferTask
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	// new workflow namespace
	deps.mockNamespaceCache.EXPECT().GetNamespace(tests.ParentNamespace).Return(tests.GlobalParentNamespaceEntry, nil).AnyTimes()
	newRunStateBuilder, err := deps.stateRebuilder.ApplyEvents(
		context.Background(), tests.NamespaceID, requestID, execution, toHistory(continueAsNewEvent), nil, "",
	)
	require.Nil(t, err)
	require.Nil(t, newRunStateBuilder)
	require.Equal(t, continueAsNewEvent.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionSignaled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{}},
	}
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionSignaled(protomock.Eq(event)).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionCancelRequested(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionCancelRequestedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeUpsertWorkflowSearchAttributes(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{},
		},
	}
	deps.mockMutableState.EXPECT().ApplyUpsertWorkflowSearchAttributesEvent(protomock.Eq(event)).Return()
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowPropertiesModified(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowPropertiesModifiedEventAttributes{
			WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{},
		},
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowPropertiesModifiedEvent(protomock.Eq(event)).Return()
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeMarkerRecorded(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_MARKER_RECORDED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{}},
	}
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

// workflow task operations
func TestApplyEvents_EventTypeWorkflowTaskScheduled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	timeout := time.Duration(11) * time.Second
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
	workflowTaskAttempt := int32(111)
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           taskqueue,
			StartToCloseTimeout: durationpb.New(timeout),
			Attempt:             workflowTaskAttempt,
		}},
	}
	wt := &historyi.WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    event.GetEventId(),
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: timeout,
		TaskQueue:           taskqueue,
		Attempt:             workflowTaskAttempt,
		Type:                enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	}
	deps.executionInfo.TaskQueue = taskqueue.GetName()
	deps.mockMutableState.EXPECT().ApplyWorkflowTaskScheduledEvent(
		event.GetVersion(), event.GetEventId(), taskqueue, durationpb.New(timeout), workflowTaskAttempt, event.GetEventTime(), event.GetEventTime(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(wt, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		wt.ScheduledEventID,
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}
func TestApplyEvents_EventTypeWorkflowTaskStarted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	timeout := time.Second * 11
	scheduledEventID := int64(111)
	workflowTaskRequestID := uuid.New()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: scheduledEventID,
			RequestId:        workflowTaskRequestID,
		}},
	}
	wt := &historyi.WorkflowTaskInfo{
		Version:             event.GetVersion(),
		ScheduledEventID:    scheduledEventID,
		StartedEventID:      event.GetEventId(),
		RequestID:           workflowTaskRequestID,
		WorkflowTaskTimeout: timeout,
		TaskQueue:           taskqueue,
		Attempt:             1,
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowTaskStartedEvent(
		(*historyi.WorkflowTaskInfo)(nil), event.GetVersion(), scheduledEventID, event.GetEventId(), workflowTaskRequestID, timestamp.TimeValue(event.GetEventTime()),
		false, gomock.Any(), nil, int64(0),
	).Return(wt, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateStartWorkflowTaskTasks(
		wt.ScheduledEventID,
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowTaskTimedOut(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		}},
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE).Return(nil)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	newScheduledEventID := int64(233)
	deps.executionInfo.TaskQueue = taskqueue.GetName()
	deps.mockMutableState.EXPECT().ApplyTransientWorkflowTaskScheduled().Return(&historyi.WorkflowTaskInfo{
		Version:          version,
		ScheduledEventID: newScheduledEventID,
		TaskQueue:        taskqueue,
	}, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		newScheduledEventID,
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowTaskFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}},
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowTaskFailedEvent().Return(nil)
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	newScheduledEventID := int64(233)
	deps.executionInfo.TaskQueue = taskqueue.GetName()
	deps.mockMutableState.EXPECT().ApplyTransientWorkflowTaskScheduled().Return(&historyi.WorkflowTaskInfo{
		Version:          version,
		ScheduledEventID: newScheduledEventID,
		TaskQueue:        taskqueue,
	}, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(
		newScheduledEventID,
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowTaskCompleted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	scheduledEventID := int64(12)
	startedEventID := int64(28)
	evenType := enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
		}},
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowTaskCompletedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

// user timer operations

func TestApplyEvents_EventTypeTimerStarted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	timerID := "timer ID"
	timeoutSecond := time.Duration(10) * time.Second
	evenType := enumspb.EVENT_TYPE_TIMER_STARTED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			TimerId:            timerID,
			StartToFireTimeout: durationpb.New(timeoutSecond),
		}},
	}
	expiryTime := timestamp.TimeValue(event.GetEventTime()).Add(timeoutSecond)
	ti := &persistencespb.TimerInfo{
		Version:        event.GetVersion(),
		TimerId:        timerID,
		ExpiryTime:     timestamppb.New(expiryTime),
		StartedEventId: event.GetEventId(),
		TaskStatus:     TimerTaskStatusNone,
	}
	deps.mockMutableState.EXPECT().ApplyTimerStartedEvent(protomock.Eq(event)).Return(ti, nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeTimerFired(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_TIMER_FIRED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyTimerFiredEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeTimerCanceled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()

	evenType := enumspb.EVENT_TYPE_TIMER_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyTimerCanceledEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

// activity operations

func TestApplyEvents_EventTypeActivityTaskScheduled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	activityID := "activity ID"
	taskqueue := "some random taskqueue"
	timeoutSecond := 10 * time.Second
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}

	ai := &persistencespb.ActivityInfo{
		Version:                 event.GetVersion(),
		ScheduledEventId:        event.GetEventId(),
		ScheduledEventBatchId:   event.GetEventId(),
		ScheduledTime:           event.GetEventTime(),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              activityID,
		ScheduleToStartTimeout:  durationpb.New(timeoutSecond),
		ScheduleToCloseTimeout:  durationpb.New(timeoutSecond),
		StartToCloseTimeout:     durationpb.New(timeoutSecond),
		HeartbeatTimeout:        durationpb.New(timeoutSecond),
		CancelRequested:         false,
		CancelRequestId:         common.EmptyEventID,
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		TaskQueue:               taskqueue,
	}
	deps.executionInfo.TaskQueue = taskqueue
	deps.mockMutableState.EXPECT().ApplyActivityTaskScheduledEvent(event.GetEventId(), protomock.Eq(event)).Return(ai, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateActivityTasks(
		event.GetEventId(),
	).Return(nil)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeActivityTaskStarted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	taskqueue := "some random taskqueue"
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
	scheduledEvent := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}

	evenType = enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
	startedEvent := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    scheduledEvent.GetEventId() + 1,
		EventTime:  timestamppb.New(timestamp.TimeValue(scheduledEvent.GetEventTime()).Add(1000 * time.Nanosecond)),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{}},
	}

	deps.executionInfo.TaskQueue = taskqueue
	deps.mockMutableState.EXPECT().ApplyActivityTaskStartedEvent(protomock.Eq(startedEvent)).Return(nil)
	mockUpdateVersion(deps, startedEvent)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(startedEvent), nil, "")
	require.Nil(t, err)
	require.Equal(t, startedEvent.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeActivityTaskTimedOut(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyActivityTaskTimedOutEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	//	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeActivityTaskFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyActivityTaskFailedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeActivityTaskCompleted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyActivityTaskCompletedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeActivityTaskCancelRequested(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyActivityTaskCancelRequestedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeActivityTaskCanceled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{}},
	}

	deps.mockMutableState.EXPECT().ApplyActivityTaskCanceledEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	// assertion on timer generated is in `mockUpdateVersion` function, since activity / user timer
	// need to be refreshed each time
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

// child workflow operations

func TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	targetWorkflowID := "some random target workflow ID"

	now := time.Now().UTC()
	createRequestID := uuid.New()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowId:  targetWorkflowID,
		}},
	}

	ci := &persistencespb.ChildExecutionInfo{
		Version:               event.GetVersion(),
		InitiatedEventId:      event.GetEventId(),
		InitiatedEventBatchId: event.GetEventId(),
		StartedEventId:        common.EmptyEventID,
		CreateRequestId:       createRequestID,
		Namespace:             tests.TargetNamespace.String(),
		NamespaceId:           tests.TargetNamespaceID.String(),
	}

	// the create request ID is generated inside, cannot assert equal
	deps.mockMutableState.EXPECT().ApplyStartChildWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event),
	).Return(ci, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(
		event.GetEventId(),
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyStartChildWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeChildWorkflowExecutionStarted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyChildWorkflowExecutionStartedEvent(protomock.Eq(event), nil).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyChildWorkflowExecutionTimedOutEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeChildWorkflowExecutionTerminated(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyChildWorkflowExecutionTerminatedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeChildWorkflowExecutionFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyChildWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeChildWorkflowExecutionCompleted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyChildWorkflowExecutionCompletedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

// cancel external workflow operations

func TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now().UTC()
	cancellationRequestID := uuid.New()
	control := "some random control"
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			ChildWorkflowOnly: childWorkflowOnly,
			Control:           control,
		}},
	}
	rci := &persistencespb.RequestCancelInfo{
		Version:          event.GetVersion(),
		InitiatedEventId: event.GetEventId(),
		CancelRequestId:  cancellationRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	deps.mockMutableState.EXPECT().ApplyRequestCancelExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event), gomock.Any(),
	).Return(rci, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateRequestCancelExternalTasks(
		protomock.Eq(event),
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyRequestCancelExternalWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyExternalWorkflowExecutionCancelRequested(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeChildWorkflowExecutionCanceled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyChildWorkflowExecutionCanceledEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

// signal external workflow operations

func TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()
	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true

	now := time.Now().UTC()
	signalRequestID := uuid.New()
	signalName := "some random signal name"
	signalInput := payloads.EncodeString("some random signal input")
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}
	control := "some random control"
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace:   tests.TargetNamespace.String(),
			NamespaceId: tests.TargetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetRunID,
			},
			SignalName:        signalName,
			Input:             signalInput,
			ChildWorkflowOnly: childWorkflowOnly,
			Header:            signalHeader,
			Control:           control,
		}},
	}
	si := &persistencespb.SignalInfo{
		Version:          event.GetVersion(),
		InitiatedEventId: event.GetEventId(),
		RequestId:        signalRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	deps.mockMutableState.EXPECT().ApplySignalExternalWorkflowExecutionInitiatedEvent(
		event.GetEventId(), protomock.Eq(event), gomock.Any(),
	).Return(si, nil)
	mockUpdateVersion(deps, event)
	deps.mockTaskGenerator.EXPECT().GenerateSignalExternalTasks(
		protomock.Eq(event),
	).Return(nil)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplySignalExternalWorkflowExecutionFailedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
	event := &historypb.HistoryEvent{
		TaskId:     rand.Int63(),
		Version:    version,
		EventId:    130,
		EventTime:  timestamppb.New(now),
		EventType:  evenType,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{}},
	}
	deps.mockMutableState.EXPECT().ApplyExternalWorkflowExecutionSignaled(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.Nil(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionUpdateAccepted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				ProtocolInstanceId: t.Name(),
			},
		},
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionUpdateAcceptedEvent(protomock.Eq(event)).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.NoError(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_EventTypeWorkflowExecutionUpdateCompleted(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	evenType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   130,
		EventTime: timestamppb.New(now),
		EventType: evenType,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateCompletedEventAttributes{
			WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{},
		},
	}
	deps.mockMutableState.EXPECT().ApplyWorkflowExecutionUpdateCompletedEvent(protomock.Eq(event), event.EventId).Return(nil)
	mockUpdateVersion(deps, event)
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.NoError(t, err)
	require.Equal(t, event.TaskId, deps.executionInfo.LastRunningClock)
}

func TestApplyEvents_HSMRegistry(t *testing.T) {
	deps := setupStateBuilderTest(t)

	version := int64(1)
	requestID := uuid.New()

	execution := &commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      tests.RunID,
	}

	now := time.Now().UTC()
	event := &historypb.HistoryEvent{
		TaskId:    rand.Int63(),
		Version:   version,
		EventId:   5,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				EndpointId:                   "endpoint-id",
				Endpoint:                     "endpoint",
				Service:                      "service",
				Operation:                    "operation",
				WorkflowTaskCompletedEventId: 4,
				RequestId:                    "request-id",
			},
		},
	}
	deps.mockMutableState.EXPECT().ClearStickyTaskQueue()
	mockUpdateVersion(deps, event)

	_, err := deps.stateRebuilder.ApplyEvents(context.Background(), tests.NamespaceID, requestID, execution, toHistory(event), nil, "")
	require.NoError(t, err)
	// Verify the event was applied.
	sm, err := nexusoperations.MachineCollection(deps.mockMutableState.HSM()).Data("5")
	require.NoError(t, err)
	require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, sm.State())
}

func (p *testTaskGeneratorProvider) NewTaskGenerator(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
) TaskGenerator {
	if mutableState == p.mockMutableState {
		return p.mockTaskGenerator
	}

	return NewTaskGenerator(
		shardContext.GetNamespaceRegistry(),
		mutableState,
		shardContext.GetConfig(),
		shardContext.GetArchivalMetadata(),
		shardContext.GetLogger(),
	)
}
