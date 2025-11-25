package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type taskRefresherTestDeps struct {
	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockNamespaceRegistry *namespace.MockRegistry
	mockTaskGenerator     *MockTaskGenerator
	namespaceEntry        *namespace.Namespace
	mutableState          historyi.MutableState
	stateMachineRegistry  *hsm.Registry
	taskRefresher         *TaskRefresherImpl
}

func setupTaskRefresherTest(t *testing.T) *taskRefresherTestDeps {
	config := tests.NewDynamicConfig()
	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{ShardId: 1},
		config,
	)
	mockNamespaceRegistry := mockShard.Resource.NamespaceCache

	stateMachineRegistry := hsm.NewRegistry()
	mockShard.SetStateMachineRegistry(stateMachineRegistry)
	require.NoError(t, RegisterStateMachine(stateMachineRegistry))

	namespaceEntry := tests.GlobalNamespaceEntry
	mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(namespaceEntry, nil).AnyTimes()
	mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(namespaceEntry, nil).AnyTimes()
	mockTaskGenerator := NewMockTaskGenerator(controller)
	mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	mutableState := TestGlobalMutableState(
		mockShard,
		mockShard.GetEventsCache(),
		mockShard.GetLogger(),
		namespaceEntry.FailoverVersion(),
		tests.WorkflowID,
		tests.RunID,
	)

	taskRefresher := NewTaskRefresher(mockShard)
	taskRefresher.taskGeneratorProvider = newMockTaskGeneratorProvider(mockTaskGenerator)

	return &taskRefresherTestDeps{
		controller:            controller,
		mockShard:             mockShard,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockTaskGenerator:     mockTaskGenerator,
		namespaceEntry:        namespaceEntry,
		mutableState:          mutableState,
		stateMachineRegistry:  stateMachineRegistry,
		taskRefresher:         taskRefresher,
	}
}

func TestRefreshWorkflowStartTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 2, Version: common.EmptyVersion},
						},
					},
				},
			},
			WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	require.NoError(t, err)

	startEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				FirstWorkflowTaskBackoff: durationpb.New(10 * time.Second),
			},
		},
	}
	deps.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		deps.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     common.FirstEventID,
			Version:     common.EmptyVersion,
		},
		common.FirstEventID,
		branchToken,
	).Return(startEvent, nil).Times(1)
	deps.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(startEvent).DoAndReturn(
		func(_ *historypb.HistoryEvent) (int32, error) {
			require.Equal(t, int32(TimerTaskStatusNone), mutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus)
			return int32(TimerTaskStatusCreated), nil
		},
	)
	deps.mockTaskGenerator.EXPECT().GenerateDelayedWorkflowTasks(startEvent).Return(nil).Times(1)

	err = RefreshTasksForWorkflowStart(context.Background(), mutableState, deps.mockTaskGenerator, EmptyVersionedTransition)
	require.NoError(t, err)
	require.Equal(t, int32(TimerTaskStatusCreated), mutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus)

	err = RefreshTasksForWorkflowStart(context.Background(), mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		// TransitionCount is higher than workflow state's last update versioned transition,
		// no task should be generated and no call to task generator should be made.
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)
}

func TestRefreshRecordWorkflowStartedTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 2, Version: common.EmptyVersion},
						},
					},
				},
			},
			VisibilityLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	require.NoError(t, err)

	startEvent := &historypb.HistoryEvent{
		EventId:    common.FirstEventID,
		Version:    common.EmptyVersion,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
	}
	deps.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		deps.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     common.FirstEventID,
			Version:     common.EmptyVersion,
		},
		common.FirstEventID,
		branchToken,
	).Return(startEvent, nil).Times(1)
	deps.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(startEvent).Return(nil).Times(1)

	err = deps.taskRefresher.refreshTasksForRecordWorkflowStarted(context.Background(), mutableState, deps.mockTaskGenerator, EmptyVersionedTransition)
	require.NoError(t, err)

	err = deps.taskRefresher.refreshTasksForRecordWorkflowStarted(context.Background(), mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		// TransitionCount is higher than workflow visibility's last update versioned transition,
		// no task should be generated and no call to task generator should be made.
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)
}

func TestRefreshWorkflowCloseTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	closeTime := timestamppb.Now()
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			CloseTime:   closeTime,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	require.NoError(t, err)

	deps.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(closeTime.AsTime(), false, false).Return(nil).Times(1)

	err = deps.taskRefresher.refreshTasksForWorkflowClose(context.Background(), mutableState, deps.mockTaskGenerator, EmptyVersionedTransition, false)
	require.NoError(t, err)

	err = deps.taskRefresher.refreshTasksForWorkflowClose(context.Background(), mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		// TransitionCount is higher than workflow state's last update versioned transition,
		TransitionCount:          3,
		NamespaceFailoverVersion: common.EmptyVersion,
	}, false)
	require.NoError(t, err)
}

func TestRefreshWorkflowTaskTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	baseMutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branchToken"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 3, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(3),
	}

	testCase := []struct {
		name                   string
		msRecordProvider       func() *persistencespb.WorkflowMutableState
		setupMock              func()
		minVersionedTransition *persistencespb.VersionedTransition
	}{
		{
			name: "Refresh/NoWorkflowTask",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				return common.CloneProto(baseMutableStateRecord)
			},
			setupMock:              func() {},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/SpeculativeWorkflowTask",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
				return record
			},
			setupMock:              func() {},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/WorkflowTaskScheduled",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				return record
			},
			setupMock: func() {
				deps.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/WorkflowTaskStarted",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskStartedEventId = 3
				record.ExecutionInfo.WorkflowTaskStartedTime = timestamppb.New(time.Now().Add(time.Second))
				record.ExecutionInfo.WorkflowTaskRequestId = uuid.New()
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				return record
			},
			setupMock: func() {
				deps.mockTaskGenerator.EXPECT().GenerateStartWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "PartialRefresh/Skipped",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				record.ExecutionInfo.WorkflowTaskLastUpdateVersionedTransition = &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}
				return record
			},
			setupMock: func() {},
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		{
			name: "PartialRefresh/Refreshed",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				record.ExecutionInfo.WorkflowTaskLastUpdateVersionedTransition = &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}
				return record
			},
			setupMock: func() {
				deps.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		{
			name: "PartialRefresh/UnknownLastUpdateVersionedTransition",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				// WorkflowTaskLastUpdateVersionedTransition not specified.
				// This could happen for ms record persisted before versioned transition is enabled.
				// We do not refresh in this case unless the refresh request is a full refresh
				// (minVersionedTransition is EmptyVersionedTransition), because the fact that
				// lastUpdateVersionedTransition is unknown means it's updated before the given
				// minVersionedTransition.
				return record
			},
			setupMock: func() {},
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			mutableState, err := NewMutableStateFromDB(
				deps.mockShard,
				deps.mockShard.GetEventsCache(),
				log.NewTestLogger(),
				tests.LocalNamespaceEntry,
				tc.msRecordProvider(),
				101,
			)
			require.NoError(t, err)

			tc.setupMock()
			err = deps.taskRefresher.refreshWorkflowTaskTasks(mutableState, deps.mockTaskGenerator, tc.minVersionedTransition)
			require.NoError(t, err)
		})
	}
}

func TestRefreshActivityTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			5: {
				ActivityId:             "5",
				ScheduledEventId:       5,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          4,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				ActivityId:             "6",
				ScheduledEventId:       6,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedTime:            timestamppb.New(time.Now().Add(time.Second)),
				StartedEventId:         8,
				RequestId:              uuid.New(),
				TimerTaskStatus:        TimerTaskStatusCreatedStartToClose,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			7: {
				ActivityId:             "7",
				ScheduledEventId:       7,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(1 * time.Second),
				StartToCloseTimeout:    durationpb.New(1 * time.Second),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}

	testCase := []struct {
		name                         string
		minVersionedTransition       *persistencespb.VersionedTransition
		getActivityScheduledEventIDs []int64
		generateActivityTaskIDs      []int64
		expectedTimerTaskStatus      map[int64]int32
		expectedRefreshedTasks       []tasks.Task
	}{
		{
			name: "PartialRefresh",
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          4,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
			getActivityScheduledEventIDs: []int64{5},
			generateActivityTaskIDs:      []int64{5},
			expectedTimerTaskStatus: map[int64]int32{
				5: TimerTaskStatusCreatedScheduleToStart,
				6: TimerTaskStatusCreatedStartToClose,
				7: TimerTaskStatusCreatedScheduleToStart,
			},
		},
		{
			name:                         "FullRefresh",
			minVersionedTransition:       EmptyVersionedTransition,
			getActivityScheduledEventIDs: []int64{5, 7},
			generateActivityTaskIDs:      []int64{5, 7},
			expectedTimerTaskStatus: map[int64]int32{
				5: TimerTaskStatusNone,
				6: TimerTaskStatusNone,
				7: TimerTaskStatusCreatedScheduleToStart,
			},
			expectedRefreshedTasks: []tasks.Task{
				&tasks.ActivityTimeoutTask{
					WorkflowKey:         deps.mutableState.GetWorkflowKey(),
					VisibilityTimestamp: mutableStateRecord.ActivityInfos[7].ScheduledTime.AsTime().Add(mutableStateRecord.ActivityInfos[7].ScheduleToStartTimeout.AsDuration()),
					EventID:             7,
					TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
					Attempt:             0,
				},
			},
		},
	}

	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			mutableState, err := NewMutableStateFromDB(
				deps.mockShard,
				deps.mockShard.GetEventsCache(),
				log.NewTestLogger(),
				tests.LocalNamespaceEntry,
				mutableStateRecord,
				10,
			)
			require.NoError(t, err)
			for _, eventID := range tc.generateActivityTaskIDs {
				deps.mockTaskGenerator.EXPECT().GenerateActivityTasks(int64(eventID)).Return(nil).Times(1)
			}

			err = deps.taskRefresher.refreshTasksForActivity(context.Background(), mutableState, deps.mockTaskGenerator, tc.minVersionedTransition)
			require.NoError(t, err)

			pendingActivityInfos := mutableState.GetPendingActivityInfos()
			require.Len(t, pendingActivityInfos, 3)
			require.Equal(t, tc.expectedTimerTaskStatus[5], pendingActivityInfos[5].TimerTaskStatus)
			require.Equal(t, tc.expectedTimerTaskStatus[6], pendingActivityInfos[6].TimerTaskStatus)
			require.Equal(t, tc.expectedTimerTaskStatus[7], pendingActivityInfos[7].TimerTaskStatus)

			refreshedTasks := mutableState.PopTasks()
			require.Len(t, refreshedTasks[tasks.CategoryTimer], len(tc.expectedRefreshedTasks))
			for idx, task := range refreshedTasks[tasks.CategoryTimer] {
				if activityTimeoutTask, ok := task.(*tasks.ActivityTimeoutTask); ok {
					require.Equal(t, tc.expectedRefreshedTasks[idx], activityTimeoutTask)
				}
			}
		})
	}

}

func TestRefreshUserTimer(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"5": {
				TimerId:        "5",
				StartedEventId: 5,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(time.Now().Add(10 * time.Second)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			"6": {
				TimerId:        "6",
				StartedEventId: 6,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(time.Now().Add(100 * time.Second)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	err = deps.taskRefresher.refreshTasksForTimer(mutableState, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)

	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	require.Len(t, pendingTimerInfos, 2)
	require.Equal(t, int64(TimerTaskStatusCreated), pendingTimerInfos["5"].TaskStatus)
	require.Equal(t, int64(TimerTaskStatusCreated), pendingTimerInfos["6"].TaskStatus)

	refreshedTasks := mutableState.PopTasks()
	require.Len(t, refreshedTasks[tasks.CategoryTimer], 1)
}

func TestRefreshUserTimer_Partial_NoUpdatedTimers_MaskNone_GeneratesEarliest(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			// Earliest timer has TaskStatus None (as on passive), lastUpdate older than minVersion
			"10": {
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(5 * time.Minute)),
				TaskStatus:     TimerTaskStatusNone,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			// Later timer remains Created
			"15": {
				TimerId:        "15",
				StartedEventId: 15,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}

	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	// minVersion is higher than both timers' lastUpdate; loop clears none, but CreateNextUserTimer should still create earliest
	err = deps.taskRefresher.refreshTasksForTimer(mutableState, &persistencespb.VersionedTransition{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)

	// Earliest timer should now be marked Created and one task enqueued
	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	require.Equal(t, int64(TimerTaskStatusCreated), pendingTimerInfos["10"].TaskStatus)
	require.Equal(t, int64(TimerTaskStatusCreated), pendingTimerInfos["15"].TaskStatus)

	refreshedTasks := mutableState.PopTasks()
	require.Len(t, refreshedTasks[tasks.CategoryTimer], 1)
}

func TestRefreshUserTimer_Partial_NoUpdatedTimers_MaskCreated_NoTask(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			// Both timers Created and older than minVersion; CreateNextUserTimer should no-op
			"10": {
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(5 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			"15": {
				TimerId:        "15",
				StartedEventId: 15,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}

	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	err = deps.taskRefresher.refreshTasksForTimer(mutableState, &persistencespb.VersionedTransition{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)

	// No new tasks since earliest already Created
	refreshedTasks := mutableState.PopTasks()
	require.Empty(t, refreshedTasks[tasks.CategoryTimer])
}

func TestRefreshUserTimer_FullRefresh_ClearsMasks_EnqueuesEarliest(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"10": {
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(5 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			"15": {
				TimerId:        "15",
				StartedEventId: 15,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}

	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	// Full refresh
	err = deps.taskRefresher.refreshTasksForTimer(mutableState, EmptyVersionedTransition)
	require.NoError(t, err)

	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	// Earliest should be Created again, later should be left as None
	require.Equal(t, int64(TimerTaskStatusCreated), pendingTimerInfos["10"].TaskStatus)
	require.Equal(t, int64(TimerTaskStatusNone), pendingTimerInfos["15"].TaskStatus)

	refreshedTasks := mutableState.PopTasks()
	require.Len(t, refreshedTasks[tasks.CategoryTimer], 1)
}

func TestRefreshUserTimer_RunExpiration_SkipsTask(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	runExpiration := now.Add(3 * time.Minute)
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:               tests.NamespaceID.String(),
			WorkflowId:                tests.WorkflowID,
			WorkflowRunExpirationTime: timestamppb.New(runExpiration),
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			// Earliest timer expires after run expiration; should be skipped by CreateNextUserTimer
			"10": {
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusNone,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          2,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}

	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	err = deps.taskRefresher.refreshTasksForTimer(mutableState, &persistencespb.VersionedTransition{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)

	// No task generated due to run-expiration guard
	refreshedTasks := mutableState.PopTasks()
	require.Empty(t, refreshedTasks[tasks.CategoryTimer])
}

func TestRefreshChildWorkflowTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		ChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{
			5: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				CreateRequestId:       uuid.New(),
				StartedWorkflowId:     "child-workflow-id-5",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				CreateRequestId:       uuid.New(),
				StartedWorkflowId:     "child-workflow-id-6",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			7: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      7,
				StartedEventId:        8,
				CreateRequestId:       uuid.New(),
				StartedWorkflowId:     "child-workflow-id-7",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	testcases := []struct {
		name                   string
		hasPendingChildIds     bool
		expectedRefreshedTasks []int64
	}{
		{
			name:                   "has pending child ids",
			hasPendingChildIds:     true,
			expectedRefreshedTasks: []int64{6},
		},
		{
			name:                   "no pending child ids",
			hasPendingChildIds:     false,
			expectedRefreshedTasks: []int64{6, 7},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			for _, eventID := range tc.expectedRefreshedTasks {
				// only the second child workflow will refresh the child workflow task
				deps.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(eventID).Return(nil).Times(1)
			}

			var previousPendingChildIds map[int64]struct{}
			if tc.hasPendingChildIds {
				previousPendingChildIds = mutableState.GetPendingChildIds()
			}
			err = deps.taskRefresher.refreshTasksForChildWorkflow(
				mutableState,
				deps.mockTaskGenerator,
				&persistencespb.VersionedTransition{
					TransitionCount:          4,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
				previousPendingChildIds,
			)
			require.NoError(t, err)
		})
	}
}

func TestRefreshRequestCancelExternalTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		RequestCancelInfos: map[int64]*persistencespb.RequestCancelInfo{
			5: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				CancelRequestId:       uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				CancelRequestId:       uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	// only the second request cancel external will refresh tasks
	initEvent := &historypb.HistoryEvent{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{},
		},
	}
	deps.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		deps.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     int64(6),
			Version:     common.EmptyVersion,
		},
		int64(4),
		branchToken,
	).Return(initEvent, nil).Times(1)

	deps.mockTaskGenerator.EXPECT().GenerateRequestCancelExternalTasks(initEvent).Return(nil).Times(1)

	err = deps.taskRefresher.refreshTasksForRequestCancelExternalWorkflow(context.Background(), mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)
}

func TestRefreshSignalExternalTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		SignalInfos: map[int64]*persistencespb.SignalInfo{
			5: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				RequestId:             uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				RequestId:             uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	require.NoError(t, err)

	// only the second signal external will refresh tasks
	initEvent := &historypb.HistoryEvent{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{},
		},
	}
	deps.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		deps.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     int64(6),
			Version:     common.EmptyVersion,
		},
		int64(4),
		branchToken,
	).Return(initEvent, nil).Times(1)

	deps.mockTaskGenerator.EXPECT().GenerateSignalExternalTasks(initEvent).Return(nil).Times(1)

	err = deps.taskRefresher.refreshTasksForSignalExternalWorkflow(context.Background(), mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)
}

func TestRefreshWorkflowSearchAttributesTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VisibilityLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          3,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		deps.mockShard,
		deps.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	require.NoError(t, err)

	deps.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil).Times(1)

	err = deps.taskRefresher.refreshTasksForWorkflowSearchAttr(mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)

	err = deps.taskRefresher.refreshTasksForWorkflowSearchAttr(mutableState, deps.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          5,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	require.NoError(t, err)
}

func TestRefreshSubStateMachineTasks(t *testing.T) {
	deps := setupTaskRefresherTest(t)
	defer deps.controller.Finish()

	stateMachineDef := hsmtest.NewDefinition("test")
	err := deps.stateMachineRegistry.RegisterTaskSerializer(hsmtest.TaskType, hsmtest.TaskSerializer{})
	require.NoError(t, err)
	err = deps.stateMachineRegistry.RegisterMachine(stateMachineDef)
	require.NoError(t, err)

	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
		TransitionCount:          3,
	}
	deps.mutableState.GetExecutionInfo().TransitionHistory = []*persistencespb.VersionedTransition{
		versionedTransition,
	}

	hsmRoot := deps.mutableState.HSM()
	child1, err := hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	_, err = child1.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1_1"}, hsmtest.NewData(hsmtest.State2))
	require.NoError(t, err)
	_, err = hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_2"}, hsmtest.NewData(hsmtest.State3))
	require.NoError(t, err)
	// Clear the dirty flag so we can test it later.
	hsmRoot.ClearTransactionState()

	// mark all nodes dirty for setting last updated versioned transition
	err = hsmRoot.Walk(func(node *hsm.Node) error {
		// Ignore the root, it is the entire mutable state.
		if node.Parent == nil {
			return nil
		}
		// After the transition, the LastUpdateVersionedTransition should have transition count 4.
		return hsm.MachineTransition(node, func(_ *hsmtest.Data) (hsm.TransitionOutput, error) {
			return hsm.TransitionOutput{}, nil
		})
	})
	require.NoError(t, err)
	hsmRoot.ClearTransactionState()

	err = deps.taskRefresher.refreshTasksForSubStateMachines(deps.mutableState, nil)
	require.NoError(t, err)
	refreshedTasks := deps.mutableState.PopTasks()
	require.Len(t, refreshedTasks[tasks.CategoryOutbound], 3)
	require.Len(t, deps.mutableState.GetExecutionInfo().StateMachineTimers, 3)
	require.Len(t, refreshedTasks[tasks.CategoryTimer], 1)
	require.False(t, hsmRoot.Dirty())

	err = deps.taskRefresher.refreshTasksForSubStateMachines(
		deps.mutableState,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
			TransitionCount:          4,
		},
	)
	require.NoError(t, err)
	refreshedTasks = deps.mutableState.PopTasks()
	require.Len(t, refreshedTasks[tasks.CategoryOutbound], 3)
	require.Len(t, deps.mutableState.GetExecutionInfo().StateMachineTimers, 3)
	require.Len(t, refreshedTasks[tasks.CategoryTimer], 1)
	require.False(t, hsmRoot.Dirty())

	err = deps.taskRefresher.refreshTasksForSubStateMachines(
		deps.mutableState,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: deps.namespaceEntry.FailoverVersion(),
			TransitionCount:          5,
		},
	)
	require.NoError(t, err)
	refreshedTasks = deps.mutableState.PopTasks()
	require.Empty(t, refreshedTasks)
	require.False(t, hsmRoot.Dirty())
}

type mockTaskGeneratorProvider struct {
	mockTaskGenerator *MockTaskGenerator
}

func newMockTaskGeneratorProvider(
	mockTaskGenerator *MockTaskGenerator,
) TaskGeneratorProvider {
	return &mockTaskGeneratorProvider{
		mockTaskGenerator: mockTaskGenerator,
	}
}

func (m *mockTaskGeneratorProvider) NewTaskGenerator(
	_ historyi.ShardContext,
	_ historyi.MutableState,
) TaskGenerator {
	return m.mockTaskGenerator
}
