package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/primitives/timestamp"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type timerSequenceTestDeps struct {
	controller       *gomock.Controller
	mockMutableState *historyi.MockMutableState
	workflowKey      definition.WorkflowKey
	timerSequence    *timerSequenceImpl
}

func setupTimerSequenceTest(t *testing.T) *timerSequenceTestDeps {
	controller := gomock.NewController(t)
	mockMutableState := historyi.NewMockMutableState(controller)

	workflowKey := definition.NewWorkflowKey(
		tests.NamespaceID.String(),
		tests.WorkflowID,
		tests.RunID,
	)
	mockMutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()
	timerSequence := NewTimerSequence(mockMutableState)

	return &timerSequenceTestDeps{
		controller:       controller,
		mockMutableState: mockMutableState,
		workflowKey:      workflowKey,
		timerSequence:    timerSequence,
	}
}

func TestCreateNextUserTimer_AlreadyCreated_AfterWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timerExpiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timerExpiry,
		TaskStatus:     TimerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(timerExpiry.AsTime().Add(-1 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextUserTimer_AlreadyCreated_BeforeWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timerExpiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timerExpiry,
		TaskStatus:     TimerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(timerExpiry.AsTime().Add(1 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextUserTimer_AlreadyCreated_NoWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timer1Expiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timer1Expiry,
		TaskStatus:     TimerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: nil,
	})

	modified, err := deps.timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextUserTimer_NotCreated_AfterWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timerExpiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timerExpiry,
		TaskStatus:     TimerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(timerExpiry.AsTime().Add(-1 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextUserTimer_NotCreated_BeforeWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timerExpiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timerExpiry,
		TaskStatus:     TimerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	deps.mockMutableState.EXPECT().GetUserTimerInfoByEventID(timerInfo.StartedEventId).Return(timerInfo, true)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(timerExpiry.AsTime().Add(1 * time.Second)),
	})

	var timerInfoUpdated = common.CloneProto(timerInfo) // make a copy
	timerInfoUpdated.TaskStatus = TimerTaskStatusCreated
	deps.mockMutableState.EXPECT().UpdateUserTimerTaskStatus(timerInfo.TimerId, timerInfoUpdated.TaskStatus).Return(nil)
	deps.mockMutableState.EXPECT().AddTasks(&tasks.UserTimerTask{
		// TaskID is set by shard
		WorkflowKey:         deps.workflowKey,
		VisibilityTimestamp: timerExpiry.AsTime(),
		EventID:             timerInfo.GetStartedEventId(),
	})

	modified, err := deps.timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.True(t, modified)
}

func TestCreateNextUserTimer_NotCreated_NoWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timerExpiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timerExpiry,
		TaskStatus:     TimerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	deps.mockMutableState.EXPECT().GetUserTimerInfoByEventID(timerInfo.StartedEventId).Return(timerInfo, true)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: nil,
	})

	var timerInfoUpdated = common.CloneProto(timerInfo) // make a copy
	timerInfoUpdated.TaskStatus = TimerTaskStatusCreated
	deps.mockMutableState.EXPECT().UpdateUserTimerTaskStatus(timerInfoUpdated.TimerId, timerInfoUpdated.TaskStatus).Return(nil)
	deps.mockMutableState.EXPECT().AddTasks(&tasks.UserTimerTask{
		// TaskID is set by shard
		WorkflowKey:         deps.workflowKey,
		VisibilityTimestamp: timerExpiry.AsTime(),
		EventID:             timerInfo.GetStartedEventId(),
	})

	modified, err := deps.timerSequence.CreateNextUserTimer()
	require.NoError(t, err)
	require.True(t, modified)
}

func TestCreateNextActivityTimer_AlreadyCreated_AfterWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(now.Add(-2000 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextActivityTimer_AlreadyCreated_BeforeWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(now.Add(2000 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextActivityTimer_AlreadyCreated_NoWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: nil,
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextActivityTimer_NotCreated_AfterWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(now.Add(-2000 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextActivityTimer_NotCreated_BeforeWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduledEventId).Return(activityInfo, true)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(now.Add(2000 * time.Second)),
	})

	var activityInfoUpdated = common.CloneProto(activityInfo) // make a copy
	activityInfoUpdated.TimerTaskStatus = TimerTaskStatusCreatedScheduleToStart
	deps.mockMutableState.EXPECT().UpdateActivityTaskStatusWithTimerHeartbeat(activityInfoUpdated.ScheduledEventId, activityInfoUpdated.TimerTaskStatus, nil).Return(nil)
	deps.mockMutableState.EXPECT().AddTasks(&tasks.ActivityTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         deps.workflowKey,
		VisibilityTimestamp: activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToStartTimeout.AsDuration()),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
}

func TestCreateNextActivityTimer_NotCreated_NoWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduledEventId).Return(activityInfo, true)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: nil,
	})

	var activityInfoUpdated = common.CloneProto(activityInfo) // make a copy
	activityInfoUpdated.TimerTaskStatus = TimerTaskStatusCreatedScheduleToStart
	deps.mockMutableState.EXPECT().UpdateActivityTaskStatusWithTimerHeartbeat(activityInfoUpdated.ScheduledEventId, activityInfoUpdated.TimerTaskStatus, nil).Return(nil)
	deps.mockMutableState.EXPECT().AddTasks(&tasks.ActivityTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         deps.workflowKey,
		VisibilityTimestamp: activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToStartTimeout.AsDuration()),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
}

func TestCreateNextActivityTimer_HeartbeatTimer_AfterWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(now.Add(-2000 * time.Second)),
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.False(t, modified)
}

func TestCreateNextActivityTimer_HeartbeatTimer_BeforeWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduledEventId).Return(activityInfo, true)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamppb.New(now.Add(2000 * time.Second)),
	})

	taskVisibilityTimestamp := activityInfo.StartedTime.AsTime().Add(activityInfo.HeartbeatTimeout.AsDuration())

	var activityInfoUpdated = common.CloneProto(activityInfo) // make a copy
	activityInfoUpdated.TimerTaskStatus = TimerTaskStatusCreatedHeartbeat
	deps.mockMutableState.EXPECT().UpdateActivityTaskStatusWithTimerHeartbeat(activityInfo.ScheduledEventId, activityInfoUpdated.TimerTaskStatus, &taskVisibilityTimestamp).Return(nil)
	deps.mockMutableState.EXPECT().AddTasks(&tasks.ActivityTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         deps.workflowKey,
		VisibilityTimestamp: taskVisibilityTimestamp,
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
}

func TestCreateNextActivityTimer_HeartbeatTimer_NoWorkflowExpiry(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	deps.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduledEventId).Return(activityInfo, true)
	deps.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: nil,
	})

	taskVisibilityTimestamp := activityInfo.StartedTime.AsTime().Add(activityInfo.HeartbeatTimeout.AsDuration())

	var activityInfoUpdated = common.CloneProto(activityInfo) // make a copy
	activityInfoUpdated.TimerTaskStatus = TimerTaskStatusCreatedHeartbeat
	deps.mockMutableState.EXPECT().UpdateActivityTaskStatusWithTimerHeartbeat(activityInfo.ScheduledEventId, activityInfoUpdated.TimerTaskStatus, &taskVisibilityTimestamp).Return(nil)
	deps.mockMutableState.EXPECT().AddTasks(&tasks.ActivityTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         deps.workflowKey,
		VisibilityTimestamp: taskVisibilityTimestamp,
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		EventID:             activityInfo.ScheduledEventId,
		Attempt:             activityInfo.Attempt,
	})

	modified, err := deps.timerSequence.CreateNextActivityTimer()
	require.NoError(t, err)
	require.True(t, modified)
}

func TestLoadAndSortUserTimers_None(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	timerInfos := map[string]*persistencespb.TimerInfo{}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortUserTimers()
	require.Empty(t, timerSequenceIDs)
}

func TestLoadAndSortUserTimers_One(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timer1Expiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timer1Expiry,
		TaskStatus:     TimerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortUserTimers()
	require.Equal(t, []TimerSequenceID{{
		EventID:      timerInfo.GetStartedEventId(),
		Timestamp:    timer1Expiry.AsTime(),
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: true,
		Attempt:      1,
	}}, timerSequenceIDs)
}

func TestLoadAndSortUserTimers_Multiple(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timer1Expiry := timestamppb.New(now.Add(100))
	timer2Expiry := timestamppb.New(now.Add(200))
	timerInfo1 := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timer1Expiry,
		TaskStatus:     TimerTaskStatusCreated,
	}
	timerInfo2 := &persistencespb.TimerInfo{
		Version:        1234,
		TimerId:        "other random timer ID",
		StartedEventId: 4567,
		ExpiryTime:     timestamppb.New(now.Add(200)),
		TaskStatus:     TimerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{
		timerInfo1.TimerId: timerInfo1,
		timerInfo2.TimerId: timerInfo2,
	}
	deps.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortUserTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      timerInfo1.GetStartedEventId(),
			Timestamp:    timer1Expiry.AsTime(),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: true,
			Attempt:      1,
		},
		{
			EventID:      timerInfo2.GetStartedEventId(),
			Timestamp:    timer2Expiry.AsTime(),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: false,
			Attempt:      1,
		},
	}, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_None(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	activityInfos := map[int64]*persistencespb.ActivityInfo{}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Empty(t, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_One_NotScheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        common.EmptyEventID,
		ScheduledTime:           nil,
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusNone,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Empty(t, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_One_Scheduled_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToStartTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_One_Scheduled_Started_WithHeartbeatTimeout(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedStartToClose | TimerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.HeartbeatTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.StartToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_One_Scheduled_Started_WithoutHeartbeatTimeout(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.StartToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_One_Scheduled_Started_Heartbeated_WithHeartbeatTimeout(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedStartToClose | TimerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.LastHeartbeatUpdateTime.AsTime().Add(activityInfo.HeartbeatTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.StartToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_One_Scheduled_Started_Heartbeated_WithoutHeartbeatTimeout(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.EqualValues(t, []TimerSequenceID{
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.StartToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func TestLoadAndSortActivityTimers_Multiple(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo1 := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfo2 := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        2345,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "other random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(11),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1001),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(101),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(6),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(800 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 21,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{
		activityInfo1.ScheduledEventId: activityInfo1,
		activityInfo2.ScheduledEventId: activityInfo2,
	}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      activityInfo2.ScheduledEventId,
			Timestamp:    activityInfo2.ScheduledTime.AsTime().Add(activityInfo2.ScheduleToStartTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			TimerCreated: false,
			Attempt:      activityInfo2.Attempt,
		},
		{
			EventID:      activityInfo1.ScheduledEventId,
			Timestamp:    activityInfo1.StartedTime.AsTime().Add(activityInfo1.StartToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			TimerCreated: false,
			Attempt:      activityInfo1.Attempt,
		},
		{
			EventID:      activityInfo1.ScheduledEventId,
			Timestamp:    activityInfo1.ScheduledTime.AsTime().Add(activityInfo1.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: false,
			Attempt:      activityInfo1.Attempt,
		},
		{
			EventID:      activityInfo2.ScheduledEventId,
			Timestamp:    activityInfo2.ScheduledTime.AsTime().Add(activityInfo2.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: false,
			Attempt:      activityInfo2.Attempt,
		},
	}, timerSequenceIDs)
}

func TestGetUserTimerTimeout(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	timerExpiry := timestamppb.New(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:        123,
		TimerId:        "some random timer ID",
		StartedEventId: 456,
		ExpiryTime:     timerExpiry,
		TaskStatus:     TimerTaskStatusCreated,
	}

	expectedTimerSequence := &TimerSequenceID{
		EventID:      timerInfo.StartedEventId,
		Timestamp:    timerExpiry.AsTime(),
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: true,
		Attempt:      1,
	}

	timerSequence := deps.timerSequence.getUserTimerTimeout(timerInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)

	timerInfo.TaskStatus = TimerTaskStatusNone
	expectedTimerSequence.TimerCreated = false
	timerSequence = deps.timerSequence.getUserTimerTimeout(timerInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)
}

func TestGetActivityScheduleToStartTimeout_WithTimeout_NotScheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        common.EmptyEventID,
		ScheduledTime:           nil,
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToStartTimeout_WithTimeout_Scheduled_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	expectedTimerSequence := &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToStartTimeout.AsDuration()),
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	expectedTimerSequence.TimerCreated = false
	timerSequence = deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)
}

func TestGetActivityScheduleToStartTimeout_WithTimeout_Scheduled_Started(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Second)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToStartTimeout_WithoutTimeout_NotScheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        common.EmptyEventID,
		ScheduledTime:           nil,
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(0),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToStartTimeout_WithoutTimeout_Scheduled_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(0),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToStartTimeout_WithoutTimeout_Scheduled_Started(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Second)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(0),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToCloseTimeout_WithTimeout_NotScheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        common.EmptyEventID,
		ScheduledTime:           nil,
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToCloseTimeout_WithTimeout_Scheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		FirstScheduledTime:      timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose,
		Attempt:                 12,
	}

	expectedTimerSequence := &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	expectedTimerSequence.TimerCreated = false
	timerSequence = deps.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)
}

func TestGetActivityScheduleToCloseTimeout_WithoutTimeout_NotScheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        common.EmptyEventID,
		ScheduledTime:           nil,
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(0),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityScheduleToCloseTimeout_WithoutTimeout_Scheduled(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(0),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedScheduleToClose,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityStartToCloseTimeout_WithTimeout_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityStartToCloseTimeout_WithTimeout_Started(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}

	expectedTimerSequence := &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.StartToCloseTimeout.AsDuration()),
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequence := deps.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	expectedTimerSequence.TimerCreated = false
	timerSequence = deps.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)
}

func TestGetActivityStartToCloseTimeout_WithoutTimeout_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(0),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityStartToCloseTimeout_WithoutTimeout_Started(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(0),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityHeartbeatTimeout_WithHeartbeat_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityHeartbeatTimeout_WithHeartbeat_Started_NoHeartbeat(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	expectedTimerSequence := &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    activityInfo.StartedTime.AsTime().Add(activityInfo.HeartbeatTimeout.AsDuration()),
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequence := deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	expectedTimerSequence.TimerCreated = false
	timerSequence = deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)
}

func TestGetActivityHeartbeatTimeout_WithHeartbeat_Started_Heartbeated(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	expectedTimerSequence := &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    activityInfo.LastHeartbeatUpdateTime.AsTime().Add(activityInfo.HeartbeatTimeout.AsDuration()),
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequence := deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	expectedTimerSequence.TimerCreated = false
	timerSequence = deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Equal(t, expectedTimerSequence, timerSequence)
}

func TestGetActivityHeartbeatTimeout_WithoutHeartbeat_NotStarted(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          common.EmptyEventID,
		StartedTime:             nil,
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityHeartbeatTimeout_WithoutHeartbeat_Started_NoHeartbeat(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: nil,
		TimerTaskStatus:         TimerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestGetActivityHeartbeatTimeout_WithoutHeartbeat_Started_Heartbeated(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduledEventId:        234,
		ScheduledTime:           timestamppb.New(now),
		StartedEventId:          345,
		StartedTime:             timestamppb.New(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamppb.New(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         TimerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	timerSequence := deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Empty(t, timerSequence)

	activityInfo.TimerTaskStatus = TimerTaskStatusNone
	timerSequence = deps.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	require.Empty(t, timerSequence)
}

func TestConversion(t *testing.T) {
	require.Equal(t, int32(TimerTaskStatusCreatedStartToClose), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
	require.Equal(t, int32(TimerTaskStatusCreatedScheduleToStart), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START))
	require.Equal(t, int32(TimerTaskStatusCreatedScheduleToClose), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE))
	require.Equal(t, int32(TimerTaskStatusCreatedHeartbeat), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_HEARTBEAT))

	require.Equal(t, TimerTaskStatusNone, 0)
	require.Equal(t, TimerTaskStatusCreated, 1)
	require.Equal(t, TimerTaskStatusCreatedStartToClose, 1)
	require.Equal(t, TimerTaskStatusCreatedScheduleToStart, 2)
	require.Equal(t, TimerTaskStatusCreatedScheduleToClose, 4)
	require.Equal(t, TimerTaskStatusCreatedHeartbeat, 8)
}

func TestLess_CompareTime(t *testing.T) {
	now := time.Now().UTC()
	timerSequenceID1 := TimerSequenceID{
		EventID:      123,
		Timestamp:    now,
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequenceID2 := TimerSequenceID{
		EventID:      123,
		Timestamp:    now.Add(time.Second),
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequenceIDs := TimerSequenceIDs([]TimerSequenceID{timerSequenceID1, timerSequenceID2})
	require.True(t, timerSequenceIDs.Less(0, 1))
	require.False(t, timerSequenceIDs.Less(1, 0))
}

func TestLess_CompareEventID(t *testing.T) {
	now := time.Now().UTC()
	timerSequenceID1 := TimerSequenceID{
		EventID:      122,
		Timestamp:    now,
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequenceID2 := TimerSequenceID{
		EventID:      123,
		Timestamp:    now,
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequenceIDs := TimerSequenceIDs([]TimerSequenceID{timerSequenceID1, timerSequenceID2})
	require.True(t, timerSequenceIDs.Less(0, 1))
	require.False(t, timerSequenceIDs.Less(1, 0))
}

func TestLess_CompareType(t *testing.T) {
	now := time.Now().UTC()
	timerSequenceID1 := TimerSequenceID{
		EventID:      123,
		Timestamp:    now,
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequenceID2 := TimerSequenceID{
		EventID:      123,
		Timestamp:    now,
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: true,
		Attempt:      12,
	}

	timerSequenceIDs := TimerSequenceIDs([]TimerSequenceID{timerSequenceID1, timerSequenceID2})
	require.True(t, timerSequenceIDs.Less(0, 1))
	require.False(t, timerSequenceIDs.Less(1, 0))
}

func TestLoadAndSortActivityTimers_FirstScheduledTime(t *testing.T) {
	deps := setupTimerSequenceTest(t)
	defer deps.controller.Finish()

	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		ScheduledEventId:       234,
		ScheduledTime:          timestamppb.New(now),
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout: timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:    timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:       timestamp.DurationFromSeconds(1),
		TimerTaskStatus:        TimerTaskStatusCreatedScheduleToClose | TimerTaskStatusCreatedScheduleToStart,
		Attempt:                12,
	}
	activityInfo.FirstScheduledTime = timestamppb.New(now.Add(1 * time.Second))
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduledEventId: activityInfo}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := deps.timerSequence.LoadAndSortActivityTimers()
	require.Equal(t, []TimerSequenceID{
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.ScheduledTime.AsTime().Add(activityInfo.ScheduleToStartTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
		{
			EventID:      activityInfo.ScheduledEventId,
			Timestamp:    activityInfo.FirstScheduledTime.AsTime().Add(activityInfo.ScheduleToCloseTimeout.AsDuration()),
			TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			TimerCreated: true,
			Attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}
