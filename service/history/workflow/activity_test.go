package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type activityTestDeps struct {
	controller            *gomock.Controller
	mockShard             *shard.ContextTest
	mockNamespaceRegistry *namespace.MockRegistry
	mockMutableState      *historyi.MockMutableState
	mutableState          *MutableStateImpl
}

func setupActivityTest(t *testing.T) *activityTestDeps {
	config := tests.NewDynamicConfig()
	controller := gomock.NewController(t)
	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{ShardId: 1},
		config,
	)

	mockMutableState := historyi.NewMockMutableState(controller)
	mockNamespaceRegistry := mockShard.Resource.NamespaceCache

	mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.GlobalNamespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	reg := hsm.NewRegistry()
	err := RegisterStateMachine(reg)
	require.NoError(t, err)
	mockShard.SetStateMachineRegistry(reg)

	mutableState := NewMutableState(
		mockShard, mockShard.MockEventsCache, mockShard.GetLogger(), tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)

	t.Cleanup(func() {
		controller.Finish()
		mockShard.StopForTest()
	})

	return &activityTestDeps{
		controller:            controller,
		mockShard:             mockShard,
		mockNamespaceRegistry: mockNamespaceRegistry,
		mockMutableState:      mockMutableState,
		mutableState:          mutableState,
	}
}

func TestGetActivityState(t *testing.T) {
	testCases := []struct {
		ai    *persistencespb.ActivityInfo
		state enumspb.PendingActivityState
	}{
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: true,
				StartedEventId:  1,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: true,
				StartedEventId:  common.EmptyEventID,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: false,
				StartedEventId:  common.EmptyEventID,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: false,
				StartedEventId:  1,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
	}

	for _, tc := range testCases {
		state := GetActivityState(tc.ai)
		require.Equal(t, tc.state, state)
	}
}

func TestGetPendingActivityInfoAcceptance(t *testing.T) {
	deps := setupActivityTest(t)

	now := deps.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	deps.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), deps.mockShard, deps.mockMutableState, ai)
	require.NoError(t, err)
	require.NotNil(t, pi)
}

func TestGetPendingActivityInfo_ActivityState(t *testing.T) {
	deps := setupActivityTest(t)

	testCases := []struct {
		paused          bool
		cancelRequested bool
		startedEventId  int64
		expectedState   enumspb.PendingActivityState
	}{
		{
			paused:          false,
			cancelRequested: false,
			startedEventId:  common.EmptyEventID,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			paused:          false,
			cancelRequested: false,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
		{
			paused:          false,
			cancelRequested: true,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			paused:          true,
			cancelRequested: false,
			startedEventId:  common.EmptyEventID,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_PAUSED,
		},
		{
			paused:          true,
			cancelRequested: false,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
		},
		{
			paused:          true,
			cancelRequested: true,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
	}

	now := deps.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	for _, tc := range testCases {
		ai.Paused = tc.paused
		ai.CancelRequested = tc.cancelRequested
		ai.StartedEventId = tc.startedEventId

		deps.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
		pi, err := GetPendingActivityInfo(context.Background(), deps.mockShard, deps.mockMutableState, ai)
		require.NoError(t, err)
		require.NotNil(t, pi)

		require.Equal(t, tc.expectedState, pi.State, fmt.Sprintf("failed for paused: %v, cancelRequested: %v, startedEventId: %v", tc.paused, tc.cancelRequested, tc.startedEventId))
	}
}

func TestGetPendingActivityInfoNoRetryPolicy(t *testing.T) {
	deps := setupActivityTest(t)

	now := deps.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	deps.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), deps.mockShard, deps.mockMutableState, ai)
	require.NoError(t, err)
	require.NotNil(t, pi)
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, pi.State)
	require.Equal(t, int32(1), pi.Attempt)
	require.Nil(t, pi.NextAttemptScheduleTime)
	require.Nil(t, pi.CurrentRetryInterval)
}

func TestGetPendingActivityInfoHasRetryPolicy(t *testing.T) {
	deps := setupActivityTest(t)

	now := deps.mockShard.GetTimeSource().Now().UTC()
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          common.EmptyEventID,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now.Add(1 * time.Minute)),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Minute)),
		HasRetryPolicy:          true,
		RetryMaximumInterval:    nil,
		RetryInitialInterval:    durationpb.New(time.Minute),
		RetryBackoffCoefficient: 1.0,
		RetryMaximumAttempts:    10,
		TaskQueue:               "task-queue",
	}

	deps.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), deps.mockShard, deps.mockMutableState, ai)
	require.NoError(t, err)
	require.NotNil(t, pi)
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pi.State)
	require.Equal(t, int32(2), pi.Attempt)
	require.Equal(t, ai.ActivityId, pi.ActivityId)
	require.Nil(t, pi.HeartbeatDetails)
	require.Nil(t, pi.LastHeartbeatTime)
	require.Nil(t, pi.LastStartedTime)
	require.NotNil(t, pi.NextAttemptScheduleTime) // activity is waiting for retry
	require.Equal(t, ai.RetryMaximumAttempts, pi.MaximumAttempts)
	require.Nil(t, pi.AssignedBuildId)
	require.Equal(t, durationpb.New(2*time.Minute), pi.CurrentRetryInterval)
	require.Equal(t, ai.ScheduledTime, pi.ScheduledTime)
	require.Equal(t, ai.LastAttemptCompleteTime, pi.LastAttemptCompleteTime)
	require.NotNil(t, pi.ActivityOptions)
	require.NotNil(t, pi.ActivityOptions.RetryPolicy)
	require.Equal(t, ai.TaskQueue, pi.ActivityOptions.TaskQueue.Name)
	require.Equal(t, ai.ScheduleToCloseTimeout, pi.ActivityOptions.ScheduleToCloseTimeout)
	require.Equal(t, ai.ScheduleToStartTimeout, pi.ActivityOptions.ScheduleToStartTimeout)
	require.Equal(t, ai.StartToCloseTimeout, pi.ActivityOptions.StartToCloseTimeout)
	require.Equal(t, ai.HeartbeatTimeout, pi.ActivityOptions.HeartbeatTimeout)
	require.Equal(t, ai.RetryMaximumInterval, pi.ActivityOptions.RetryPolicy.MaximumInterval)
	require.Equal(t, ai.RetryInitialInterval, pi.ActivityOptions.RetryPolicy.InitialInterval)
	require.Equal(t, ai.RetryBackoffCoefficient, pi.ActivityOptions.RetryPolicy.BackoffCoefficient)
	require.Equal(t, ai.RetryMaximumAttempts, pi.ActivityOptions.RetryPolicy.MaximumAttempts)
}

func addActivityInfo(t *testing.T, deps *activityTestDeps) *persistencespb.ActivityInfo {
	activityId := "activity-id"
	activityScheduledEvent := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				WorkflowTaskCompletedEventId: 4,
				ActivityId:                   activityId,
				ActivityType:                 &commonpb.ActivityType{Name: "activity-type"},
				TaskQueue:                    &taskqueuepb.TaskQueue{Name: "task-queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:                        nil,
				ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
				ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
				StartToCloseTimeout:          durationpb.New(20 * time.Second),
				HeartbeatTimeout:             durationpb.New(20 * time.Second),
			}},
	}

	deps.mockShard.MockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	ai, err := deps.mutableState.ApplyActivityTaskScheduledEvent(0, activityScheduledEvent)
	require.NoError(t, err)
	return ai
}

func TestResetPausedActivityAcceptance(t *testing.T) {
	deps := setupActivityTest(t)
	ai := addActivityInfo(t, deps)

	prevStamp := ai.Stamp
	pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
		PauseTime: timestamppb.New(time.Now()),
		PausedBy: &persistencespb.ActivityInfo_PauseInfo_Manual_{
			Manual: &persistencespb.ActivityInfo_PauseInfo_Manual{
				Identity: "test_identity",
				Reason:   "test_reason",
			},
		},
	}

	err := PauseActivity(deps.mutableState, ai.ActivityId, pauseInfo)
	require.NoError(t, err)
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.NotNil(t, ai.PauseInfo)
	require.Equal(t, ai.PauseInfo.GetManual().Identity, "test_identity")
	require.Equal(t, ai.PauseInfo.GetManual().Reason, "test_reason")

	prevStamp = ai.Stamp
	err = ResetActivity(context.Background(), deps.mockShard, deps.mutableState, ai.ActivityId,
		false, true, false, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is not reset")
	require.Equal(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should not change")
	require.True(t, ai.Paused, "ActivityInfo.Paused shouldn't change by reset")
}

func TestResetAndUnPauseActivityAcceptance(t *testing.T) {
	deps := setupActivityTest(t)
	ai := addActivityInfo(t, deps)

	prevStamp := ai.Stamp
	pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
		PauseTime: timestamppb.New(time.Now()),
		PausedBy: &persistencespb.ActivityInfo_PauseInfo_Manual_{
			Manual: &persistencespb.ActivityInfo_PauseInfo_Manual{
				Identity: "test_identity",
				Reason:   "test_reason",
			},
		},
	}

	err := PauseActivity(deps.mutableState, ai.ActivityId, pauseInfo)
	require.NoError(t, err)
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.NotNil(t, ai.PauseInfo)
	require.Equal(t, ai.PauseInfo.GetManual().Identity, "test_identity")
	require.Equal(t, ai.PauseInfo.GetManual().Reason, "test_reason")

	prevStamp = ai.Stamp
	err = ResetActivity(context.Background(), deps.mockShard, deps.mutableState, ai.ActivityId,
		false, false, false, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is not reset")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.False(t, ai.Paused, "ActivityInfo.Paused shouldn't change by reset")
}

func TestUnpauseActivityWithResumeAcceptance(t *testing.T) {
	deps := setupActivityTest(t)
	ai := addActivityInfo(t, deps)

	prevStamp := ai.Stamp
	err := PauseActivity(deps.mutableState, ai.ActivityId, nil)
	require.NoError(t, err)
	require.Nil(t, ai.PauseInfo)

	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.Equal(t, true, ai.Paused, "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.Stamp
	_, err = UnpauseActivityWithResume(deps.mockShard, deps.mutableState, ai, false, 0)
	require.NoError(t, err)

	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.Equal(t, false, ai.Paused, "ActivityInfo.Paused was not unpaused")
}

func TestUnpauseActivityWithNewRun(t *testing.T) {
	deps := setupActivityTest(t)
	ai := addActivityInfo(t, deps)

	prevStamp := ai.Stamp
	err := PauseActivity(deps.mutableState, ai.ActivityId, nil)
	require.NoError(t, err)

	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.Equal(t, true, ai.Paused, "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.Stamp
	fakeScheduledTime := time.Now().UTC().Add(5 * time.Minute)
	ai.ScheduledTime = timestamppb.New(fakeScheduledTime)
	_, err = UnpauseActivityWithResume(deps.mockShard, deps.mutableState, ai, true, 0)
	require.NoError(t, err)

	// scheduled time should be reset to
	require.NotEqual(t, fakeScheduledTime, ai.ScheduledTime.AsTime())
	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.Equal(t, false, ai.Paused, "ActivityInfo.Paused was not unpaused")
}

func TestUnpauseActivityWithResetAcceptance(t *testing.T) {
	deps := setupActivityTest(t)
	ai := addActivityInfo(t, deps)

	prevStamp := ai.Stamp
	pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
		PauseTime: timestamppb.New(time.Now()),
		PausedBy: &persistencespb.ActivityInfo_PauseInfo_RuleId{
			RuleId: "rule_id",
		},
	}

	err := PauseActivity(deps.mutableState, ai.ActivityId, pauseInfo)
	require.NoError(t, err)
	require.NotNil(t, ai.PauseInfo)
	require.Equal(t, ai.PauseInfo.GetRuleId(), "rule_id")

	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.Equal(t, true, ai.Paused, "ActivityInfo.Paused was not unpaused")

	prevStamp = ai.Stamp
	_, err = UnpauseActivityWithReset(deps.mockShard, deps.mutableState, ai, false, true, 0)
	require.NoError(t, err)
	require.Equal(t, int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	require.Equal(t, false, ai.Paused, "ActivityInfo.Paused was not unpaused")
	require.NotEqual(t, prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	require.Nil(t, ai.LastHeartbeatUpdateTime)
	require.Nil(t, ai.LastHeartbeatDetails)
}
