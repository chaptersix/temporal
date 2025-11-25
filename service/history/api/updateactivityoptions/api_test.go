package updateactivityoptions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestApplyActivityOptionsAcceptance(t *testing.T) {
	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

	testCases := []struct {
		name      string
		mergeInto *activitypb.ActivityOptions
		mergeFrom *activitypb.ActivityOptions
		expected  *activitypb.ActivityOptions
		mask      *fieldmaskpb.FieldMask
	}{
		{
			name:      "full mix - CamelCase",
			mergeFrom: options,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  options,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
					"ScheduleToCloseTimeout",
					"ScheduleToStartTimeout",
					"StartToCloseTimeout",
					"HeartbeatTimeout",
					"RetryPolicy.BackoffCoefficient",
					"RetryPolicy.InitialInterval",
					"RetryPolicy.MaximumInterval",
					"RetryPolicy.MaximumAttempts",
				},
			},
		},
		{
			name:      "full mix - snake_case",
			mergeFrom: options,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  options,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"start_to_close_timeout",
					"heartbeat_timeout",
					"retry_policy.backoff_coefficient",
					"retry_policy.initial_interval",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
		{
			name: "partial",
			mergeFrom: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
				ScheduleToCloseTimeout: durationpb.New(time.Second),
				ScheduleToStartTimeout: durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval: durationpb.New(time.Second),
					MaximumAttempts: 5,
				},
			},
			mergeInto: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(time.Second),
				HeartbeatTimeout:    durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			expected: options,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updateFields := util.ParseFieldMask(tc.mask)
			err := mergeActivityOptions(tc.mergeInto, tc.mergeFrom, updateFields)
			require.NoError(t, err)
			require.Equal(t, tc.expected.RetryPolicy.InitialInterval, tc.mergeInto.RetryPolicy.InitialInterval, "RetryInitialInterval")
			require.Equal(t, tc.expected.RetryPolicy.MaximumInterval, tc.mergeInto.RetryPolicy.MaximumInterval, "RetryMaximumInterval")
			require.Equal(t, tc.expected.RetryPolicy.BackoffCoefficient, tc.mergeInto.RetryPolicy.BackoffCoefficient, "RetryBackoffCoefficient")
			require.Equal(t, tc.expected.RetryPolicy.MaximumAttempts, tc.mergeInto.RetryPolicy.MaximumAttempts, "RetryMaximumAttempts")

			require.Equal(t, tc.expected.TaskQueue, tc.mergeInto.TaskQueue, "TaskQueue")

			require.Equal(t, tc.expected.ScheduleToCloseTimeout, tc.mergeInto.ScheduleToCloseTimeout, "ScheduleToCloseTimeout")
			require.Equal(t, tc.expected.ScheduleToStartTimeout, tc.mergeInto.ScheduleToStartTimeout, "ScheduleToStartTimeout")
			require.Equal(t, tc.expected.StartToCloseTimeout, tc.mergeInto.StartToCloseTimeout, "StartToCloseTimeout")
			require.Equal(t, tc.expected.HeartbeatTimeout, tc.mergeInto.HeartbeatTimeout, "HeartbeatTimeout")
		})
	}
}

func TestApplyActivityOptionsErrors(t *testing.T) {
	var err error
	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_interval"}}))
	require.Error(t, err)

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}}))
	require.Error(t, err)

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.backoff_coefficient"}}))
	require.Error(t, err)

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}}))
	require.Error(t, err)

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"taskQueue.name"}}))
	require.Error(t, err)
}

func TestApplyActivityOptionsReset(t *testing.T) {
	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

	fullMask := &fieldmaskpb.FieldMask{
		Paths: []string{
			"schedule_to_close_timeout",
			"schedule_to_start_timeout",
			"start_to_close_timeout",
			"heartbeat_timeout",
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	updateFields := util.ParseFieldMask(fullMask)

	err := mergeActivityOptions(options,
		&activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    5,
				BackoffCoefficient: 1.0,
			},
		},
		updateFields)
	require.NoError(t, err)

	require.Nil(t, options.ScheduleToCloseTimeout)
	require.Nil(t, options.ScheduleToStartTimeout)
	require.Nil(t, options.StartToCloseTimeout)
	require.Nil(t, options.HeartbeatTimeout)

	require.Nil(t, options.RetryPolicy.InitialInterval)
	require.Nil(t, options.RetryPolicy.MaximumInterval)
}

type activityOptionsTestDeps struct {
	controller                 *gomock.Controller
	mockShard                  *shard.ContextTest
	mockEventsCache            *events.MockCache
	mockNamespaceCache         *namespace.MockRegistry
	mockTaskGenerator          *workflow.MockTaskGenerator
	mockMutableState           *historyi.MockMutableState
	mockClusterMetadata        *cluster.MockMetadata
	executionInfo              *persistencespb.WorkflowExecutionInfo
	validator                  *api.CommandAttrValidator
	workflowCache              *wcache.MockCache
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	logger                     log.Logger
}

func setupActivityOptionsTest(t *testing.T) *activityOptionsTestDeps {
	controller := gomock.NewController(t)
	mockTaskGenerator := workflow.NewMockTaskGenerator(controller)
	mockMutableState := historyi.NewMockMutableState(controller)

	mockShard := shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

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
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
	mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()

	validator := api.NewCommandAttrValidator(
		mockShard.GetNamespaceRegistry(),
		mockShard.GetConfig(),
		nil,
	)

	workflowCache := wcache.NewMockCache(controller)
	workflowConsistencyChecker := api.NewWorkflowConsistencyChecker(
		mockShard,
		workflowCache,
	)

	return &activityOptionsTestDeps{
		controller:                 controller,
		mockShard:                  mockShard,
		mockEventsCache:            mockEventsCache,
		mockNamespaceCache:         mockNamespaceCache,
		mockTaskGenerator:          mockTaskGenerator,
		mockMutableState:           mockMutableState,
		mockClusterMetadata:        mockClusterMetadata,
		executionInfo:              executionInfo,
		validator:                  validator,
		workflowCache:              workflowCache,
		workflowConsistencyChecker: workflowConsistencyChecker,
		logger:                     logger,
	}
}

func Test_updateActivityOptionsWfNotRunning(t *testing.T) {
	deps := setupActivityOptionsTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	request := &historyservice.UpdateActivityOptionsRequest{}

	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	_, err := processActivityOptionsRequest(deps.validator, deps.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())
	require.Error(t, err)
	require.ErrorAs(t, err, &consts.ErrWorkflowCompleted)
}

func Test_updateActivityOptionsWfNoActivity(t *testing.T) {
	deps := setupActivityOptionsTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			ActivityOptions: &activitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task_queue_name"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
				},
			},
			Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
		},
	}

	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	deps.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err := processActivityOptionsRequest(deps.validator, deps.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())
	require.Error(t, err)
	require.ErrorAs(t, err, &consts.ErrActivityNotFound)
}

func Test_updateActivityOptionsAcceptance(t *testing.T) {
	deps := setupActivityOptionsTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	fullActivityInfo := &persistencespb.ActivityInfo{
		TaskQueue:               "task_queue_name",
		ScheduleToCloseTimeout:  durationpb.New(time.Second),
		ScheduleToStartTimeout:  durationpb.New(time.Second),
		StartToCloseTimeout:     durationpb.New(time.Second),
		HeartbeatTimeout:        durationpb.New(time.Second),
		RetryBackoffCoefficient: 1.0,
		RetryInitialInterval:    durationpb.New(time.Second),
		RetryMaximumInterval:    durationpb.New(time.Second),
		RetryMaximumAttempts:    5,
		ActivityId:              "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

	updateMask := &fieldmaskpb.FieldMask{
		Paths: []string{
			"task_queue.name",
			"schedule_to_close_timeout",
			"schedule_to_start_timeout",
			"start_to_close_timeout",
			"heartbeat_timeout",
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		StartToCloseTimeout:    durationpb.New(2 * time.Second),
		ScheduleToStartTimeout: durationpb.New(2 * time.Second),
		HeartbeatTimeout:       durationpb.New(2 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(2 * time.Second),
		},
	}

	deps.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	deps.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(fullActivityInfo, true)
	deps.mockMutableState.EXPECT().RegenerateActivityRetryTask(gomock.Any(), gomock.Any()).Return(nil)
	deps.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			ActivityOptions: options,
			UpdateMask:      updateMask,
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
		},
	}

	response, err := processActivityOptionsRequest(
		deps.validator, deps.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())

	require.NoError(t, err)
	require.NotNil(t, response)
}

func Test_updateActivityOptions_RestoreDefaultFail(t *testing.T) {
	deps := setupActivityOptionsTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	updateMask := &fieldmaskpb.FieldMask{
		Paths: []string{
			"task_queue.name",
			"schedule_to_close_timeout",
			"schedule_to_start_timeout",
			"start_to_close_timeout",
			"heartbeat_timeout",
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		StartToCloseTimeout:    durationpb.New(2 * time.Second),
		ScheduleToStartTimeout: durationpb.New(2 * time.Second),
		HeartbeatTimeout:       durationpb.New(2 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(2 * time.Second),
		},
	}

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			ActivityOptions: options,
			UpdateMask:      updateMask,
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
			RestoreOriginal: true,
		},
	}
	ctx := context.Background()

	// both restore flag and update mask are set
	_, err := Invoke(ctx, request, deps.mockShard, deps.workflowConsistencyChecker)
	require.Error(t, err)

	// not pending activity with such type
	request.UpdateRequest.ActivityOptions = nil
	request.UpdateRequest.UpdateMask = nil
	request.UpdateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Type{Type: "activity_type"}
	activityInfos := map[int64]*persistencespb.ActivityInfo{}
	deps.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	_, err = restoreOriginalOptions(ctx, deps.mockMutableState, request.GetUpdateRequest())
	require.Error(t, err)

	// not pending activity with such id
	request.UpdateRequest.ActivityOptions = nil
	request.UpdateRequest.UpdateMask = nil
	request.UpdateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"}
	deps.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err = restoreOriginalOptions(ctx, deps.mockMutableState, request.GetUpdateRequest())
	require.Error(t, err)

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		TaskQueue:        "task_queue_name",
		ScheduledEventId: 1,
	}

	// event not found
	err = errors.New("some error")
	deps.mockMutableState.EXPECT().GetActivityScheduledEvent(gomock.Any(), gomock.Any()).Return(nil, err)
	deps.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(ai, true)
	_, err = restoreOriginalOptions(ctx, deps.mockMutableState, request.GetUpdateRequest())
	require.Error(t, err)
}

func Test_updateActivityOptions_RestoreDefaultSuccess(t *testing.T) {
	deps := setupActivityOptionsTest(t)
	defer deps.controller.Finish()
	defer deps.mockShard.StopForTest()

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
			RestoreOriginal: true,
		},
	}
	ctx := context.Background()

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		TaskQueue:        "task_queue_name",
		ScheduledEventId: 1,
		StartedEventId:   2,
	}

	he := &historypb.HistoryEvent{
		EventId: -123,
		Version: 123,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId: "activity_id",
				ActivityType: &commonpb.ActivityType{
					Name: "activity_type",
				},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: "task_queue_name",
				},
				ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
				StartToCloseTimeout:    durationpb.New(2 * time.Second),
				ScheduleToStartTimeout: durationpb.New(2 * time.Second),
				HeartbeatTimeout:       durationpb.New(2 * time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(2 * time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(2 * time.Second),
				},
			},
		},
	}

	// event not found
	deps.mockMutableState.EXPECT().GetActivityScheduledEvent(gomock.Any(), gomock.Any()).Return(he, nil)
	deps.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(ai, true)
	deps.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)
	response, err := restoreOriginalOptions(ctx, deps.mockMutableState, request.GetUpdateRequest())
	require.NotNil(t, response)
	require.NoError(t, err)
}
