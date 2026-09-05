package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestScheduleActivityBufferAll(t *testing.T) {
	t.Parallel()
	env := newScheduleEnv(t, append(scheduleCommonOpts(t),
		testcore.WithDynamicConfig(activity.Enabled, true),
		testcore.WithDynamicConfig(activity.EnableCallbacks, true),
	)...)
	ctx, cancel := context.WithTimeout(chasmContextFactory(testcore.NewContext()), 2*awaitTimeout)
	defer cancel()
	scheduleID := testcore.RandomizeStr("schedule-activity-buffer-all")
	taskQueue := testcore.RandomizeStr("schedule-activity-buffer-all")

	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec: intervalSpec(noOpInterval),
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartActivity{
			StartActivity: &schedulepb.StartActivityExecutionInfo{
				ActivityId:          "scheduled-activity",
				ActivityType:        &commonpb.ActivityType{Name: "scheduled-activity-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
				StartToCloseTimeout: durationpb.New(time.Minute),
				SearchAttributes:    &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"CustomKeywordField": sadefs.MustEncodeValue("scheduled-user-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)}},
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(time.Second),
					MaximumAttempts: 2,
				},
			},
		}},
		Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
		State:    &schedulepb.ScheduleState{Paused: true},
	})

	triggerAt := func(at time.Time) {
		patchSchedule(ctx, t, env, scheduleID, &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			ScheduledTime: timestamppb.New(at),
		}})
	}
	triggerAt(time.Now().UTC())
	triggerAt(time.Now().UTC().Add(time.Second))

	poll := func() *workflowservice.PollActivityTaskQueueResponse {
		pollCtx, cancel := context.WithTimeout(ctx, awaitTimeout)
		defer cancel()
		response, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "schedule activity test worker",
		})
		require.NoError(t, err)
		return response
	}
	complete := func(task *workflowservice.PollActivityTaskQueueResponse) {
		_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: task.GetTaskToken(),
			Identity:  "schedule activity test worker",
		})
		require.NoError(t, err)
	}

	first := poll()
	require.NotEmpty(t, first.GetTaskToken())

	await.RequireTruef(t, func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: env.Namespace().String(), ScheduleId: scheduleID,
		})
		if err != nil {
			return false
		}
		return desc.GetInfo().GetActionKind() == enumspb.EXECUTION_TYPE_ACTIVITY &&
			desc.GetInfo().GetActionType() == "scheduled-activity-type" &&
			len(desc.GetInfo().GetRunningExecutions()) == 1 &&
			len(desc.GetInfo().GetRunningWorkflows()) == 0
	}, awaitTimeout, pollInterval, "activity schedule should expose its generic running execution")

	_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: first.GetTaskToken(),
		Failure: &failurepb.Failure{
			Message: "retryable scheduled activity failure",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
			},
		},
		Identity: "schedule activity test worker",
	})
	require.NoError(t, err)

	await.RequireTruef(t, func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: env.Namespace().String(), ScheduleId: scheduleID,
		})
		return err == nil && desc.GetInfo().GetActionCount() == 1 &&
			desc.GetInfo().GetBufferSize() == 1 && len(desc.GetInfo().GetRunningExecutions()) == 1
	}, awaitTimeout, pollInterval, "activity retries must retain the original action and overlap slot")

	retry := poll()
	require.Equal(t, first.GetActivityId(), retry.GetActivityId())
	complete(retry)
	second := poll()
	require.NotEmpty(t, second.GetTaskToken())
	complete(second)

	await.RequireTruef(t, func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: env.Namespace().String(), ScheduleId: scheduleID,
		})
		if err != nil || len(desc.GetInfo().GetRecentActions()) < 2 {
			return false
		}
		for _, result := range desc.GetInfo().GetRecentActions() {
			if result.GetActionExecutionResult().GetExecution().GetType() != enumspb.EXECUTION_TYPE_ACTIVITY ||
				result.GetActionExecutionResult().GetActivityStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED ||
				result.GetStartWorkflowResult() != nil {
				return false
			}
		}
		return true
	}, awaitTimeout, pollInterval, "activity completions should be generic and leave workflow fields unset")

	listEntry := func(query string) *schedulepb.ScheduleListEntry {
		response, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace: env.Namespace().String(), MaximumPageSize: 10, Query: query,
		})
		if err != nil {
			return nil
		}
		for _, entry := range response.GetSchedules() {
			if entry.GetScheduleId() == scheduleID {
				return entry
			}
		}
		return nil
	}
	await.RequireTruef(t, func() bool {
		entry := listEntry("")
		if entry == nil || entry.GetInfo().GetActionKind() != enumspb.EXECUTION_TYPE_ACTIVITY ||
			entry.GetInfo().GetActionType() != "scheduled-activity-type" || entry.GetInfo().GetWorkflowType() != nil {
			return false
		}
		for _, result := range entry.GetInfo().GetRecentActions() {
			generic := result.GetActionExecutionResult()
			if generic.GetExecution().GetType() != enumspb.EXECUTION_TYPE_ACTIVITY ||
				generic.GetExecution().GetBusinessId() == "" || generic.GetExecution().GetRunId() == "" ||
				generic.GetActivityStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED ||
				result.GetCloseTime() == nil || result.GetStartWorkflowResult() != nil {
				return false
			}
		}
		return len(entry.GetInfo().GetRecentActions()) >= 2
	}, awaitTimeout, pollInterval, "list schedules should preserve generic activity terminal summaries")

	await.RequireTruef(t, func() bool {
		return listEntry(fmt.Sprintf("%s = 'Activity' AND %s = 'scheduled-activity-type'", chasmscheduler.ScheduleActionKindName, chasmscheduler.ScheduleActionTypeName)) != nil
	}, awaitTimeout, pollInterval, "activity schedules should be queryable by action kind and type")

	await.RequireTruef(t, func() bool {
		response, err := env.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("TemporalScheduledById = '%s' AND TemporalScheduledStartTime IS NOT NULL AND CustomKeywordField = 'scheduled-user-value'", scheduleID),
		})
		return err == nil && len(response.GetExecutions()) == 2
	}, awaitTimeout, pollInterval, "scheduled activity visibility should retain scheduling metadata and user search attributes")

}

func TestScheduleActivityTerminateOther(t *testing.T) {
	t.Parallel()
	env := newScheduleEnv(t, append(scheduleCommonOpts(t),
		testcore.WithDynamicConfig(activity.Enabled, true),
		testcore.WithDynamicConfig(activity.EnableCallbacks, true),
	)...)
	ctx, cancel := context.WithTimeout(chasmContextFactory(testcore.NewContext()), 2*awaitTimeout)
	defer cancel()
	scheduleID := testcore.RandomizeStr("schedule-activity-terminate")
	taskQueue := testcore.RandomizeStr("schedule-activity-terminate")

	createSchedule(ctx, t, env, scheduleID, &schedulepb.Schedule{
		Spec: intervalSpec(noOpInterval),
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartActivity{
			StartActivity: &schedulepb.StartActivityExecutionInfo{
				ActivityId:          "scheduled-activity",
				ActivityType:        &commonpb.ActivityType{Name: "scheduled-activity-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
				StartToCloseTimeout: durationpb.New(time.Minute),
			},
		}},
		Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER},
		State:    &schedulepb.ScheduleState{Paused: true},
	})

	poll := func() *workflowservice.PollActivityTaskQueueResponse {
		pollCtx, cancel := context.WithTimeout(ctx, awaitTimeout)
		defer cancel()
		response, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "schedule activity test worker",
		})
		require.NoError(t, err)
		return response
	}
	trigger := func(at time.Time) {
		patchSchedule(ctx, t, env, scheduleID, &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
			ScheduledTime: timestamppb.New(at),
		}})
	}

	trigger(time.Now().UTC())
	first := poll()
	require.NotEmpty(t, first.GetActivityRunId())
	trigger(time.Now().UTC().Add(time.Second))

	await.RequireTruef(t, func() bool {
		response, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace: env.Namespace().String(), ActivityId: first.GetActivityId(), RunId: first.GetActivityRunId(),
		})
		return err == nil && response.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
	}, awaitTimeout, pollInterval, "terminate-other should terminate the active standalone activity")

	second := poll()
	require.NotEqual(t, first.GetActivityId(), second.GetActivityId())
	_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(), TaskToken: second.GetTaskToken(), Identity: "schedule activity test worker",
	})
	require.NoError(t, err)

	await.RequireTruef(t, func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: env.Namespace().String(), ScheduleId: scheduleID,
		})
		if err != nil {
			return false
		}
		var terminated, completed bool
		for _, result := range desc.GetInfo().GetRecentActions() {
			generic := result.GetActionExecutionResult()
			if generic.GetExecution().GetType() != enumspb.EXECUTION_TYPE_ACTIVITY || result.GetStartWorkflowResult() != nil {
				return false
			}
			terminated = terminated || generic.GetActivityStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
			completed = completed || generic.GetActivityStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
		}
		return terminated && completed
	}, awaitTimeout, pollInterval, "activity terminal states should be exposed through generic schedule results")
}
