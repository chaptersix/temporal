package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func activitySchedule() *schedulepb.Schedule {
	schedule := defaultSchedule()
	schedule.Action = &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartActivity{StartActivity: &schedulepb.StartActivityExecutionInfo{
		ActivityId: "scheduled-activity", ActivityType: &commonpb.ActivityType{Name: "activity-type"}, TaskQueue: &taskqueuepb.TaskQueue{Name: "activities"}, StartToCloseTimeout: durationpb.New(time.Minute),
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("initial-input")}}},
	}}}
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	return schedule
}

func TestActivitySchedulePolicyValidation(t *testing.T) {
	for _, custom := range []bool{false, true} {
		schedule := activitySchedule()
		if custom {
			schedule.Policies.OverlapPolicy = 0
			schedule.Policies.CustomOverlapPolicy = &schedulepb.CustomOverlapPolicy{Name: "temporal.buffer_latest"}
		}
		require.NoError(t, scheduler.ValidateScheduleActionPolicies(schedule, nil))
		for _, patch := range []*schedulepb.SchedulePatch{
			{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}},
			{BackfillRequest: []*schedulepb.BackfillRequest{{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}}},
			{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{CustomOverlapPolicy: &schedulepb.CustomOverlapPolicy{Name: "unknown"}}},
		} {
			require.Error(t, scheduler.ValidateScheduleActionPolicies(schedule, patch))
		}
	}
	schedule := activitySchedule()
	schedule.Policies.OverlapPolicy = 0
	require.Error(t, scheduler.ValidateScheduleActionPolicies(schedule, &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}}))
	schedule.Policies.CustomOverlapPolicy = &schedulepb.CustomOverlapPolicy{}
	require.Error(t, scheduler.ValidateScheduleActionPolicies(schedule, nil))
	schedule.Policies.CustomOverlapPolicy = &schedulepb.CustomOverlapPolicy{Name: "temporal.buffer_latest"}
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	require.Error(t, scheduler.ValidateScheduleActionPolicies(schedule, nil))
	workflow := defaultSchedule()
	workflow.Policies.CustomOverlapPolicy = &schedulepb.CustomOverlapPolicy{Name: "temporal.buffer_latest"}
	require.Error(t, scheduler.ValidateScheduleActionPolicies(workflow, nil))
}

func TestActivityStartRetryUsesCurrentConfigurationAndStableIdentity(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	env.Scheduler.Schedule = activitySchedule()
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := timestamppb.New(env.TimeSource.Now())
	start := &schedulespb.BufferedStart{RequestId: "request", OccurrenceId: "1", ActualTime: now, NominalTime: now, Attempt: 1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, Execution: &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "stable-target"}}
	invoker.BufferedStarts = []*schedulespb.BufferedStart{start}
	invoker.LastProcessedTime = now
	var requests []*workflowservice.StartActivityExecutionRequest
	env.mockFrontendClient.EXPECT().StartActivityExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *workflowservice.StartActivityExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartActivityExecutionResponse, error) {
		requests = append(requests, proto.Clone(request).(*workflowservice.StartActivityExecutionRequest))
		if len(requests) == 1 {
			return nil, serviceerror.NewUnavailable("lost response")
		}
		return &workflowservice.StartActivityExecutionResponse{RunId: "activity-run", Started: false}, nil
	}).Times(2)
	executeTaskOnce(t, env, ctx, invoker)
	require.EqualValues(t, 0, env.Scheduler.Info.ActionCount)
	env.Scheduler.Schedule.GetAction().GetStartActivity().Input.Payloads[0].Data = []byte("updated-input")
	env.Scheduler.Schedule.GetAction().GetStartActivity().ActivityId = "edited-base"
	invoker.LastProcessedTime = start.BackoffTime
	executeTaskOnce(t, env, ctx, invoker)
	require.EqualValues(t, 1, env.Scheduler.Info.ActionCount)
	require.Equal(t, requests[0].RequestId, requests[1].RequestId)
	require.Equal(t, "stable-target", requests[1].ActivityId)
	require.Equal(t, []byte("updated-input"), requests[1].Input.Payloads[0].Data)
	require.Equal(t, enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE, requests[1].IdReusePolicy)
	require.Equal(t, enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL, requests[1].IdConflictPolicy)
	require.Len(t, requests[1].CompletionCallbacks, 1)
	require.Empty(t, start.WorkflowId)
	require.Empty(t, start.RunId)
	require.Equal(t, "activity-run", start.Execution.RunId)
	info := env.Scheduler.ListInfo(env.ReadContext())
	require.Nil(t, info.WorkflowType)
	require.Equal(t, enumspb.EXECUTION_TYPE_ACTIVITY, info.ActionKind)
	require.Equal(t, "activity-type", info.ActionType)
	require.EqualValues(t, 1, info.RunningExecutionCount)
	require.Nil(t, info.RecentActions[0].StartWorkflowResult)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, info.RecentActions[0].ActionExecutionResult.GetActivityStatus())
}

func TestActivityCompletionBeforeStartAckSurvivesBufferMovement(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	env.Scheduler.Schedule = activitySchedule()
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := timestamppb.New(env.TimeSource.Now())
	start := &schedulespb.BufferedStart{RequestId: "request", OccurrenceId: "1", ActualTime: now, NominalTime: now, Attempt: 1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, Execution: &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "target"}}
	invoker.BufferedStarts = []*schedulespb.BufferedStart{start}
	invoker.LastProcessedTime = now
	env.mockFrontendClient.EXPECT().StartActivityExecution(gomock.Any(), gomock.Any()).Return(&workflowservice.StartActivityExecutionResponse{RunId: "run", Started: true}, nil)
	env.ExpectReadComponent(ctx, invoker)
	batch, err := env.handler.LoadExecutionBatchForTest(env.EngineContext(), chasm.ComponentRef{})
	require.NoError(t, err)
	result := env.handler.ExecuteBatchForTest(env.EngineContext(), batch)
	completion := &persistencespb.ChasmNexusCompletion{RequestId: "request", Outcome: &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{Data: []byte("activity-output")}}, CloseTime: now}
	require.NoError(t, env.Scheduler.HandleNexusCompletion(ctx, completion))
	invoker.BufferedStarts = append([]*schedulespb.BufferedStart{{RequestId: "unrelated", OccurrenceId: "2", Attempt: -1}}, invoker.BufferedStarts...)
	env.ExpectUpdateComponent(ctx, invoker)
	_, err = env.handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
	require.NoError(t, err)
	require.EqualValues(t, 1, env.Scheduler.Info.ActionCount)
	require.Equal(t, "run", start.Execution.RunId)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, start.Completion.GetActivityStatus())
	require.Nil(t, start.Completed)
	require.Nil(t, env.Scheduler.LastCompletionResult.Get(ctx).Success)
	require.NoError(t, env.Scheduler.HandleNexusCompletion(ctx, completion))
	env.ExpectUpdateComponent(ctx, invoker)
	_, err = env.handler.CommitExecutionResultForTest(env.EngineContext(), chasm.ComponentRef{}, result)
	require.NoError(t, err)
	require.EqualValues(t, 1, env.Scheduler.Info.ActionCount)
}

func TestActivityScheduleRejectsKindChangesAndMigration(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule = activitySchedule()
	_, err := env.Scheduler.MigrateToWorkflow(env.MutableContext(), &schedulerpb.MigrateToWorkflowRequest{})
	require.Error(t, err)
	_, err = env.Scheduler.Update(env.MutableContext(), &schedulerpb.UpdateScheduleRequest{FrontendRequest: &workflowservice.UpdateScheduleRequest{Schedule: defaultSchedule()}})
	require.Error(t, err)
	require.NotNil(t, env.Scheduler.Schedule.Action.GetStartActivity())
}

func TestActivityOverlapPoliciesPersistAndRelease(t *testing.T) {
	cases := []struct {
		name                    string
		policy                  enumspb.ScheduleOverlapPolicy
		custom                  string
		pending, skipped, ready int
		terminate               bool
	}{
		{name: "skip", policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, skipped: 2},
		{name: "buffer-one", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, pending: 1, skipped: 1},
		{name: "buffer-all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, pending: 2},
		{name: "allow-all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, pending: 2, ready: 2},
		{name: "terminate-other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, pending: 2, terminate: true},
		{name: "buffer-latest", custom: "temporal.buffer_latest", pending: 1, skipped: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schedule := activitySchedule()
			schedule.Policies.OverlapPolicy = tc.policy
			if tc.custom != "" {
				schedule.Policies.CustomOverlapPolicy = &schedulepb.CustomOverlapPolicy{Name: tc.custom}
			}
			schedule.State.LimitedActions = true
			schedule.State.RemainingActions = 5
			env := newSchedulerTestEngine(t, schedule)
			now := env.timeSource.Now()
			handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: env.logger})
			require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				invoker := s.Invoker.Get(ctx)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "active", OccurrenceId: "active", Attempt: 1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, StartAccepted: true, Execution: &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "active-id", RunId: "active-run"}}}
				for _, id := range []string{"older", "newest"} {
					start := &schedulespb.BufferedStart{RequestId: id, OccurrenceId: id, NominalTime: timestamppb.New(now), ActualTime: timestamppb.New(now), Manual: true, OverlapPolicy: tc.policy, Execution: &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: id}}
					if tc.custom != "" {
						start.CustomOverlapPolicy = &schedulepb.CustomOverlapPolicy{Name: tc.custom}
					}
					invoker.BufferedStarts = append(invoker.BufferedStarts, start)
				}
				return handler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
			}))
			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				invoker := s.Invoker.Get(ctx)
				require.Len(t, invoker.BufferedStarts, tc.pending+1)
				require.EqualValues(t, tc.skipped, s.Info.OverlapSkipped)
				require.EqualValues(t, 5, s.Schedule.State.RemainingActions)
				ready := 0
				for _, start := range invoker.BufferedStarts[1:] {
					if start.Attempt > 0 {
						ready++
					}
					require.Empty(t, start.WorkflowId)
					require.Empty(t, start.RunId)
				}
				require.Equal(t, tc.ready, ready)
				if tc.terminate {
					require.Len(t, invoker.TerminateExecutions, 1)
					require.Empty(t, invoker.TerminateWorkflows)
				}
				if tc.custom != "" {
					require.Equal(t, "newest", invoker.BufferedStarts[1].RequestId)
					require.Equal(t, tc.custom, invoker.BufferedStarts[1].CustomOverlapPolicy.Name)
				}
				return nil
			}))
			require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				if err := s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{RequestId: "active", CloseTime: timestamppb.New(now), Outcome: &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}}}); err != nil {
					return err
				}
				return handler.Execute(ctx, s.Invoker.Get(ctx), chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
			}))
			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				if tc.pending > 0 {
					found := false
					for _, start := range s.Invoker.Get(ctx).BufferedStarts {
						if start.RequestId != "active" && start.Attempt > 0 {
							found = true
						}
					}
					require.True(t, found, "completion must release waiting work")
				}
				require.Nil(t, s.LastCompletionResult.Get(ctx).Success)
				return nil
			}))
		})
	}
}

func TestActivityTriggerUsesScheduledOccurrenceTime(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule = activitySchedule()
	ctx := env.MutableContext()
	scheduled := env.TimeSource.Now().Add(-time.Minute)
	backfiller := env.Scheduler.NewImmediateBackfiller(ctx, &schedulepb.TriggerImmediatelyRequest{ScheduledTime: timestamppb.New(scheduled)})
	require.Equal(t, scheduled.UTC(), backfiller.LastProcessedTime.AsTime())
}

func TestSelectedStartOwnsOverlapSlotBeforeAcknowledgment(t *testing.T) {
	for _, activity := range []bool{false, true} {
		for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
			t.Run(map[bool]string{false: "workflow", true: "activity"}[activity]+policy.String(), func(t *testing.T) {
				schedule := defaultSchedule()
				kind := enumspb.EXECUTION_TYPE_WORKFLOW
				if activity {
					schedule = activitySchedule()
					kind = enumspb.EXECUTION_TYPE_ACTIVITY
				}
				schedule.Policies.OverlapPolicy = policy
				env := newSchedulerTestEngine(t, schedule)
				now := timestamppb.New(env.timeSource.Now())
				handler := scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: env.logger})
				require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
					invoker := s.Invoker.Get(ctx)
					invoker.BufferedStarts = []*schedulespb.BufferedStart{
						{RequestId: "selected", OccurrenceId: "selected", Attempt: 1, NominalTime: now, ActualTime: now, OverlapPolicy: policy, Execution: &commonpb.Execution{Type: kind, BusinessId: "selected"}},
						{RequestId: "waiting", OccurrenceId: "waiting", NominalTime: now, ActualTime: now, OverlapPolicy: policy, Execution: &commonpb.Execution{Type: kind, BusinessId: "waiting"}},
					}
					return handler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
				}))
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					invoker := s.Invoker.Get(ctx)
					require.Len(t, invoker.BufferedStarts, 2)
					require.EqualValues(t, 1, invoker.BufferedStarts[0].Attempt)
					require.EqualValues(t, -1, invoker.BufferedStarts[1].Attempt)
					require.Empty(t, invoker.TerminateExecutions)
					require.Empty(t, invoker.TerminateWorkflows)
					require.Zero(t, s.Info.ActionCount)
					return nil
				}))
			})
		}
	}
}

func TestActivityCustomPolicySurvivesManualOccurrenceSerialization(t *testing.T) {
	for _, trigger := range []bool{true, false} {
		t.Run(map[bool]string{true: "trigger", false: "backfill"}[trigger], func(t *testing.T) {
			env := newTestEnv(t)
			env.Scheduler.Schedule = activitySchedule()
			custom := &schedulepb.CustomOverlapPolicy{Name: "temporal.buffer_latest"}
			c := &backfillTestCase{ExpectedComplete: true, ExpectedBufferedStarts: 1, ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
				start := invoker.BufferedStarts[0]
				data, err := proto.Marshal(start)
				require.NoError(t, err)
				restored := &schedulespb.BufferedStart{}
				require.NoError(t, proto.Unmarshal(data, restored))
				require.Equal(t, custom.Name, restored.GetCustomOverlapPolicy().GetName())
				require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, restored.OverlapPolicy)
				require.True(t, restored.Manual)
				require.NotEmpty(t, restored.RequestId)
				require.NotEmpty(t, restored.OccurrenceId)
				require.NotEmpty(t, restored.Execution.BusinessId)
				require.Equal(t, enumspb.EXECUTION_TYPE_ACTIVITY, restored.Execution.Type)
				require.Empty(t, restored.WorkflowId)
			}}
			if trigger {
				c.InitialTriggerRequest = &schedulepb.TriggerImmediatelyRequest{CustomOverlapPolicy: custom}
			} else {
				now := env.TimeSource.Now()
				c.InitialBackfillRequest = &schedulepb.BackfillRequest{StartTime: timestamppb.New(now.Add(-defaultInterval)), EndTime: timestamppb.New(now), CustomOverlapPolicy: custom}
			}
			runBackfillTestCase(t, env, c)
		})
	}
}
