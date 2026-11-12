package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSchedule2CreateScheduleAlreadyExists(t *testing.T) {
	t.Parallel()
	newContext := schedule2Backend.context
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})

	sid := "sched-test-already-exists"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-already-exists",
					WorkflowType: &commonpb.WorkflowType{Name: "action"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := newContext(t.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	require.NoError(t, err)

	// Try to create again with a different request ID - should fail with AlreadyExists
	req.RequestId = uuid.NewString()
	_, err = s.FrontendClient().CreateSchedule(ctx, req)
	require.Error(t, err)

	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &alreadyStarted)
	s.Contains(err.Error(), sid)
}

func TestSchedule2PatchRejectsExcessBackfillers(t *testing.T) {
	t.Parallel()
	newContext := schedule2Backend.context
	// Hold all 100 backfillers alive through legitimate buffer backpressure rather
	// than starvation. Backfillers are given a real (non-zero) share of an empty
	// buffer, but each is pointed at a range far larger than the buffer under
	// BUFFER_ALL with no worker registered: the first start runs forever (nothing
	// completes it) and the rest fill the shared buffer, so every backfiller stalls
	// with range still to process and none can finish and self-delete before the
	// 101st patch is rejected. (Before the capacity fix this test passed only
	// because all 100 backfillers were starved to zero capacity and never drained.)
	tweakables := chasmscheduler.DefaultTweakables
	tweakables.MaxBufferSize = 300
	tweakables.GeneratorBufferReserveSize = 25
	opts := []testcore.TestOption{testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweakables)}
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, opts...)
	sid := "sched-test-too-many-backfillers"
	wt := "sched-test-too-many-backfillers-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(fastInterval)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-too-many-backfillers",
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}

	ctx := newContext(t.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Patch with 50 backfill requests at a time until we reach the limit of 100.
	now := time.Now()
	for i := 0; i < 100; i += 50 {
		backfills := make([]*schedulepb.BackfillRequest, 50)
		for j := range backfills {
			backfills[j] = &schedulepb.BackfillRequest{
				// A range far larger than the buffer (fastInterval fires over an hour)
				// so no backfiller can finish processing it and self-delete.
				StartTime:     timestamppb.New(now.Add(-time.Hour)),
				EndTime:       timestamppb.New(now),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			}
		}
		_, err = s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Patch: &schedulepb.SchedulePatch{
				BackfillRequest: backfills,
			},
			Identity:  "test",
			RequestId: uuid.NewString(),
		})
		require.NoError(t, err)
	}

	// The next patch should be rejected.
	_, err = s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			BackfillRequest: []*schedulepb.BackfillRequest{
				{
					StartTime:     timestamppb.New(now.Add(-time.Hour)),
					EndTime:       timestamppb.New(now),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.Error(t, err)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.Contains(err.Error(), "too many concurrent backfillers")
}

func TestSchedule2ScheduleRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	s := newScheduleEnv(t, []scheduleBackend{schedule2Backend})
	ctx := schedule2Backend.context(testcontext.For(t))
	invalidPolicy := enumspb.ScheduleOverlapPolicy(99)

	validSchedule := func(scheduleID string) *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec:   intervalSpec(noOpInterval),
			State:  &schedulepb.ScheduleState{Paused: true},
			Action: startWorkflowAction(s, "wf-"+scheduleID, "wt-"+scheduleID),
		}
	}

	testCases := []struct {
		name             string
		requiresExisting bool
		errorMessage     string
		request          func(scheduleID string) error
	}{
		{
			name:         "CreateSchedule schedule policy",
			errorMessage: "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Policies: &schedulepb.SchedulePolicies{
						OverlapPolicy: invalidPolicy,
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule initial patch",
			errorMessage: "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{},
					InitialPatch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						OverlapPolicy: invalidPolicy,
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule initial patch timestamp",
			errorMessage: "backfill request 0 start time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{},
					InitialPatch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						StartTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule timestamp",
			errorMessage: "start time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{
						StartTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule remaining actions",
			errorMessage: "remaining actions cannot be negative",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{State: &schedulepb.ScheduleState{RemainingActions: -1}},
					RequestId:  uuid.NewString(),
					Identity:   "test",
				})
				return err
			},
		},
		{
			name:             "UpdateSchedule schedule policy",
			requiresExisting: true,
			errorMessage:     "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Policies: &schedulepb.SchedulePolicies{
						OverlapPolicy: invalidPolicy,
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "UpdateSchedule timestamp",
			requiresExisting: true,
			errorMessage:     "start time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{
						StartTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "UpdateSchedule remaining actions",
			requiresExisting: true,
			errorMessage:     "remaining actions cannot be negative",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{State: &schedulepb.ScheduleState{RemainingActions: -1}},
					RequestId:  uuid.NewString(),
					Identity:   "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule trigger",
			requiresExisting: true,
			errorMessage:     "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
						OverlapPolicy: invalidPolicy,
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule backfill",
			requiresExisting: true,
			errorMessage:     "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						OverlapPolicy: invalidPolicy,
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule trigger timestamp",
			requiresExisting: true,
			errorMessage:     "trigger immediately request scheduled time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
						ScheduledTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule backfill timestamp",
			requiresExisting: true,
			errorMessage:     "backfill request 0 end time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						EndTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheduleID := testcore.RandomizeStr("sched-invalid-request")
			if tc.requiresExisting {
				createSchedule(ctx, t, s, scheduleID, validSchedule(scheduleID))
			}

			var invalidArgument *serviceerror.InvalidArgument
			err := tc.request(scheduleID)
			require.ErrorAs(t, err, &invalidArgument)
			require.ErrorContains(t, err, tc.errorMessage)

			describeResponse, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
				Namespace:  s.Namespace().String(),
				ScheduleId: scheduleID,
			})
			if tc.requiresExisting {
				require.NoError(t, err)
				require.NotNil(t, describeResponse)
				return
			}
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
		})
	}
}
