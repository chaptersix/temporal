package scheduler2

import (
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewPublicSchedule initializes the paused, empty-definition public test profile.
// The frontend validates public payloads before this transaction starts.
func NewPublicSchedule(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest) (*Scheduler, error) {
	if request == nil || request.Schedule == nil {
		return nil, serviceerror.NewInvalidArgument("schedule is required")
	}
	if request.Schedule.GetAction().GetStartWorkflow() == nil {
		return nil, serviceerror.NewInvalidArgument("Only StartWorkflow action is supported for schedules")
	}
	schedule := common.CloneProto(request.Schedule)
	spec := schedule.GetSpec()
	if !schedule.GetState().GetPaused() || len(spec.GetInterval())+len(spec.GetStructuredCalendar())+len(spec.GetCalendar())+len(spec.GetCronString()) != 0 || proto.Size(request.InitialPatch) != 0 {
		return nil, serviceerror.NewUnimplemented("scheduler2 public execution and initial patches are not wired")
	}
	if schedule.Spec == nil {
		schedule.Spec = &schedulepb.ScheduleSpec{}
	}
	if _, err := occurrence.New(schedule.Spec, "", 1); err != nil {
		return nil, err
	}
	if schedule.State.RemainingActions < 0 {
		return nil, serviceerror.NewInvalidArgument("remaining actions must be nonnegative")
	}
	if schedule.Policies == nil {
		schedule.Policies = &schedulepb.SchedulePolicies{}
	}
	policy := schedule.Policies.OverlapPolicy
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	if !decision.ValidPolicy(policy) {
		return nil, serviceerror.NewInvalidArgument("invalid overlap policy")
	}
	s, err := NewExecution(ctx, schedule.State.RemainingActions)
	if err != nil {
		return nil, err
	}
	s.Paused, s.Limited, s.Policy = true, schedule.State.LimitedActions, policy
	s.PauseOnFailure = schedule.Policies.PauseOnFailure
	s.PublicSchedule = chasm.NewDataField(ctx, schedule)
	s.PublicInfo = chasm.NewDataField(ctx, &schedulepb.ScheduleInfo{
		CreateTime: timestamppb.New(ctx.Now(s)), UpdateTime: timestamppb.New(time.Time{}),
	})
	listSpec := common.CloneProto(schedule.Spec)
	legacyscheduler.CleanSpec(listSpec)
	listSpec.TimezoneData = nil
	s.PublicList = chasm.NewDataField(ctx, &schedulepb.ScheduleListInfo{
		Spec: listSpec, WorkflowType: common.CloneProto(schedule.GetAction().GetStartWorkflow().GetWorkflowType()),
		Notes: schedule.State.Notes, Paused: true,
	})
	visibility := chasm.NewVisibility(ctx)
	s.Visibility = chasm.NewComponentField(ctx, visibility)
	sa := common.CloneProto(request.SearchAttributes)
	if err := visibility.MergeCustomSearchAttributes(ctx, sa.GetIndexedFields()); err != nil {
		return nil, err
	}
	visibility.MergeCustomMemo(ctx, common.CloneProto(request.Memo).GetFields())
	return s, nil
}

func publicConflictToken(runID string, revision int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", runID, revision))
}

// DescribePublic returns detached data without consulting runtime configuration.
func (s *Scheduler) DescribePublic(ctx chasm.Context) (*workflowservice.DescribeScheduleResponse, error) {
	if s.Closed {
		return nil, serviceerror.NewNotFound("schedule is closed")
	}
	stored, ok := s.PublicSchedule.TryGet(ctx)
	if !ok {
		return nil, serviceerror.NewFailedPrecondition("schedule has no public definition")
	}
	schedule := common.CloneProto(stored)
	schedule.State.Paused, schedule.State.LimitedActions, schedule.State.RemainingActions = s.Paused, s.Limited, s.Remaining
	schedule.Policies.OverlapPolicy = enumspb.ScheduleOverlapPolicy(s.Policy)
	window := schedule.Policies.GetCatchupWindow().AsDuration()
	if window <= 0 {
		window = 365 * 24 * time.Hour
	}
	schedule.Policies.CatchupWindow = durationpb.New(max(window, 10*time.Second))
	legacyscheduler.CleanSpec(schedule.Spec)
	info := common.CloneProto(s.PublicInfo.Get(ctx))
	info.StateSizeBytes = int64(ctx.ExecutionInfo().ApproximateStateSize)
	info.TimeSkippingInfo = ctx.GetTimeSkippingInfo()
	visibility := s.Visibility.Get(ctx)
	return &workflowservice.DescribeScheduleResponse{
		Schedule: schedule, Info: info, ConflictToken: publicConflictToken(s.Incarnation, s.Revision),
		Memo:             common.CloneProto(&commonpb.Memo{Fields: visibility.CustomMemo(ctx)}),
		SearchAttributes: common.CloneProto(&commonpb.SearchAttributes{IndexedFields: visibility.CustomSearchAttributes(ctx)}),
	}, nil
}

// Memo reads only the compact list partition, avoiding the action payload.
func (s *Scheduler) Memo(ctx chasm.Context) proto.Message {
	stored, ok := s.PublicList.TryGet(ctx)
	if !ok {
		return nil
	}
	info := common.CloneProto(stored)
	info.Paused = s.Paused
	return info
}

func (s *Scheduler) SearchAttributes(chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{chasm.SearchAttributeTemporalSchedulePaused.Value(s.Paused)}
}
