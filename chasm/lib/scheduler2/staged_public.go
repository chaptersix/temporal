package scheduler2

import (
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
)

// These envelopes are private immutable ingress values. Callers consume their
// detached request, never combine a stage with a separately supplied schedule.
type stagedPublicDefinition struct {
	schedule                *schedulepb.Schedule
	definition              *StagedDefinitionSource
	static                  *schedulepb.ScheduleListInfo
	namespaceID, scheduleID string
	expectedRevision        int64
	listLimit               int64
}
type stagedPublicCreate struct {
	request    *workflowservice.CreateScheduleRequest
	definition *stagedPublicDefinition
}
type stagedPublicUpdate struct {
	request    *workflowservice.UpdateScheduleRequest
	definition *stagedPublicDefinition
}

func (s *PreparationService) stagePublicCreate(request *workflowservice.CreateScheduleRequest, namespaceID string, listLimit int64) (*stagedPublicCreate, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("schedule is required")
	}
	detached := common.CloneProto(request)
	stage, err := s.stagePublicDefinition(detached.Schedule, namespaceID, detached.ScheduleId, 0, listLimit)
	if err != nil {
		return nil, err
	}
	detached.Schedule = stage.schedule
	return &stagedPublicCreate{request: detached, definition: stage}, nil
}
func (s *PreparationService) stagePublicUpdate(request *workflowservice.UpdateScheduleRequest, namespaceID string, expectedRevision int64, listLimit int64) (*stagedPublicUpdate, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("schedule is required")
	}
	if expectedRevision < 1 {
		return nil, serviceerror.NewFailedPrecondition("configuration revision must be positive")
	}
	detached := common.CloneProto(request)
	stage, err := s.stagePublicDefinition(detached.Schedule, namespaceID, detached.ScheduleId, expectedRevision, listLimit)
	if err != nil {
		return nil, err
	}
	detached.Schedule = stage.schedule
	return &stagedPublicUpdate{request: detached, definition: stage}, nil
}

// stagePublicDefinition performs all input-proportional normalization and source
// resolution outside the mutation. List truncation configuration is sampled here.
func (s *PreparationService) stagePublicDefinition(input *schedulepb.Schedule, namespaceID, scheduleID string, expectedRevision, listLimit int64) (*stagedPublicDefinition, error) {
	if s == nil {
		return nil, serviceerror.NewUnavailable("source preparation service is not configured")
	}
	if expectedRevision < 0 || expectedRevision == math.MaxInt64 {
		return nil, serviceerror.NewFailedPrecondition("configuration revision exhausted")
	}
	if listLimit < 0 {
		return nil, serviceerror.NewInvalidArgument("list limit must be nonnegative")
	}
	schedule, err := canonicalPublicDefaults(input)
	if err != nil {
		return nil, err
	}
	definition, _, err := s.PrepareDefinition(schedule.Spec, namespaceID+"-"+scheduleID, expectedRevision+1)
	if err != nil {
		return nil, err
	}
	schedule.Spec, err = definition.CanonicalSpec()
	if err != nil {
		return nil, err
	}
	static, err := stagePublicListStatic(schedule, listLimit)
	if err != nil {
		return nil, err
	}
	return &stagedPublicDefinition{schedule: schedule, definition: definition, static: static, namespaceID: namespaceID, scheduleID: scheduleID, expectedRevision: expectedRevision, listLimit: listLimit}, nil
}
func canonicalPublicDefaults(input *schedulepb.Schedule) (*schedulepb.Schedule, error) {
	if input == nil {
		return nil, serviceerror.NewInvalidArgument("schedule is required")
	}
	if input.GetAction().GetStartWorkflow() == nil {
		return nil, serviceerror.NewInvalidArgument("Only StartWorkflow action is supported for schedules")
	}
	schedule := common.CloneProto(input)
	if schedule.Spec == nil {
		schedule.Spec = &schedulepb.ScheduleSpec{}
	}
	if schedule.State == nil {
		schedule.State = &schedulepb.ScheduleState{}
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
	return schedule, nil
}

// validateBinding uses only immutable envelope metadata. Zero currentRevision
// means fresh creation; the constructor must reject attaching it to an old root.
func (p *stagedPublicDefinition) validateBinding(namespaceID, scheduleID string, currentRevision int64) error {
	if p == nil || p.definition == nil || p.definition.source == nil || p.schedule == nil || p.static == nil || p.namespaceID != namespaceID || p.scheduleID != scheduleID || p.expectedRevision != currentRevision || currentRevision < 0 || currentRevision == math.MaxInt64 || p.definition.revision != currentRevision+1 || string(p.definition.source.Seed) != namespaceID+"-"+scheduleID {
		return serviceerror.NewFailedPrecondition("staged public definition binding does not match")
	}
	return nil
}

// prepareStagedPublicList captures an unpublished coherent window obligation.
// Publishing its static slot does not expose a memo before the window completes.
func (s *Scheduler) prepareStagedPublicList(ctx chasm.Context, p *stagedPublicDefinition, after time.Time, recent []*schedulepb.ScheduleActionResult) (publicListPlan, *pb.PublicListPreview, error) {
	notes := p.schedule.State.Notes
	plan, err := s.prepareListRefresh(ctx, p.definition.owner, p.static, p.listLimit, recent, &notes)
	if err != nil {
		return publicListPlan{}, nil, err
	}
	preview, err := newListPreview(p.definition.owner, plan.control.PendingStamp, after, p.schedule.State.LimitedActions, p.schedule.State.RemainingActions)
	if err != nil {
		return publicListPlan{}, nil, err
	}
	return plan, preview, nil
}
