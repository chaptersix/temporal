package scheduler2

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newExecutingPublicSchedule publishes the definition, generation and initial
// controls in the same creation transaction.
func newExecutingPublicSchedule(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, tweaks schedulerconfig.Tweakables) (*Scheduler, error) {
	return newExecutingPublicScheduleWithPayloadStore(ctx, request, tweaks, false)
}

func newExecutingPublicScheduleWithPayloadStore(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, tweaks schedulerconfig.Tweakables, keyedPayloads bool) (*Scheduler, error) {
	return newExecutingPublicScheduleWithStores(ctx, request, tweaks, keyedPayloads, false)
}

// newAddressedPublicSchedule selects the complete addressed runtime only for a
// fresh private execution. It does not migrate an existing representation.
func newAddressedPublicSchedule(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, tweaks schedulerconfig.Tweakables) (*Scheduler, error) {
	return newExecutingPublicScheduleWithStores(ctx, request, tweaks, true, true)
}

func newExecutingPublicScheduleWithStores(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, tweaks schedulerconfig.Tweakables, keyedPayloads, keyedRuntime bool) (*Scheduler, error) {
	return newExecutingPublicScheduleWithStage(ctx, request, tweaks, keyedPayloads, keyedRuntime, nil)
}

func newPreparedPublicSchedule(ctx chasm.MutableContext, input *stagedPublicCreate, tweaks schedulerconfig.Tweakables) (*Scheduler, error) {
	if input == nil || input.definition == nil {
		return nil, serviceerror.NewInvalidArgument("staged create input is required")
	}
	return newExecutingPublicScheduleWithStage(ctx, input.request, tweaks, true, true, input.definition)
}

func newExecutingPublicScheduleWithStage(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, tweaks schedulerconfig.Tweakables, keyedPayloads, keyedRuntime bool, staged *stagedPublicDefinition) (*Scheduler, error) {
	if request != nil && request.InitialPatch.GetTimeSkippingConfig() != nil {
		ctx.SetTimeSkippingConfig(request.InitialPatch.TimeSkippingConfig)
	}
	s, err := newPublicScheduleWithStage(ctx, request, true, keyedPayloads, keyedRuntime, staged)
	if err != nil {
		return nil, err
	}
	definition, err := s.publicDefinition(ctx)
	if err != nil {
		return nil, err
	}
	config := publicGeneration(ctx, request.Namespace, request.ScheduleId, definition, s.Revision, tweaks)
	if staged != nil {
		config.prepared = staged.definition
	}
	if err := s.ConfigureGeneration(ctx, config); err != nil {
		return nil, err
	}
	if err := s.ConfigureIdle(ctx, tweaks.IdleTime); err != nil {
		return nil, err
	}
	// The initial clock configuration precedes root initialization. Apply the
	// remaining controls without setting that configuration a second time.
	patch := common.CloneProto(request.InitialPatch)
	if patch != nil {
		patch.TimeSkippingConfig = nil
	}
	if err := s.applyPublicPatch(ctx, patch); err != nil {
		return nil, err
	}
	s.collectPayloadBlobs(ctx)
	return s, nil
}

func publicGeneration(ctx chasm.Context, namespace, scheduleID string, schedule *schedulepb.Schedule, revision int64, tweaks schedulerconfig.Tweakables) GenerationConfig {
	action := common.CloneProto(schedule.GetAction().GetStartWorkflow())
	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: namespace, WorkflowId: action.WorkflowId, WorkflowType: action.WorkflowType,
		TaskQueue: action.TaskQueue, Input: action.Input, Header: action.Header, Memo: action.Memo,
		SearchAttributes: action.SearchAttributes, UserMetadata: action.UserMetadata, Priority: action.Priority,
		WorkflowExecutionTimeout: action.WorkflowExecutionTimeout, WorkflowRunTimeout: action.WorkflowRunTimeout,
		WorkflowTaskTimeout: action.WorkflowTaskTimeout, RetryPolicy: action.RetryPolicy,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Identity:              "temporal-scheduler-" + namespace + "-" + scheduleID,
	}
	if tweaks.EnableVersioningOverride {
		request.VersioningOverride = action.VersioningOverride
	}
	window := schedule.GetPolicies().GetCatchupWindow().AsDuration()
	if window <= 0 {
		window = tweaks.DefaultCatchupWindow
	}
	policy := schedule.GetPolicies().GetOverlapPolicy()
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return GenerationConfig{Policy: policy, Spec: schedule.Spec, Seed: ctx.ExecutionKey().NamespaceID + "-" + scheduleID,
		Revision: revision, After: ctx.Now(nil), CatchupWindow: max(window, tweaks.MinCatchupWindow), Request: request}
}

func (s *Scheduler) checkPublicMutation(ctx chasm.Context) error {
	if err := s.validatePublicProjection(ctx); err != nil {
		return err
	}
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
	if !s.PublicSchedule.IsSet() {
		return serviceerror.NewFailedPrecondition("schedule has no public definition")
	}
	if !s.ListControl.IsSet() {
		if _, ok := s.PublicSchedule.TryGet(ctx); !ok {
			return serviceerror.NewFailedPrecondition("schedule has no public definition")
		}
	}
	if s.PublicRevision == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("schedule conflict token exhausted")
	}
	return nil
}

// UpdatePublic replaces the complete future definition without changing admitted requests.
func (s *Scheduler) UpdatePublic(ctx chasm.MutableContext, request *workflowservice.UpdateScheduleRequest, tweaks schedulerconfig.Tweakables) error {
	return s.updatePublicStaged(ctx, request, tweaks, nil)
}

func (s *Scheduler) UpdatePreparedPublic(ctx chasm.MutableContext, input *stagedPublicUpdate, tweaks schedulerconfig.Tweakables) error {
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
	if input == nil || input.definition == nil {
		return serviceerror.NewInvalidArgument("staged update input is required")
	}
	if err := input.definition.validateBinding(ctx.ExecutionKey().NamespaceID, input.request.ScheduleId, s.Revision); err != nil {
		return err
	}
	return s.updatePublicStaged(ctx, input.request, tweaks, input.definition)
}

func (s *Scheduler) updatePublicStaged(ctx chasm.MutableContext, request *workflowservice.UpdateScheduleRequest, tweaks schedulerconfig.Tweakables, staged *stagedPublicDefinition) error {
	if err := s.checkPublicMutation(ctx); err != nil {
		return err
	}
	var memoErr error
	ctx, memoErr = s.reserveMemoVersion(ctx)
	if memoErr != nil {
		return memoErr
	}

	if request.ConflictToken != nil && !bytes.Equal(request.ConflictToken, publicConflictToken(s.Incarnation, s.PublicRevision)) {
		return serviceerror.NewFailedPrecondition("mismatched conflict token")
	}
	if request.Schedule.GetAction().GetStartWorkflow() == nil {
		return serviceerror.NewInvalidArgument("Only StartWorkflow action is supported for schedules")
	}
	if s.Revision == math.MaxInt64 {
		return serviceerror.NewFailedPrecondition("configuration revision exhausted")
	}
	schedule := common.CloneProto(request.Schedule)
	if schedule.Spec == nil {
		schedule.Spec = &schedulepb.ScheduleSpec{}
	}
	if schedule.State == nil {
		schedule.State = &schedulepb.ScheduleState{}
	}
	if schedule.Policies == nil {
		schedule.Policies = &schedulepb.SchedulePolicies{}
	}
	config := UpdateConfig{Generation: publicGeneration(ctx, request.Namespace, request.ScheduleId, schedule, s.Revision+1, tweaks),
		Paused: schedule.State.Paused, Limited: schedule.State.LimitedActions, Remaining: schedule.State.RemainingActions, PauseOnFailure: schedule.Policies.PauseOnFailure}
	if staged != nil {
		config.Generation.prepared = staged.definition
	}
	generation, err := s.prepareConfiguration(ctx, config)
	if err != nil {
		return err
	}
	definition, err := s.preparePublicDefinition(ctx, schedule)
	if err != nil {
		return err
	}
	prepared := payloadPreparation{}
	maps.Copy(prepared, generation.blobs)
	maps.Copy(prepared, definition.blobs)
	blobPlan, err := s.prepareBlobDelta(ctx, []blobOwnerChange{generation.owner, definition.owner}, prepared)
	if err != nil {
		return err
	}
	var list publicListPlan
	var preview *scheduler2pb.PublicListPreview
	if staged != nil {
		list, preview, err = s.prepareStagedPublicList(ctx, staged, ctx.Now(s), s.publicRecent(ctx, 5))
		if err != nil {
			return err
		}
	}
	visibility := s.Visibility.Get(ctx)
	if request.SearchAttributes != nil {
		if err := visibility.ReplaceCustomSearchAttributes(ctx, common.CloneProto(request.SearchAttributes).IndexedFields); err != nil {
			return err
		}
	}
	if request.Memo != nil {
		visibility.ReplaceCustomMemo(ctx, common.CloneProto(request.Memo).Fields)
	}
	s.publishBlobDelta(ctx, blobPlan)
	s.publishGeneration(ctx, generation)
	s.publishPublicDefinition(ctx, definition)
	if staged != nil {
		s.publishListRefresh(ctx, list)
		s.ListPreview = chasm.NewDataField(ctx, preview)
	}
	if err := s.reconcileWork(ctx); err != nil {
		return err
	}
	s.publishPublicMutation(ctx)
	s.collectPayloadBlobs(ctx)
	return nil
}

// PatchPublic applies all controls under one request deduplication transaction.
func (s *Scheduler) PatchPublic(ctx chasm.MutableContext, patch *schedulepb.SchedulePatch) error {
	if err := s.checkPublicMutation(ctx); err != nil {
		return err
	}
	var memoErr error
	ctx, memoErr = s.reserveMemoVersion(ctx)
	if memoErr != nil {
		return memoErr
	}

	if err := s.applyPublicPatch(ctx, patch); err != nil {
		return err
	}
	s.publishPublicMutation(ctx)
	return nil
}

func (s *Scheduler) applyPublicPatch(ctx chasm.MutableContext, patch *schedulepb.SchedulePatch) error {
	if patch == nil {
		return nil
	}
	var memoErr error
	ctx, memoErr = s.reserveMemoVersion(ctx)
	if memoErr != nil {
		return memoErr
	}
	newCommands := len(patch.BackfillRequest)
	if patch.TriggerImmediately != nil {
		newCommands++
	}
	if err := s.checkPublicCommandCapacity(newCommands); err != nil {
		return err
	}
	if s.HasGenerator && s.Generator.Get(ctx).Token == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("generation token exhausted")
	}
	if config := patch.GetTimeSkippingConfig(); config != nil {
		ctx.SetTimeSkippingConfig(config)
	}
	if s.ListControl.IsSet() {
		return s.applyPreparedPublicPatch(ctx, patch)
	}
	schedule := common.CloneProto(s.PublicSchedule.Get(ctx))
	if patch.Pause != "" {
		s.Paused, schedule.State.Paused, schedule.State.Notes = true, true, patch.Pause
	}
	if patch.Unpause != "" {
		s.Paused, schedule.State.Paused, schedule.State.Notes = false, false, patch.Unpause
	}
	if patch.TriggerImmediately != nil {
		if err := s.AcceptManual(ctx, patch.TriggerImmediately); err != nil {
			return err
		}
	}
	for _, request := range patch.BackfillRequest {
		backfill := common.CloneProto(request)
		if err := s.AcceptBackfill(ctx, backfill); err != nil {
			return err
		}
	}
	s.PublicSchedule = chasm.NewDataField(ctx, schedule)
	s.publishPublicList(ctx, schedule)
	if s.HasGenerator {
		s.Generator.Get(ctx).arm(ctx, ctx.Now(s))
	}
	return s.reconcileWork(ctx)
}

func (s *Scheduler) publishPublicMutation(ctx chasm.MutableContext) {
	s.PublicRevision++
	info := common.CloneProto(s.PublicInfo.Get(ctx))
	info.UpdateTime = timestamppb.New(ctx.Now(s))
	s.PublicInfo = chasm.NewDataField(ctx, info)
	if s.ListControl.IsSet() {
		s.publishMemoVersion(ctx)
	} else {
		s.publishPublicList(ctx, s.PublicSchedule.Get(ctx))
	}
}

func (s *Scheduler) publishPublicList(ctx chasm.MutableContext, schedule *schedulepb.Schedule) {
	spec := common.CloneProto(schedule.Spec)
	legacyscheduler.CleanSpec(spec)
	spec.TimezoneData = nil
	list := &schedulepb.ScheduleListInfo{Spec: spec,
		WorkflowType: common.CloneProto(schedule.GetAction().GetStartWorkflow().GetWorkflowType()), Notes: schedule.GetState().GetNotes(), Paused: s.Paused}
	s.publicListProjection(ctx, list, schedule.Spec)
	s.publishMemoVersion(ctx)
	s.PublicList = chasm.NewDataField(ctx, list)
}

// MatchingTimesPublic is a read-only projection using the shared compiled spec.
func (s *Scheduler) MatchingTimesPublic(ctx chasm.Context, request *workflowservice.ListScheduleMatchingTimesRequest, builder *legacyscheduler.SpecBuilder) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	if s.Closed {
		return nil, serviceerror.NewNotFound("schedule is closed")
	}
	if request.StartTime == nil || request.EndTime == nil {
		return nil, serviceerror.NewInvalidArgument("missing or invalid query")
	}
	compiled, err := builder.NewCompiledSpec(s.PublicSchedule.Get(ctx).Spec)
	if err != nil {
		return nil, err
	}
	response := &workflowservice.ListScheduleMatchingTimesResponse{}
	after := request.StartTime.AsTime()
	seed := ctx.ExecutionKey().NamespaceID + "-" + request.ScheduleId
	for range 1000 {
		next, err := compiled.GetNextTime(seed, after)
		if err != nil {
			return nil, legacyscheduler.ErrScheduleSpecLimitHit
		}
		after = next.Next
		if after.IsZero() || after.After(request.EndTime.AsTime()) {
			break
		}
		response.StartTime = append(response.StartTime, timestamppb.New(after))
	}
	return response, nil
}

// projectPublicPause records the same failure note as the existing scheduler.
// The caller owns the accompanying conflict-token increment.
func (s *Scheduler) projectPublicPause(ctx chasm.MutableContext, status enumspb.WorkflowExecutionStatus, workflowID string) {
	stored, ok := s.PublicSchedule.TryGet(ctx)
	if !ok {
		return
	}
	schedule := common.CloneProto(stored)
	schedule.State.Paused = true
	schedule.State.Notes = publicPauseNote(status, workflowID)
	s.PublicSchedule = chasm.NewDataField(ctx, schedule)
	s.publishPublicList(ctx, schedule)
}

// checkPublicCommandCapacity mirrors the combined pending trigger/range ingress bound.
// Accepted execution queues and unfinished operations have separate capacity limits.
func (s *Scheduler) checkPublicCommandCapacity(additional int) error {
	if s.PublicRevision != 0 && additional > 100-len(s.Manuals)-len(s.Backfills) {
		return schedulerconfig.ErrTooManyBackfillers
	}
	return nil
}

func publicPauseNote(status enumspb.WorkflowExecutionStatus, workflowID string) string {
	return fmt.Sprintf("paused, workflow %s: %s", strings.ToLower(status.String()), workflowID)
}

// applyPreparedPublicPatch changes bounded controls without rebuilding the spec
// or replacing an unfinished preview for a notes-only change.
func (s *Scheduler) applyPreparedPublicPatch(ctx chasm.MutableContext, patch *schedulepb.SchedulePatch) error {
	c, err := s.listControl(ctx)
	if err != nil {
		return err
	}
	var notes *string
	paused := s.Paused
	if patch.Pause != "" {
		notes, paused = &patch.Pause, true
	}
	if patch.Unpause != "" {
		notes, paused = &patch.Unpause, false
	}
	list, err := s.prepareListDynamic(ctx, c.RecentActions, notes)
	if err != nil {
		return err
	}
	s.Paused = paused
	if patch.TriggerImmediately != nil {
		if err := s.AcceptManual(ctx, patch.TriggerImmediately); err != nil {
			return err
		}
	}
	for _, request := range patch.BackfillRequest {
		if err := s.AcceptBackfill(ctx, common.CloneProto(request)); err != nil {
			return err
		}
	}
	s.publishListRefresh(ctx, list)
	if s.HasGenerator {
		s.Generator.Get(ctx).arm(ctx, ctx.Now(s))
	}
	return s.reconcileWork(ctx)
}

// packedFreshProfileKey is consumed only while constructing a new root. Existing
// executions never infer this layout from missing records or migrate implicitly.
type packedFreshProfileKey struct{}

func newPackedAddressedPublicSchedule(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, tweaks schedulerconfig.Tweakables) (*Scheduler, error) {
	return newAddressedPublicSchedule(chasm.ContextWithValue(ctx, packedFreshProfileKey{}, true), request, tweaks)
}

func newPackedPreparedPublicSchedule(ctx chasm.MutableContext, input *stagedPublicCreate, tweaks schedulerconfig.Tweakables) (*Scheduler, error) {
	return newPreparedPublicSchedule(chasm.ContextWithValue(ctx, packedFreshProfileKey{}, true), input, tweaks)
}
