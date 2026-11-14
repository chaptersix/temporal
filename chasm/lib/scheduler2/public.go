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
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
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
	return newPublicSchedule(ctx, request, false)
}

func newPublicSchedule(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, execution bool) (*Scheduler, error) {
	return newPublicScheduleWithPayloadStore(ctx, request, execution, false)
}

func newPublicScheduleWithPayloadStore(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, execution, keyedPayloads bool) (*Scheduler, error) {
	return newPublicScheduleWithStores(ctx, request, execution, keyedPayloads, false)
}

func newPublicScheduleWithStores(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, execution, keyedPayloads, keyedRuntime bool) (*Scheduler, error) {
	return newPublicScheduleWithStage(ctx, request, execution, keyedPayloads, keyedRuntime, nil)
}

func newPublicScheduleWithStage(ctx chasm.MutableContext, request *workflowservice.CreateScheduleRequest, execution, keyedPayloads, keyedRuntime bool, staged *stagedPublicDefinition) (*Scheduler, error) {
	if request == nil || request.Schedule == nil {
		return nil, serviceerror.NewInvalidArgument("schedule is required")
	}
	if request.Schedule.GetAction().GetStartWorkflow() == nil {
		return nil, serviceerror.NewInvalidArgument("Only StartWorkflow action is supported for schedules")
	}
	schedule := common.CloneProto(request.Schedule)
	spec := schedule.GetSpec()
	if !execution && (!schedule.GetState().GetPaused() || len(spec.GetInterval())+len(spec.GetStructuredCalendar())+len(spec.GetCalendar())+len(spec.GetCronString()) != 0 || proto.Size(request.InitialPatch) != 0) {
		return nil, serviceerror.NewUnimplemented("scheduler2 public execution and initial patches are not wired")
	}
	if schedule.Spec == nil {
		schedule.Spec = &schedulepb.ScheduleSpec{}
	}
	if staged == nil {
		if _, err := occurrence.NewPublic(schedule.Spec, "", 1); err != nil {
			return nil, err
		}
	} else if err := staged.validateBinding(ctx.ExecutionKey().NamespaceID, request.ScheduleId, 0); err != nil {
		return nil, err
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
	s, err := NewExecution(ctx, schedule.State.RemainingActions)
	if err != nil {
		return nil, err
	}
	s.IndexedQueue = newQueueState()
	s.QueueRecords = chasm.NewDataMap[*scheduler2pb.QueuedOccurrence](ctx)
	s.QueuePages = chasm.NewDataMap[*commonpb.Payload](ctx)
	if keyedPayloads {
		s.PayloadStore = &scheduler2pb.PayloadStoreControl{FormatVersion: payloadStoreVersion}
		s.BlobRecords = chasm.NewDataMap[*scheduler2pb.Payload](ctx)
		s.BlobReferences = chasm.NewDataMap[*scheduler2pb.BlobReference](ctx)
	}
	if keyedRuntime {
		if selected, ok := ctx.Value(packedFreshProfileKey{}).(bool); ok && selected {
			s.PackedStorage = &scheduler2pb.PackedStorageControl{FormatVersion: packedStorageVersion, NextLocator: 1}
			s.PackedPages = chasm.NewDataMap[*commonpb.Payload](ctx)
			s.PackedRequests = chasm.NewDataMap[*scheduler2pb.Payload](ctx)
		}
		s.BackfillWork = chasm.NewDataMap[*scheduler2pb.Payload](ctx)
		s.OperationStore = &scheduler2pb.OperationStoreControl{FormatVersion: operationStoreVersion, NextReadySequence: 1, StartDispatch: &scheduler2pb.StartDispatchControl{Token: 1}}
		s.OperationRecords = chasm.NewDataMap[*scheduler2pb.OperationState](ctx)
		s.RequestRecords = chasm.NewDataMap[*scheduler2pb.Payload](ctx)
		s.OperationPages = chasm.NewDataMap[*commonpb.Payload](ctx)
		s.ReadyRecords = chasm.NewDataMap[*scheduler2pb.ReadyRecord](ctx)
		s.ProjectionStore = &scheduler2pb.ProjectionStoreControl{FormatVersion: projectionStoreVersion, NextActionSequence: 1}
		s.ActionRecords = chasm.NewDataMap[*scheduler2pb.PublicActionRecord](ctx)
		s.RunningRecords = chasm.NewDataMap[*scheduler2pb.RunningPublicAction](ctx)
		s.ProjectionPages = chasm.NewDataMap[*commonpb.Payload](ctx)
		s.PublicRuntime = chasm.NewDataField(ctx, &scheduler2pb.PublicRuntime{})
	}
	s.PublicRevision = 1
	s.MemoRevision = 1
	s.Paused, s.Limited, s.Policy = schedule.State.Paused, schedule.State.LimitedActions, policy
	s.PauseOnFailure = schedule.Policies.PauseOnFailure
	if err := s.storePublicDefinition(ctx, schedule); err != nil {
		return nil, err
	}
	s.PublicInfo = chasm.NewDataField(ctx, &schedulepb.ScheduleInfo{
		CreateTime: timestamppb.New(ctx.Now(s)), UpdateTime: timestamppb.New(time.Time{}),
	})
	if err := s.initializePublicList(ctx, schedule, staged); err != nil {
		return nil, err
	}
	visibility := chasm.NewVisibility(ctx)
	s.Visibility = chasm.NewComponentField(ctx, visibility)
	sa := common.CloneProto(request.SearchAttributes)
	if err := visibility.MergeCustomSearchAttributes(ctx, sa.GetIndexedFields()); err != nil {
		return nil, err
	}
	visibility.MergeCustomMemo(ctx, common.CloneProto(request.Memo).GetFields())
	return s, nil
}

func publicConflictToken(runID string, revision uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d", runID, revision))
}

// DescribePublic returns detached data without consulting runtime configuration.
func (s *Scheduler) DescribePublic(ctx chasm.Context) (*workflowservice.DescribeScheduleResponse, error) {
	if err := s.validatePublicProjection(ctx); err != nil {
		return nil, err
	}
	if err := s.validateOperationStore(); err != nil {
		return nil, err
	}
	if s.Closed {
		return nil, serviceerror.NewNotFound("schedule is closed")
	}
	_, ok := s.PublicSchedule.TryGet(ctx)
	if !ok {
		return nil, serviceerror.NewFailedPrecondition("schedule has no public definition")
	}
	schedule, err := s.publicDefinition(ctx)
	if err != nil {
		return nil, err
	}
	schedule.State.Paused, schedule.State.LimitedActions, schedule.State.RemainingActions = s.Paused, s.Limited, s.Remaining
	schedule.Policies.OverlapPolicy = enumspb.ScheduleOverlapPolicy(s.Policy)
	window := schedule.Policies.GetCatchupWindow().AsDuration()
	if s.HasGenerator {
		window = time.Duration(s.Generator.Get(ctx).CatchupWindow)
	} else {
		if window <= 0 {
			window = 365 * 24 * time.Hour
		}
		window = max(window, 10*time.Second)
	}
	schedule.Policies.CatchupWindow = durationpb.New(window)
	legacyscheduler.CleanSpec(schedule.Spec)
	info := common.CloneProto(s.PublicInfo.Get(ctx))
	if err := s.publicInfoProjection(ctx, info, schedule.Spec); err != nil {
		return nil, err
	}
	info.StateSizeBytes = int64(ctx.ExecutionInfo().ApproximateStateSize)
	info.TimeSkippingInfo = ctx.GetTimeSkippingInfo()
	visibility := s.Visibility.Get(ctx)
	return &workflowservice.DescribeScheduleResponse{
		Schedule: schedule, Info: info, ConflictToken: publicConflictToken(s.Incarnation, s.PublicRevision),
		Memo:             common.CloneProto(&commonpb.Memo{Fields: visibility.CustomMemo(ctx)}),
		SearchAttributes: common.CloneProto(&commonpb.SearchAttributes{IndexedFields: visibility.CustomSearchAttributes(ctx)}),
	}, nil
}

// Memo reads only the compact list partition, avoiding the action payload.
func (s *Scheduler) Memo(ctx chasm.Context) proto.Message {
	if s.ListControl.IsSet() {
		info, err := s.listMemo(ctx)
		if err != nil {
			panic(err) //nolint:forbidigo // Framework invariant failure, as with DATA decoding.
		}
		if info == nil {
			return nil
		}
		return info
	}
	stored, ok := s.PublicList.TryGet(ctx)
	if !ok {
		return nil
	}
	info := common.CloneProto(stored)
	info.Paused = s.Paused
	return info
}

func (s *Scheduler) initializePublicList(ctx chasm.MutableContext, schedule *schedulepb.Schedule, staged *stagedPublicDefinition) error {
	if staged == nil {
		listSpec := common.CloneProto(schedule.Spec)
		legacyscheduler.CleanSpec(listSpec)
		listSpec.TimezoneData = nil
		list := &schedulepb.ScheduleListInfo{
			Spec: listSpec, WorkflowType: common.CloneProto(schedule.GetAction().GetStartWorkflow().GetWorkflowType()),
			Notes: schedule.State.Notes, Paused: schedule.State.Paused,
		}
		s.publicListProjection(ctx, list, schedule.Spec)
		s.PublicList = chasm.NewDataField(ctx, list)

	} else {
		var err error
		ctx, err = s.reserveMemoVersion(ctx)
		if err != nil {
			return err
		}
		s.ListControl = chasm.NewDataField(ctx, &scheduler2pb.PublicListControl{})
		list, preview, err := s.prepareStagedPublicList(ctx, staged, ctx.Now(s), nil)
		if err != nil {
			return err
		}
		s.publishListRefresh(ctx, list)
		s.ListPreview = chasm.NewDataField(ctx, preview)
	}

	return nil
}
