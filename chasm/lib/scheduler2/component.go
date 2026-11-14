// Package scheduler2 implements a locally wired, bounded scheduler prototype.
package scheduler2

import (
	"maps"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

// Scheduler keeps control metadata separate from immutable request payloads.
type Scheduler struct {
	chasm.UnimplementedComponent
	*scheduler2pb.RootState
	QueueRecords   chasm.DataMap[*scheduler2pb.QueuedOccurrence]
	QueuePages     chasm.DataMap[*commonpb.Payload]
	Operations     chasm.Map[string, *scheduler2pb.OperationState]
	Requests       chasm.Map[string, *scheduler2pb.Payload]
	PayloadBlobs   chasm.Map[string, *scheduler2pb.Payload]
	EffectRequests chasm.Map[string, *scheduler2pb.Payload]
	Generator      chasm.Field[*Generator]
	Carryover      chasm.Field[*scheduler2pb.CarryoverState]
	Diagnostics    chasm.Field[*scheduler2pb.Payload]
	PublicSchedule chasm.Field[*schedulepb.Schedule]
	PublicInfo     chasm.Field[*schedulepb.ScheduleInfo]
	PublicRuntime  chasm.Field[*scheduler2pb.PublicRuntime]
	PublicList     chasm.Field[*schedulepb.ScheduleListInfo]
	Visibility     chasm.Field[*chasm.Visibility]
}

func New(incarnation string, remaining int64) *Scheduler {
	return &Scheduler{RootState: &scheduler2pb.RootState{
		Incarnation: incarnation, Revision: 1, Limited: true, Remaining: remaining,
	}, Operations: make(chasm.Map[string, *scheduler2pb.OperationState]), Requests: make(chasm.Map[string, *scheduler2pb.Payload]), EffectRequests: make(chasm.Map[string, *scheduler2pb.Payload])}
}

// state reads only control and selected operation metadata.
func (s *Scheduler) state(ctx chasm.Context) (decision.State, error) {
	return s.stateFor(ctx, s.OperationId)
}

func (s *Scheduler) stateFor(ctx chasm.Context, operationID string) (decision.State, error) {
	state := decision.State{Root: decision.Root{
		ActivitySet: s.ActivitySet, ActivityAt: s.ActivityAt, UnconfirmedStarts: s.UnconfirmedStarts,
		ManualSequence: s.ManualSequence, Incarnation: s.Incarnation, Revision: s.Revision, Cursor: s.Cursor,
		Paused: s.Paused, Limited: s.Limited, Remaining: s.Remaining, Started: s.Started,
		CarryoverSet: s.CarryoverSet, PauseOnFailure: s.PauseOnFailure, Policy: s.Policy, BufferDropped: s.BufferDropped,
		Unfinished: s.Unfinished, BlockingOperationID: s.BlockingOperationId, CapacityDropped: s.CapacityDropped, OverlapSuperseded: s.OverlapSuperseded,
	}}
	summary, err := s.queueSummary()
	if err != nil {
		return decision.State{}, err
	}
	state.IndexedQueue = summary
	state.Capacity = s.capacity(ctx)
	if err := decision.ValidateQueueSummary(state); err != nil {
		return decision.State{}, err
	}
	for _, m := range s.Manuals {
		state.Manuals = append(state.Manuals, decision.ManualCommand{ID: m.Id, Nominal: m.Nominal, NominalTime: decodeDecisionTime(m.NominalTime), Policy: m.Policy})
	}
	for _, e := range s.Effects {
		state.Effects = append(state.Effects, decodeEffect(e))
	}
	if !summary.Enabled {
		for _, q := range s.Queue {
			state.Queue = append(state.Queue, decodeQueued(q))
		}
	}
	if field, ok := s.Operations[operationID]; ok {
		op := field.Get(ctx)
		state.Operation = &decision.Operation{
			StartObserved: op.StartObserved, StartObservedAt: op.StartObservedAt,
			OccurrenceID: op.OccurrenceId, ActionID: op.ActionId, ID: op.Id,
			ChainID: op.ChainId, Nominal: op.Nominal, NominalTime: decodeDecisionTime(op.NominalTime), Deadline: op.Deadline,
			BackfillID: op.BackfillId, ManualID: op.ManualId, RetryAt: op.RetryAt, Phase: decision.Phase(op.Phase), WorkflowID: op.WorkflowId, Policy: op.Policy,
		}
	}
	return state, nil
}

// Apply records the transition and successor task in the same CHASM transaction.
func (s *Scheduler) Apply(ctx chasm.MutableContext, event decision.Event) error {
	if s.Closed {
		if event.Kind == decision.Update || event.Kind == decision.AcceptManual {
			return serviceerror.NewNotFound("schedule is closed")
		}
		return nil
	}
	if event.Incarnation != s.Incarnation {
		return nil
	}
	if admissionEvent(event.Kind) {
		return serviceerror.NewFailedPrecondition("admission requires coordinator ownership")
	}
	if err := s.applyTransition(ctx, event); err != nil {
		return err
	}
	return s.reconcileWork(ctx)
}

func admissionEvent(kind decision.Kind) bool {
	return kind == decision.Due || kind == decision.Drain || kind == decision.DispatchManual || kind == decision.DispatchBackfill
}

func (s *Scheduler) prepareFailureCarryover(ctx chasm.Context, before *decision.State, event decision.Event) error {
	if event.Kind == decision.Callback && event.Outcome == decision.Failure && event.Incarnation == s.Incarnation && before.Operation != nil && before.Operation.ID == event.OperationID && !decision.Terminal(before.Operation.Phase) && before.Operation.Policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && s.CarryoverSet {
		c, err := s.carryoverValue(ctx)
		if err != nil {
			return err
		}
		before.Carryover = &decision.Carryover{Result: c.Result, Failure: c.Failure, FailurePresent: c.FailurePresent}
	}
	return nil
}

// applyTransition is called only by a coordinator grant or a non-admission event.
func (s *Scheduler) applyTransition(ctx chasm.MutableContext, event decision.Event) error {
	if s.Closed {
		return nil
	}
	if event.Incarnation != s.Incarnation {
		return nil
	}
	now := ctx.Now(s)
	if !time.Unix(0, now.UnixNano()).Equal(now) {
		return serviceerror.NewInvalidArgument("execution time exceeds nanosecond range")
	}
	event.Now = now.UnixNano()
	operationID := event.OperationID
	if admissionEvent(event.Kind) || event.Kind == decision.Update {
		operationID = s.OperationId
	}
	before, err := s.stateFor(ctx, operationID)
	if err != nil {
		return err
	}
	if err := s.prepareManual(ctx, before, &event); err != nil {
		return err
	}
	if err := s.prepareDrain(ctx, before, &event); err != nil {
		return err
	}
	if err := s.prepareCompletion(before, &event); err != nil {
		return err
	}
	if err := s.prepareFailureCarryover(ctx, &before, event); err != nil {
		return err
	}

	next, err := decision.Step(before, event)
	if err != nil {
		return err
	}
	publicMutation := s.PublicRevision != 0 && ((admissionEvent(event.Kind) && next.Root.Remaining < before.Root.Remaining) || (event.Kind == decision.Callback && !before.Root.Paused && next.Root.Paused))
	if publicMutation && s.PublicRevision == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("public mutation counter exhausted")
	}
	next, effectRequests, err := s.prepareEffects(ctx, next)
	if err != nil {
		return err
	}
	newAdmission := admissionEvent(event.Kind) && next.Operation != nil && (before.Operation == nil || next.Operation.ID != before.Operation.ID)
	if newAdmission {
		if err := s.freezeAdmission(ctx, next.Operation); err != nil {
			return err
		}
	}
	if decision.QueueLength(next) > 0 && !s.HasGenerator {
		return serviceerror.NewFailedPrecondition("buffering requires a configured template")
	}
	if err := s.checkNextWorkToken(ctx, next); err != nil {
		return err
	}
	if err := s.checkNextIdle(ctx, next); err != nil {
		return err
	}
	eviction, err := s.retentionEviction(ctx, before, next)
	if err != nil {
		return err
	}
	prepared := payloadPreparation{}
	var requestRecord *scheduler2pb.Payload
	var carryoverRecord *scheduler2pb.CarryoverState
	if newAdmission {
		var blobs payloadPreparation
		requestRecord, blobs, err = s.splitRequest(ctx, next.Operation.Request)
		if err != nil {
			return err
		}
		maps.Copy(prepared, blobs)
	}
	if next.Carryover != nil {
		var blobs payloadPreparation
		carryoverRecord, blobs, err = s.prepareCarryover(ctx, &scheduler2pb.CarryoverState{Result: next.Carryover.Result, Failure: next.Carryover.Failure, FailurePresent: next.Carryover.FailurePresent})
		if err != nil {
			return err
		}
		maps.Copy(prepared, blobs)
	}
	if next.QueueChange.Appended {
		if err := s.stageQueueAppend(ctx, next.QueueChange.Append); err != nil {
			return err
		}
	} else if next.QueueChange.Drained {
		if err := s.stageQueueDrain(ctx, next.QueueChange.Drain, before); err != nil {
			return err
		}
	}
	s.publishBlobs(ctx, prepared)
	s.publishTransition(ctx, before, next, event.Kind, newAdmission, eviction, effectRequests, requestRecord, carryoverRecord)
	s.publishPublicTransition(ctx, before, next, event, newAdmission)
	if publicMutation {
		s.PublicRevision++
		if event.Kind == decision.Callback && !before.Root.Paused && next.Root.Paused {
			s.projectPublicPause(ctx, event.Status, next.Operation.WorkflowID)
		}
	}
	s.collectPayloadBlobs(ctx)
	return nil
}

// publishTransition cannot reject: validation and successor-token checks precede
// every DATA, control-state and task write in this publication phase.
func (s *Scheduler) publishTransition(ctx chasm.MutableContext, before, next decision.State, kind decision.Kind, newAdmission bool, eviction string, effectRequests map[string][]byte, requestRecord *scheduler2pb.Payload, carryoverRecord *scheduler2pb.CarryoverState) {
	if eviction != "" {
		delete(s.Operations, eviction)
		delete(s.Requests, eviction)
	}
	pendingStarts := maps.Clone(s.PendingStarts)
	if pendingStarts == nil {
		pendingStarts = make(map[string]int64)
	}
	delete(pendingStarts, eviction)
	if next.Operation != nil {
		if wake, pending := decision.Wake(next); pending {
			pendingStarts[next.Operation.ID] = wake
		} else {
			delete(pendingStarts, next.Operation.ID)
		}
	}
	hasGenerator, latestOperationID := s.HasGenerator, s.OperationId
	drainToken, drainArmed, drainAt := s.DrainToken, s.DrainArmed, s.DrainAt
	manualToken, manualArmed, manualAt := s.ManualToken, s.ManualArmed, s.ManualAt
	if kind == decision.DispatchManual {
		manualArmed = false
	}
	previousRoot := s.RootState
	s.RootState = &scheduler2pb.RootState{
		PublicRevision: previousRoot.PublicRevision, PublicActionRef: previousRoot.PublicActionRef, IndexedQueue: previousRoot.IndexedQueue,
		ActivitySet: next.Root.ActivitySet, ActivityAt: next.Root.ActivityAt, UnconfirmedStarts: next.Root.UnconfirmedStarts,
		IdleDuration: previousRoot.IdleDuration, IdleToken: previousRoot.IdleToken, IdleArmed: previousRoot.IdleArmed, IdleAt: previousRoot.IdleAt,
		Closed: previousRoot.Closed, Coordination: previousRoot.Coordination, BackfillSequence: previousRoot.BackfillSequence, Backfills: previousRoot.Backfills, BackfillFocus: previousRoot.BackfillFocus, LastBackfill: previousRoot.LastBackfill,
		ManualSequence: next.Root.ManualSequence, ManualToken: manualToken, ManualArmed: manualArmed, ManualAt: manualAt,
		Incarnation: next.Root.Incarnation, Revision: next.Root.Revision, Cursor: next.Root.Cursor,
		Paused: next.Root.Paused, Limited: next.Root.Limited, Remaining: next.Root.Remaining,
		DrainToken: drainToken, DrainArmed: drainArmed, DrainAt: drainAt, Policy: next.Root.Policy, BufferDropped: next.Root.BufferDropped,
		Unfinished: next.Root.Unfinished, BlockingOperationId: next.Root.BlockingOperationID, CapacityDropped: next.Root.CapacityDropped, OverlapSuperseded: next.Root.OverlapSuperseded,
		PendingStarts: pendingStarts, OperationId: latestOperationID, HasGenerator: hasGenerator, Started: next.Root.Started, CarryoverSet: next.Root.CarryoverSet, PauseOnFailure: next.Root.PauseOnFailure,
	}
	for _, m := range next.Manuals {
		s.Manuals = append(s.Manuals, &scheduler2pb.ManualCommand{Id: m.ID, Nominal: m.Nominal, NominalTime: encodeDecisionTime(m.NominalTime), Policy: m.Policy})
	}
	if !next.IndexedQueue.Enabled {
		for _, q := range next.Queue {
			s.Queue = append(s.Queue, encodeQueued(q))
		}
	}
	if next.Carryover != nil {
		s.Carryover = chasm.NewDataField(ctx, carryoverRecord)
	}
	if op := next.Operation; op != nil {
		if newAdmission {
			s.OperationId = op.ID
		}
		metadata := &scheduler2pb.OperationState{
			StartObserved: op.StartObserved, StartObservedAt: op.StartObservedAt,
			OccurrenceId: op.OccurrenceID, ActionId: op.ActionID, Id: op.ID, ChainId: op.ChainID,
			Nominal: op.Nominal, NominalTime: encodeDecisionTime(op.NominalTime), WorkflowId: op.WorkflowID, Deadline: op.Deadline, RetryAt: op.RetryAt, Phase: scheduler2pb.Phase(op.Phase), Policy: op.Policy, ManualId: op.ManualID, BackfillId: op.BackfillID,
		}
		s.Operations[op.ID] = chasm.NewDataField(ctx, metadata)
		if newAdmission {
			s.Requests[op.ID] = chasm.NewDataField(ctx, requestRecord)
		}
		if decision.Terminal(op.Phase) {
			// Terminal metadata fences late deliveries without retaining issuance payloads.
			delete(s.Requests, op.ID)
		}
	}
	oldWake, hadWake := decision.Wake(before)
	wake, hasWake := decision.Wake(next)
	if hasWake && (!hadWake || before.Operation.ID != next.Operation.ID || oldWake != wake) {
		scheduled := time.Unix(0, wake)
		if s.workQuanta(ctx) > 0 && !scheduled.After(ctx.Now(s)) {
			// Due side effects use the transfer queue, as in the legacy invoker.
			// The payload retains the logical wake/deadline fencing; future retries remain timers.
			scheduled = chasm.TaskScheduledTimeImmediate
		}
		ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: scheduled},
			&scheduler2pb.StartTask{OperationId: next.Operation.ID, WakeTime: wake})
	}
	s.publishEffects(ctx, before, next, effectRequests)

}

// request returns a detached request after checking the selected operation's deadline.
func (s *Scheduler) request(ctx chasm.Context, operationID string) (*workflowservice.StartWorkflowExecutionRequest, error) {
	if s.Closed {
		return nil, nil
	}
	state, err := s.stateFor(ctx, operationID)
	if err != nil {
		return nil, err
	}
	if state.Operation == nil {
		return nil, nil
	}
	now := ctx.Now(s).UnixNano()
	if state.Operation.Phase == decision.Pending && now < state.Operation.RetryAt {
		return nil, serviceerror.NewUnavailable("workflow start is not due")
	}
	if _, ok := decision.Issue(state, now); !ok {
		return nil, nil
	}
	return s.assembleRequest(ctx, s.Requests[operationID].Get(ctx))
}

// retentionEviction chooses at most one completed record; active work is never reclaimed.
func (s *Scheduler) retentionEviction(ctx chasm.Context, before, next decision.State) (string, error) {
	if next.Operation == nil || (before.Operation != nil && before.Operation.ID == next.Operation.ID) || len(s.Operations) < 4 {
		return "", nil
	}
	var oldest *scheduler2pb.OperationState
	for _, field := range s.Operations {
		op := field.Get(ctx)
		if decision.Terminal(decision.Phase(op.Phase)) && (s.PublicRevision == 0 || op.Phase == scheduler2pb.PHASE_START_ONLY) && (oldest == nil || op.Nominal < oldest.Nominal || (op.Nominal == oldest.Nominal && op.Id < oldest.Id)) {
			oldest = op
		}
	}
	if oldest == nil {
		if s.PublicRevision != 0 {
			return "", nil
		}
		return "", serviceerror.NewFailedPrecondition("operation capacity has no completed record to reclaim")
	}
	return oldest.Id, nil
}

func (s *Scheduler) freezeAdmission(ctx chasm.Context, op *decision.Operation) error {
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(op.Request, request); err != nil {
		return serviceerror.NewInvalidArgument("invalid workflow start request")
	}
	if op.Policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		request.LastCompletionResult = nil
		request.ContinuedFailure = nil
	} else if s.CarryoverSet {
		if err := s.materializeCarryover(ctx, request); err != nil {
			return err
		}
	}
	if op.WorkflowID != "" {
		request.WorkflowId = op.WorkflowID
	}
	if op.ManualID != 0 || op.BackfillID != 0 {
		request.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	request.RequestId = op.ID
	if _, public := s.PublicInfo.TryGet(ctx); public {
		ref, err := ctx.Ref(s)
		if err != nil {
			return err
		}
		callback, err := chasm.GenerateNexusCallback(ref, op.ID, true)
		if err != nil {
			return err
		}
		request.CompletionCallbacks = []*commonpb.Callback{callback}
		fields := request.GetSearchAttributes().GetIndexedFields()
		if fields == nil {
			fields = make(map[string]*commonpb.Payload)
		}
		scheduledBy := chasm.SearchAttributeTemporalScheduledByID.Value(ctx.ExecutionKey().BusinessID)
		scheduledAt := chasm.SearchAttributeTemporalScheduledStartTime.Value(op.NominalTime.Time(op.Nominal))
		fields[scheduledBy.Field] = scheduledBy.Value.MustEncode()
		fields[scheduledAt.Field] = scheduledAt.Value.MustEncode()
		request.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: fields}
	}
	frozen, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return err
	}
	op.Request = frozen
	return nil
}
