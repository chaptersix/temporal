// Package scheduler2 implements a locally wired, bounded scheduler prototype.
package scheduler2

import (
	"bytes"
	"maps"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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
	Operations     chasm.Map[string, *scheduler2pb.OperationState]
	Requests       chasm.Map[string, *scheduler2pb.Payload]
	EffectRequests chasm.Map[string, *scheduler2pb.Payload]
	Generator      chasm.Field[*Generator]
	Carryover      chasm.Field[*scheduler2pb.CarryoverState]
	Diagnostics    chasm.Field[*scheduler2pb.Payload]
}

func New(incarnation string, remaining int64) *Scheduler {
	return &Scheduler{RootState: &scheduler2pb.RootState{
		Incarnation: incarnation, Revision: 1, Limited: true, Remaining: remaining,
	}, Operations: make(chasm.Map[string, *scheduler2pb.OperationState]), Requests: make(chasm.Map[string, *scheduler2pb.Payload]), EffectRequests: make(chasm.Map[string, *scheduler2pb.Payload])}
}

func (*Scheduler) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
func (*Scheduler) ContextMetadata(chasm.Context) map[string]string { return nil }
func (*Scheduler) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("prototype termination is unsupported")
}

// state reads only control and selected operation metadata.
func (s *Scheduler) state(ctx chasm.Context) decision.State {
	return s.stateFor(ctx, s.OperationId)
}

func (s *Scheduler) stateFor(ctx chasm.Context, operationID string) decision.State {
	state := decision.State{Root: decision.Root{
		Incarnation: s.Incarnation, Revision: s.Revision, Cursor: s.Cursor,
		Paused: s.Paused, Limited: s.Limited, Remaining: s.Remaining, Started: s.Started,
		CarryoverSet: s.CarryoverSet, PauseOnFailure: s.PauseOnFailure, Policy: s.Policy, BufferDropped: s.BufferDropped,
		Unfinished: s.Unfinished, BlockingOperationID: s.BlockingOperationId, CapacityDropped: s.CapacityDropped, OverlapSuperseded: s.OverlapSuperseded,
	}}
	for _, e := range s.Effects {
		state.Effects = append(state.Effects, decodeEffect(e))
	}
	for _, q := range s.Queue {
		state.Queue = append(state.Queue, decision.Queued{Nominal: q.Nominal, Actual: q.Actual, WorkflowID: q.WorkflowId, Policy: q.Policy})
	}
	if field, ok := s.Operations[operationID]; ok {
		op := field.Get(ctx)
		state.Operation = &decision.Operation{
			OccurrenceID: op.OccurrenceId, ActionID: op.ActionId, ID: op.Id,
			ChainID: op.ChainId, Nominal: op.Nominal, Deadline: op.Deadline,
			RetryAt: op.RetryAt, Phase: decision.Phase(op.Phase), WorkflowID: op.WorkflowId, Policy: op.Policy,
		}
	}
	return state
}

// Apply records the transition and successor task in the same CHASM transaction.
func (s *Scheduler) Apply(ctx chasm.MutableContext, event decision.Event) error {
	if event.Incarnation != s.Incarnation {
		return nil
	}
	event.Now = ctx.Now(s).UnixNano()
	operationID := event.OperationID
	if event.Kind == decision.Due || event.Kind == decision.Drain || event.Kind == decision.Update {
		operationID = s.OperationId
	}
	before := s.stateFor(ctx, operationID)
	if err := s.prepareDrain(ctx, before, &event); err != nil {
		return err
	}
	if err := s.prepareCompletion(before, &event); err != nil {
		return err
	}
	if event.Kind == decision.Callback && event.Outcome == decision.Failure && event.Incarnation == s.Incarnation && before.Operation != nil && before.Operation.ID == event.OperationID && !decision.Terminal(before.Operation.Phase) && before.Operation.Policy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && s.CarryoverSet {
		c := s.Carryover.Get(ctx)
		before.Carryover = &decision.Carryover{Result: c.Result, Failure: c.Failure, FailurePresent: c.FailurePresent}
	}

	next, err := decision.Step(before, event)
	if err != nil {
		return err
	}
	next, effectRequests, err := s.prepareEffects(ctx, next)
	if err != nil {
		return err
	}
	newAdmission := (event.Kind == decision.Due || event.Kind == decision.Drain) && next.Operation != nil && (before.Operation == nil || next.Operation.ID != before.Operation.ID)
	if newAdmission {
		if err := s.freezeAdmission(ctx, next.Operation); err != nil {
			return err
		}
	}
	if len(next.Queue) > 0 && !s.HasGenerator {
		return serviceerror.NewFailedPrecondition("buffering requires a configured template")
	}
	if s.canDrain(ctx, next) && !s.DrainArmed && s.DrainToken == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("drain token exhausted")
	}
	eviction, err := s.retentionEviction(ctx, before, next)
	if err != nil {
		return err
	}
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
	s.RootState = &scheduler2pb.RootState{
		Incarnation: next.Root.Incarnation, Revision: next.Root.Revision, Cursor: next.Root.Cursor,
		Paused: next.Root.Paused, Limited: next.Root.Limited, Remaining: next.Root.Remaining,
		DrainToken: drainToken, DrainArmed: drainArmed, DrainAt: drainAt, Policy: next.Root.Policy, BufferDropped: next.Root.BufferDropped,
		Unfinished: next.Root.Unfinished, BlockingOperationId: next.Root.BlockingOperationID, CapacityDropped: next.Root.CapacityDropped, OverlapSuperseded: next.Root.OverlapSuperseded,
		PendingStarts: pendingStarts, OperationId: latestOperationID, HasGenerator: hasGenerator, Started: next.Root.Started, CarryoverSet: next.Root.CarryoverSet, PauseOnFailure: next.Root.PauseOnFailure,
	}
	for _, q := range next.Queue {
		s.Queue = append(s.Queue, &scheduler2pb.QueuedOccurrence{Nominal: q.Nominal, Actual: q.Actual, WorkflowId: q.WorkflowID, Policy: q.Policy})
	}
	if next.Carryover != nil {
		s.Carryover = chasm.NewDataField(ctx, &scheduler2pb.CarryoverState{Result: bytes.Clone(next.Carryover.Result), Failure: bytes.Clone(next.Carryover.Failure), FailurePresent: next.Carryover.FailurePresent})
	}
	if op := next.Operation; op != nil {
		if newAdmission {
			s.OperationId = op.ID
		}
		metadata := &scheduler2pb.OperationState{
			OccurrenceId: op.OccurrenceID, ActionId: op.ActionID, Id: op.ID, ChainId: op.ChainID,
			Nominal: op.Nominal, WorkflowId: op.WorkflowID, Deadline: op.Deadline, RetryAt: op.RetryAt, Phase: scheduler2pb.Phase(op.Phase), Policy: op.Policy,
		}
		s.Operations[op.ID] = chasm.NewDataField(ctx, metadata)
		if newAdmission {
			s.Requests[op.ID] = chasm.NewDataField(ctx, &scheduler2pb.Payload{Data: bytes.Clone(op.Request)})
		}
	}
	oldWake, hadWake := decision.Wake(before)
	wake, hasWake := decision.Wake(next)
	if hasWake && (!hadWake || before.Operation.ID != next.Operation.ID || oldWake != wake) {
		ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, wake)},
			&scheduler2pb.StartTask{OperationId: next.Operation.ID})
	}
	s.publishEffects(ctx, before, next, effectRequests)
	s.reconcileDrain(ctx, next)
	return nil
}

// request returns a detached request after checking the selected operation's deadline.
func (s *Scheduler) request(ctx chasm.Context, operationID string) (*workflowservice.StartWorkflowExecutionRequest, error) {
	state := s.stateFor(ctx, operationID)
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
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(s.Requests[operationID].Get(ctx).Data, request); err != nil {
		return nil, err
	}
	return request, nil
}

// retentionEviction chooses at most one completed record; active work is never reclaimed.
func (s *Scheduler) retentionEviction(ctx chasm.Context, before, next decision.State) (string, error) {
	if next.Operation == nil || (before.Operation != nil && before.Operation.ID == next.Operation.ID) || len(s.Operations) < 4 {
		return "", nil
	}
	var oldest *scheduler2pb.OperationState
	for _, field := range s.Operations {
		op := field.Get(ctx)
		if decision.Terminal(decision.Phase(op.Phase)) && (oldest == nil || op.Nominal < oldest.Nominal) {
			oldest = op
		}
	}
	if oldest == nil {
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
	request.RequestId = op.ID
	frozen, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return err
	}
	op.Request = frozen
	return nil
}
