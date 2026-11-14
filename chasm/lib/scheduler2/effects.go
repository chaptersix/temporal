package scheduler2

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

func decodeEffect(e *scheduler2pb.EffectState) decision.Effect {
	return decision.Effect{ID: e.Id, TargetID: e.TargetId, Kind: decision.EffectKind(e.Kind), Phase: decision.EffectPhase(e.Phase), ChainID: e.ChainId, Deadline: e.Deadline, RetryAt: e.RetryAt}
}

func (s *Scheduler) prepareEffects(ctx chasm.Context, next decision.State) (decision.State, map[string][]byte, error) {
	if err := validateDecisionEffects(next); err != nil {
		return decision.State{}, nil, err
	}
	for _, effect := range next.Effects {
		if effect.Phase != decision.AwaitingTarget {
			continue
		}
		target := next.Operation
		if target == nil || target.ID != effect.TargetID {
			targetState, err := s.stateFor(ctx, effect.TargetID)
			if err != nil {
				return decision.State{}, nil, err
			}
			target = targetState.Operation
		}
		if target == nil {
			return decision.State{}, nil, serviceerror.NewFailedPrecondition("effect target metadata is missing")
		}
		var err error
		next, err = decision.BindEffects(next, target, ctx.Now(s).UnixNano())
		if err != nil {
			return decision.State{}, nil, err
		}
		break
	}
	requests := make(map[string][]byte)
	for _, effect := range next.Effects {
		if effect.Phase != decision.EffectPending {
			continue
		}
		if _, exists := s.EffectRequests[effect.ID]; exists {
			continue
		}
		// Effect identity lives in the small skeleton; large fields are irrelevant.
		start := &workflowservice.StartWorkflowExecutionRequest{}
		record, err := s.requiredRequestRecord(ctx, effect.TargetID)
		if err != nil {
			return decision.State{}, nil, err
		}
		if err := proto.Unmarshal(record.Data, start); err != nil {
			return decision.State{}, nil, err
		}
		wire, err := freezeEffect(effect, start)
		if err != nil {
			return decision.State{}, nil, err
		}
		requests[effect.ID] = wire
	}
	return next, requests, nil
}

func freezeEffect(effect decision.Effect, start *workflowservice.StartWorkflowExecutionRequest) ([]byte, error) {
	target := &commonpb.WorkflowExecution{WorkflowId: start.WorkflowId}
	var request proto.Message
	switch effect.Kind {
	case decision.Cancel:
		request = &workflowservice.RequestCancelWorkflowExecutionRequest{Namespace: start.Namespace, WorkflowExecution: target, FirstExecutionRunId: effect.ChainID, RequestId: effect.ID, Reason: "cancelled by schedule overlap policy", Identity: "scheduler2"}
	case decision.Terminate:
		request = &workflowservice.TerminateWorkflowExecutionRequest{Namespace: start.Namespace, WorkflowExecution: target, FirstExecutionRunId: effect.ChainID, Reason: "terminated by schedule overlap policy", Identity: "scheduler2"}
	default:
		return nil, serviceerror.NewInvalidArgument("unknown overlap effect")
	}
	if effect.ChainID == "" || start.WorkflowId == "" {
		return nil, serviceerror.NewInvalidArgument("overlap effect requires workflow and first-run identity")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	return wire, nil
}

func (s *Scheduler) publishEffects(ctx chasm.MutableContext, before, next decision.State, requests map[string][]byte) {
	retained := make(map[string]bool, len(next.Effects))
	oldWakes := make(map[string]int64)
	for _, effect := range before.Effects {
		if wake, pending := decision.EffectWake(effect); pending {
			oldWakes[effect.ID] = wake
		}
	}
	for _, effect := range next.Effects {
		retained[effect.ID] = true
		s.Effects = append(s.Effects, &scheduler2pb.EffectState{Id: effect.ID, TargetId: effect.TargetID, Kind: scheduler2pb.EffectKind(effect.Kind), Phase: scheduler2pb.EffectPhase(effect.Phase), ChainId: effect.ChainID, Deadline: effect.Deadline, RetryAt: effect.RetryAt})
		if wire, ok := requests[effect.ID]; ok {
			s.EffectRequests[effect.ID] = chasm.NewDataField(ctx, &scheduler2pb.Payload{Data: wire})
		}
		if wake, pending := decision.EffectWake(effect); pending {
			previous, existed := oldWakes[effect.ID]
			if !existed || previous != wake {
				ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, wake)}, &scheduler2pb.EffectTask{EffectId: effect.ID, WakeTime: wake})
			}
		}
	}
	for _, effect := range before.Effects {
		if !retained[effect.ID] {
			delete(s.EffectRequests, effect.ID)
		}
	}
}

// EffectRequest is detached from persisted payloads before an RPC is issued.
type EffectRequest struct {
	Cancel    *workflowservice.RequestCancelWorkflowExecutionRequest
	Terminate *workflowservice.TerminateWorkflowExecutionRequest
}

func (s *Scheduler) effectRequest(ctx chasm.Context, id string) (*EffectRequest, error) {
	if s.Closed {
		return nil, nil
	}
	if err := s.validateEffects(); err != nil {
		return nil, err
	}
	for _, metadata := range s.Effects {
		effect := decodeEffect(metadata)
		if effect.ID != id || effect.TargetID != s.BlockingOperationId || effect.Phase != decision.EffectPending {
			continue
		}
		now := ctx.Now(s).UnixNano()
		if now < effect.RetryAt {
			return nil, serviceerror.NewUnavailable("overlap effect is not due")
		}
		if now >= effect.Deadline {
			return nil, nil
		}
		request := &EffectRequest{}
		var message proto.Message
		switch effect.Kind {
		case decision.Cancel:
			request.Cancel = &workflowservice.RequestCancelWorkflowExecutionRequest{}
			message = request.Cancel
		case decision.Terminate:
			request.Terminate = &workflowservice.TerminateWorkflowExecutionRequest{}
			message = request.Terminate
		default:
			return nil, serviceerror.NewInvalidArgument("unknown overlap effect")
		}
		if err := proto.Unmarshal(s.EffectRequests[id].Get(ctx).Data, message); err != nil {
			return nil, err
		}
		return request, nil
	}
	return nil, nil
}
