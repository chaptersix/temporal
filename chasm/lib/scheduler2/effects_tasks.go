package scheduler2

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

// EffectHandler sends fenced overlap requests outside the execution lock.
type EffectHandler struct {
	chasm.SideEffectTaskHandlerBase[*scheduler2pb.EffectTask]
	cancel    func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error
	terminate func(context.Context, *workflowservice.TerminateWorkflowExecutionRequest) error
	permit    chan struct{}
}

func NewEffectHandler(cancel func(context.Context, *workflowservice.RequestCancelWorkflowExecutionRequest) error, terminate func(context.Context, *workflowservice.TerminateWorkflowExecutionRequest) error) *EffectHandler {
	return &EffectHandler{cancel: cancel, terminate: terminate, permit: make(chan struct{}, 1)}
}

func (*EffectHandler) Validate(_ chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.EffectTask) (bool, error) {
	for _, metadata := range s.Effects {
		effect := decodeEffect(metadata)
		if effect.ID != task.EffectId || effect.TargetID != s.BlockingOperationId {
			continue
		}
		wake, pending := decision.EffectWake(effect)
		return pending && attrs.ScheduledTime.UnixNano() == wake, nil
	}
	return false, nil
}

func (h *EffectHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, task *scheduler2pb.EffectTask) error {
	if h == nil {
		return serviceerror.NewUnavailable("local overlap executor is not configured")
	}
	select {
	case h.permit <- struct{}{}:
		defer func() { <-h.permit }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := chasm.ReadComponent(ctx, ref, (*Scheduler).effectRequest, task.EffectId)
	if err != nil {
		return err
	}
	event := decision.Event{Kind: decision.EffectExpired, Incarnation: ref.RunID, EffectID: task.EffectId}
	if request != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rpcErr error
		if request.Cancel != nil {
			if h.cancel == nil {
				return serviceerror.NewUnavailable("local cancel executor is not configured")
			}
			rpcErr = h.cancel(ctx, request.Cancel)
		} else {
			if h.terminate == nil {
				return serviceerror.NewUnavailable("local terminate executor is not configured")
			}
			rpcErr = h.terminate(ctx, request.Terminate)
		}
		event.Kind = decision.EffectSucceeded
		if rpcErr != nil {
			event.Kind = decision.EffectFailed
			if _, ok := errors.AsType[*serviceerror.NotFound](rpcErr); ok {
				event.Kind = decision.EffectNotFound
			}
		}
	}
	resultRef := chasm.NewComponentRef[*Scheduler](ref.ExecutionKey)
	_, _, err = chasm.UpdateComponent(ctx, resultRef, func(s *Scheduler, ctx chasm.MutableContext, event decision.Event) (chasm.NoValue, error) {
		return nil, s.Apply(ctx, event)
	}, event)
	return err
}
