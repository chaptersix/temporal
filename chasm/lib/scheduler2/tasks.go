package scheduler2

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

// StartHandler issues the cloned request outside the execution lock.
type StartHandler struct {
	chasm.SideEffectTaskHandlerBase[*scheduler2pb.StartTask]
	Start func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error)
}

func (*StartHandler) Validate(ctx chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.StartTask) (bool, error) {
	state := s.state(ctx)
	wake, pending := decision.Wake(state)
	return pending && task.OperationId == state.Operation.ID && attrs.ScheduledTime.UnixNano() == wake, nil
}

func (h *StartHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, task *scheduler2pb.StartTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := chasm.ReadComponent(ctx, ref, (*Scheduler).request, task.OperationId)
	if err != nil {
		return err
	}
	event := decision.Event{Kind: decision.Expire, Incarnation: ref.RunID, OperationID: task.OperationId}
	if request != nil {
		if h == nil || h.Start == nil {
			return serviceerror.NewUnavailable("local workflow starter is not configured")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		chain, startErr := h.Start(ctx, request)
		if startErr != nil {
			event.Kind = decision.RetryableFailure
		} else {
			event.Kind = decision.Acknowledged
			event.ChainID = chain
		}
	}
	_, _, err = chasm.UpdateComponent(ctx, ref,
		func(s *Scheduler, ctx chasm.MutableContext, event decision.Event) (chasm.NoValue, error) {
			return nil, s.Apply(ctx, event)
		}, event)
	return err
}
