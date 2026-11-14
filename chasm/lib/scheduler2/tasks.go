package scheduler2

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

// StartHandler issues the cloned request outside the execution lock.
type StartHandler struct {
	chasm.SideEffectTaskHandlerBase[*scheduler2pb.StartTask]
	start         func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error)
	startWorkflow func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error)
	permit        chan struct{}
	wallNow       func() time.Time
}

// NewStartHandler bounds concurrent local issuance to one RPC.
func NewStartHandler(start func(context.Context, *workflowservice.StartWorkflowExecutionRequest) (string, error)) *StartHandler {
	// The RPC runs outside a CHASM transaction. Its observation clock is converted
	// to execution time using the issuance snapshot, as in the reference scheduler.
	return &StartHandler{start: start, permit: make(chan struct{}, 1), wallNow: time.Now} //nolint:forbidigo // Post-RPC wall observation plus captured skipped duration reconstructs execution time.
}

func (*StartHandler) Validate(_ chasm.Context, s *Scheduler, _ chasm.TaskInvocation, task *scheduler2pb.StartTask) (bool, error) {
	wake, pending := s.PendingStarts[task.OperationId]
	return !s.Closed && pending && task.WakeTime == wake, nil
}

func (h *StartHandler) Execute(ctx context.Context, ref chasm.ComponentRef, _ chasm.TaskAttributes, task *scheduler2pb.StartTask) error {
	if h == nil || (h.start == nil && h.startWorkflow == nil) {
		return serviceerror.NewUnavailable("local workflow starter is not configured")
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
	snapshot, err := chasm.ReadComponent(ctx, ref, (*Scheduler).startSnapshot, task.OperationId)
	if err != nil {
		return err
	}
	event := decision.Event{Kind: decision.Expire, Incarnation: ref.RunID, OperationID: task.OperationId}
	event, err = h.issueStart(ctx, snapshot, event)
	if err != nil {
		return err
	}
	// Task validity can change while the RPC is in flight. Merge against the same
	// execution incarnation and let operation identity/phase validate the result.
	resultRef := chasm.NewComponentRef[*Scheduler](ref.ExecutionKey)
	_, _, err = chasm.UpdateComponent(ctx, resultRef,
		func(s *Scheduler, ctx chasm.MutableContext, event decision.Event) (chasm.NoValue, error) {
			return nil, s.Apply(ctx, event)
		}, event)
	return err
}

// issueStart uses the detached issuance snapshot after its caller establishes
// durable authority and acquires the handler's shared local permit.
func (h *StartHandler) issueStart(ctx context.Context, snapshot startSnapshot, event decision.Event) (decision.Event, error) {
	if snapshot.request != nil {
		if err := ctx.Err(); err != nil {
			return event, err
		}
		var chain string
		var startErr error
		if h.startWorkflow != nil {
			var response *workflowservice.StartWorkflowExecutionResponse
			response, startErr = h.startWorkflow(ctx, snapshot.request)
			chain, event.PublicRunID = response.GetFirstExecutionRunId(), response.GetRunId()
		} else {
			chain, startErr = h.start(ctx, snapshot.request)
		}
		if startErr != nil {
			// Client retries can hide acceptance before the final error, including
			// a permanent status. Preserve authority until positive reconciliation.
			event.Kind = decision.RetryableFailure
		} else {
			event.Kind = decision.Acknowledged
			event.ChainID = chain
			var err error
			event.StartObservedAt, err = activityNano(h.wallNow().Add(snapshot.skipped))
			if err != nil {
				return event, err
			}
			event.StartObserved = true
		}
	}
	return event, nil
}
