package scheduler2

import (
	"context"
	"errors"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/proto"
)

// Trigger accepts a local manual command using the framework's bounded request deduplication.
func Trigger(ctx context.Context, ref chasm.ComponentRef, requestID string, request *schedulepb.TriggerImmediatelyRequest) error {
	_, _, err := chasm.UpdateComponent(ctx, ref, func(s *Scheduler, cc chasm.MutableContext, request *schedulepb.TriggerImmediatelyRequest) (chasm.NoValue, error) {
		return nil, s.AcceptManual(cc, request)
	}, request, chasm.WithRequestID(requestID))
	if errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		return nil
	}
	return err
}

// AcceptManual records payload-free ingress; execution uses a separately fenced task.
func (s *Scheduler) AcceptManual(ctx chasm.MutableContext, request *schedulepb.TriggerImmediatelyRequest) error {
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
	if request == nil || !s.HasGenerator {
		return serviceerror.NewInvalidArgument("manual trigger requires request and configured template")
	}
	if err := s.checkPublicCommandCapacity(1); err != nil {
		return err
	}
	nominal := ctx.Now(s)
	if request.ScheduledTime != nil {
		if err := request.ScheduledTime.CheckValid(); err != nil {
			return serviceerror.NewInvalidArgument("invalid manual scheduled time")
		}
		nominal = request.ScheduledTime.AsTime()
	}
	// Preserve the timestamp exactly before converting into persisted nanoseconds.
	if !time.Unix(0, nominal.UnixNano()).Equal(nominal) {
		return serviceerror.NewInvalidArgument("manual scheduled time exceeds nanosecond range")
	}
	return s.Apply(ctx, decision.Event{Kind: decision.AcceptManual, PublicManualIngress: s.PublicRevision != 0, Incarnation: s.Incarnation, Nominal: nominal.UnixNano(), Policy: request.OverlapPolicy})
}

func (s *Scheduler) prepareManual(ctx chasm.Context, state decision.State, event *decision.Event) error {
	if event.Kind != decision.DispatchManual || !decision.ManualNeedsTemplate(state) {
		return nil
	}
	if !s.HasGenerator {
		return serviceerror.NewFailedPrecondition("manual trigger requires configured template")
	}
	template, err := s.generationRequest(ctx)
	if err != nil {
		return err
	}
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(template, request); err != nil {
		return err
	}
	event.WorkflowID = request.WorkflowId + "-" + time.Unix(0, state.Manuals[0].Nominal).UTC().Format(time.RFC3339)
	event.Request = template
	return nil
}

// ManualHandler transfers at most one accepted command per transaction.
type ManualHandler struct{ chasm.PureTaskHandlerBase }

// Validate rejects legacy manual tasks that bypass coordinator arbitration.
func (*ManualHandler) Validate(chasm.Context, *Scheduler, chasm.TaskInvocation, *scheduler2pb.ManualTask) (bool, error) {
	return false, nil
}
func (*ManualHandler) Execute(chasm.MutableContext, *Scheduler, chasm.TaskAttributes, *scheduler2pb.ManualTask) error {
	return nil
}
