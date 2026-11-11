package scheduler2

import (
	"context"
	"errors"
	"math"
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
	if request == nil || !s.HasGenerator {
		return serviceerror.NewInvalidArgument("manual trigger requires request and configured template")
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
	return s.Apply(ctx, decision.Event{Kind: decision.AcceptManual, Incarnation: s.Incarnation, Nominal: nominal.UnixNano(), Policy: request.OverlapPolicy})
}

func (s *Scheduler) prepareManual(ctx chasm.Context, state decision.State, event *decision.Event) error {
	if event.Kind != decision.DispatchManual || !decision.ManualNeedsTemplate(state) {
		return nil
	}
	if !s.HasGenerator {
		return serviceerror.NewFailedPrecondition("manual trigger requires configured template")
	}
	template := s.Generator.Get(ctx).Definition.Get(ctx).Request
	request := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(template, request); err != nil {
		return err
	}
	event.WorkflowID = request.WorkflowId + "-" + time.Unix(0, state.Manuals[0].Nominal).UTC().Format(time.RFC3339)
	event.Request = template
	return nil
}

func (s *Scheduler) reconcileManual(ctx chasm.MutableContext, state decision.State) {
	if !decision.CanDispatchManual(state) {
		s.ManualArmed = false
		return
	}
	if s.ManualArmed {
		return
	}
	s.ManualToken++
	s.ManualArmed = true
	s.ManualAt = ctx.Now(s).UnixNano()
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.ManualAt)}, &scheduler2pb.ManualTask{Token: s.ManualToken})
}

// ManualHandler transfers at most one accepted command per transaction.
type ManualHandler struct{ chasm.PureTaskHandlerBase }

func (*ManualHandler) Validate(_ chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.ManualTask) (bool, error) {
	return s.ManualArmed && s.ManualToken == task.Token && s.ManualAt == attrs.ScheduledTime.UnixNano(), nil
}
func (*ManualHandler) Execute(ctx chasm.MutableContext, s *Scheduler, attrs chasm.TaskAttributes, task *scheduler2pb.ManualTask) error {
	if !s.ManualArmed || s.ManualToken != task.Token || s.ManualAt != attrs.ScheduledTime.UnixNano() {
		return nil
	}
	if ctx.Now(s).Before(time.Unix(0, s.ManualAt)) {
		return serviceerror.NewUnavailable("manual trigger transfer is not due")
	}
	return s.Apply(ctx, decision.Event{Kind: decision.DispatchManual, Incarnation: s.Incarnation})
}

func (s *Scheduler) checkManualToken(state decision.State, consuming bool) error {
	if decision.CanDispatchManual(state) && (!s.ManualArmed || consuming) && s.ManualToken == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("manual trigger token exhausted")
	}
	return nil
}
