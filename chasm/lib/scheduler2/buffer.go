package scheduler2

import (
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func (s *Scheduler) reconcileDrain(ctx chasm.MutableContext, state decision.State) {
	if !decision.NeedsDrain(state) {
		s.DrainArmed = false
		return
	}
	if s.DrainArmed {
		return
	}
	s.DrainToken++
	s.DrainArmed = true
	s.DrainAt = ctx.Now(s).UnixNano()
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.DrainAt)}, &scheduler2pb.DrainTask{Token: s.DrainToken})
}

// DrainHandler admits at most one queued occurrence without depending on generation.
type DrainHandler struct{ chasm.PureTaskHandlerBase }

func (*DrainHandler) Validate(_ chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.DrainTask) (bool, error) {
	return s.DrainArmed && s.DrainToken == task.Token && s.DrainAt == attrs.ScheduledTime.UnixNano(), nil
}
func (*DrainHandler) Execute(ctx chasm.MutableContext, s *Scheduler, attrs chasm.TaskAttributes, task *scheduler2pb.DrainTask) error {
	if !s.DrainArmed || s.DrainToken != task.Token || s.DrainAt != attrs.ScheduledTime.UnixNano() {
		return nil
	}
	if ctx.Now(s).Before(time.Unix(0, s.DrainAt)) {
		return serviceerror.NewUnavailable("draining is not due")
	}
	return s.Apply(ctx, decision.Event{Kind: decision.Drain, Incarnation: s.Incarnation})
}

func (s *Scheduler) prepareDrain(ctx chasm.Context, state decision.State, event *decision.Event) error {
	if event.Kind != decision.Drain || event.Incarnation != s.Incarnation || !decision.NeedsDrain(state) {
		return nil
	}
	if !s.HasGenerator {
		return serviceerror.NewFailedPrecondition("buffered admission requires a configured template")
	}
	g := s.Generator.Get(ctx)
	event.CatchupWindow = g.CatchupWindow
	if decision.DrainIndex(state, event.Now, event.CatchupWindow) >= 0 {
		event.Request = g.Definition.Get(ctx).Request
	}
	return nil
}
