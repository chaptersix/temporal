package scheduler2

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

// DrainHandler admits at most one queued occurrence without depending on generation.
type DrainHandler struct{ chasm.PureTaskHandlerBase }

// Validate rejects legacy drain tasks that bypass coordinator arbitration.
func (*DrainHandler) Validate(chasm.Context, *Scheduler, chasm.TaskInvocation, *scheduler2pb.DrainTask) (bool, error) {
	return false, nil
}
func (*DrainHandler) Execute(chasm.MutableContext, *Scheduler, chasm.TaskAttributes, *scheduler2pb.DrainTask) error {
	return nil
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
	if state.Root.Unfinished < 4 && decision.DrainIndex(state, event.Now, event.CatchupWindow) >= 0 {
		event.Request = g.Definition.Get(ctx).Request
	}
	return nil
}

func (s *Scheduler) canDrain(ctx chasm.Context, state decision.State) bool {
	if !decision.NeedsDrain(state) || !s.HasGenerator {
		return false
	}
	return decision.CanDrain(state, ctx.Now(s).UnixNano(), s.Generator.Get(ctx).CatchupWindow)
}
