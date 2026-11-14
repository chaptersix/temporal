package scheduler2

import (
	"math"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
)

// ConfigureIdle changes the local retention duration without refreshing activity.
// Zero disables idle closure. Initial setup supplies a clock for low-level roots.
func (s *Scheduler) ConfigureIdle(ctx chasm.MutableContext, duration time.Duration) error {
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
	if duration < 0 || duration > 365*24*time.Hour {
		return serviceerror.NewInvalidArgument("idle duration must be between zero and 365 days")
	}
	previous := s.RootState
	next := common.CloneProto(previous)
	next.IdleDuration = int64(duration)
	if !next.ActivitySet {
		atNano, err := activityNano(ctx.Now(s))
		if err != nil {
			return err
		}
		next.ActivitySet, next.ActivityAt = true, atNano
	}
	s.RootState = next
	if err := s.reconcileIdle(ctx); err != nil {
		s.RootState = previous
		return err
	}
	return nil
}

// idleDeadline separates accepted obligations from mere operation occupancy.
func idleDeadline(state decision.State, continuation *scheduler2pb.OccurrenceContinuation, initialized, backfills bool, duration int64) (int64, bool, error) {
	if duration == 0 || !state.Root.ActivitySet {
		return 0, false, nil
	}
	if duration < 0 || duration > int64(365*24*time.Hour) || state.Root.ActivityAt > math.MaxInt64-duration {
		return 0, false, serviceerror.NewFailedPrecondition("idle deadline exceeds supported range")
	}
	deadlineNano := state.Root.ActivityAt + duration
	if state.IndexedQueue.Maintenance || !initialized || state.Root.Paused || backfills || len(state.Manuals) > 0 || decision.QueueLength(state) > 0 || state.Root.UnconfirmedStarts != 0 {
		return deadlineNano, false, nil
	}
	for _, effect := range state.Effects {
		if effect.Phase == decision.AwaitingTarget || effect.Phase == decision.EffectPending || effect.Phase == decision.EffectAmbiguous {
			return deadlineNano, false, nil
		}
	}
	if !state.Root.Limited || state.Root.Remaining > 0 {
		if continuation == nil {
			return deadlineNano, false, nil
		}
		switch continuation.Terminal {
		case scheduler2pb.ENUMERATION_STATUS_EMPTY, scheduler2pb.ENUMERATION_STATUS_IMPOSSIBLE, scheduler2pb.ENUMERATION_STATUS_EXHAUSTED:
		default:
			return deadlineNano, false, nil
		}
	}
	return deadlineNano, true, nil
}

func (s *Scheduler) checkIdlePlan(state decision.State, continuation *scheduler2pb.OccurrenceContinuation, initialized, backfills bool) (int64, bool, error) {
	atNano, armed, err := idleDeadline(state, continuation, initialized, backfills, s.IdleDuration)
	if err != nil {
		return 0, false, err
	}
	if armed && (!s.IdleArmed || s.IdleAt != atNano) && s.IdleToken == math.MaxUint64 {
		return 0, false, serviceerror.NewFailedPrecondition("idle token exhausted")
	}
	return atNano, armed, nil
}

func (s *Scheduler) planIdle(ctx chasm.Context, state decision.State) (int64, bool, error) {
	if s.Closed || s.IdleDuration == 0 || s.sourcePreparationPending(ctx) {
		return 0, false, nil
	}
	if ready, err := s.preparedMaintenanceReady(ctx, false); err != nil || ready {
		return 0, false, err
	}
	var continuation *scheduler2pb.OccurrenceContinuation
	if s.HasGenerator {
		continuation = s.Generator.Get(ctx).Continuation
	}
	return s.checkIdlePlan(state, continuation, s.HasGenerator, len(s.Backfills) > 0)
}

// checkNextIdle rejects new scheduling requirements before transition publication.
func (s *Scheduler) checkNextIdle(ctx chasm.Context, next decision.State) error {
	_, _, err := s.planIdle(ctx, next)
	return err
}

func (s *Scheduler) reconcileIdle(ctx chasm.MutableContext) error {
	if s.Closed {
		return nil
	}
	if s.IdleDuration == 0 {
		s.IdleArmed = false
		return nil
	}
	state, err := s.stateFor(ctx, "")
	if err != nil {
		return err
	}
	atNano, armed, err := s.planIdle(ctx, state)
	if err != nil {
		return err
	}
	if !armed {
		s.IdleArmed = false
		return nil
	}
	if s.IdleArmed && s.IdleAt == atNano {
		return nil
	}
	s.IdleToken++
	s.IdleAt = atNano
	s.IdleArmed = true
	ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, atNano)}, &scheduler2pb.IdleTask{Token: s.IdleToken})
	return nil
}

// IdleHandler closes only the currently eligible execution at its stable deadline.
type IdleHandler struct{ chasm.PureTaskHandlerBase }

func (*IdleHandler) Validate(ctx chasm.Context, s *Scheduler, attrs chasm.TaskInvocation, task *scheduler2pb.IdleTask) (bool, error) {
	if s.Closed || !s.IdleArmed || task.Token != s.IdleToken || attrs.ScheduledTime.UnixNano() != s.IdleAt {
		return false, nil
	}
	state, err := s.stateFor(ctx, "")
	if err != nil {
		return false, err
	}
	atNano, armed, err := s.planIdle(ctx, state)
	return err == nil && armed && atNano == s.IdleAt, err
}
func (h *IdleHandler) Execute(ctx chasm.MutableContext, s *Scheduler, attrs chasm.TaskAttributes, task *scheduler2pb.IdleTask) error {
	valid, err := h.Validate(ctx, s, chasm.TaskInvocation{TaskAttributes: attrs}, task)
	if err != nil || !valid {
		return err
	}
	if ctx.Now(s).Before(attrs.ScheduledTime) {
		return serviceerror.NewUnavailable("idle close is not due")
	}
	return s.Close(ctx)
}
