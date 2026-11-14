package scheduler2

import "go.temporal.io/server/chasm"

var _ chasm.TimeSkippingRuntimeGate = (*Scheduler)(nil)

// IsExecutionSkippable permits timer advancement only after internal work and
// tracked workflow executions settle. Future coordinator wakes remain eligible
// skip targets; due work must be serviced before advancing the execution clock.
func (s *Scheduler) IsExecutionSkippable(ctx chasm.Context) bool {
	if ready, err := s.preparedMaintenanceReady(ctx, s.sourcePreparationPending(ctx)); err != nil || ready {
		return false
	}
	if err := s.validateOperationStore(); err != nil {
		return false
	}
	if s.hasPendingDispatch(ctx) {
		return false
	}
	if _, err := s.queueSummary(); err != nil {
		return false
	}
	if s.sourcePreparationPending(ctx) || s.queueMaintenancePending() || s.Closed || s.Paused || !s.HasGenerator || s.Unfinished != 0 || s.UnconfirmedStarts != 0 ||
		len(s.PendingStarts) != 0 || len(s.Effects) != 0 || s.queueLength() != 0 || len(s.Manuals) != 0 || len(s.Backfills) != 0 {
		return false
	}
	now := ctx.Now(s).UnixNano()
	if c := s.Coordination; c != nil && (c.OwnerKey != "" || c.Armed && c.At <= now) {
		return false
	}
	return s.automaticCanSkip(ctx, now)
}

func (s *Scheduler) hasPendingDispatch(ctx chasm.Context) bool {
	c := s.OperationStore
	if c == nil || c.StartDispatch == nil {
		return false
	}
	d := c.StartDispatch
	return c.PendingCount != 0 || d.Claim != nil || d.Armed && d.At <= ctx.Now(s).UnixNano()
}

func (s *Scheduler) automaticCanSkip(ctx chasm.Context, now int64) bool {
	g := s.Generator.Get(ctx)
	if !g.Armed || g.WakeAt > now {
		return true
	}
	// A control patch re-arms the generator at now even when its next candidate
	// is already cached. The coordinator waits for that candidate's actual time;
	// the old wake is not runnable work in that case (see automaticDemand).
	pending := g.Continuation.GetPending()
	return pending != nil && !pending.Excluded && pending.Actual > now
}
