package scheduler2

import "go.temporal.io/server/chasm"

var _ chasm.TimeSkippingRuntimeGate = (*Scheduler)(nil)

// IsExecutionSkippable permits timer advancement only after internal work and
// tracked workflow executions settle. Future coordinator wakes remain eligible
// skip targets; due work must be serviced before advancing the execution clock.
func (s *Scheduler) IsExecutionSkippable(ctx chasm.Context) bool {
	if err := s.validateOperationStore(); err != nil {
		return false
	}
	if c := s.OperationStore; c != nil && c.StartDispatch != nil && (c.PendingCount != 0 || c.StartDispatch.Claim != nil || c.StartDispatch.Armed && c.StartDispatch.At <= ctx.Now(s).UnixNano()) {
		return false
	}
	if _, err := s.queueSummary(); err != nil {
		return false
	}
	if s.queueMaintenancePending() || s.Closed || s.Paused || !s.HasGenerator || s.Unfinished != 0 || s.UnconfirmedStarts != 0 ||
		len(s.PendingStarts) != 0 || len(s.Effects) != 0 || s.queueLength() != 0 || len(s.Manuals) != 0 || len(s.Backfills) != 0 {
		return false
	}
	now := ctx.Now(s).UnixNano()
	if c := s.Coordination; c != nil && (c.OwnerKey != "" || c.Armed && c.At <= now) {
		return false
	}
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
