package scheduler2

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
)

// LifecycleState exposes terminal state without loading payload partitions.
func (s *Scheduler) LifecycleState(chasm.Context) chasm.LifecycleState {
	if s.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}
func (*Scheduler) ContextMetadata(chasm.Context) map[string]string { return nil }

// Close revokes future scheduling authority while preserving historical evidence.
// A request snapshot authorized before close may still be issued outside the
// lock; its result is ignored.
func (s *Scheduler) Close(_ chasm.MutableContext) error {
	if s.Closed {
		return nil
	}
	s.Closed = true
	s.IdleArmed = false
	if s.Coordination != nil {
		s.Coordination = common.CloneProto(s.Coordination)
		s.Coordination.Armed = false
		s.Coordination.OwnerKey = ""
	}
	s.ManualArmed = false
	s.DrainArmed = false
	s.PendingStarts = nil
	return nil
}

// Terminate implements the framework hook used by execution deletion.
func (s *Scheduler) Terminate(ctx chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, s.Close(ctx)
}
