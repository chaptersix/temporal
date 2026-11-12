package tests

import "testing"

// This runs the complete public workflow-pause matrix against the explicitly
// registered scheduler2 backend, preserving the original helpers and assertions.
// Subgroups remain sequential, as in the existing suite, to bound simultaneous
// dedicated clusters while each helper runs its full policy matrix.
func TestSchedule2WorkflowPauseInteraction(t *testing.T) {
	t.Parallel()
	t.Run("Overlap", func(t *testing.T) { runSchedulePauseOverlapMatrix(t, schedule2Backend) })
	t.Run("UnpauseRecovery", func(t *testing.T) { runSchedulePauseRecoveryMatrix(t, schedule2Backend) })
	t.Run("ContinueAsNew", func(t *testing.T) { testSchedulePauseContinueAsNew(t, schedule2Backend) })
	t.Run("Reset", func(t *testing.T) { testSchedulePauseReset(t, schedule2Backend) })
}
