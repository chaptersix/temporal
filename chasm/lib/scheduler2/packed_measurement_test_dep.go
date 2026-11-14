//go:build test_dep

package scheduler2

import (
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
)

// AdmitPackedMeasurementForTest admits one generated occurrence through the actual
// publication path. The caller supplies one History update lease per occurrence;
// this helper never seeds records directly or changes the configured capacity.
func (s *Scheduler) AdmitPackedMeasurementForTest(ctx chasm.MutableContext, nominal time.Time) error {
	if s.PackedStorage == nil || !s.HasGenerator || !s.Generator.Get(ctx).PreparedDefinition.IsSet() {
		return serviceerror.NewInternal("measurement requires fresh packed prepared profile")
	}
	template, err := s.generationRequest(ctx)
	if err != nil {
		return err
	}
	before := s.OperationStore.OperationCount
	ctx = chasm.ContextWithValue(ctx, workQuantaKey{}, 0)
	if err := s.admitGenerated(ctx, template, occurrence.Candidate{Nominal: nominal, Actual: nominal}, time.Minute); err != nil {
		return err
	}
	if s.OperationStore.OperationCount != before+1 {
		return serviceerror.NewInternal("measurement occurrence was not admitted")
	}
	return nil
}
