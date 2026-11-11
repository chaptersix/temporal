package scheduler

import (
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
)

// NextTimeCandidate examines one raw matching instant, including excluded ones.
// It performs at most two raw calendar lookups, without looping over exclusions.
// Callers enforce spec time bounds and retain the nominal continuation on yield.
// A zero nominal means there are no more raw matches. The boolean reports exclusion.
func (cs *CompiledSpec) NextTimeCandidate(seed string, after time.Time) (GetNextTimeResult, bool) {
	nominal := cs.rawNextTime(after)
	if nominal.IsZero() {
		return GetNextTimeResult{}, false
	}
	if cs.excluded(nominal) {
		return GetNextTimeResult{Nominal: nominal}, true
	}
	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	if following := cs.rawNextTime(nominal); !following.IsZero() {
		maxJitter = min(maxJitter, following.Sub(nominal))
	}
	return GetNextTimeResult{Nominal: nominal, Next: cs.addJitter(seed, nominal, maxJitter)}, false
}
