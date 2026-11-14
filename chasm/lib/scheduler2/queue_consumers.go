package scheduler2

import "go.temporal.io/server/chasm/lib/scheduler2/internal/decision"

// queueLength reads validated logical membership, excluding cleanup records.
// CHASM projection callbacks have no error result; invariant errors propagate
// to the outer caller using the same convention as DATA decoding failures.
func (s *Scheduler) queueLength() int64 {
	summary, err := s.queueSummary()
	if err != nil {
		//nolint:forbidigo // Projection callbacks cannot return DATA invariant errors.
		panic(err)
	}
	if !summary.Enabled {
		return int64(len(s.Queue))
	}
	return decision.QueueLength(decision.State{IndexedQueue: summary})
}

func (s *Scheduler) queueMaintenancePending() bool {
	return s.IndexedQueue != nil && s.IndexedQueue.CompletedDrain != nil
}
