package tests

import "testing"

func TestSchedule2TimeSkippingDynamicConfigGate(t *testing.T) {
	t.Parallel()
	testTimeSkippingDynamicConfigGateForBackend(t, schedule2Backend)
}

func TestSchedule2MaxSkipCapsScheduleTimeSkipping(t *testing.T) {
	t.Parallel()
	testMaxSkipCapsScheduleTimeSkippingForBackend(t, schedule2Backend)
}

func TestSchedule2TimeSkippingFeatureDisabledWhileWorkflowRuns(t *testing.T) {
	t.Parallel()
	testTimeSkippingFeatureDisabledWhileWorkflowRunsForBackend(t, schedule2Backend)
}
