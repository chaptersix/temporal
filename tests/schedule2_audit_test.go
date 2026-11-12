package tests

import "testing"

// This preserves the audit loader's public workflow visibility assertions with
// scheduler2's actual scheduled and buffered workflow executions.
func TestSchedule2AuditVisibility(t *testing.T) {
	t.Parallel()
	testScheduleAuditVisibility(t, schedule2Backend)
}
