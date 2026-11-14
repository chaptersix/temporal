//go:build windows

package scheduler2

import (
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common/clock"
)

// NewProcessPreparationService establishes one Windows source history for the
// live frontend and task handlers; offline registration does not call it.
func NewProcessPreparationService() (*PreparationService, error) {
	wall := clock.NewRealTimeSource()
	return NewPreparationService(timezonesource.NewWindowsEnvironment(wall), wall)
}
