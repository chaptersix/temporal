//go:build unix && !ios && !android

package scheduler2

import (
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common/clock"
)

// NewProcessPreparationService establishes the live process provider boundary.
// Call once at shared frontend/task initialization; never from NewNilLibrary.
func NewProcessPreparationService() (*PreparationService, error) {
	wall := clock.NewRealTimeSource()
	return NewPreparationService(timezonesource.NewUnixEnvironment(wall), wall)
}
