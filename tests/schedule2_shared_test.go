package tests

import (
	"context"
	"testing"

	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// The complete shared suite uses an explicitly isolated live scheduler2 frontend.
// No per-test skips or fallback to the original scheduler are configured here.
var schedule2Backend = scheduleBackend{
	name:            "scheduler2",
	assertPersisted: assertSchedule2Persisted,
	context:         func(ctx context.Context) context.Context { return ctx },
	options: []testcore.TestOption{
		testcore.WithScheduleFrontend(scheduler2.NewPublicExecutionTestFrontend, scheduler2.NewNilLibrary()),
	},
}

func TestSchedule2SharedSuite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleSuite{}, schedule2Backend)
}
