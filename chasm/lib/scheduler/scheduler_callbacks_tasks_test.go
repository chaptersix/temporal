package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestSchedulerCallbacksTask_Validate_Closed(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	env.Scheduler.Closed = true
	env.Scheduler.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
		RunId: "running-workflow",
	}}
	handler := scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
		Config: defaultConfig(),
	})

	valid, err := handler.Validate(
		env.ReadContext(),
		env.Scheduler,
		chasm.TaskInvocation{},
		&schedulerpb.SchedulerCallbacksTask{},
	)
	require.NoError(t, err)
	require.False(t, valid)
}
