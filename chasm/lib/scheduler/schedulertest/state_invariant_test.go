package schedulertest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestDerivedState pins the classification tuple. Without this, a refactor
// that made derivedState never return stateStuck would silently disarm every
// stuckness assertion in the package while leaving the suite green.
func TestDerivedState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		state     *schedulerpb.SchedulerState
		liveTasks int
		want      schedState
	}{
		{
			name:      "open with an armed task is running",
			state:     &schedulerpb.SchedulerState{},
			liveTasks: 1,
			want:      stateRunning,
		},
		{
			name:      "open with an armed task and an idle deadline is idle",
			state:     &schedulerpb.SchedulerState{IdleCloseTime: timestamppb.New(time.Unix(100, 0))},
			liveTasks: 1,
			want:      stateIdle,
		},
		{
			name:      "open with no armed task is stuck",
			state:     &schedulerpb.SchedulerState{},
			liveTasks: 0,
			want:      stateStuck,
		},
		{
			name:      "an idle deadline does not excuse a missing task",
			state:     &schedulerpb.SchedulerState{IdleCloseTime: timestamppb.New(time.Unix(100, 0))},
			liveTasks: 0,
			want:      stateStuck,
		},
		{
			name:      "closed with no armed task is terminal, not stuck",
			state:     &schedulerpb.SchedulerState{Closed: true},
			liveTasks: 0,
			want:      stateCompleted,
		},
		{
			name:      "sentinel is inert by design",
			state:     &schedulerpb.SchedulerState{Sentinel: true},
			liveTasks: 0,
			want:      stateSentinel,
		},
		{
			name:      "mid-migration is excused",
			state:     &schedulerpb.SchedulerState{WorkflowMigration: &schedulerpb.WorkflowMigrationState{}},
			liveTasks: 0,
			want:      stateMigrating,
		},
		{
			name:      "closed takes precedence over a stale idle deadline",
			state:     &schedulerpb.SchedulerState{Closed: true, IdleCloseTime: timestamppb.New(time.Unix(100, 0))},
			liveTasks: 0,
			want:      stateCompleted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sched := &scheduler.Scheduler{SchedulerState: tc.state}
			require.Equal(t, tc.want, derivedState(sched, tc.liveTasks))
		})
	}
}
