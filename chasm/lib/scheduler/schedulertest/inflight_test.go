package schedulertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
)

func TestStartResultSurvivesBufferReordering(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.Paused = true
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	d := NewDriver(t, WithSchedule(schedule), WithControlledStarts())
	require.NoError(t, d.TriggerImmediately())
	require.NoError(t, d.TriggerImmediately())
	require.True(t, d.BeginSideEffect())
	first, second := d.NextStart(), d.NextStart()
	acceptedFirst, err := first.Accept()
	require.NoError(t, err)
	acceptedSecond, err := second.Accept()
	require.NoError(t, err)
	_, err = d.Engine.UpdateComponent(chasm.NewEngineContext(context.Background(), d.Engine), d.ref,
		func(ctx chasm.MutableContext, c chasm.Component) error {
			starts := c.(*scheduler.Scheduler).Invoker.Get(ctx).BufferedStarts
			starts[0], starts[1] = starts[1], starts[0]
			return nil
		})
	require.NoError(t, err)
	second.Respond(acceptedSecond, nil)
	first.Respond(acceptedFirst, nil)
	d.FinishSideEffect()
	snap := d.Snapshot()
	require.Empty(t, CheckInvariants(nil, snap))
	require.Equal(t, int64(2), snap.ActionCount)
	require.Equal(t, acceptedFirst, snap.RecordedStarts[first.Request.GetRequestId()])
	require.Equal(t, acceptedSecond, snap.RecordedStarts[second.Request.GetRequestId()])
}

// This detector documents the reservation gap from #59/#62 without importing
// their experimental action schema into the lifecycle driver.
func TestSelectedStartReservationCounterexample(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.Paused = true
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	d := NewDriver(t, WithSchedule(schedule), WithControlledStarts())
	require.NoError(t, d.TriggerImmediately())
	require.True(t, d.BeginSideEffect())
	call := d.NextStart()
	accepted, err := call.Accept()
	require.NoError(t, err)
	require.NoError(t, d.TriggerImmediately())
	selected := 0
	d.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		for _, start := range s.Invoker.Get(ctx).BufferedStarts {
			if start.GetAttempt() > 0 && start.GetCompleted() == nil {
				selected++
			}
		}
	})
	call.Respond(accepted, nil)
	d.FinishSideEffect()
	require.Equal(t, 2, selected, "known counterexample: two BUFFER_ALL starts selected before the first acknowledgement; replace with <=1 when fixed")
}

// Migration is refused while a start is ready to execute, since V1 cannot resume
// it, and succeeds once the invoker has started it. The migration-untransferable
// invariant stays as a defence in case the refusal regresses.
func TestMigrationRefusedWhileExecutingObligations(t *testing.T) {
	t.Parallel()
	schedule := DefaultSchedule()
	schedule.State.Paused = true
	d := NewDriver(t, WithSchedule(schedule))
	require.NoError(t, d.TriggerImmediately())
	require.ErrorIs(t, d.MigrateToWorkflow(), scheduler.ErrMigrationActionsPending)
	snap := d.Snapshot()
	require.False(t, snap.Migrating)
	require.Equal(t, 1, snap.UntransferableActions)
	require.Empty(t, CheckInvariants(nil, snap))

	for range 20 {
		if d.Snapshot().UntransferableActions == 0 {
			break
		}
		require.True(t, d.Step())
	}
	require.Equal(t, 0, d.Snapshot().UntransferableActions)
	require.NoError(t, d.MigrateToWorkflow())
	require.True(t, d.Snapshot().Migrating)
	require.Empty(t, CheckInvariants(nil, d.Snapshot()))
}

func TestMigrationUntransferableInvariant(t *testing.T) {
	t.Parallel()
	violations := CheckInvariants(nil, Snapshot{Migrating: true, UntransferableActions: 1})
	require.Contains(t, violations, Violation{Name: "migration-untransferable", Message: "1 execution/retry/cancel/terminate obligations lost native task ownership"})
}
