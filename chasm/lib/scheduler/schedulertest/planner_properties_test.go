package schedulertest_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"pgregory.net/rapid"
)

func TestDeferredBufferOneProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		schedule := schedulertest.DefaultSchedule()
		schedule.State.Paused = true
		schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
		d := schedulertest.NewDriver(rt, schedulertest.WithSchedule(schedule))
		require.NoError(rt, d.TriggerImmediately())
		require.True(rt, d.StepSideEffect())
		n := rapid.IntRange(1, 20).Draw(rt, "manualOccurrences")
		for range n {
			require.NoError(rt, d.TriggerImmediately())
		}
		snap := d.Snapshot()
		if len(snap.PendingStarts) != 1 || snap.OverlapSkipped != int64(n-1) {
			rt.Fatalf("BUFFER_ONE lost conservation: pending=%d skipped=%d occurrences=%d", len(snap.PendingStarts), snap.OverlapSkipped, n)
		}
		if snap.ActionCount != 1 {
			rt.Fatalf("pending work consumed starts: %d", snap.ActionCount)
		}
		require.True(rt, d.CompleteWorkflow())
		require.True(rt, d.StepSideEffect())
		if cur := d.Snapshot(); cur.ActionCount != 2 || len(cur.PendingStarts) != 0 {
			rt.Fatalf("completion failed to release exactly one start: count=%d pending=%d", cur.ActionCount, len(cur.PendingStarts))
		}
	})
}
