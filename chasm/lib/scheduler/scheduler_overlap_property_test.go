package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestSchedulerOverlapPoliciesProperty(t *testing.T) {
	tests := []struct {
		name             string
		policy           enumspb.ScheduleOverlapPolicy
		startsBeforeDone int
		cancelCalls      int
		terminateCalls   int
		overlapSkipped   int64
	}{
		{name: "allow all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, startsBeforeDone: 2},
		{name: "skip", policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, startsBeforeDone: 1, overlapSkipped: 1},
		{name: "buffer one", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, startsBeforeDone: 1},
		{name: "buffer all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, startsBeforeDone: 1},
		{name: "cancel other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, startsBeforeDone: 1, cancelCalls: 1},
		{name: "terminate other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, startsBeforeDone: 1, terminateCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newSchedulerPropertyEnvWithPolicy(t, false, test.policy)
			_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
			require.NoError(t, err)

			env.trigger(t)
			_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
			require.NoError(t, err)
			firstCall := env.services.Start.Calls()[0]

			env.trigger(t)
			_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
			require.NoError(t, err)
			require.Len(t, env.services.Start.Calls(), test.startsBeforeDone)
			require.Len(t, env.services.Cancel.Calls(), test.cancelCalls)
			require.Len(t, env.services.Terminate.Calls(), test.terminateCalls)
			require.Equal(t, test.overlapSkipped, env.describe(t).GetInfo().GetOverlapSkipped())

			if test.policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE ||
				test.policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL ||
				test.policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER ||
				test.policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
				env.complete(t, firstCall.Request.GetRequestId())
				_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
				require.NoError(t, err)
				require.Len(t, env.services.Start.Calls(), 2)
			}
		})
	}
}
