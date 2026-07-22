package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"pgregory.net/rapid"
)

func TestSchedulerCancelTerminateRetryProfileProperty(t *testing.T) {
	tests := []struct {
		name   string
		policy enumspb.ScheduleOverlapPolicy
		queue  func(schedulerRPCProfiles, *schedulerPropertyEnv)
		calls  func(*schedulerPropertyEnv) int
	}{
		{
			name:   "cancel",
			policy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
			queue: func(profiles schedulerRPCProfiles, env *schedulerPropertyEnv) {
				profiles.cancelRetryable().Queue(&env.services.Cancel)
			},
			calls: func(env *schedulerPropertyEnv) int { return len(env.services.Cancel.Calls()) },
		},
		{
			name:   "terminate",
			policy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
			queue: func(profiles schedulerRPCProfiles, env *schedulerPropertyEnv) {
				profiles.terminateRetryable().Queue(&env.services.Terminate)
			},
			calls: func(env *schedulerPropertyEnv) int { return len(env.services.Terminate.Calls()) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				env := newSchedulerPropertyEnvWithPolicy(t, false, test.policy)
				_, err := env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
				require.NoError(t, err)
				env.trigger(t)
				_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
				require.NoError(t, err)

				test.queue(schedulerRPCProfiles{}, env)
				env.trigger(t)
				_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
				require.NoError(t, err)
				require.Equal(t, 2, test.calls(env))
			})
		})
	}
}
