package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/tests/testcore"
)

func assertScheduleCallbackBackend(t *testing.T, env *testcore.TestEnv, backend scheduleBackend, id string) {
	t.Helper()
	if backend.assertPersisted != nil {
		backend.assertPersisted(t, env, id)
	}
}

func assertSchedule2Persisted(t *testing.T, env *testcore.TestEnv, id string) {
	t.Helper()
	ctx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	runID, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: id}), func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (string, error) {
		require.Equal(t, cc.ExecutionKey().RunID, s.Incarnation)
		return s.Incarnation, nil
	}, struct{}{})
	require.NoError(t, err)
	require.NotEmpty(t, runID)
	stored, err := env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: runID}, Archetype: chasm.FullyQualifiedName("scheduler2", "scheduler")})
	require.NoError(t, err)
	require.NotNil(t, stored.DatabaseMutableState)
	idStored, err := chasm.ExecutionArchetypeID(stored.DatabaseMutableState)
	require.NoError(t, err)
	require.NotEqual(t, chasm.SchedulerArchetypeID, idStored)
	require.NotEqual(t, chasm.WorkflowArchetypeID, idStored)
}

func TestSchedule2CallbackChains(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		chasmCallbacks, dedup bool
	}{
		{"DoubleResetHSM", false, true}, {"DoubleResetCHASM", true, true}, {"DoubleResetDedupDisabled", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) { testScheduledWorkflowDoubleReset(t, schedule2Backend, tc.chasmCallbacks, tc.dedup) })
	}
	t.Run("ResetThenContinueAsNew", func(t *testing.T) { testScheduledWorkflowResetAndContinueAsNew(t, schedule2Backend, true) })
	t.Run("ContinueAsNewThenReset", func(t *testing.T) { testScheduledWorkflowResetAndContinueAsNew(t, schedule2Backend, false) })
	t.Run("AdditionalCallbackHSM", func(t *testing.T) { testResetWithAdditionalCallback(t, schedule2Backend, false) })
	t.Run("AdditionalCallbackCHASM", func(t *testing.T) { testResetWithAdditionalCallback(t, schedule2Backend, true) })
}
