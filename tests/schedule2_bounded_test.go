package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// The bounded profile runs the same public contracts with its actual persisted
// selectors asserted. It cannot silently fall back to the finite runtime.
var schedule2BoundedBackend = scheduleBackend{
	name:            "scheduler2-bounded",
	context:         func(ctx context.Context) context.Context { return ctx },
	assertPersisted: assertSchedule2BoundedPersisted,
	options: []testcore.TestOption{
		testcore.WithScheduleFrontend(scheduler2.NewBoundedPublicExecutionTestFrontend, scheduler2.NewBoundedPublicTestNilLibrary()),
	},
}

func assertSchedule2BoundedPersisted(t *testing.T, env *testcore.TestEnv, id string) {
	t.Helper()
	assertSchedule2Persisted(t, env, id)
	ctx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	_, err = chasm.ReadComponent(ctx, chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: id}), func(s *scheduler2.Scheduler, _ chasm.Context, _ struct{}) (struct{}, error) {
		require.NotNil(t, s.IndexedQueue)
		require.NotNil(t, s.PayloadStore)
		require.NotNil(t, s.OperationStore)
		require.NotNil(t, s.OperationStore.StartDispatch)
		require.NotNil(t, s.ProjectionStore)
		require.Empty(t, s.Operations)
		require.Empty(t, s.Requests)
		require.Empty(t, s.PayloadBlobs)
		require.Empty(t, s.EffectRequests)
		return struct{}{}, nil
	}, struct{}{})
	require.NoError(t, err)
}

func TestSchedule2BoundedSharedSuite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleSuite{}, schedule2BoundedBackend)
}
