package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/testing/await"
)

func TestSchedule2PreparedAutomaticPhysicalProgress(t *testing.T) {
	t.Parallel()
	env := newScheduleEnv(t, []scheduleBackend{schedule2PreparedBackend})
	id := "prepared-automatic-progress"
	spec := intervalSpec(5 * time.Second)
	spec.Calendar = []*schedulepb.CalendarSpec{{DayOfMonth: "10", Year: "2010"}}
	spec.CronString = []string{"11 11/11 11 11 1 2011"}
	createSchedule(t.Context(), t, env, id, &schedulepb.Schedule{Spec: spec, Action: startWorkflowAction(env, "automatic-workflow", "automatic-type")})
	ctx, err := env.GetTestCluster().Host().ChasmContext(t.Context())
	require.NoError(t, err)
	await.Require(t.Context(), t, func(at *await.T) {
		snapshot, err := chasm.ReadComponent(ctx, chasm.NewComponentRef[*scheduler2.Scheduler](chasm.ExecutionKey{NamespaceID: env.NamespaceID().String(), BusinessID: id}), func(s *scheduler2.Scheduler, cc chasm.Context, _ struct{}) (automaticProgressSnapshot, error) {
			g := s.Generator.Get(cc)
			request, _ := s.SourceRequest.TryGet(cc)
			return automaticProgressSnapshot{started: s.Started, details: fmt.Sprintf("generator=%s source=%s coordination=%s", g.GeneratorState, request, s.Coordination)}, nil
		}, struct{}{})
		require.NoError(at, err)
		t.Logf("started=%d %s", snapshot.started, snapshot.details)
		require.GreaterOrEqual(at, snapshot.started, int64(2))
	}, 15*time.Second, time.Second)
}

type automaticProgressSnapshot struct {
	started int64
	details string
}
