package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestSchedule2MutationContextMetadata(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"patch", "delete"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			env := newScheduleEnv(t, []scheduleBackend{schedule2Backend}, testcore.WithDynamicConfig(dynamicconfig.FrontendContextMetadataSetTrailer, true))
			id := testcore.RandomizeStr("schedule-metadata")
			workflowType := "metadata-workflow"
			schedule := &schedulepb.Schedule{Spec: intervalSpec(noOpInterval), Action: startWorkflowAction(env, id+"-workflow", workflowType), State: &schedulepb.ScheduleState{Paused: true}}
			createSchedule(t.Context(), t, env, id, schedule)
			assertScheduleCallbackBackend(t, env, schedule2Backend, id)
			var trailer metadata.MD
			var err error
			if operation == "patch" {
				_, err = env.FrontendClient().PatchSchedule(t.Context(), &workflowservice.PatchScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: id, RequestId: uuid.NewString(), Patch: &schedulepb.SchedulePatch{Pause: "test pause"}}, grpc.Trailer(&trailer))
			} else {
				_, err = env.FrontendClient().DeleteSchedule(t.Context(), &workflowservice.DeleteScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: id}, grpc.Trailer(&trailer))
			}
			require.NoError(t, err)
			require.Equal(t, []string{workflowType}, trailer.Get("workflow-type"))
			require.Equal(t, []string{env.WorkerTaskQueue()}, trailer.Get("workflow-task-queue"))
		})
	}
}

func TestSchedule2ClosedMutationMatchesPublicCHASM(t *testing.T) {
	t.Parallel()
	for _, backend := range []scheduleBackend{scheduleCHASMBackend, schedule2Backend} {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			env := newScheduleEnv(t, []scheduleBackend{backend})
			ctx := backend.context(t.Context())
			id := testcore.RandomizeStr("closed-mutation")
			schedule := &schedulepb.Schedule{Spec: intervalSpec(noOpInterval), Action: startWorkflowAction(env, id+"-workflow", "closed-workflow"), State: &schedulepb.ScheduleState{Paused: true}}
			createSchedule(ctx, t, env, id, schedule)
			_, err := env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: id})
			require.NoError(t, err)
			_, err = env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: id, RequestId: uuid.NewString(), Schedule: schedule})
			var missing *serviceerror.NotFound
			require.ErrorAs(t, err, &missing)
			_, err = env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: id, RequestId: uuid.NewString(), Patch: &schedulepb.SchedulePatch{Pause: "closed"}})
			require.ErrorAs(t, err, &missing)
		})
	}
}
