package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSchedulerInitialPatchPauseStateAudit(t *testing.T) {
	tests := []struct {
		name            string
		initiallyPaused bool
		patch           *schedulepb.SchedulePatch
		wantPaused      bool
		wantNote        string
	}{
		{
			name:       "pause active schedule",
			patch:      &schedulepb.SchedulePatch{Pause: "maintenance"},
			wantPaused: true,
			wantNote:   "maintenance",
		},
		{
			name:            "unpause paused schedule",
			initiallyPaused: true,
			patch:           &schedulepb.SchedulePatch{Unpause: "resume"},
			wantNote:        "resume",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newSchedulerPropertyEnvWithPolicyAndInitialPatch(
				t,
				test.initiallyPaused,
				enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				test.patch,
			)
			description := env.describe(t)
			require.Equal(t, test.wantPaused, description.GetSchedule().GetState().GetPaused())
			require.Equal(t, test.wantNote, description.GetSchedule().GetState().GetNotes())
		})
	}
}

func TestSchedulerPauseOnFailureInvalidatesConflictTokenAudit(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	_, err := env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		component.(*scheduler.Scheduler).Schedule.Policies.PauseOnFailure = true
		return nil
	})
	require.NoError(t, err)
	_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	env.trigger(t)
	_, err = env.engine.DrainTasks(t.Context(), env.ref, schedulerConformanceDrainLimit)
	require.NoError(t, err)
	requestID := env.services.Start.Calls()[0].Request.GetRequestId()
	before := env.describe(t)

	_, err = env.engine.UpdateComponent(env.engineCtx, env.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		return component.(*scheduler.Scheduler).HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: requestID,
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{Failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{},
			}},
			CloseTime: timestamppb.New(env.timeSource.Now()),
		})
	})
	require.NoError(t, err)

	_, err = env.handler.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:     namespace,
			ScheduleId:    scheduleID,
			ConflictToken: before.GetConflictToken(),
			Schedule:      proto.CloneOf(before.GetSchedule()),
			Memo:          &commonpb.Memo{},
		},
	})
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
}
