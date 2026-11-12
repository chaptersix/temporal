package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler2"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type deniedScheduleFrontend struct {
	testhooks.ScheduleFrontend
	calls    atomic.Int64
	checking atomic.Bool
	mutation testhooks.ScheduleMutationFrontend
}

func (f *deniedScheduleFrontend) CreateSchedule(ctx context.Context, id namespace.ID, request *workflowservice.CreateScheduleRequest) (*workflowservice.CreateScheduleResponse, error) {
	if !f.checking.Load() {
		return f.ScheduleFrontend.CreateSchedule(ctx, id, request)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) DescribeSchedule(ctx context.Context, id namespace.ID, request *workflowservice.DescribeScheduleRequest) (*workflowservice.DescribeScheduleResponse, error) {
	if !f.checking.Load() {
		return f.ScheduleFrontend.DescribeSchedule(ctx, id, request)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) DeleteSchedule(ctx context.Context, id namespace.ID, request *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
	if !f.checking.Load() {
		return f.ScheduleFrontend.DeleteSchedule(ctx, id, request)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) ListSchedules(ctx context.Context, id namespace.ID, request *workflowservice.ListSchedulesRequest, query string) (*workflowservice.ListSchedulesResponse, error) {
	if !f.checking.Load() {
		return f.ScheduleFrontend.ListSchedules(ctx, id, request, query)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) CountSchedules(ctx context.Context, id namespace.ID, request *workflowservice.CountSchedulesRequest, query string) (*workflowservice.CountSchedulesResponse, error) {
	if !f.checking.Load() {
		return f.ScheduleFrontend.CountSchedules(ctx, id, request, query)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) UpdateSchedule(ctx context.Context, id namespace.ID, request *workflowservice.UpdateScheduleRequest) (*workflowservice.UpdateScheduleResponse, error) {
	if !f.checking.Load() {
		return f.mutation.UpdateSchedule(ctx, id, request)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) PatchSchedule(ctx context.Context, id namespace.ID, request *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error) {
	if !f.checking.Load() {
		return f.mutation.PatchSchedule(ctx, id, request)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func (f *deniedScheduleFrontend) ListScheduleMatchingTimes(ctx context.Context, id namespace.ID, request *workflowservice.ListScheduleMatchingTimesRequest) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	if !f.checking.Load() {
		return f.mutation.ListScheduleMatchingTimes(ctx, id, request)
	}
	f.calls.Add(1)
	return nil, serviceerror.NewInternal("denied request reached schedule backend")
}

func TestSchedule2AuthorizationPrecedesEveryPublicOperation(t *testing.T) {
	t.Parallel()
	var selected *deniedScheduleFrontend
	backend := scheduleBackend{name: "scheduler2-authorization", context: schedule2Backend.context, options: []testcore.TestOption{
		testcore.WithScheduleFrontend(func(runtime testhooks.HistoryChasmRuntime) (testhooks.ScheduleFrontend, error) {
			frontend, err := scheduler2.NewPublicExecutionTestFrontend(runtime)
			if err != nil {
				return nil, err
			}
			mutation, ok := frontend.(testhooks.ScheduleMutationFrontend)
			if !ok {
				return nil, serviceerror.NewInternal("execution backend has no mutation interface")
			}
			selected = &deniedScheduleFrontend{ScheduleFrontend: frontend, mutation: mutation}
			return selected, nil
		}, scheduler2.NewNilLibrary()),
	}}
	env := newScheduleEnv(t, []scheduleBackend{backend})
	selected.checking.Store(true)
	env.SetOnAuthorize(func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
		return authorization.Result{Decision: authorization.DecisionDeny}, nil
	})
	t.Cleanup(func() {
		selected.checking.Store(false)
		env.SetOnAuthorize(func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
			return authorization.Result{Decision: authorization.DecisionAllow}, nil
		})
	})
	ctx := t.Context()
	client := env.FrontendClient()
	ns := env.Namespace().String()
	id := "denied-schedule"
	schedule := &schedulepb.Schedule{Spec: intervalSpec(noOpInterval), Action: startWorkflowAction(env, "denied-workflow", "denied-type"), State: &schedulepb.ScheduleState{Paused: true}}
	tests := []struct {
		name string
		call func() error
	}{
		{"CreateSchedule", func() error {
			_, err := client.CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{Namespace: ns, ScheduleId: id, RequestId: uuid.NewString(), Schedule: schedule})
			return err
		}},
		{"DescribeSchedule", func() error {
			_, err := client.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: ns, ScheduleId: id})
			return err
		}},
		{"DeleteSchedule", func() error {
			_, err := client.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: ns, ScheduleId: id})
			return err
		}},
		{"ListSchedules", func() error {
			_, err := client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: ns, MaximumPageSize: 1})
			return err
		}},
		{"CountSchedules", func() error {
			_, err := client.CountSchedules(ctx, &workflowservice.CountSchedulesRequest{Namespace: ns})
			return err
		}},
		{"UpdateSchedule", func() error {
			_, err := client.UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{Namespace: ns, ScheduleId: id, RequestId: uuid.NewString(), Schedule: schedule})
			return err
		}},
		{"PatchSchedule", func() error {
			_, err := client.PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{Namespace: ns, ScheduleId: id, RequestId: uuid.NewString(), Patch: &schedulepb.SchedulePatch{Pause: "denied"}})
			return err
		}},
		{"ListScheduleMatchingTimes", func() error {
			_, err := client.ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{Namespace: ns, ScheduleId: id, StartTime: timestamppb.Now(), EndTime: timestamppb.New(time.Now().Add(time.Hour))})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var denied *serviceerror.PermissionDenied
			require.ErrorAs(t, tc.call(), &denied)
			require.Zero(t, selected.calls.Load(), "denial must precede selected backend dispatch")
		})
	}
}
