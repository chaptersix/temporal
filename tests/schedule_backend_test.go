package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// scheduleBackend describes functional-test setup, not production rollout.
// Scheduler2 will get a descriptor only when its actual frontend wiring exists.
type scheduleBackend struct {
	name                         string
	context                      contextFactory
	worker                       bool
	preserveResultOnEmptySuccess bool
	sdkOptions                   []testcore.TestOption
	options                      []testcore.TestOption
}

var (
	scheduleV1Backend    = scheduleBackend{name: "v1", context: v1ContextFactory, worker: true, preserveResultOnEmptySuccess: true}
	scheduleCHASMBackend = scheduleBackend{name: "chasm", context: chasmContextFactory, sdkOptions: []testcore.TestOption{testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true)}}
)

func scheduleCommonOpts(t *testing.T, backends ...scheduleBackend) []testcore.TestOption {
	t.Helper()
	require.NotEmpty(t, backends, "schedule fixture must declare its backend(s)")
	opts := []testcore.TestOption{testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"})}
	worker := false
	for _, backend := range backends {
		require.NotEmpty(t, backend.name)
		require.NotNil(t, backend.context)
		worker = worker || backend.worker
		opts = append(opts, backend.options...)
	}
	if worker {
		opts = append(opts, testcore.WithWorkerService("V1 scheduler"))
	}
	return opts
}

func newScheduleEnv(t *testing.T, backends []scheduleBackend, opts ...testcore.TestOption) *testcore.TestEnv {
	t.Helper()
	opts = append(scheduleCommonOpts(t, backends...), opts...)
	env := testcore.NewEnv(t, opts...)
	// Copy the slice so caller reuse cannot silently alter cleanup routing.
	cleanupBackends := append([]scheduleBackend(nil), backends...)
	t.Cleanup(func() {
		for _, backend := range cleanupBackends {
			ctx, cancel := context.WithTimeout(backend.context(testcore.NewContext()), 30*time.Second)
			err := cleanupScheduleBackend(ctx, env.FrontendClient(), env.Namespace().String())
			cancel()
			if err != nil {
				if t.Failed() {
					t.Logf("schedule cleanup (%s) failed: %v", backend.name, err)
				} else {
					t.Errorf("schedule cleanup (%s) failed: %v", backend.name, err)
				}
			}
		}
	})
	return env
}

// Collect all pages before deletion so visibility changes do not disturb paging.
func cleanupScheduleBackend(ctx context.Context, client workflowservice.WorkflowServiceClient, namespace string) error {
	var ids []string
	var token []byte
	for {
		response, err := client.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: namespace, MaximumPageSize: 1000, NextPageToken: token})
		if err != nil {
			return fmt.Errorf("list schedules: %w", err)
		}
		for _, schedule := range response.GetSchedules() {
			ids = append(ids, schedule.GetScheduleId())
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}
	var result error
	for _, id := range ids {
		_, err := client.DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{Namespace: namespace, ScheduleId: id, Identity: "test cleanup"})
		_, notFound := errors.AsType[*serviceerror.NotFound](err)
		if err != nil && !notFound {
			result = errors.Join(result, fmt.Errorf("delete schedule %q: %w", id, err))
		}
	}
	return result
}

// Embed the unused methods; these tests can call only the two explicit overrides.
type scheduleCleanupClient struct {
	workflowservice.WorkflowServiceClient
	list   func(context.Context, *workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error)
	delete func(context.Context, *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error)
}

func (c scheduleCleanupClient) ListSchedules(ctx context.Context, r *workflowservice.ListSchedulesRequest, _ ...grpc.CallOption) (*workflowservice.ListSchedulesResponse, error) {
	return c.list(ctx, r)
}
func (c scheduleCleanupClient) DeleteSchedule(ctx context.Context, r *workflowservice.DeleteScheduleRequest, _ ...grpc.CallOption) (*workflowservice.DeleteScheduleResponse, error) {
	return c.delete(ctx, r)
}

func TestScheduleBackendDescriptorAndCleanup(t *testing.T) {
	t.Parallel()
	for _, backend := range []scheduleBackend{scheduleV1Backend, scheduleCHASMBackend} {
		t.Run("arbitrary_name_"+backend.name, func(t *testing.T) {
			require.Equal(t, backend.name == "v1", backend.worker)
			expectedHeader := ""
			if backend.name == "chasm" {
				expectedHeader = "chasm-scheduler"
			}
			checkContext := func(ctx context.Context) {
				md, _ := metadata.FromOutgoingContext(ctx)
				require.Equal(t, expectedHeader, firstScheduleHeader(md.Get(headers.ExperimentHeaderName)))
			}
			listed := 0
			var deleted []string
			client := scheduleCleanupClient{
				list: func(ctx context.Context, r *workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error) {
					checkContext(ctx)
					require.Equal(t, "namespace", r.Namespace)
					require.EqualValues(t, 1000, r.MaximumPageSize)
					listed++
					if listed == 1 {
						require.Empty(t, r.NextPageToken)
						return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "one"}}, NextPageToken: []byte("second")}, nil
					}
					require.Equal(t, []byte("second"), r.NextPageToken)
					return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "two"}}}, nil
				},
				delete: func(ctx context.Context, r *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
					checkContext(ctx)
					require.Equal(t, 2, listed)
					deleted = append(deleted, r.ScheduleId)
					return nil, serviceerror.NewNotFound("already gone")
				},
			}
			require.NoError(t, cleanupScheduleBackend(backend.context(t.Context()), client, "namespace"))
			require.Equal(t, []string{"one", "two"}, deleted)
		})
	}
}
func firstScheduleHeader(values []string) string {
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

func TestScheduleBackendCleanupFailureAndCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client := scheduleCleanupClient{list: func(ctx context.Context, _ *workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error) {
		return nil, ctx.Err()
	}, delete: func(context.Context, *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
		t.Fatal("delete after failed list")
		return nil, nil
	}}
	require.ErrorIs(t, cleanupScheduleBackend(ctx, client, "namespace"), context.Canceled)
	rejected := errors.New("delete rejected")
	var ids []string
	client.list = func(context.Context, *workflowservice.ListSchedulesRequest) (*workflowservice.ListSchedulesResponse, error) {
		return &workflowservice.ListSchedulesResponse{Schedules: []*schedulepb.ScheduleListEntry{{ScheduleId: "one"}, {ScheduleId: "two"}}}, nil
	}
	client.delete = func(_ context.Context, r *workflowservice.DeleteScheduleRequest) (*workflowservice.DeleteScheduleResponse, error) {
		ids = append(ids, r.ScheduleId)
		return nil, rejected
	}
	require.ErrorIs(t, cleanupScheduleBackend(t.Context(), client, "namespace"), rejected)
	require.Equal(t, []string{"one", "two"}, ids)
}

// The name deliberately does not select a backend; the descriptor must start
// the V1 worker required to execute this public trigger.
func TestScheduleDescriptorStartsLegacyWorker(t *testing.T) {
	testTriggerImmediatelyOnActiveSchedule(t, scheduleV1Backend)
}
