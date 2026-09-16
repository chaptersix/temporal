package frontend

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type migrationDeleteRaceClient struct {
	schedulerpb.SchedulerServiceClient
	engineCtx context.Context
	ref       chasm.ComponentRef
}

func (c *migrationDeleteRaceClient) DeleteSchedule(_ context.Context, req *schedulerpb.DeleteScheduleRequest, _ ...grpc.CallOption) (*schedulerpb.DeleteScheduleResponse, error) {
	response, _, err := chasm.UpdateComponent(c.engineCtx, c.ref, func(s *chasmscheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (*schedulerpb.DeleteScheduleResponse, error) {
		return s.Delete(ctx, req)
	}, struct{}{})
	return response, err
}

func TestMigrationDeleteNativeControl(t *testing.T) { testMigrationDeleteRace(t, false) }
func TestMigrationDeleteCounterexample(t *testing.T) {
	if os.Getenv("TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES") != "1" {
		t.Skip("set TEMPORAL_RUN_MIGRATION_COUNTEREXAMPLES=1")
	}
	testMigrationDeleteRace(t, true)
}

func testMigrationDeleteRace(t *testing.T, inFlight bool) {
	t.Helper()
	logger := log.NewNoopLogger()
	config := &chasmscheduler.Config{Tweakables: func(string) chasmscheduler.Tweakables { return chasmscheduler.DefaultTweakables }}
	builder := legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	processor := chasmscheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler, logger, builder)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(chasmscheduler.NewLibrary(config, nil,
		chasmscheduler.NewSchedulerIdleTaskHandler(chasmscheduler.SchedulerIdleTaskHandlerOptions{Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger}), nil,
		chasmscheduler.NewGeneratorTaskHandler(chasmscheduler.GeneratorTaskHandlerOptions{Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, SpecBuilder: builder, SpecProcessor: processor}), nil, nil, nil, nil)))
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(clock.NewEventTimeSource().Update(now)))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	key := chasm.ExecutionKey{NamespaceID: "ns-id", BusinessID: "schedule-id"}
	ref := chasm.NewComponentRef[*chasmscheduler.Scheduler](key)
	create := func() error {
		_, err := chasm.StartExecution(engineCtx, key, chasmscheduler.CreateSchedulerFromMigration, &schedulerpb.CreateFromMigrationStateRequest{NamespaceId: key.NamespaceID, State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{Namespace: "ns", NamespaceId: key.NamespaceID, ScheduleId: key.BusinessID, Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{Paused: true}}, Info: &schedulepb.ScheduleInfo{}},
			GeneratorState: &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now)}, InvokerState: &schedulerpb.InvokerState{},
		}})
		return err
	}
	started, release, created := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	if inFlight {
		go func() { close(started); <-release; created <- create() }()
		<-started
	} else {
		require.NoError(t, create())
	}
	ctrl := gomock.NewController(t)
	namespaces := namespace.NewMockRegistry(ctrl)
	namespaces.EXPECT().GetNamespaceID(namespace.Name("ns")).Return(namespace.ID("ns-id"), nil).Times(2)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)
	history.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
		require.Equal(t, legacyscheduler.WorkflowIDPrefix+key.BusinessID, req.TerminateRequest.WorkflowExecution.WorkflowId)
		return &historyservice.TerminateWorkflowExecutionResponse{}, nil
	}).Times(1)
	handler := &WorkflowHandler{logger: logger, namespaceRegistry: namespaces, historyClient: history, schedulerClient: &migrationDeleteRaceClient{engineCtx: engineCtx, ref: ref}, config: &Config{EnableSchedules: func(string) bool { return true }, EnableCHASMSchedulerCreation: func(string) bool { return true }}}
	_, deleteErr := handler.DeleteSchedule(context.Background(), &workflowservice.DeleteScheduleRequest{Namespace: "ns", ScheduleId: key.BusinessID})
	if inFlight {
		close(release)
		require.NoError(t, <-created)
	}
	require.NoError(t, deleteErr)
	_, err := chasm.ReadComponent(engineCtx, ref, func(s *chasmscheduler.Scheduler, _ chasm.Context, _ struct{}) (struct{}, error) {
		require.True(t, s.Closed, "seed=delete-create: successful DeleteSchedule must not be followed by an active migrated scheduler")
		return struct{}{}, nil
	}, struct{}{})
	if err != nil {
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
	}
}
