package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/chasmtest/rapidtest"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

var schedulerPropertyStartTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

type schedulerPropertyEnv struct {
	handler     schedulerpb.SchedulerServiceServer
	engine      *chasmtest.Engine
	engineCtx   context.Context
	execution   *rapidtest.Execution
	ref         chasm.ComponentRef
	timeSource  *clock.EventTimeSource
	startScript rpctest.Script[
		*workflowservice.StartWorkflowExecutionRequest,
		*workflowservice.StartWorkflowExecutionResponse,
	]
}

type schedulerPropertyTestingT interface {
	require.TestingT
	Helper()
}

func newSchedulerPropertyEnv(t *rapid.T, initiallyPaused bool) *schedulerPropertyEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	config := schedulerPropertyConfig()
	specBuilder := legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	specProcessor := scheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler, logger, specBuilder)
	frontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	history := historyservicemock.NewMockHistoryServiceClient(ctrl)
	handler := scheduler.NewTestHandler(logger)
	env := &schedulerPropertyEnv{handler: handler}
	env.startScript.SetDefault("success", func(request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-" + request.GetRequestId()}, nil
	})
	frontend.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(env.startScript.Handle).
		AnyTimes()

	invokerOptions := scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		HistoryClient:  history,
		FrontendClient: frontend,
	}
	library := scheduler.NewLibrary(
		config,
		handler,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config: config, HistoryClient: history, FrontendClient: frontend,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
			SpecProcessor: specProcessor, SpecBuilder: specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOptions),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOptions),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
			SpecProcessor: specProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
		}),
	)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(library))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(schedulerPropertyStartTime)
	tv := testvars.New(t).
		WithNamespaceID(namespacepkg.ID(namespaceID)).
		WithNamespaceName(namespacepkg.Name(namespace))
	engine := chasmtest.NewEngine(
		t,
		registry,
		chasmtest.WithTimeSource(timeSource),
		chasmtest.WithNodeBackendDecorator(func(backend *chasm.MockNodeBackend) {
			backend.HandleGetNamespaceEntry = tv.Namespace
		}),
	)
	engineCtx := chasm.NewEngineContext(t.Context(), engine)
	schedule := proto.CloneOf(defaultSchedule())
	schedule.State.Paused = initiallyPaused
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	schedule.Action.GetStartWorkflow().TaskQueue = &taskqueuepb.TaskQueue{Name: "property-task-queue"}
	schedule.Action.GetStartWorkflow().Input = &commonpb.Payloads{}
	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID, RequestId: "property-create", Schedule: schedule,
		},
	})
	require.NoError(t, err)

	ref := chasm.ComponentRef{ExecutionKey: chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	}}
	env.engine = engine
	env.engineCtx = engineCtx
	env.ref = ref
	env.timeSource = timeSource
	env.execution = &rapidtest.Execution{Engine: engine, Ref: ref, TimeSource: timeSource}
	return env
}

func schedulerPropertyConfig() *scheduler.Config {
	tweakables := scheduler.DefaultTweakables
	tweakables.MaxBufferSize = 8
	tweakables.GeneratorBufferReserveSize = 0
	tweakables.MaxActionsPerExecution = 2
	tweakables.IdleTime = 10 * time.Minute
	config := defaultConfig()
	config.Tweakables = func(string) scheduler.Tweakables { return tweakables }
	return config
}

func (e *schedulerPropertyEnv) describe(t require.TestingT) *workflowservice.DescribeScheduleResponse {
	response, err := e.handler.DescribeSchedule(e.engineCtx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
		},
	})
	require.NoError(t, err)
	return response.GetFrontendResponse()
}

func (e *schedulerPropertyEnv) setPaused(t require.TestingT, paused bool) {
	patch := &schedulepb.SchedulePatch{Pause: "property pause"}
	if !paused {
		patch = &schedulepb.SchedulePatch{Unpause: "property unpause"}
	}
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID, Patch: patch,
		},
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) trigger(t require.TestingT) {
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) check(t schedulerPropertyTestingT, paused bool) {
	t.Helper()
	beforeTasks, err := e.engine.Tasks(e.ref)
	require.NoError(t, err)
	beforeCalls := e.startScript.Calls()
	description := e.describe(t)
	afterTasks, err := e.engine.Tasks(e.ref)
	require.NoError(t, err)
	require.Equal(t, beforeTasks, afterTasks, "Describe changed physical tasks")
	require.Equal(t, beforeCalls, e.startScript.Calls(), "Describe made an external call")
	require.Equal(t, paused, description.GetSchedule().GetState().GetPaused())
	require.GreaterOrEqual(t, description.GetInfo().GetBufferSize(), int64(0))

	seen := make(map[string]struct{})
	for _, call := range e.startScript.Calls() {
		requestID := call.Request.GetRequestId()
		require.NotEmpty(t, requestID)
		_, exists := seen[requestID]
		require.False(t, exists, "duplicate successful start request ID %q", requestID)
		seen[requestID] = struct{}{}
		require.Equal(t, "run-"+requestID, call.Response.GetRunId())
	}
}

func TestSchedulerPropertyEnvironment(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		paused := rapid.Bool().Draw(t, "initially-paused")
		env := newSchedulerPropertyEnv(t, paused)
		env.check(t, paused)

		t.Repeat(rapidtest.ActionMap(
			rapidtest.Action{Name: "deliver", Weight: 4, Run: env.execution.DeliverRunnable},
			rapidtest.Action{Name: "advance", Weight: 2, Run: env.execution.AdvanceToNextTask},
			rapidtest.Action{Name: "redeliver", Weight: 1, Run: env.execution.Redeliver},
			rapidtest.Action{Name: "reload", Weight: 1, Run: env.execution.Reload},
			rapidtest.Action{Name: "pause", Weight: 1, Run: func(t *rapid.T) {
				if paused {
					t.Skip("already paused")
				}
				env.setPaused(t, true)
				paused = true
			}},
			rapidtest.Action{Name: "unpause", Weight: 1, Run: func(t *rapid.T) {
				if !paused {
					t.Skip("already unpaused")
				}
				env.setPaused(t, false)
				paused = false
			}},
			rapidtest.Action{Name: "trigger", Weight: 1, Run: func(t *rapid.T) {
				env.trigger(t)
			}},
			rapidtest.Action{Name: "", Run: func(t *rapid.T) {
				env.check(t, paused)
			}},
		))
	})
}
