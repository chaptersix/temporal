package scheduler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

var schedulerPropertyStartTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

const schedulerPropertyMaxBufferSize = 16

type schedulerPropertyEnv struct {
	handler    schedulerpb.SchedulerServiceServer
	engine     *chasmtest.Engine
	engineCtx  context.Context
	ref        chasm.ComponentRef
	timeSource *clock.EventTimeSource
	services   schedulerServiceScripts
	delivered  []tasks.Task
	history    []string
}

type schedulerServiceScripts struct {
	Start rpctest.Script[
		*workflowservice.StartWorkflowExecutionRequest,
		*workflowservice.StartWorkflowExecutionResponse,
	]
	Describe rpctest.Script[
		*historyservice.DescribeWorkflowExecutionRequest,
		*historyservice.DescribeWorkflowExecutionResponse,
	]
	Cancel rpctest.Script[
		*historyservice.RequestCancelWorkflowExecutionRequest,
		*historyservice.RequestCancelWorkflowExecutionResponse,
	]
	Terminate rpctest.Script[
		*historyservice.TerminateWorkflowExecutionRequest,
		*historyservice.TerminateWorkflowExecutionResponse,
	]
	Migrate rpctest.Script[
		*historyservice.StartWorkflowExecutionRequest,
		*historyservice.StartWorkflowExecutionResponse,
	]
}

type schedulerPropertyTestingT interface {
	require.TestingT
	Helper()
}

type schedulerPropertyOwner interface {
	testlogger.TestingT
	Context() context.Context
}

func newSchedulerPropertyEnv(t schedulerPropertyOwner, initiallyPaused bool) *schedulerPropertyEnv {
	return newSchedulerPropertyEnvWithPolicy(t, initiallyPaused, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
}

func newSchedulerPropertyEnvWithPolicy(
	t schedulerPropertyOwner,
	initiallyPaused bool,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
) *schedulerPropertyEnv {
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
	env.services.Start.SetDefault("success", func(request *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
		return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-" + request.GetRequestId()}, nil
	})
	env.services.Describe.SetDefault("running", func(request *historyservice.DescribeWorkflowExecutionRequest) (*historyservice.DescribeWorkflowExecutionResponse, error) {
		return &historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: request.GetRequest().GetExecution(),
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		}, nil
	})
	env.services.Cancel.SetDefault(
		"success",
		rpctest.Return[*historyservice.RequestCancelWorkflowExecutionRequest](
			&historyservice.RequestCancelWorkflowExecutionResponse{},
		),
	)
	env.services.Terminate.SetDefault(
		"success",
		rpctest.Return[*historyservice.TerminateWorkflowExecutionRequest](
			&historyservice.TerminateWorkflowExecutionResponse{},
		),
	)
	env.services.Migrate.SetDefault("success", func(request *historyservice.StartWorkflowExecutionRequest) (*historyservice.StartWorkflowExecutionResponse, error) {
		return &historyservice.StartWorkflowExecutionResponse{RunId: "migration-" + request.GetStartRequest().GetRequestId()}, nil
	})
	frontend.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(env.services.Start.Handle).
		AnyTimes()
	history.EXPECT().
		DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(env.services.Describe.Handle).
		AnyTimes()
	history.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(env.services.Cancel.Handle).
		AnyTimes()
	history.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(env.services.Terminate.Handle).
		AnyTimes()
	history.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(env.services.Migrate.Handle).
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
			HistoryClient: history, SaMapperProvider: searchattribute.NewTestMapperProvider(nil),
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
		chasmtest.WithNamespaceEntry(tv.Namespace),
	)
	engineCtx := chasm.NewEngineContext(t.Context(), engine)
	schedule := proto.CloneOf(defaultSchedule())
	schedule.State.Paused = initiallyPaused
	schedule.Policies.OverlapPolicy = overlapPolicy
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
	return env
}

func schedulerPropertyConfig() *scheduler.Config {
	tweakables := scheduler.DefaultTweakables
	tweakables.MaxBufferSize = schedulerPropertyMaxBufferSize
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
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
			},
		},
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) backfill(
	t require.TestingT,
	start time.Time,
	end time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
) {
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime: timestamppb.New(start), EndTime: timestamppb.New(end), OverlapPolicy: overlapPolicy,
				}},
			},
		},
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) delete(t require.TestingT) {
	_, err := e.handler.DeleteSchedule(e.engineCtx, &schedulerpb.DeleteScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.DeleteScheduleRequest{
			Namespace: namespace, ScheduleId: scheduleID,
		},
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) complete(t require.TestingT, requestID string) {
	_, err := e.engine.UpdateComponent(e.engineCtx, e.ref, func(ctx chasm.MutableContext, component chasm.Component) error {
		return component.(*scheduler.Scheduler).HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: requestID,
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{},
			},
			CloseTime: timestamppb.New(e.timeSource.Now()),
		})
	})
	require.NoError(t, err)
}

func (e *schedulerPropertyEnv) deliverRunnable(t *rapid.T) {
	runnable, err := e.engine.RunnableTasks(e.ref)
	if err != nil {
		e.fail(t, "inspect runnable tasks", nil, err)
	}
	if len(runnable) == 0 {
		t.Skip("no runnable CHASM tasks")
	}
	task := rapid.SampledFrom(runnable).Draw(t, "runnable task")
	e.history = append(e.history, "deliver "+schedulerPropertyDescribeTask(task))
	if _, err := e.engine.ExecuteTask(t.Context(), e.ref, task); err != nil {
		e.fail(t, "deliver task", task, err)
	}
	e.delivered = append(e.delivered, task)
}

func (e *schedulerPropertyEnv) redeliver(t *rapid.T) {
	if len(e.delivered) == 0 {
		t.Skip("no previously delivered CHASM tasks")
	}
	task := rapid.SampledFrom(e.delivered).Draw(t, "delivered task")
	e.history = append(e.history, "redeliver "+schedulerPropertyDescribeTask(task))
	if _, err := e.engine.ExecuteTask(t.Context(), e.ref, task); err != nil {
		e.fail(t, "redeliver task", task, err)
	}
}

func (e *schedulerPropertyEnv) advanceToNextTask(t *rapid.T) {
	queued, err := e.engine.Tasks(e.ref)
	if err != nil {
		e.fail(t, "inspect queued tasks", nil, err)
	}
	now := e.timeSource.Now()
	var next time.Time
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			switch task.(type) {
			case *tasks.ChasmTask, *tasks.ChasmTaskPure:
			default:
				continue
			}
			visibilityTime := task.GetVisibilityTime()
			if !visibilityTime.After(now) || !next.IsZero() && !visibilityTime.Before(next) {
				continue
			}
			next = visibilityTime
		}
	}
	if next.IsZero() {
		t.Skip("no future CHASM tasks")
	}
	e.history = append(e.history, "advance time to "+next.Format(time.RFC3339Nano))
	e.timeSource.Update(next)
}

func (e *schedulerPropertyEnv) reloadExecution(t *rapid.T) {
	e.history = append(e.history, "reload execution")
	if err := e.engine.ReloadExecution(t.Context(), e.ref); err != nil {
		e.fail(t, "reload execution", nil, err)
	}
}

func (e *schedulerPropertyEnv) fail(t *rapid.T, operation string, task tasks.Task, err error) {
	t.Helper()
	t.Fatalf("%s failed: execution=%+v time=%s task=%s error=%v history=%v", operation, e.ref.ExecutionKey, e.timeSource.Now().Format(time.RFC3339Nano), schedulerPropertyDescribeTask(task), err, e.history)
}

func schedulerPropertyDescribeTask(task tasks.Task) string {
	if task == nil {
		return "<none>"
	}
	return fmt.Sprintf("%T(category=%s, visibility=%s)", task, task.GetCategory().Name(), task.GetVisibilityTime().Format(time.RFC3339Nano))
}

func (e *schedulerPropertyEnv) check(t schedulerPropertyTestingT, paused bool) {
	t.Helper()
	require.NoError(t, validateSchedulerSnapshot(e.snapshot(t), schedulerPropertyMaxBufferSize))
	beforeTasks, err := e.engine.Tasks(e.ref)
	require.NoError(t, err)
	beforeCalls := e.services.Start.Calls()
	description := e.describe(t)
	afterTasks, err := e.engine.Tasks(e.ref)
	require.NoError(t, err)
	require.Equal(t, beforeTasks, afterTasks, "Describe changed physical tasks")
	require.Equal(t, beforeCalls, e.services.Start.Calls(), "Describe made an external call")
	require.Equal(t, paused, description.GetSchedule().GetState().GetPaused())
	require.GreaterOrEqual(t, description.GetInfo().GetBufferSize(), int64(0))

	seen := make(map[string]struct{})
	for _, call := range e.services.Start.Calls() {
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

		t.Repeat(map[string]func(*rapid.T){
			"deliver":   env.deliverRunnable,
			"advance":   env.advanceToNextTask,
			"redeliver": env.redeliver,
			"reload":    env.reload,
			"pause": func(t *rapid.T) {
				if paused {
					t.Skip("already paused")
				}
				env.setPaused(t, true)
				paused = true
			},
			"unpause": func(t *rapid.T) {
				if !paused {
					t.Skip("already unpaused")
				}
				env.setPaused(t, false)
				paused = false
			},
			"trigger": func(t *rapid.T) {
				env.trigger(t)
			},
			"": func(t *rapid.T) {
				env.check(t, paused)
			},
		})
	})
}
