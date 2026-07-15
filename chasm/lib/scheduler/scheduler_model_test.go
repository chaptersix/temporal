package scheduler_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	namespacepkg "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

const modelDrainLimit = 1000

type (
	modelWorkflowService struct {
		workflowservice.WorkflowServiceClient

		mu         sync.Mutex
		starts     map[string]*workflowservice.StartWorkflowExecutionRequest
		startCalls []string
	}

	schedulerModelWorkflow struct {
		workflowID string
		runID      string
		callback   *commonpb.Callback
	}

	schedulerModel struct {
		paused          bool
		activeIntervals int
		manualTriggers  int
		running         map[string]schedulerModelWorkflow
		completed       []schedulerModelWorkflow
	}

	schedulerModelEnv struct {
		handler     schedulerpb.SchedulerServiceServer
		engine      *chasmtest.Engine
		engineCtx   context.Context
		ref         chasm.ComponentRef
		timeSource  *clock.EventTimeSource
		workflows   *modelWorkflowService
		model       schedulerModel
		completionN int
	}
)

func TestSchedulerModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		env := newSchedulerModelEnv(t)
		t.Repeat(map[string]func(*rapid.T){
			"advance time":        env.advanceTime,
			"complete workflow":   env.completeWorkflow,
			"pause or unpause":    env.patchPaused,
			"trigger immediately": env.triggerImmediately,
			"":                    env.check,
		})
	})
}

func (s *modelWorkflowService) StartWorkflowExecution(
	_ context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.startCalls = append(s.startCalls, request.RequestId)
	if _, exists := s.starts[request.RequestId]; !exists {
		s.starts[request.RequestId] = proto.CloneOf(request)
	}
	return &workflowservice.StartWorkflowExecutionResponse{
		RunId: modelRunID(request.RequestId),
	}, nil
}

func (s *modelWorkflowService) snapshot() (map[string]*workflowservice.StartWorkflowExecutionRequest, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	starts := make(map[string]*workflowservice.StartWorkflowExecutionRequest, len(s.starts))
	for requestID, request := range s.starts {
		starts[requestID] = proto.CloneOf(request)
	}
	return starts, slices.Clone(s.startCalls)
}

func newSchedulerModelEnv(t *rapid.T) *schedulerModelEnv {
	t.Helper()

	startTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(startTime)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	workflows := &modelWorkflowService{
		starts: make(map[string]*workflowservice.StartWorkflowExecutionRequest),
	}
	config := modelSchedulerConfig()
	specBuilder := legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	specProcessor := scheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler, logger, specBuilder)
	handler := scheduler.NewTestHandler(logger)
	invokerOpts := scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		FrontendClient: workflows,
	}
	library := scheduler.NewLibrary(
		config,
		handler,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config:         config,
			FrontendClient: workflows,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
			SpecBuilder:    specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOpts),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		}),
	)
	registry := chasm.NewRegistry(logger)
	mustNoError(t, registry.Register(&chasm.CoreLibrary{}))
	mustNoError(t, registry.Register(library))

	namespaceEntry := namespacepkg.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: namespace},
		nil,
		"active",
	)
	engine := chasmtest.NewEngine(
		t,
		registry,
		chasmtest.WithTimeSource(timeSource),
		chasmtest.WithNodeBackendDecorator(func(backend *chasm.MockNodeBackend) {
			backend.HandleGetNamespaceEntry = func() *namespacepkg.Namespace { return namespaceEntry }
		}),
	)
	engineCtx := chasm.NewEngineContext(t.Context(), engine)

	schedule := defaultSchedule()
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	schedule.Policies.CatchupWindow = durationpb.New(24 * time.Hour)
	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			RequestId:  "create-request",
			Schedule:   schedule,
		},
	})
	mustNoError(t, err)

	return &schedulerModelEnv{
		handler:    handler,
		engine:     engine,
		engineCtx:  engineCtx,
		ref:        chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID}),
		timeSource: timeSource,
		workflows:  workflows,
		model: schedulerModel{
			running: make(map[string]schedulerModelWorkflow),
		},
	}
}

func modelSchedulerConfig() *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(string) scheduler.Tweakables {
			tweakables := scheduler.DefaultTweakables
			tweakables.IdleTime = 365 * 24 * time.Hour
			tweakables.MaxActionsPerExecution = 100
			return tweakables
		},
		ServiceCallTimeout: func() time.Duration { return 5 * time.Second },
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(time.Second)
		},
		EncodeInternalTokenWithEnvelope: func(string) bool { return true },
	}
}

func (e *schedulerModelEnv) advanceTime(t *rapid.T) {
	intervals := rapid.IntRange(1, 3).Draw(t, "intervals")
	expectedStarts := 0
	if !e.model.paused {
		e.model.activeIntervals += intervals
		expectedStarts = intervals
	}
	e.timeSource.Update(e.timeSource.Now().Add(time.Duration(intervals) * defaultInterval))
	e.drainAndRecordStarts(t, expectedStarts)
}

func (e *schedulerModelEnv) patchPaused(t *rapid.T) {
	paused := rapid.Bool().Draw(t, "paused")
	patch := &schedulepb.SchedulePatch{}
	if paused {
		patch.Pause = "paused by model"
	} else {
		patch.Unpause = "unpaused by model"
	}
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch:      patch,
		},
	})
	mustNoError(t, err)
	e.model.paused = paused
	e.drainAndRecordStarts(t, 0)
}

func (e *schedulerModelEnv) triggerImmediately(t *rapid.T) {
	_, err := e.handler.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
	})
	mustNoError(t, err)
	e.model.manualTriggers++
	e.drainAndRecordStarts(t, 1)
}

func (e *schedulerModelEnv) completeWorkflow(t *rapid.T) {
	requestIDs := make([]string, 0, len(e.model.running))
	for requestID := range e.model.running {
		requestIDs = append(requestIDs, requestID)
	}
	slices.Sort(requestIDs)
	if len(requestIDs) == 0 {
		t.Skip("no running workflows")
	}
	requestID := requestIDs[rapid.IntRange(0, len(requestIDs)-1).Draw(t, "workflow")]
	workflow := e.model.running[requestID]

	callback := workflow.callback.GetNexus()
	if callback == nil || len(callback.Header) != 1 {
		t.Fatalf("request %q has invalid completion callback: %v", requestID, workflow.callback)
	}
	var callbackToken string
	for _, value := range callback.Header {
		callbackToken = value
	}
	componentRef, callbackRequestID, err := chasm.UnpackNexusCallbackToken(callbackToken)
	mustNoError(t, err)
	if callbackRequestID != requestID {
		t.Fatalf("callback request ID mismatch: got %q, want %q", callbackRequestID, requestID)
	}

	e.completionN++
	closeTime := e.timeSource.Now().Add(time.Duration(e.completionN) * time.Nanosecond)
	_, _, err = chasm.UpdateComponent(
		e.engineCtx,
		componentRef,
		func(
			component chasm.NexusCompletionHandler,
			ctx chasm.MutableContext,
			completion *persistencespb.ChasmNexusCompletion,
		) (chasm.NoValue, error) {
			return nil, component.HandleNexusCompletion(ctx, completion)
		},
		&persistencespb.ChasmNexusCompletion{
			RequestId: requestID,
			CloseTime: timestamppb.New(closeTime),
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("completed")},
			},
		},
	)
	mustNoError(t, err)

	delete(e.model.running, requestID)
	e.model.completed = append(e.model.completed, workflow)
	if len(e.model.completed) > 10 {
		e.model.completed = e.model.completed[len(e.model.completed)-10:]
	}
	e.drainAndRecordStarts(t, 0)
}

func (e *schedulerModelEnv) drainAndRecordStarts(t *rapid.T, expected int) {
	before, _ := e.workflows.snapshot()
	_, err := e.engine.DrainTasks(e.engineCtx, e.ref, modelDrainLimit)
	mustNoError(t, err)
	after, _ := e.workflows.snapshot()

	newRequestIDs := make([]string, 0, len(after)-len(before))
	for requestID := range after {
		if _, existed := before[requestID]; !existed {
			newRequestIDs = append(newRequestIDs, requestID)
		}
	}
	if len(newRequestIDs) != expected {
		t.Fatalf("new workflow starts: got %d, want %d", len(newRequestIDs), expected)
	}
	for _, requestID := range newRequestIDs {
		request := after[requestID]
		if len(request.CompletionCallbacks) != 1 {
			t.Fatalf("request %q has %d completion callbacks, want 1", requestID, len(request.CompletionCallbacks))
		}
		e.model.running[requestID] = schedulerModelWorkflow{
			workflowID: request.WorkflowId,
			runID:      modelRunID(requestID),
			callback:   request.CompletionCallbacks[0],
		}
	}
}

func (e *schedulerModelEnv) check(t *rapid.T) {
	response, err := e.handler.DescribeSchedule(e.engineCtx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		},
	})
	mustNoError(t, err)
	description := response.GetFrontendResponse()

	if actual := description.GetSchedule().GetState().GetPaused(); actual != e.model.paused {
		t.Fatalf("paused: got %v, want %v", actual, e.model.paused)
	}
	expectedActions := e.model.activeIntervals + e.model.manualTriggers
	if actual := description.GetInfo().GetActionCount(); actual != int64(expectedActions) {
		t.Fatalf("action count: got %d, want %d", actual, expectedActions)
	}

	expectedRunning := make([]string, 0, len(e.model.running))
	for _, workflow := range e.model.running {
		expectedRunning = append(expectedRunning, workflow.workflowID+"/"+workflow.runID)
	}
	actualRunning := make([]string, 0, len(description.GetInfo().GetRunningWorkflows()))
	for _, workflow := range description.GetInfo().GetRunningWorkflows() {
		actualRunning = append(actualRunning, workflow.WorkflowId+"/"+workflow.RunId)
	}
	slices.Sort(expectedRunning)
	slices.Sort(actualRunning)
	if !slices.Equal(actualRunning, expectedRunning) {
		t.Fatalf("running workflow run IDs: got %v, want %v", actualRunning, expectedRunning)
	}

	expectedRecent := make([]string, 0, len(e.model.running)+len(e.model.completed))
	for _, workflow := range e.model.running {
		expectedRecent = append(expectedRecent, workflow.runID)
	}
	for _, workflow := range e.model.completed {
		expectedRecent = append(expectedRecent, workflow.runID)
	}
	actualRecent := make([]string, 0, len(description.GetInfo().GetRecentActions()))
	for _, action := range description.GetInfo().GetRecentActions() {
		actualRecent = append(actualRecent, action.GetStartWorkflowResult().GetRunId())
	}
	slices.Sort(expectedRecent)
	slices.Sort(actualRecent)
	if !slices.Equal(actualRecent, expectedRecent) {
		t.Fatalf("recent workflow run IDs: got %v, want %v", actualRecent, expectedRecent)
	}

	starts, calls := e.workflows.snapshot()
	if len(starts) != expectedActions {
		t.Fatalf("unique request IDs: got %d, want %d", len(starts), expectedActions)
	}
	seen := make(map[string]struct{}, len(calls))
	for _, requestID := range calls {
		if _, duplicate := seen[requestID]; duplicate {
			t.Fatalf("duplicate StartWorkflowExecution request ID %q", requestID)
		}
		seen[requestID] = struct{}{}
	}

	runnable, err := e.engine.RunnableTasks(e.ref)
	mustNoError(t, err)
	if len(runnable) != 0 {
		t.Fatalf("found %d runnable tasks after drain; next is %T", len(runnable), runnable[0])
	}
}

func modelRunID(requestID string) string {
	return fmt.Sprintf("run-%s", requestID)
}

func mustNoError(t *rapid.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
