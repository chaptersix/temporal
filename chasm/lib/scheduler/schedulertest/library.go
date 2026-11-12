// Package schedulertest provides a lifecycle-driving harness for the CHASM
// scheduler. Unlike the per-handler unit tests in package scheduler_test, this
// package builds a real component on top of [chasmtest.Engine] and steps it
// forward through every task it schedules for itself, so that invariants can be
// asserted across sequences of transitions (e.g. "the scheduler never gets
// stuck without a task to drive the next state").
//
// It deliberately lives in a non-test package so that property tests
// (pgregory.net/rapid), the V1<->V2 migration round-trip test and the unit tests
// in package scheduler_test can all import it. It is the single home of the
// scheduler fixtures (default schedule and config, spec processor, library
// wiring) and of [SchedulerEngine], the typed wrapper around a chasmtest engine
// that helper_test.go and [Driver] are both built on.
package schedulertest

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	Namespace   = "ns"
	NamespaceID = "ns-id"
	ScheduleID  = "sched-id"

	DefaultInterval      = 1 * time.Minute
	DefaultCatchupWindow = 5 * time.Minute
)

// DefaultSchedule returns a schedule with a single 1-minute interval and a
// 5-minute catchup window, matching the package's other defaults.
func DefaultSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(DefaultInterval),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "scheduled-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			CatchupWindow: durationpb.New(DefaultCatchupWindow),
		},
		State: &schedulepb.ScheduleState{
			Paused:           false,
			LimitedActions:   false,
			RemainingActions: 0,
		},
	}
}

// DefaultConfig returns a scheduler config wired with the package defaults.
func DefaultConfig() *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			return scheduler.DefaultTweakables
		},
		ServiceCallTimeout: func() time.Duration {
			return 5 * time.Second
		},
		EncodeInternalTokenWithEnvelope: func(string) bool {
			return true
		},
		RetryPolicy: func() backoff.RetryPolicy { return deterministicRetryPolicy{} },
	}
}

// deterministicRetryPolicy backs off exponentially from one second to a minute
// like the production policy but without its random jitter, so that a rapid
// run replays identically while it shrinks a failure.
type deterministicRetryPolicy struct{}

func (deterministicRetryPolicy) ComputeNextDelay(_ time.Duration, numAttempts int, _ error) time.Duration {
	return min(time.Second<<min(max(numAttempts-1, 0), 6), time.Minute)
}

var _ backoff.RetryPolicy = deterministicRetryPolicy{}

// NewSpecBuilder returns a legacy SpecBuilder with the given warn/max compute-limit
// bounds. A value of 0 means "use the default": GetNextTime treats a non-positive
// warn bound as its default and a non-positive max bound as unlimited.
func NewSpecBuilder(warnIter, maxIter int) *legacyscheduler.SpecBuilder {
	return legacyscheduler.NewSpecBuilder(func() int { return warnIter }, func() int { return maxIter })
}

// DefaultSpecBuilder returns a SpecBuilder bounded by the production dynamic
// config defaults.
func DefaultSpecBuilder() *legacyscheduler.SpecBuilder {
	return legacyscheduler.NewSpecBuilder(
		dynamicconfig.SchedulerSpecWarnIterations.Get(dynamicconfig.NewNoopCollection()),
		dynamicconfig.SchedulerSpecMaxIterations.Get(dynamicconfig.NewNoopCollection()),
	)
}

// NewSpecProcessor builds a real SpecProcessor over builder with no-op metrics,
// matching production spec processing so generated buffered starts and
// next-times are realistic.
func NewSpecProcessor(ctrl *gomock.Controller, logger log.Logger, builder *legacyscheduler.SpecBuilder) scheduler.SpecProcessor {
	mockMetrics := metrics.NewMockHandler(ctrl)
	mockMetrics.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	mockMetrics.EXPECT().WithTags(gomock.Any()).Return(mockMetrics).AnyTimes()
	mockMetrics.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	return scheduler.NewSpecProcessor(DefaultConfig(), mockMetrics, logger, builder)
}

// NewRealSpecProcessor is NewSpecProcessor over [DefaultSpecBuilder].
func NewRealSpecProcessor(ctrl *gomock.Controller, logger log.Logger) scheduler.SpecProcessor {
	return NewSpecProcessor(ctrl, logger, DefaultSpecBuilder())
}

// LibraryOptions wires a scheduler Library. Zero values select the defaults
// noted on each field.
type LibraryOptions struct {
	// Config defaults to [DefaultConfig].
	Config *scheduler.Config
	Logger log.Logger
	// MetricsHandler is given to the invoker task handlers; the other handlers
	// always use the no-op handler. Defaults to the no-op handler.
	MetricsHandler metrics.Handler
	// TimeSource stamps StartTime in the invoker. Nil leaves the handler's
	// default (real time).
	TimeSource    clock.TimeSource
	SpecProcessor scheduler.SpecProcessor
	// SpecBuilder defaults to [DefaultSpecBuilder].
	SpecBuilder *legacyscheduler.SpecBuilder
	// FrontendClient and HistoryClient back the side-effect task handlers
	// (execute/callbacks/migrate). Nil unless the test exercises those paths.
	FrontendClient workflowservice.WorkflowServiceClient
	HistoryClient  resource.HistoryClient
}

// NewTestLibrary builds a scheduler Library with all task handlers wired to the
// supplied options.
func NewTestLibrary(o LibraryOptions) *scheduler.Library {
	config := o.Config
	if config == nil {
		config = DefaultConfig()
	}
	invokerMetrics := o.MetricsHandler
	if invokerMetrics == nil {
		invokerMetrics = metrics.NoopMetricsHandler
	}
	specBuilder := o.SpecBuilder
	if specBuilder == nil {
		specBuilder = DefaultSpecBuilder()
	}
	logger := o.Logger
	invokerOpts := scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: invokerMetrics,
		BaseLogger:     logger,
		SpecProcessor:  o.SpecProcessor,
		HistoryClient:  o.HistoryClient,
		FrontendClient: o.FrontendClient,
		TimeSource:     o.TimeSource,
	}
	return scheduler.NewLibrary(
		config,
		nil,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			HistoryClient:  o.HistoryClient,
			FrontendClient: o.FrontendClient,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  o.SpecProcessor,
			SpecBuilder:    specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOpts),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  o.SpecProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config:           config,
			MetricsHandler:   metrics.NoopMetricsHandler,
			BaseLogger:       logger,
			HistoryClient:    o.HistoryClient,
			SaMapperProvider: searchattribute.NewTestMapperProvider(nil),
		}),
	)
}
