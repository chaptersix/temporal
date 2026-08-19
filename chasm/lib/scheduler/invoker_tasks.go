package scheduler

import (
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

type (
	InvokerTaskHandlerOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor

		HistoryClient resource.HistoryClient

		// FrontendClient is used for specifically StartWorkflow calls, to ensure that
		// the request makes it through metering's interceptor. Because we don't change for
		// terminate/cancels, we can go directly to history for other service calls.
		FrontendClient workflowservice.WorkflowServiceClient
	}

	InvokerProcessBufferTaskHandler struct {
		chasm.PureTaskHandlerBase
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		historyClient  resource.HistoryClient
		frontendClient workflowservice.WorkflowServiceClient
	}
)

const (
	// Lower bound for the deadline in which buffered actions are dropped.
	startWorkflowMinDeadline = 5 * time.Second
)

// Invoker task invalidation and buffered-start drop reasons. Limited cardinality
// for ReasonTag.
const (
	invokerProcessBufferInvalidatedStaleHWM metrics.ReasonString = "stale_hwm"
	bufferedStartDroppedMissedCatchup       metrics.ReasonString = "missed_catchup_window"
	bufferedStartDroppedPausedOrLimited     metrics.ReasonString = "paused_or_limited"
)

func NewInvokerProcessBufferTaskHandler(opts InvokerTaskHandlerOptions) *InvokerProcessBufferTaskHandler {
	return &InvokerProcessBufferTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
	}
}

func (h *InvokerProcessBufferTaskHandler) Validate(
	ctx chasm.Context,
	invoker *Invoker,
	attrs chasm.TaskInvocation,
	_ *schedulerpb.InvokerProcessBufferTask,
) (bool, error) {
	if invoker.Scheduler.Get(ctx).WorkflowMigration != nil {
		return false, nil
	}
	valid, err := validateTaskHighWaterMark(invoker.GetLastProcessedTime(), attrs.ScheduledTime)
	if err != nil {
		return false, err
	}
	if !valid {
		newTaggedMetricsHandler(h.metricsHandler, invoker.Scheduler.Get(ctx)).
			Counter(metrics.ScheduleInvokerProcessBufferTask.Name()).
			Record(1, metrics.OutcomeTag(outcomeInvalidated), metrics.ReasonTag(invokerProcessBufferInvalidatedStaleHWM))
	}
	return valid, nil
}

func (h *InvokerProcessBufferTaskHandler) Execute(
	ctx chasm.MutableContext,
	invoker *Invoker,
	_ chasm.TaskAttributes,
	_ *schedulerpb.InvokerProcessBufferTask,
) error {
	scheduler := invoker.Scheduler.Get(ctx)
	newTaggedMetricsHandler(h.metricsHandler, scheduler).
		Counter(metrics.ScheduleInvokerProcessBufferTask.Name()).
		Record(1, metrics.OutcomeTag(outcomeFired), metrics.ReasonTag(reasonNone))

	invoker.getOrCreateEventLog(ctx).LogEvent(ctx, "processBufferTask executed")

	// Make sure we have something to start.
	executionInfo := scheduler.Schedule.GetAction().GetStartWorkflow()
	if executionInfo == nil {
		return queueerrors.NewUnprocessableTaskError("schedules must have an Action set")
	}

	tweakables := h.config.Tweakables(scheduler.Namespace)
	snapshot := newBufferProcessingSnapshot(invoker, scheduler, catchupWindow(scheduler, tweakables))
	plan := schedulerinternal.PlanBufferProcessing(snapshot, ctx.Now(invoker))
	result := applyBufferPlan(ctx, scheduler, invoker, plan).result

	h.recordBufferProcessingMetrics(scheduler, result)
	return nil
}

func (h *InvokerProcessBufferTaskHandler) recordBufferProcessingMetrics(
	scheduler *Scheduler,
	result processBufferResult,
) {
	metricsHandler := newTaggedMetricsHandler(h.metricsHandler, scheduler)
	for _, reason := range result.bufferedStartDropReasons {
		metricsHandler.Counter(metrics.ScheduleBufferedStartDropped.Name()).
			Record(1, metrics.ReasonTag(reason))
	}
	for overlapPolicy, count := range result.overlapSkippedByPolicy {
		newTaggedMetricsHandler(h.metricsHandler, scheduler).WithTags(
			metrics.StringTag(metrics.ScheduleOverlapPolicyTag, overlapPolicy.String()),
		).Counter(metrics.ScheduleOverlapSkipped.Name()).Record(count)
	}
	for actionRunning, count := range result.missedCatchupByActionRunning {
		newTaggedMetricsHandler(h.metricsHandler, scheduler).WithTags(
			metrics.StringTag(metrics.ScheduleMissedReasonTag, metrics.ScheduleMissedReasonBufferExpired),
			metrics.StringTag(metrics.ScheduleActionRunningTag, fmt.Sprintf("%t", actionRunning)),
		).Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(count)
	}
}

// startWorkflowDeadline returns the latest time at which a buffered workflow
// should be started, instead of dropped. The deadline puts an upper bound on
// the number of retry attempts per buffered start.
func (h *InvokerProcessBufferTaskHandler) startWorkflowDeadline(
	ctx chasm.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
) time.Time {
	var timeout time.Duration

	if start.Manual {
		// For manual starts, use a default value in the future, as the catchup window
		// doesn't apply. Manual starts may only time out through max attempt count,
		// not deadline.
		return ctx.Now(scheduler).Add(time.Hour)
	}

	// Set request deadline based on the schedule's catchup window, which is the
	// latest time that it's acceptable to start this workflow.
	tweakables := h.config.Tweakables(scheduler.Namespace)
	timeout = catchupWindow(scheduler, tweakables)

	timeout = max(timeout, startWorkflowMinDeadline)

	return start.ActualTime.AsTime().Add(timeout)
}
