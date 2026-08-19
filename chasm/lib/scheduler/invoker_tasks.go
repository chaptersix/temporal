package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	InvokerExecuteTaskHandler struct {
		chasm.SideEffectTaskHandlerBase[*schedulerpb.InvokerExecuteTask]
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		historyClient  resource.HistoryClient
		frontendClient workflowservice.WorkflowServiceClient
	}

	InvokerProcessBufferTaskHandler struct {
		chasm.PureTaskHandlerBase
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		historyClient  resource.HistoryClient
		frontendClient workflowservice.WorkflowServiceClient
	}

	rateLimitedError struct {
		// The requested interval to delay processing by rescheduilng.
		delay time.Duration
	}
)

const (
	// Lower bound for the deadline in which buffered actions are dropped.
	startWorkflowMinDeadline = 5 * time.Second

	// InvokerMaxStartAttempts is the maximum number of StartWorkflowExecution
	// RPCs issued for an individual buffered action, counting the first call.
	// Attempt numbers are 1-based: recordProcessBufferResult readies a start at
	// Attempt 1, and each retryable failure increments it. The bound is
	// therefore inclusive - a start is refused locally (and dropped) only once
	// its Attempt exceeds this value - so a value of 10 permits ten start RPCs,
	// i.e. the initial call plus nine retries.
	InvokerMaxStartAttempts = 10 // TODO - dial this up/remove it
)

var (
	errRetryLimitExceeded       = queueerrors.NewUnprocessableTaskError("retry limit exceeded")
	_                     error = &rateLimitedError{}
)

// Invoker task invalidation and buffered-start drop reasons. Limited cardinality
// for ReasonTag.
const (
	invokerProcessBufferInvalidatedStaleHWM   metrics.ReasonString = "stale_hwm"
	invokerExecuteInvalidatedMigrationPending metrics.ReasonString = "migration_pending"
	invokerExecuteInvalidatedNoWork           metrics.ReasonString = "no_work"
	invokerExecuteInvalidatedAlreadyRecorded  metrics.ReasonString = "already_recorded"
	invokerExecuteInvalidatedStateChanged     metrics.ReasonString = "state_changed"
	bufferedStartDroppedMissedCatchup         metrics.ReasonString = "missed_catchup_window"
	bufferedStartDroppedPausedOrLimited       metrics.ReasonString = "paused_or_limited"
)

type invokerExecuteTaskValidity struct {
	valid  bool
	reason metrics.ReasonString
}

func evaluateInvokerExecuteTaskValidity(invoker *Invoker, scheduler *Scheduler) invokerExecuteTaskValidity {
	if scheduler.WorkflowMigration != nil {
		return invokerExecuteTaskValidity{reason: invokerExecuteInvalidatedMigrationPending}
	}
	if len(invoker.GetTerminateWorkflows())+len(invoker.GetCancelWorkflows())+len(invoker.getEligibleBufferedStarts()) == 0 {
		return invokerExecuteTaskValidity{reason: invokerExecuteInvalidatedNoWork}
	}
	return invokerExecuteTaskValidity{valid: true, reason: reasonNone}
}

func NewInvokerExecuteTaskHandler(opts InvokerTaskHandlerOptions) *InvokerExecuteTaskHandler {
	return &InvokerExecuteTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
	}
}

func NewInvokerProcessBufferTaskHandler(opts InvokerTaskHandlerOptions) *InvokerProcessBufferTaskHandler {
	return &InvokerProcessBufferTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
	}
}

func (h *InvokerExecuteTaskHandler) recordExecutionInvalidations(
	scheduler *Scheduler,
	outcome executionCommitOutcome,
) {
	metricsHandler := newTaggedMetricsHandler(h.metricsHandler, scheduler)
	if outcome.duplicateInvalidations > 0 {
		metricsHandler.Counter(metrics.ScheduleInvokerExecuteTask.Name()).Record(
			int64(outcome.duplicateInvalidations),
			metrics.OutcomeTag(outcomeInvalidated),
			metrics.ReasonTag(invokerExecuteInvalidatedAlreadyRecorded),
		)
		newTaggedLogger(h.baseLogger, scheduler).Debug(
			"duplicate ExecuteTask result invalidated",
			tag.NewInt("count", outcome.duplicateInvalidations))
	}
	if outcome.stateChangedInvalidations > 0 {
		metricsHandler.Counter(metrics.ScheduleInvokerExecuteTask.Name()).Record(
			int64(outcome.stateChangedInvalidations),
			metrics.OutcomeTag(outcomeInvalidated),
			metrics.ReasonTag(invokerExecuteInvalidatedStateChanged),
		)
	}
}

func (h *InvokerExecuteTaskHandler) Validate(
	ctx chasm.Context,
	invoker *Invoker,
	_ chasm.TaskInvocation,
	_ *schedulerpb.InvokerExecuteTask,
) (bool, error) {
	scheduler := invoker.Scheduler.Get(ctx)
	validity := evaluateInvokerExecuteTaskValidity(invoker, scheduler)
	if validity.valid || validity.reason == invokerExecuteInvalidatedMigrationPending {
		return validity.valid, nil
	}
	newTaggedMetricsHandler(h.metricsHandler, scheduler).
		Counter(metrics.ScheduleInvokerExecuteTask.Name()).
		Record(1, metrics.OutcomeTag(outcomeInvalidated), metrics.ReasonTag(validity.reason))
	return false, nil
}

type loadedWorkflowExecution struct {
	index    int
	expected *commonpb.WorkflowExecution
}

type loadedBufferedStart struct {
	index    int
	expected *schedulespb.BufferedStart
}

type executionBatch struct {
	scheduler           *Scheduler
	lastCompletionState *schedulerpb.LastCompletionResult
	schedulerRef        []byte
	now                 time.Time
	maxActions          int
	terminations        []loadedWorkflowExecution
	cancellations       []loadedWorkflowExecution
	starts              []loadedBufferedStart
}

type startExecutionOutcome int

const (
	startExecutionCompleted startExecutionOutcome = iota
	startExecutionRetryable
	startExecutionFailed
)

type startExecutionResult struct {
	loaded      loadedBufferedStart
	outcome     startExecutionOutcome
	runID       string
	startTime   *timestamppb.Timestamp
	backoffTime *timestamppb.Timestamp
}

type executionBatchResult struct {
	terminations  []loadedWorkflowExecution
	cancellations []loadedWorkflowExecution
	starts        []startExecutionResult
}

func (h *InvokerExecuteTaskHandler) Execute(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.InvokerExecuteTask,
) error {
	batch, err := h.loadExecutionBatch(ctx, invokerRef)
	if err != nil {
		return err
	}
	result := h.executeBatch(ctx, batch)
	_, err = h.commitExecutionResult(ctx, invokerRef, result)
	return err
}

func (h *InvokerExecuteTaskHandler) loadExecutionBatch(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
) (executionBatch, error) {
	var batch executionBatch
	_, err := chasm.ReadComponent(
		ctx,
		invokerRef,
		func(i *Invoker, ctx chasm.Context, _ any) (struct{}, error) {
			s := i.Scheduler.Get(ctx)
			batch.scheduler = &Scheduler{
				SchedulerState: common.CloneProto(s.SchedulerState),
			}
			batch.lastCompletionState = common.CloneProto(s.LastCompletionResult.Get(ctx))

			// Capture the scheduler's component ref so a per-start completion callback (carrying that
			// start's request ID in its token) can be built outside the MS lock.
			ref, err := ctx.Ref(s)
			if err != nil {
				return struct{}{}, err
			}
			batch.schedulerRef = slices.Clone(ref)
			batch.now = ctx.Now(i)
			batch.maxActions = h.maxActionsPerExecution(batch.scheduler)

			// Position plus exact state is the commit identity. Request IDs are
			// not unique for compatibility with existing schedules.
			for index, target := range i.GetTerminateWorkflows() {
				batch.terminations = append(batch.terminations, loadedWorkflowExecution{index: index, expected: common.CloneProto(target)})
			}
			for index, target := range i.GetCancelWorkflows() {
				batch.cancellations = append(batch.cancellations, loadedWorkflowExecution{index: index, expected: common.CloneProto(target)})
			}
			lastProcessed := i.GetLastProcessedTime().AsTime()
			for index, start := range i.GetBufferedStarts() {
				if start.GetAttempt() > 0 && start.GetRunId() == "" && !start.GetBackoffTime().AsTime().After(lastProcessed) {
					batch.starts = append(batch.starts, loadedBufferedStart{index: index, expected: common.CloneProto(start)})
				}
			}

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return executionBatch{}, fmt.Errorf("failed to read component: %w", err)
	}
	if batch.scheduler == nil {
		return executionBatch{}, errors.New("scheduler component was nil after read")
	}
	return batch, nil
}

func (h *InvokerExecuteTaskHandler) executeBatch(ctx context.Context, batch executionBatch) executionBatchResult {
	logger := newTaggedLogger(h.baseLogger, batch.scheduler)
	metricsHandler := newTaggedMetricsHandler(h.metricsHandler, batch.scheduler)
	metricsHandler.Counter(metrics.ScheduleInvokerExecuteTask.Name()).Record(1, metrics.OutcomeTag(outcomeFired), metrics.ReasonTag(reasonNone))

	actionsTaken := 0
	result := executionBatchResult{}
	result.terminations = h.terminateWorkflows(ctx, logger, metricsHandler, batch.scheduler, batch.terminations, &actionsTaken, batch.maxActions)
	result.cancellations = h.cancelWorkflows(ctx, logger, metricsHandler, batch.scheduler, batch.cancellations, &actionsTaken, batch.maxActions)
	result.starts = h.startWorkflows(ctx, logger, metricsHandler, batch, &actionsTaken)
	return result
}

func (h *InvokerExecuteTaskHandler) commitExecutionResult(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
	result executionBatchResult,
) (executionCommitOutcome, error) {
	var outcome executionCommitOutcome
	_, _, err := chasm.UpdateComponent(
		ctx,
		invokerRef,
		func(i *Invoker, ctx chasm.MutableContext, _ any) (chasm.NoValue, error) {
			s := i.Scheduler.Get(ctx)
			outcome = i.commitExecutionResult(ctx, result)
			s.recordActionResult(&schedulerActionResult{actionCount: int64(outcome.appliedStarts)})
			h.recordExecutionInvalidations(s, outcome)
			return nil, nil
		},
		nil,
	)
	if err != nil {
		return executionCommitOutcome{}, fmt.Errorf("failed to update component state: %w", err)
	}
	return outcome, nil
}

// cancelWorkflows does a best-effort attempt to cancel all workflow executions provided in targets.
func (h *InvokerExecuteTaskHandler) cancelWorkflows(
	ctx context.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	scheduler *Scheduler,
	targets []loadedWorkflowExecution,
	actionsTaken *int,
	maxActions int,
) (completed []loadedWorkflowExecution) {
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, target := range targets {
		if !takeExecutionAction(actionsTaken, maxActions) {
			break
		}

		// Run all cancels concurrently.
		wg.Go(func() {
			err := h.cancelWorkflow(ctx, scheduler, target.expected)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Info("failed to cancel workflow", tag.Error(err), tag.WorkflowID(target.expected.WorkflowId))
				metricsHandler.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Record(1)
			}

			// Cancels are only attempted once here: transient failures are
			// already retried at the history-client layer (resource.HistoryClient
			// is retry-wrapped), so a single best-effort attempt is intentional.
			// todo: consider splitting these out to individual tasks so they can be retried
			// independently
			completed = append(completed, target)
		})
	}

	wg.Wait()
	return completed
}

// terminateWorkflows does a best-effort attempt to terminate all workflow executions provided in targets.
func (h *InvokerExecuteTaskHandler) terminateWorkflows(
	ctx context.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	scheduler *Scheduler,
	targets []loadedWorkflowExecution,
	actionsTaken *int,
	maxActions int,
) (completed []loadedWorkflowExecution) {
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, target := range targets {
		if !takeExecutionAction(actionsTaken, maxActions) {
			break
		}

		// Run all terminates concurrently.
		wg.Go(func() {
			err := h.terminateWorkflow(ctx, scheduler, target.expected)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Info("failed to terminate workflow", tag.Error(err), tag.WorkflowID(target.expected.WorkflowId))
				metricsHandler.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
			}

			// Terminates are only attempted once here: transient failures are
			// already retried at the history-client layer (resource.HistoryClient
			// is retry-wrapped), so a single best-effort attempt is intentional.
			// todo: consider splitting these out to individual tasks so they can be retried
			// independently
			completed = append(completed, target)
		})
	}

	wg.Wait()
	return completed
}

// startWorkflows executes the provided list of starts, returning a result with their outcomes.
func (h *InvokerExecuteTaskHandler) startWorkflows(
	ctx context.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	batch executionBatch,
	actionsTaken *int,
) (results []startExecutionResult) {
	metricsWithTag := metricsHandler.WithTags(
		metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))

	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, loaded := range batch.starts {
		// Starts that haven't been executed yet will remain in `BufferedStarts`,
		// without change, so another ExecuteTask will be immediately created to continue
		// processing in a new task.
		if !takeExecutionAction(actionsTaken, batch.maxActions) {
			break
		}

		// Run all starts concurrently.
		wg.Go(func() {
			started, err := h.startWorkflow(
				ctx,
				metricsHandler,
				batch.scheduler,
				loaded.expected,
				batch.lastCompletionState,
				batch.schedulerRef,
			)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Info("failed to start workflow", tag.Error(err))

				// Don't count "already started" for the error metric or retry, as it is most likely
				// due to misconfiguration.
				if !isAlreadyStartedError(err) {
					metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Record(1)
				}

				if isRetryableError(err) {
					results = append(results, startExecutionResult{
						loaded:      loaded,
						outcome:     startExecutionRetryable,
						backoffTime: h.nextBackoffTime(loaded.expected, err, batch.now),
					})
				} else {
					results = append(results, startExecutionResult{loaded: loaded, outcome: startExecutionFailed})
				}

				return
			}

			metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Record(1)
			results = append(results, startExecutionResult{
				loaded:    loaded,
				outcome:   startExecutionCompleted,
				runID:     started.runID,
				startTime: timestamppb.New(started.startTime),
			})
		})
	}

	wg.Wait()
	return results
}

func takeExecutionAction(actionsTaken *int, maxActions int) bool {
	if *actionsTaken >= maxActions {
		return false
	}
	*actionsTaken = *actionsTaken + 1
	return true
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

// nextBackoffTime uses the framework clock captured during load so retry
// eligibility remains in the same time domain as LastProcessedTime.
func (h *InvokerExecuteTaskHandler) nextBackoffTime(
	start *schedulespb.BufferedStart,
	err error,
	now time.Time,
) *timestamppb.Timestamp {
	if err == nil {
		return nil
	}

	var delay time.Duration
	if rateLimitDelay, ok := isRateLimitedError(err); ok {
		delay = rateLimitDelay
	} else {
		// Elapsed time is left at 0 because we bound on number of attempts.
		delay = h.config.RetryPolicy().ComputeNextDelay(0, int(start.Attempt), nil)
	}

	return timestamppb.New(now.Add(delay))
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

func (h *InvokerExecuteTaskHandler) startWorkflow(
	ctx context.Context,
	metricsHandler metrics.Handler,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	lastCompletionState *schedulerpb.LastCompletionResult,
	schedulerRef []byte,
) (startedWorkflow, error) {
	requestSpec := scheduler.GetSchedule().GetAction().GetStartWorkflow()

	// Inclusive bound: Attempt is 1-based, so the attempt numbered
	// InvokerMaxStartAttempts is the last one that gets an RPC.
	if start.Attempt > InvokerMaxStartAttempts {
		return startedWorkflow{}, errRetryLimitExceeded
	}

	// Get rate limiter permission once per buffered start, on the first attempt only.
	if start.Attempt == 1 {
		delay, err := h.getRateLimiterPermission()
		if err != nil {
			return startedWorkflow{}, err
		}
		if delay > 0 {
			return startedWorkflow{}, newRateLimitedError(delay)
		}
	}

	reusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	if start.Manual {
		reusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}

	var lcr []*commonpb.Payload
	if lastCompletionState.Success != nil {
		lcr = append(lcr, lastCompletionState.Success)
	}
	// Build the completion callback with this start's request ID packed into its token, so the
	// completion is matched by a request ID that rides in the callback header and survives
	// continue-as-new, rather than the started workflow's callback state which is re-stamped on each
	// new run.
	callback, err := chasm.GenerateNexusCallback(schedulerRef, start.RequestId, h.config.EncodeInternalTokenWithEnvelope(scheduler.Namespace))
	if err != nil {
		return startedWorkflow{}, err
	}
	request := &workflowservice.StartWorkflowExecutionRequest{
		CompletionCallbacks:      []*commonpb.Callback{callback},
		Header:                   requestSpec.Header,
		Identity:                 scheduler.identity(),
		Input:                    requestSpec.Input,
		Memo:                     requestSpec.Memo,
		Namespace:                scheduler.Namespace,
		RequestId:                start.RequestId,
		RetryPolicy:              requestSpec.RetryPolicy,
		SearchAttributes:         scheduler.startWorkflowSearchAttributes(start.NominalTime.AsTime()),
		TaskQueue:                requestSpec.TaskQueue,
		UserMetadata:             requestSpec.UserMetadata,
		WorkflowExecutionTimeout: requestSpec.WorkflowExecutionTimeout,
		WorkflowId:               start.WorkflowId,
		WorkflowIdReusePolicy:    reusePolicy,
		WorkflowRunTimeout:       requestSpec.WorkflowRunTimeout,
		WorkflowTaskTimeout:      requestSpec.WorkflowTaskTimeout,
		WorkflowType:             requestSpec.WorkflowType,
		Priority:                 requestSpec.Priority,
		ContinuedFailure:         lastCompletionState.Failure,
		LastCompletionResult: &commonpb.Payloads{
			Payloads: lcr,
		},
	}
	if h.config.Tweakables(scheduler.Namespace).EnableVersioningOverride {
		request.VersioningOverride = requestSpec.VersioningOverride
	}

	result, err := h.frontendClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return startedWorkflow{}, err
	}
	actualStartTime := time.Now()

	// Record time taken from action eligible to workflow started.
	if !start.Manual {
		desiredTime := cmp.Or(start.DesiredTime, start.ActualTime)
		metricsHandler.
			Timer(metrics.ScheduleActionDelay.Name()).
			Record(actualStartTime.Sub(desiredTime.AsTime()))
		// Record total delay from original schedule time, including any overlap policy wait.
		metricsHandler.
			Timer(metrics.ScheduleActionE2EDelay.Name()).
			Record(actualStartTime.Sub(start.ActualTime.AsTime()))
	}
	return startedWorkflow{runID: result.RunId, startTime: actualStartTime}, nil
}

type startedWorkflow struct {
	runID     string
	startTime time.Time
}

func (h *InvokerExecuteTaskHandler) terminateWorkflow(
	ctx context.Context,
	scheduler *Scheduler,
	target *commonpb.WorkflowExecution,
) error {
	request := &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:           scheduler.Namespace,
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: target.WorkflowId},
			Reason:              "terminated by schedule overlap policy",
			Identity:            scheduler.identity(),
			FirstExecutionRunId: target.RunId,
		},
	}
	_, err := h.historyClient.TerminateWorkflowExecution(ctx, request)
	return err
}

func (h *InvokerExecuteTaskHandler) cancelWorkflow(
	ctx context.Context,
	scheduler *Scheduler,
	target *commonpb.WorkflowExecution,
) error {
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:           scheduler.Namespace,
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: target.WorkflowId},
			Reason:              "cancelled by schedule overlap policy",
			Identity:            scheduler.identity(),
			FirstExecutionRunId: target.RunId,
		},
	}
	_, err := h.historyClient.RequestCancelWorkflowExecution(ctx, request)
	return err
}

// getRateLimiterPermission returns a delay for which the caller should wait
// before proceeding. If an error is returned, execution should not proceed, and
// reservation should be retried.
func (h *InvokerExecuteTaskHandler) getRateLimiterPermission() (delay time.Duration, err error) {
	// For now, we're only going to rate limit via APS.
	return
}

func isAlreadyStartedError(err error) bool {
	var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &expectedErr)
}

func isRateLimitedError(err error) (time.Duration, bool) {
	var expectedErr *rateLimitedError
	if errors.As(err, &expectedErr) {
		return expectedErr.delay, true
	}
	return 0, false
}

func isRetryableError(err error) bool {
	_, rateLimited := isRateLimitedError(err)
	return !errors.Is(err, errRetryLimitExceeded) &&
		(rateLimited ||
			common.IsServiceTransientError(err) ||
			common.IsContextDeadlineExceededErr(err))
}

func newRateLimitedError(delay time.Duration) error {
	return &rateLimitedError{delay}
}

func (r *rateLimitedError) Error() string {
	return fmt.Sprintf("rate limited for %s", r.delay)
}

func (h *InvokerExecuteTaskHandler) maxActionsPerExecution(scheduler *Scheduler) int {
	tweakables := h.config.Tweakables(scheduler.Namespace)
	maxActions := tweakables.MaxActionsPerExecution
	if maxActions <= 0 {
		maxActions = DefaultTweakables.MaxActionsPerExecution
	}
	return maxActions
}
