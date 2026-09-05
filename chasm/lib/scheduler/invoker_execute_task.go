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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	InvokerExecuteTaskHandler struct {
		chasm.SideEffectTaskHandlerBase[*schedulerpb.InvokerExecuteTask]
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
	// InvokerMaxStartAttempts is the maximum number of start-request
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
	invokerExecuteInvalidatedMigrationPending metrics.ReasonString = "migration_pending"
	invokerExecuteInvalidatedNoWork           metrics.ReasonString = "no_work"
	invokerExecuteInvalidatedAlreadyRecorded  metrics.ReasonString = "already_recorded"
	invokerExecuteInvalidatedStateChanged     metrics.ReasonString = "state_changed"
)

type invokerExecuteTaskValidity struct {
	valid  bool
	reason metrics.ReasonString
}

func evaluateInvokerExecuteTaskValidity(invoker *Invoker, scheduler *Scheduler) invokerExecuteTaskValidity {
	if scheduler.WorkflowMigration != nil {
		return invokerExecuteTaskValidity{reason: invokerExecuteInvalidatedMigrationPending}
	}
	if len(invoker.terminationExecutions())+len(invoker.cancellationExecutions())+len(invoker.getEligibleBufferedStarts()) == 0 {
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

type loadedExecution struct {
	index    int
	expected *commonpb.Execution
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
	terminations        []loadedExecution
	cancellations       []loadedExecution
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
	terminations  []loadedExecution
	cancellations []loadedExecution
	starts        []startExecutionResult
}

type executionCommitOutcome struct {
	appliedStarts             int
	removedActions            int
	committedRetries          int
	duplicateInvalidations    int
	stateChangedInvalidations int
	executeTaskScheduled      bool
	latestStartTime           time.Time
	startOnlyActions          []*schedulespb.BufferedStart
}

func (i *Invoker) commitExecutionResult(
	ctx chasm.MutableContext,
	result executionBatchResult,
) (outcome executionCommitOutcome) {
	removeTerminations := revalidateExecutionResults(i.terminationExecutions(), result.terminations, &outcome)
	i.setTerminationExecutions(deleteIndexed(i.terminationExecutions(), removeTerminations))
	removeCancels := revalidateExecutionResults(i.cancellationExecutions(), result.cancellations, &outcome)
	i.setCancellationExecutions(deleteIndexed(i.cancellationExecutions(), removeCancels))

	removeStarts := make(map[int]bool)
	matched := make(map[int]bool)
	for _, startResult := range result.starts {
		expected := startResult.loaded.expected
		index := -1
		for candidate, live := range i.BufferedStarts {
			if !matched[candidate] && sameOccurrence(live, expected) {
				index = candidate
				break
			}
		}
		if index < 0 {
			outcome.stateChangedInvalidations++
			continue
		}
		matched[index] = true
		start := i.BufferedStarts[index]
		if outcome.applyStartResult(start, startResult) {
			removeStarts[index] = true
		}
	}

	i.BufferedStarts = deleteIndexed(i.BufferedStarts, removeStarts)
	if outcome.appliedStarts > 0 || len(removeStarts) > 0 {
		i.reconsiderWaiting()
	}

	i.getOrCreateEventLog(ctx).LogEvent(ctx,
		fmt.Sprintf("recordExecuteResult kicked off %d starts, removed %d starts, retried %d starts",
			outcome.appliedStarts,
			outcome.removedActions,
			outcome.committedRetries))
	i.addTasks(ctx)
	outcome.executeTaskScheduled = i.hasExecutableWork()
	if outcome.appliedStarts > 0 {
		i.Scheduler.Get(ctx).Generator.Get(ctx).Generate(ctx)
	}
	return outcome
}

func (outcome *executionCommitOutcome) applyStartResult(start *schedulespb.BufferedStart, startResult startExecutionResult) bool {
	if start.GetStartAccepted() || schedulerinternal.RunID(start) != "" && !schedulerinternal.IsCompleted(start) {
		if startResult.outcome == startExecutionCompleted {
			outcome.duplicateInvalidations++
		} else {
			outcome.stateChangedInvalidations++
		}
		return false
	}
	if !compatibleAttempt(start, startResult.loaded.expected) {
		outcome.stateChangedInvalidations++
		return false
	}
	switch startResult.outcome {
	case startExecutionCompleted:
		if runID := schedulerinternal.RunID(start); runID != "" && runID != startResult.runID {
			outcome.stateChangedInvalidations++
			return false
		}
		if runID := start.GetCompletion().GetExecution().GetRunId(); runID != "" && runID != startResult.runID {
			outcome.stateChangedInvalidations++
			return false
		}
		schedulerinternal.MarkStartStarted(start, startResult.runID, startResult.startTime)
		start.HasCallback = true
		start.StartAccepted = true
		outcome.appliedStarts++
		if start.GetStartTime().AsTime().After(outcome.latestStartTime) {
			outcome.latestStartTime = start.GetStartTime().AsTime()
		}
		if !schedulerinternal.TracksExecution(start) {
			outcome.startOnlyActions = append(outcome.startOnlyActions, start)
			outcome.removedActions++
			return true
		}
	case startExecutionRetryable:
		schedulerinternal.MarkStartRetrying(start, start.GetAttempt()+1, startResult.backoffTime)
		outcome.committedRetries++
	case startExecutionFailed:
		if schedulerinternal.IsCompleted(start) {
			return false
		}
		outcome.removedActions++
		return true
	default:
		outcome.stateChangedInvalidations++
	}
	return false
}

func sameOccurrence(live, expected *schedulespb.BufferedStart) bool {
	if live.GetOccurrenceId() != "" || expected.GetOccurrenceId() != "" {
		return live.GetOccurrenceId() == expected.GetOccurrenceId()
	}
	return live.GetRequestId() == expected.GetRequestId() && schedulerinternal.TargetID(live) == schedulerinternal.TargetID(expected) &&
		proto.Equal(live.GetNominalTime(), expected.GetNominalTime()) && proto.Equal(live.GetActualTime(), expected.GetActualTime()) && live.GetManual() == expected.GetManual()
}

func compatibleAttempt(live, expected *schedulespb.BufferedStart) bool {
	return live.GetAttempt() == expected.GetAttempt() && proto.Equal(live.GetBackoffTime(), expected.GetBackoffTime())
}

func revalidateExecutionResults(
	live []*commonpb.Execution,
	results []loadedExecution,
	outcome *executionCommitOutcome,
) map[int]bool {
	remove := make(map[int]bool, len(results))
	for _, result := range results {
		index := slices.IndexFunc(live, func(target *commonpb.Execution) bool { return proto.Equal(target, result.expected) })
		if index < 0 || remove[index] {
			outcome.stateChangedInvalidations++
			continue
		}
		remove[index] = true
		outcome.removedActions++
	}

	return remove
}

func deleteIndexed[T any](values []T, remove map[int]bool) []T {
	index := 0
	return slices.DeleteFunc(values, func(T) bool {
		removed := remove[index]
		index++
		return removed
	})
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
			batch.lastCompletionState = &schedulerpb.LastCompletionResult{}
			if implementation(s.Schedule.GetAction()).ParticipatesInCompletionHistory() {
				batch.lastCompletionState = common.CloneProto(s.LastCompletionResult.Get(ctx))
			}

			// Capture the scheduler's component ref so a per-start completion callback (carrying that
			// start's request ID in its token) can be built outside the MS lock.
			ref, err := ctx.Ref(s)
			if err != nil {
				return struct{}{}, err
			}
			batch.schedulerRef = slices.Clone(ref)
			batch.now = ctx.Now(i)
			batch.maxActions = h.maxActionsPerExecution(batch.scheduler)

			// Occurrence identity survives buffer movement while external calls are in flight.
			for index, target := range i.terminationExecutions() {
				batch.terminations = append(batch.terminations, loadedExecution{index: index, expected: common.CloneProto(target)})
			}
			for index, target := range i.cancellationExecutions() {
				batch.cancellations = append(batch.cancellations, loadedExecution{index: index, expected: common.CloneProto(target)})
			}
			lastProcessed := i.GetLastProcessedTime().AsTime()
			for index, start := range i.GetBufferedStarts() {
				if start.GetAttempt() > 0 && !start.GetStartAccepted() && schedulerinternal.RunID(start) == "" && !start.GetBackoffTime().AsTime().After(lastProcessed) {
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
	result.terminations = h.terminateExecutions(ctx, logger, metricsHandler, batch.scheduler, batch.terminations, &actionsTaken, batch.maxActions)
	result.cancellations = h.cancelExecutions(ctx, logger, metricsHandler, batch.scheduler, batch.cancellations, &actionsTaken, batch.maxActions)
	result.starts = h.startActions(ctx, logger, metricsHandler, batch, &actionsTaken)
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
			s.advanceLastEventTimeTo(outcome.latestStartTime)
			s.recordStartOnlyActions(ctx, outcome.startOnlyActions)
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

// cancelExecutions does a best-effort attempt to cancel all executions provided in targets.
func (h *InvokerExecuteTaskHandler) cancelExecutions(
	ctx context.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	scheduler *Scheduler,
	targets []loadedExecution,
	actionsTaken *int,
	maxActions int,
) (completed []loadedExecution) {
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, target := range targets {
		if !takeExecutionAction(actionsTaken, maxActions) {
			break
		}

		// Run all cancels concurrently.
		wg.Go(func() {
			err := implementation(scheduler.Schedule.GetAction()).Cancel(ctx, actionClients{Frontend: h.frontendClient, History: h.historyClient}, scheduler, target.expected)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Info("failed to cancel execution", tag.Error(err), tag.NewStringTag("target-id", target.expected.BusinessId))
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

// terminateExecutions does a best-effort attempt to terminate all executions provided in targets.
func (h *InvokerExecuteTaskHandler) terminateExecutions(
	ctx context.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	scheduler *Scheduler,
	targets []loadedExecution,
	actionsTaken *int,
	maxActions int,
) (completed []loadedExecution) {
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, target := range targets {
		if !takeExecutionAction(actionsTaken, maxActions) {
			break
		}

		// Run all terminates concurrently.
		wg.Go(func() {
			err := implementation(scheduler.Schedule.GetAction()).Terminate(ctx, actionClients{Frontend: h.frontendClient, History: h.historyClient}, scheduler, target.expected)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Info("failed to terminate execution", tag.Error(err), tag.NewStringTag("target-id", target.expected.BusinessId))
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

// startActions executes the provided list of starts, returning a result with their outcomes.
func (h *InvokerExecuteTaskHandler) startActions(
	ctx context.Context,
	logger log.Logger,
	metricsHandler metrics.Handler,
	batch executionBatch,
	actionsTaken *int,
) (results []startExecutionResult) {
	metricsWithTag := metricsHandler.WithTags(
		metrics.StringTag(metrics.ScheduleActionTypeTag, batch.scheduler.actionMetadata().Kind.String()))

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
			started, err := h.startAction(
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
				logger.Info("failed to start action", tag.Error(err))

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

func (h *InvokerExecuteTaskHandler) startAction(
	ctx context.Context,
	metricsHandler metrics.Handler,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	lastCompletionState *schedulerpb.LastCompletionResult,
	schedulerRef []byte,
) (startedExecution, error) {

	// Inclusive bound: Attempt is 1-based, so the attempt numbered
	// InvokerMaxStartAttempts is the last one that gets an RPC.
	if start.Attempt > InvokerMaxStartAttempts {
		return startedExecution{}, errRetryLimitExceeded
	}

	// Get rate limiter permission once per buffered start, on the first attempt only.
	if start.Attempt == 1 {
		delay, err := h.getRateLimiterPermission()
		if err != nil {
			return startedExecution{}, err
		}
		if delay > 0 {
			return startedExecution{}, newRateLimitedError(delay)
		}
	}

	callback, err := chasm.GenerateNexusCallback(schedulerRef, start.RequestId, h.config.EncodeInternalTokenWithEnvelope(scheduler.Namespace))
	if err != nil {
		return startedExecution{}, err
	}
	runID, err := implementation(scheduler.Schedule.GetAction()).Start(ctx, actionClients{Frontend: h.frontendClient, History: h.historyClient}, actionStartInput{
		Scheduler: scheduler, Occurrence: start, Callback: callback, Previous: lastCompletionState, EnableVersioningOverride: h.config.Tweakables(scheduler.Namespace).EnableVersioningOverride,
	})
	if err != nil {
		return startedExecution{}, err
	}
	// This timestamp measures the external RPC, so it must use wall-clock time rather than CHASM transaction time.
	actualStartTime := time.Now() //nolint:forbidigo

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
	return startedExecution{runID: runID, startTime: actualStartTime}, nil
}

type startedExecution struct {
	runID     string
	startTime time.Time
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
	var activityErr *serviceerror.ActivityExecutionAlreadyStarted
	return errors.As(err, &expectedErr) || errors.As(err, &activityErr)
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
