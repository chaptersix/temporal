package scheduler

import (
	"context"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

// Export unexported methods for testing.

// ExecutionStatus search-attribute values, exported for tests.
var (
	ExecutionStatusRunning   = executionStatusRunning
	ExecutionStatusCompleted = executionStatusCompleted
)

func NewTestHandler(logger log.Logger) *handler {
	return newHandler(logger, legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 }))
}

func (h *handler) TestCreateFromMigrationState(ctx context.Context, req *schedulerpb.CreateFromMigrationStateRequest) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	return h.CreateFromMigrationState(ctx, req)
}

func (h *handler) TestMigrateToWorkflow(ctx context.Context, req *schedulerpb.MigrateToWorkflowRequest) (*schedulerpb.MigrateToWorkflowResponse, error) {
	return h.MigrateToWorkflow(ctx, req)
}

func (s *Scheduler) RecordCompletedAction(
	ctx chasm.MutableContext,
	completed *schedulespb.CompletedResult,
	requestID string,
) time.Time {
	invoker := s.Invoker.Get(ctx)
	return invoker.recordCompletedAction(ctx, completed, requestID)
}

func (i *Invoker) RunningWorkflowID(requestID string) string {
	return i.runningWorkflowID(requestID)
}

func ContextWithTweakables(ctx chasm.Context, tweakables Tweakables) chasm.Context {
	config := Config{
		Tweakables: func(string) Tweakables { return tweakables },
	}
	return chasm.ContextWithValue(ctx, tweakablesCtxKey, config.Tweakables)
}

// RecentActionCount exposes the completed-retention limit for tests.
const RecentActionCount = recentActionCount

// ComputeLastEventTime exposes the non-monotonic, recompute-from-state value so
// tests can pin the regression it is subject to.
func (s *Scheduler) ComputeLastEventTime(ctx chasm.Context) time.Time {
	return s.computeLastEventTime(ctx)
}

// GetLastEventTimeFloored exposes the monotonic read path (recomputed value
// floored at the persisted high water mark).
func (s *Scheduler) GetLastEventTimeFloored(ctx chasm.Context) time.Time {
	return s.getLastEventTime(ctx)
}

// AdvanceLastEventTime exposes the high-water-mark write performed by the
// Generator tick.
func (s *Scheduler) AdvanceLastEventTime(ctx chasm.MutableContext) time.Time {
	return s.advanceLastEventTime(ctx)
}

// IdleDeadline exposes the deadline the idle task is armed against and that
// SchedulerIdleTaskHandler.Validate recomputes.
func (s *Scheduler) IdleDeadline(ctx chasm.Context, idleTime time.Duration) time.Time {
	return s.idleDeadline(ctx, idleTime)
}

// ApplyCompletedRetention exposes applyCompletedRetention for tests.
func (i *Invoker) ApplyCompletedRetention() {
	i.applyCompletedRetention()
}

// RecordExecuteResult adapts the old test helper to the execution batch commit
// path. It intentionally matches starts by request ID because callers model
// the legacy frontend response shape.
func (i *Invoker) RecordExecuteResult(
	ctx chasm.MutableContext,
	completed []*schedulespb.BufferedStart,
	retryable []*schedulespb.BufferedStart,
) (newlyStarted, droppedDuplicates int, startOnlyActions []*schedulespb.BufferedStart) {
	result := executionBatchResult{}
	for _, completion := range completed {
		for index, start := range i.BufferedStarts {
			if start.GetRequestId() == completion.GetRequestId() {
				result.starts = append(result.starts, startExecutionResult{
					loaded:    loadedBufferedStart{index: index, expected: start},
					outcome:   startExecutionCompleted,
					runID:     completion.GetRunId(),
					startTime: completion.GetStartTime(),
				})
				break
			}
		}
	}
	for _, retry := range retryable {
		for index, start := range i.BufferedStarts {
			if start.GetRequestId() == retry.GetRequestId() {
				result.starts = append(result.starts, startExecutionResult{
					loaded:      loadedBufferedStart{index: index, expected: start},
					outcome:     startExecutionRetryable,
					backoffTime: retry.GetBackoffTime(),
				})
				break
			}
		}
	}
	outcome := i.commitExecutionResult(ctx, result)
	i.Scheduler.Get(ctx).advanceLastEventTimeTo(outcome.latestStartTime)
	return outcome.appliedStarts, outcome.duplicateInvalidations, outcome.startOnlyActions
}

type ExecutionBatchForTest struct {
	batch executionBatch
}

type ExecutionBatchResultForTest struct {
	result executionBatchResult
}

func (h *InvokerExecuteTaskHandler) LoadExecutionBatchForTest(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
) (ExecutionBatchForTest, error) {
	batch, err := h.loadExecutionBatch(ctx, invokerRef)
	return ExecutionBatchForTest{batch: batch}, err
}

func (h *InvokerExecuteTaskHandler) ExecuteBatchForTest(
	ctx context.Context,
	batch ExecutionBatchForTest,
) ExecutionBatchResultForTest {
	return ExecutionBatchResultForTest{result: h.executeBatch(ctx, batch.batch)}
}

func (h *InvokerExecuteTaskHandler) CommitExecutionResultForTest(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
	result ExecutionBatchResultForTest,
) (bool, error) {
	outcome, err := h.commitExecutionResult(ctx, invokerRef, result.result)
	return outcome.executeTaskScheduled, err
}

func EvaluateInvokerExecuteTaskValidityForTest(
	invoker *Invoker,
	scheduler *Scheduler,
) (bool, string) {
	validity := evaluateInvokerExecuteTaskValidity(invoker, scheduler)
	return validity.valid, string(validity.reason)
}

func (h *InvokerProcessBufferTaskHandler) PlanBufferProcessingForTest(
	invoker *Invoker,
	scheduler *Scheduler,
	now time.Time,
) func(chasm.MutableContext, *Scheduler, *Invoker) int64 {
	tweakables := h.config.Tweakables(scheduler.Namespace)
	snapshot := newBufferProcessingSnapshot(invoker, scheduler, catchupWindow(scheduler, tweakables))
	plan := schedulerinternal.PlanBufferProcessing(snapshot, now)
	return func(ctx chasm.MutableContext, scheduler *Scheduler, invoker *Invoker) int64 {
		return applyBufferPlan(ctx, scheduler, invoker, plan).invalidatedDecisions
	}
}

func (h *InvokerProcessBufferTaskHandler) ExecuteProcessBufferLegacyForTest(
	ctx chasm.MutableContext,
	invoker *Invoker,
) error {
	scheduler := invoker.Scheduler.Get(ctx)
	newTaggedMetricsHandler(h.metricsHandler, scheduler).
		Counter(metrics.ScheduleInvokerProcessBufferTask.Name()).
		Record(1, metrics.OutcomeTag(outcomeFired), metrics.ReasonTag(reasonNone))

	invoker.getOrCreateEventLog(ctx).LogEvent(ctx, "processBufferTask executed")
	if scheduler.Schedule.GetAction().GetStartWorkflow() == nil {
		return queueerrors.NewUnprocessableTaskError("schedules must have an Action set")
	}

	result := h.processBufferLegacy(ctx, invoker, scheduler)
	var totalMissedCatchup int64
	for _, count := range result.missedCatchupByActionRunning {
		totalMissedCatchup += count
	}
	scheduler.recordActionResult(&schedulerActionResult{
		overlapSkipped:      result.overlapSkipped,
		missedCatchupWindow: totalMissedCatchup,
	})
	invoker.recordProcessBufferResult(ctx, &result)
	h.recordBufferProcessingMetrics(scheduler, result)
	return nil
}

func (s *Scheduler) RecordStartOnlyActions(
	ctx chasm.MutableContext,
	starts []*schedulespb.BufferedStart,
) {
	s.recordStartOnlyActions(ctx, starts)
}

func (b *BackfillerTaskHandler) ProcessBackfill(
	scheduler *Scheduler,
	backfiller *Backfiller,
	limit int,
) (backfillProgressResult, error) {
	return b.processBackfill(nil, scheduler, backfiller, limit)
}

func (b *BackfillerTaskHandler) AllowedBufferedStarts(
	ctx chasm.Context,
	scheduler *Scheduler,
	invoker *Invoker,
	tweakables Tweakables,
) (int, error) {
	return b.allowedBufferedStarts(ctx, scheduler, invoker, tweakables)
}
