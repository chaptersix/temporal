package scheduler

import (
	"context"
	"fmt"
	"slices"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/util"
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

func (s *Scheduler) ComputeLastEventTime(ctx chasm.Context) time.Time {
	return s.computeLastEventTime(ctx)
}

func (s *Scheduler) GetLastEventTimeFloored(ctx chasm.Context) time.Time {
	return s.getLastEventTime(ctx)
}

func (s *Scheduler) AdvanceLastEventTime(ctx chasm.MutableContext) time.Time {
	return s.advanceLastEventTime(ctx)
}

func (s *Scheduler) IdleDeadline(ctx chasm.Context, idleTime time.Duration) time.Time {
	return s.idleDeadline(ctx, idleTime)
}

// ApplyCompletedRetention exposes applyCompletedRetention for tests.
func (i *Invoker) ApplyCompletedRetention() {
	i.applyCompletedRetention()
}

// RecordExecuteResult retains the pre-refactor request-ID matching behavior as
// a test-only oracle for existing event-log and race characterizations.
func (i *Invoker) RecordExecuteResult(
	ctx chasm.MutableContext,
	completed []*schedulespb.BufferedStart,
	retryable []*schedulespb.BufferedStart,
) (newlyStarted, droppedDuplicates int, startOnlyActions []*schedulespb.BufferedStart) {
	completedByRequestID := make(map[string]*schedulespb.BufferedStart)
	retryableByRequestID := make(map[string]*schedulespb.BufferedStart)
	for _, start := range completed {
		completedByRequestID[start.GetRequestId()] = start
	}
	for _, start := range retryable {
		retryableByRequestID[start.GetRequestId()] = start
	}

	retriedStarts := 0
	startedUntracked := make(map[string]struct{})
	var latestStartTime time.Time
	for _, start := range i.GetBufferedStarts() {
		if start.GetRunId() != "" {
			if _, duplicate := completedByRequestID[start.GetRequestId()]; duplicate {
				droppedDuplicates++
			}
			continue
		}
		if completedStart, ok := completedByRequestID[start.GetRequestId()]; ok {
			newlyStarted++
			latestStartTime = util.MaxTime(latestStartTime, completedStart.GetStartTime().AsTime())
			if !schedulerinternal.TracksCompletionResult(start.GetOverlapPolicy()) {
				startOnlyActions = append(startOnlyActions, completedStart)
				startedUntracked[start.GetRequestId()] = struct{}{}
				continue
			}
			schedulerinternal.MarkStartStarted(start, completedStart.GetRunId(), completedStart.GetStartTime())
			start.HasCallback = true
		}
		if retry, ok := retryableByRequestID[start.GetRequestId()]; ok {
			schedulerinternal.MarkStartRetrying(start, start.GetAttempt()+1, retry.GetBackoffTime())
			retriedStarts++
		}
	}
	i.BufferedStarts = slices.DeleteFunc(i.BufferedStarts, func(start *schedulespb.BufferedStart) bool {
		_, remove := startedUntracked[start.GetRequestId()]
		return remove
	})
	i.getOrCreateEventLog(ctx).LogEvent(ctx,
		fmt.Sprintf("recordExecuteResult kicked off %d starts, removed 0 starts, retried %d starts", newlyStarted, retriedStarts))
	i.addTasks(ctx)
	i.Scheduler.Get(ctx).advanceLastEventTimeTo(latestStartTime)
	if newlyStarted > 0 {
		i.Scheduler.Get(ctx).Generator.Get(ctx).Generate(ctx)
	}
	return newlyStarted, droppedDuplicates, startOnlyActions
}

func (s *Scheduler) RecordStartOnlyActions(ctx chasm.MutableContext, starts []*schedulespb.BufferedStart) {
	s.recordStartOnlyActions(ctx, starts)
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
