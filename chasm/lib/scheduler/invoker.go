package scheduler

import (
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Invoker component is responsible for executing buffered actions.
//
// BufferedStart lifecycle is encoded by a tuple rather than one state field.
// The states produced by current writers are:
//
//	Attempt == 0                       unprocessed by overlap policy
//	Attempt == -1                      deferred by overlap policy
//	Attempt > 0, RunId == "", Completed == nil   ready or backing off
//	Attempt > 0, RunId != "", Completed == nil   started
//	Attempt > 0, RunId != "", Completed != nil   completed and retained as history
//
// For a ready or backing-off start, BackoffTime is ready when it is less than
// or equal to Invoker.LastProcessedTime and is otherwise still backing off.
// Attempt is therefore both a lifecycle sentinel and a 1-based retry count.
// HasCallback is normally set by a successful start. A started tuple with it
// unset needs callback repair after V1 migration.
//
// A completion callback can commit before the matching StartWorkflow result,
// producing a tuple with Completed set but no RunId. Current eligibility uses
// Attempt, RunId, BackoffTime, and LastProcessedTime, but not Completed, so an
// Execute task may still select it. A successful late result fills in the RunId;
// failure paths can retain the unusual tuple while retry handling continues.
type Invoker struct {
	chasm.UnimplementedComponent

	*schedulerpb.InvokerState

	Scheduler chasm.ParentPtr[*Scheduler]

	EventLog chasm.Field[*EventLog]
}

func (i *Invoker) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// NewInvoker returns an initialized Invoker component, which should
// be parented under a Scheduler root component.
func NewInvoker(ctx chasm.MutableContext) *Invoker {
	return newInvokerWithState(ctx, &schedulerpb.InvokerState{
		BufferedStarts: []*schedulespb.BufferedStart{},
	})
}

func newInvokerWithState(ctx chasm.MutableContext, state *schedulerpb.InvokerState) *Invoker {
	i := &Invoker{
		InvokerState: state,
		EventLog:     chasm.NewComponentField(ctx, NewEventLog(ctx)),
	}
	return i
}

// EnqueueBufferedStarts adds new BufferedStarts to the invocation queue,
// immediately kicking off a processing task.
func (i *Invoker) EnqueueBufferedStarts(ctx chasm.MutableContext, starts []*schedulespb.BufferedStart) {
	i.BufferedStarts = append(i.BufferedStarts, starts...)
	if len(starts) > 0 {
		i.getOrCreateEventLog(ctx).LogEvent(ctx, fmt.Sprintf("enqueued %d buffered start(s)", len(starts)))
	}
	i.addTasks(ctx)
}

type processBufferResult struct {
	startWorkflows     []*schedulespb.BufferedStart
	cancelWorkflows    []*commonpb.WorkflowExecution
	terminateWorkflows []*commonpb.WorkflowExecution

	// discardStarts will be dropped from the Invoker's BufferedStarts without execution.
	discardStarts []*schedulespb.BufferedStart

	// Number of buffered starts dropped due to overlap policy during processing.
	overlapSkipped         int64
	overlapSkippedByPolicy map[enumspb.ScheduleOverlapPolicy]int64

	// Number of buffered starts dropped from missing the catchup window,
	// bucketed by whether a running action contributed to the miss.
	missedCatchupByActionRunning map[bool]int64
	bufferedStartDropReasons     []metrics.ReasonString

	// processedStarts limits lifecycle transitions to identities covered by a
	// revalidated plan. A nil map preserves the legacy processor's behavior.
	processedStarts map[*schedulespb.BufferedStart]bool
}

// recordProcessBufferResult updates the Invoker's internal state based on result, as well as the
// LastProcessedTime watermark. Tasks to continue execution are added, if needed.
func (i *Invoker) recordProcessBufferResult(ctx chasm.MutableContext, result *processBufferResult) {
	discards := make(map[*schedulespb.BufferedStart]bool)
	ready := make(map[*schedulespb.BufferedStart]bool)
	for _, start := range result.discardStarts {
		discards[start] = true
	}
	for _, start := range result.startWorkflows {
		ready[start] = true
	}

	// Drop discarded starts, and update requested starts for execution.
	var starts []*schedulespb.BufferedStart
	readiedStarts := 0
	deferredStarts := 0
	for _, start := range i.GetBufferedStarts() {
		if discards[start] {
			continue
		}

		// Starts ready for execution are set to their first attempt.
		if ready[start] && start.Attempt < 1 {
			schedulerinternal.MarkStartReady(start)
			readiedStarts++
		} else if start.Attempt == 0 && (result.processedStarts == nil || result.processedStarts[start]) {
			// Start was processed but deferred (e.g., BUFFER_ONE policy with running workflow).
			// Mark as deferred (-1) to distinguish from newly-enqueued starts so addTasks
			// won't schedule an immediate ProcessBuffer task for them - they wait on
			// recordCompletedAction to re-enable.
			schedulerinternal.MarkStartDeferred(start)
			deferredStarts++
		}

		starts = append(starts, start)
	}

	if readiedStarts > 0 || deferredStarts > 0 {
		i.getOrCreateEventLog(ctx).LogEvent(ctx,
			fmt.Sprintf("recordProcessBufferResult readied %d starts, deferred %d starts", readiedStarts, deferredStarts))
	}

	// Update internal state.
	i.BufferedStarts = starts
	i.CancelWorkflows = append(i.GetCancelWorkflows(), result.cancelWorkflows...)
	i.TerminateWorkflows = append(i.GetTerminateWorkflows(), result.terminateWorkflows...)
	i.LastProcessedTime = timestamppb.New(ctx.Now(i))

	// Re-arm tasks if this call changed state, or if the LastProcessedTime advance
	// just unblocked backed-off starts.
	i.addTasks(ctx)
}

// runningWorkflowID returns the workflow ID associated with the given
// outstanding request.
func (i *Invoker) runningWorkflowID(requestID string) string {
	for _, start := range i.GetBufferedStarts() {
		if start.GetRequestId() == requestID && start.GetCompleted() == nil {
			return start.GetWorkflowId()
		}
	}
	return ""
}

// recordCompletedAction updates Invoker metadata and kicks off tasks after
// an action completes. It marks the BufferedStart as completed by setting
// the Completed field.
//
// Returns the schedule time of the completed action for metrics.
func (i *Invoker) recordCompletedAction(
	ctx chasm.MutableContext,
	completed *schedulespb.CompletedResult,
	requestID string,
) (scheduleTime time.Time) {
	i.getOrCreateEventLog(ctx).LogEvent(ctx, fmt.Sprintf("recording completed action: %s", requestID))

	// Find the BufferedStart and mark it as completed.
	for _, start := range i.BufferedStarts {
		if start.GetRequestId() == requestID {
			scheduleTime = start.DesiredTime.AsTime()
			schedulerinternal.MarkStartCompleted(start, completed)
			break
		}
	}

	// Re-enable deferred starts (Attempt == -1) so they can be re-processed by
	// ProcessBuffer now that a workflow has completed. This allows the overlap
	// policy to be re-evaluated.
	for _, start := range i.BufferedStarts {
		if start.Attempt == -1 {
			schedulerinternal.MarkStartUnprocessed(start)
		}
	}

	// Update DesiredTime on the first pending start for metrics. DesiredTime is used
	// to drive action latency between buffered starts (the time it takes between
	// completing one start and kicking off the next). It also signals in processBuffer
	// that this start was blocked behind a running action: if DesiredTime (the previous
	// action's CloseTime) is past the start's catchup deadline, the previous action's
	// duration caused the miss.
	idx := slices.IndexFunc(i.BufferedStarts, func(start *schedulespb.BufferedStart) bool {
		return start.Attempt == 0
	})
	if idx >= 0 {
		i.BufferedStarts[idx].DesiredTime = timestamppb.New(completed.GetCloseTime().AsTime())
	}

	// Apply retention to keep only the last N completed actions.
	i.applyCompletedRetention()

	// addTasks will add an immediate ProcessBufferTask if we have any starts pending
	// kick-off.
	i.addTasks(ctx)

	return
}

// addTasks adds both ProcessBuffer and Execute tasks as needed. It should be
// called when completing processing/executing tasks, to drive backoff/retry.
func (i *Invoker) addTasks(ctx chasm.MutableContext) {
	// If we have Attempt = 0 starts, generate a ProcessBufferTask immediately. If we
	// have starts that are backing off, add a timer task for the earliest backoff time.
	if i.hasUnprocessedStarts() {
		i.getOrCreateEventLog(ctx).LogEvent(ctx, "scheduled processBufferTask immediately")
		ctx.AddTask(i, chasm.TaskAttributes{
			ScheduledTime: chasm.TaskScheduledTimeImmediate,
		}, &schedulerpb.InvokerProcessBufferTask{})
	} else if deadline := i.nextBackoffDeadline(); !deadline.IsZero() {
		i.getOrCreateEventLog(ctx).LogEvent(ctx,
			fmt.Sprintf("scheduled processBufferTask for %s", deadline.Format(time.RFC3339)))
		ctx.AddTask(i, chasm.TaskAttributes{
			ScheduledTime: deadline,
		}, &schedulerpb.InvokerProcessBufferTask{})
	}

	// Execute drains work that's ready now: pending cancels/terminates, and
	// starts that are past their backoff.
	if i.hasExecutableWork() {
		i.getOrCreateEventLog(ctx).LogEvent(ctx, "scheduled executeTask")
		ctx.AddTask(i, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	}
}

func (i *Invoker) hasExecutableWork() bool {
	return len(i.GetCancelWorkflows()) > 0 ||
		len(i.GetTerminateWorkflows()) > 0 ||
		len(i.getEligibleBufferedStarts()) > 0
}

// hasUnprocessedStarts reports whether any BufferedStart is still awaiting its
// initial ProcessBuffer pass (Attempt == 0).
func (i *Invoker) hasUnprocessedStarts() bool {
	for _, start := range i.GetBufferedStarts() {
		if start.GetAttempt() == 0 {
			return true
		}
	}
	return false
}

// nextBackoffDeadline returns the earliest BackoffTime among starts that are
// retrying, or the zero time if none are.
func (i *Invoker) nextBackoffDeadline() time.Time {
	var deadline time.Time
	lastProcessedTime := i.LastProcessedTime.AsTime()
	for _, start := range i.GetBufferedStarts() {
		backoff := start.GetBackoffTime().AsTime()
		// We only care about starts that are retrying.
		if start.GetAttempt() <= 0 ||
			start.GetRunId() != "" ||
			start.GetCompleted() != nil ||
			// Backed-off starts will be selected by getEligibleBufferedStarts and kick off
			// an Execute task, instead.
			start.BackoffTime.AsTime().Before(lastProcessedTime) {
			continue
		}
		if deadline.IsZero() || backoff.Before(deadline) {
			deadline = backoff
		}
	}

	return deadline
}

// getEligibleBufferedStarts returns all BufferedStarts that are marked for
// execution (Attempt > 0), haven't been started yet (no RunId), and aren't
// presently backing off, based on last processed time.
func (i *Invoker) getEligibleBufferedStarts() []*schedulespb.BufferedStart {
	lastProcessed := i.GetLastProcessedTime().AsTime()
	return util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt > 0 &&
			start.GetRunId() == "" &&
			!start.GetBackoffTime().AsTime().After(lastProcessed)
	})
}

// runningWorkflowExecutions returns the list of workflow executions that
// have been started but not yet completed.
func (i *Invoker) runningWorkflowExecutions() []*commonpb.WorkflowExecution {
	var running []*commonpb.WorkflowExecution
	for _, start := range i.GetBufferedStarts() {
		if start.GetRunId() != "" && start.GetCompleted() == nil {
			running = append(running, &commonpb.WorkflowExecution{
				WorkflowId: start.GetWorkflowId(),
				RunId:      start.GetRunId(),
			})
		}
	}
	return running
}

// recentActions returns started/completed actions as ScheduleActionResults.
// This includes both running workflows (with status RUNNING) and completed
// workflows (with their final status).
func (i *Invoker) recentActions() []*schedulepb.ScheduleActionResult {
	var results []*schedulepb.ScheduleActionResult
	for _, start := range i.GetBufferedStarts() {
		// Only include workflows that have been started (have a RunId).
		if start.GetRunId() == "" {
			continue
		}
		status := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		if start.GetCompleted() != nil {
			status = start.GetCompleted().GetStatus()
		}
		results = append(results, &schedulepb.ScheduleActionResult{
			ScheduleTime: start.GetActualTime(),
			ActualTime:   start.GetStartTime(),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: start.GetWorkflowId(),
				RunId:      start.GetRunId(),
			},
			StartWorkflowStatus: status,
		})
	}
	return results
}

// applyCompletedRetention removes the oldest completed BufferedStarts beyond
// the retention limit.
func (i *Invoker) applyCompletedRetention() {
	var completed []*schedulespb.BufferedStart
	var nonCompleted []*schedulespb.BufferedStart

	for _, start := range i.BufferedStarts {
		if start.GetCompleted() != nil {
			completed = append(completed, start)
		} else {
			nonCompleted = append(nonCompleted, start)
		}
	}

	// Sort by oldest first.
	slices.SortFunc(completed, func(a, b *schedulespb.BufferedStart) int {
		return a.GetCompleted().GetCloseTime().AsTime().Compare(b.GetCompleted().GetCloseTime().AsTime())
	})

	if len(completed) > recentActionCount {
		completed = completed[len(completed)-recentActionCount:]
	}

	i.BufferedStarts = append(nonCompleted, completed...)
}
