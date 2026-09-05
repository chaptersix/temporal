package scheduler

import (
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/util"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

func (h *InvokerProcessBufferTaskHandler) processBufferLegacy(
	ctx chasm.MutableContext,
	invoker *Invoker,
	scheduler *Scheduler,
) (result processBufferResult) {
	runningWorkflows := invoker.runningWorkflowExecutions()
	isRunning := len(runningWorkflows) > 0
	// A selected start owns the overlap slot while its start RPC is in flight.
	for _, start := range invoker.BufferedStarts {
		if start.Attempt > 0 && !internal.IsCompleted(start) && internal.TracksExecution(start) {
			isRunning = true
		}
	}
	result.missedCatchupByActionRunning = make(map[bool]int64)

	// Processing ignores starts that are already executing or backing off. An existing
	// deferred BUFFER_ONE start still participates so it can reject later starts.
	pendingBufferedStarts := util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt == 0 ||
			(start.Attempt == -1 && scheduler.resolveOverlapPolicy(start.GetOverlapPolicy()) == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
	})

	// Resolve overlap policies and trim BufferedStarts that are skipped by policy.
	action := legacyscheduler.ProcessBuffer(pendingBufferedStarts, isRunning, scheduler.resolveOverlapPolicy)

	// ProcessBuffer will drop starts by omitting them from NewBuffer. Start with the
	// diff between the input and NewBuffer, and add any executing starts.
	keepStarts := make(map[*schedulespb.BufferedStart]struct{})
	for _, start := range action.NewBuffer {
		keepStarts[start] = struct{}{}
	}

	// Combine all available starts.
	readyStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		readyStarts = append(readyStarts, action.NonOverlappingStart)
	}

	// Update result metrics.
	result.overlapSkipped = action.OverlapSkipped
	result.overlapSkippedByPolicy = action.OverlapSkippedByPolicy

	// Add starting workflows to result, trim others. Catchup-window expiry is
	// checked before consumeScheduledAction so that a start past its catchup
	// window doesn't consume a LimitedActions slot.
	for _, start := range readyStarts {
		deadline := h.startActionDeadline(ctx, scheduler, start)
		if ctx.Now(invoker).After(deadline) {
			// Action was buffered in time but expired before execution
			// (e.g., due to overlap deferral, retries, or system delay).
			// Only emit the metric if the schedule would have run this
			// start -- skip paused or action-exhausted schedules.
			if start.Manual || scheduler.canTakeScheduledAction() {
				// Determine if a running action contributed: either one is still
				// running, or the previous action's CloseTime (stored in DesiredTime)
				// was already past this start's deadline.
				// Note: if no prior action completed, DesiredTime is zero-valued,
				// so After(deadline) is false, correctly yielding actionRunning=false.
				actionRunning := isRunning ||
					start.GetDesiredTime().AsTime().After(deadline)
				result.missedCatchupByActionRunning[actionRunning]++
			}
			result.discardStarts = append(result.discardStarts, start)
			result.bufferedStartDropReasons = append(result.bufferedStartDropReasons, bufferedStartDroppedMissedCatchup)
			continue
		}

		// Ensure we can take more actions. Manual actions are always allowed.
		if !start.Manual && !scheduler.consumeScheduledAction() {
			// Drop buffered automated actions while paused or out of actions.
			result.discardStarts = append(result.discardStarts, start)
			result.bufferedStartDropReasons = append(result.bufferedStartDropReasons, bufferedStartDroppedPausedOrLimited)
			continue
		}

		keepStarts[start] = struct{}{}
		result.startActions = append(result.startActions, start)
	}

	result.discardStarts = util.FilterSlice(pendingBufferedStarts, func(start *schedulespb.BufferedStart) bool {
		_, keep := keepStarts[start]
		return !keep
	})

	// Terminate overrides cancel if both are requested.
	if action.NeedTerminate {
		result.terminateExecutions = workflowExecutions(runningWorkflows)
	} else if action.NeedCancel {
		result.cancelExecutions = workflowExecutions(runningWorkflows)
	}

	return
}
