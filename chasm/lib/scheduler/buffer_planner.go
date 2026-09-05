package scheduler

import (
	"maps"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	schedulerinternal "go.temporal.io/server/chasm/lib/scheduler/internal"
)

// appliedBufferPlan collects the live-state result and the number of decisions invalidated during revalidation.
type appliedBufferPlan struct {
	result               processBufferResult
	invalidatedDecisions int64
}

// newBufferProcessingSnapshot projects live CHASM state into values that the pure planner cannot mutate.
func newBufferProcessingSnapshot(invoker *Invoker, scheduler *Scheduler, catchupWindow time.Duration) schedulerinternal.BufferProcessingSnapshot {
	state := scheduler.Schedule.GetState()
	snapshot := schedulerinternal.BufferProcessingSnapshot{
		Starts:               make([]schedulerinternal.BufferedStartSnapshot, 0, len(invoker.GetBufferedStarts())),
		DefaultOverlapPolicy: scheduler.overlapPolicy(),
		CatchupWindow:        catchupWindow,
		MinimumCatchupWindow: startWorkflowMinDeadline,
		Paused:               state.GetPaused(),
		LimitedActions:       state.GetLimitedActions(),
		RemainingActions:     state.GetRemainingActions(),
	}
	for index, start := range invoker.GetBufferedStarts() {
		projected := projectBufferedStart(start, index)
		snapshot.Starts = append(snapshot.Starts, projected)
		if projected.RunID != "" && !projected.Completed &&
			schedulerinternal.TracksCompletionResult(start.GetOverlapPolicy()) {
			snapshot.RunningWorkflows = append(snapshot.RunningWorkflows, schedulerinternal.WorkflowExecutionSnapshot{
				WorkflowID: projected.WorkflowID,
				RunID:      projected.RunID,
			})
		}
	}
	return snapshot
}

func projectBufferedStart(start *schedulespb.BufferedStart, occurrence int) schedulerinternal.BufferedStartSnapshot {
	return schedulerinternal.BufferedStartSnapshot{
		Occurrence:    occurrence,
		RequestID:     start.GetRequestId(),
		WorkflowID:    start.GetWorkflowId(),
		RunID:         start.GetRunId(),
		Attempt:       start.GetAttempt(),
		Manual:        start.GetManual(),
		OverlapPolicy: start.GetOverlapPolicy(),
		ActualTime:    start.GetActualTime().AsTime(),
		DesiredTime:   start.GetDesiredTime().AsTime(),
		Completed:     start.GetCompleted() != nil,
	}
}

// applyBufferPlan revalidates a plan, resolves its value decisions back to live
// BufferedStart pointers, and applies only decisions whose expected state still matches.
func applyBufferPlan(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	invoker *Invoker,
	plan schedulerinternal.BufferPlan,
) appliedBufferPlan {
	applied := newAppliedBufferPlan()
	currentSnapshot := newBufferProcessingSnapshot(invoker, scheduler, plan.Snapshot.CatchupWindow)
	if !bufferProcessingSnapshotsEqual(plan.Snapshot, currentSnapshot) {
		return invalidateBufferPlan(plan, applied)
	}
	startsByRequestID := make(map[string][]indexedBufferedStart)
	for occurrence, start := range invoker.GetBufferedStarts() {
		startsByRequestID[start.GetRequestId()] = append(startsByRequestID[start.GetRequestId()], indexedBufferedStart{start: start, occurrence: occurrence})
	}

	for _, decision := range plan.Decisions {
		if !decision.MutatesState() {
			continue
		}
		start, ok := popMatchingBufferedStart(startsByRequestID, decision)
		if !ok {
			applied.recordInvalidatedDecision()
			continue
		}

		if decision.ConsumesScheduledAction && !scheduler.consumeScheduledAction() {
			applied.recordInvalidatedDecision()
			continue
		}

		applyBufferDecision(&applied.result, decision, start)
	}
	if applied.invalidatedDecisions == 0 {
		applied.result.overlapSkipped = plan.OverlapSkipped
		applied.result.overlapSkippedByPolicy = maps.Clone(plan.OverlapSkippedByPolicy)
	}

	for _, target := range plan.TerminateWorkflows {
		if currentRunningWorkflow(invoker, target) {
			applied.result.terminateWorkflows = append(applied.result.terminateWorkflows, workflowExecutionFromSnapshot(target))
		}
	}
	for _, target := range plan.CancelWorkflows {
		if currentRunningWorkflow(invoker, target) {
			applied.result.cancelWorkflows = append(applied.result.cancelWorkflows, workflowExecutionFromSnapshot(target))
		}
	}

	var totalMissedCatchup int64
	for _, count := range applied.result.missedCatchupByActionRunning {
		totalMissedCatchup += count
	}
	scheduler.recordActionResult(&schedulerActionResult{
		overlapSkipped:      applied.result.overlapSkipped,
		missedCatchupWindow: totalMissedCatchup,
	})
	invoker.recordProcessBufferResult(ctx, &applied.result)
	return applied
}

func newAppliedBufferPlan() appliedBufferPlan {
	return appliedBufferPlan{result: processBufferResult{
		overlapSkippedByPolicy:       make(map[enumspb.ScheduleOverlapPolicy]int64),
		missedCatchupByActionRunning: make(map[bool]int64),
		processedStarts:              make(map[*schedulespb.BufferedStart]bool),
	}}
}

func invalidateBufferPlan(plan schedulerinternal.BufferPlan, applied appliedBufferPlan) appliedBufferPlan {
	for _, decision := range plan.Decisions {
		if decision.MutatesState() {
			applied.recordInvalidatedDecision()
		}
	}
	return applied
}

func (a *appliedBufferPlan) recordInvalidatedDecision() {
	a.invalidatedDecisions++
}

// popMatchingBufferedStart resolves one decision to its live protobuf pointer.
// Removing the match ensures duplicate request IDs cannot reuse the same start.
type indexedBufferedStart struct {
	start      *schedulespb.BufferedStart
	occurrence int
}

func popMatchingBufferedStart(
	startsByRequestID map[string][]indexedBufferedStart,
	decision schedulerinternal.BufferDecision,
) (*schedulespb.BufferedStart, bool) {
	matches := startsByRequestID[decision.RequestID]
	for index, candidate := range matches {
		if projectBufferedStart(candidate.start, candidate.occurrence) == decision.Expected {
			startsByRequestID[decision.RequestID] = append(matches[:index], matches[index+1:]...)
			return candidate.start, true
		}
	}
	return nil, false
}

func applyBufferDecision(result *processBufferResult, decision schedulerinternal.BufferDecision, start *schedulespb.BufferedStart) {
	result.processedStarts[start] = true
	switch decision.Action {
	case schedulerinternal.BufferDecisionExecute:
		result.startWorkflows = append(result.startWorkflows, start)
	case schedulerinternal.BufferDecisionDiscard:
		result.discardStarts = append(result.discardStarts, start)
	default:
	}
	recordAppliedDecisionMetrics(result, decision)
}

func recordAppliedDecisionMetrics(result *processBufferResult, decision schedulerinternal.BufferDecision) {
	switch decision.Reason {
	case schedulerinternal.BufferDecisionReasonMissedCatchupWindow:
		result.bufferedStartDropReasons = append(result.bufferedStartDropReasons, bufferedStartDroppedMissedCatchup)
	case schedulerinternal.BufferDecisionReasonPausedOrLimited:
		result.bufferedStartDropReasons = append(result.bufferedStartDropReasons, bufferedStartDroppedPausedOrLimited)
	default:
	}
	if decision.MissedCatchupMetric {
		result.missedCatchupByActionRunning[decision.MissedCatchupActionRunning]++
	}
}

func bufferProcessingSnapshotsEqual(left, right schedulerinternal.BufferProcessingSnapshot) bool {
	return left.DefaultOverlapPolicy == right.DefaultOverlapPolicy &&
		left.CatchupWindow == right.CatchupWindow &&
		left.MinimumCatchupWindow == right.MinimumCatchupWindow &&
		left.Paused == right.Paused &&
		left.LimitedActions == right.LimitedActions &&
		left.RemainingActions == right.RemainingActions &&
		slices.Equal(left.Starts, right.Starts) &&
		slices.Equal(left.RunningWorkflows, right.RunningWorkflows)
}

func currentRunningWorkflow(invoker *Invoker, target schedulerinternal.WorkflowExecutionSnapshot) bool {
	for _, start := range invoker.GetBufferedStarts() {
		if start.GetWorkflowId() == target.WorkflowID && start.GetRunId() == target.RunID && start.GetCompleted() == nil {
			return true
		}
	}
	return false
}

func workflowExecutionFromSnapshot(execution schedulerinternal.WorkflowExecutionSnapshot) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{WorkflowId: execution.WorkflowID, RunId: execution.RunID}
}
