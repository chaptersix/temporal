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

type appliedBufferPlan struct {
	result         processBufferResult
	decisions      []schedulerinternal.BufferDecision
	staleDecisions int64
}

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
	for _, start := range invoker.GetBufferedStarts() {
		projected := projectBufferedStart(start)
		snapshot.Starts = append(snapshot.Starts, projected)
		if projected.RunID != "" && !projected.Completed {
			snapshot.RunningWorkflows = append(snapshot.RunningWorkflows, schedulerinternal.WorkflowExecutionSnapshot{
				WorkflowID: projected.WorkflowID,
				RunID:      projected.RunID,
			})
		}
	}
	return snapshot
}

func projectBufferedStart(start *schedulespb.BufferedStart) schedulerinternal.BufferedStartSnapshot {
	return schedulerinternal.BufferedStartSnapshot{
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

func applyBufferPlan(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	invoker *Invoker,
	plan schedulerinternal.BufferPlan,
) appliedBufferPlan {
	applied := newAppliedBufferPlan()
	currentSnapshot := newBufferProcessingSnapshot(invoker, scheduler, plan.Snapshot.CatchupWindow)
	if !bufferProcessingSnapshotsEqual(plan.Snapshot, currentSnapshot) {
		return staleBufferPlan(plan, applied)
	}
	startsByRequestID := make(map[string][]*schedulespb.BufferedStart)
	for _, start := range invoker.GetBufferedStarts() {
		startsByRequestID[start.GetRequestId()] = append(startsByRequestID[start.GetRequestId()], start)
	}

	for _, decision := range plan.Decisions {
		if !decision.MutatesState() {
			applied.decisions = append(applied.decisions, decision)
			continue
		}
		start, ok := popMatchingBufferedStart(startsByRequestID, decision)
		if !ok {
			applied.recordStaleDecision(decision)
			continue
		}

		if decision.Action == schedulerinternal.BufferDecisionExecute && !start.GetManual() && !scheduler.consumeScheduledAction() {
			applied.recordStaleDecision(decision)
			continue
		}

		applyBufferDecision(&applied.result, decision, start)
		applied.decisions = append(applied.decisions, decision)
	}
	if applied.staleDecisions == 0 {
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
		processedStarts:              make(map[string]bool),
	}}
}

func staleBufferPlan(plan schedulerinternal.BufferPlan, applied appliedBufferPlan) appliedBufferPlan {
	for _, decision := range plan.Decisions {
		if decision.MutatesState() {
			applied.recordStaleDecision(decision)
		} else {
			applied.decisions = append(applied.decisions, decision)
		}
	}
	return applied
}

func (a *appliedBufferPlan) recordStaleDecision(decision schedulerinternal.BufferDecision) {
	decision.Action = schedulerinternal.BufferDecisionRetain
	decision.Reason = schedulerinternal.BufferDecisionReasonStale
	a.staleDecisions++
	a.decisions = append(a.decisions, decision)
}

func popMatchingBufferedStart(
	startsByRequestID map[string][]*schedulespb.BufferedStart,
	decision schedulerinternal.BufferDecision,
) (*schedulespb.BufferedStart, bool) {
	matches := startsByRequestID[decision.RequestID]
	for index, start := range matches {
		if projectBufferedStart(start) == decision.Expected {
			startsByRequestID[decision.RequestID] = append(matches[:index], matches[index+1:]...)
			return start, true
		}
	}
	return nil, false
}

func applyBufferDecision(result *processBufferResult, decision schedulerinternal.BufferDecision, start *schedulespb.BufferedStart) {
	result.processedStarts[decision.RequestID] = true
	switch decision.Action {
	case schedulerinternal.BufferDecisionExecute:
		result.startWorkflows = append(result.startWorkflows, start)
	case schedulerinternal.BufferDecisionDiscard:
		result.discardStarts = append(result.discardStarts, start)
		recordAppliedDiscard(result, decision)
	default:
	}
}

func recordAppliedDiscard(result *processBufferResult, decision schedulerinternal.BufferDecision) {
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
