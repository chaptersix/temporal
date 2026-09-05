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
		Starts:                     make([]schedulerinternal.BufferedStartSnapshot, 0, len(invoker.GetBufferedStarts())),
		DefaultOverlapPolicy:       scheduler.Schedule.GetPolicies().GetOverlapPolicy(),
		DefaultCustomOverlapPolicy: scheduler.Schedule.GetPolicies().GetCustomOverlapPolicy().GetName(),
		Policies:                   actionPolicies(scheduler.Schedule.GetAction()),
		CatchupWindow:              catchupWindow,
		MinimumCatchupWindow:       startActionMinDeadline,
		Paused:                     state.GetPaused(),
		LimitedActions:             state.GetLimitedActions(),
		RemainingActions:           state.GetRemainingActions(),
	}
	for index, start := range invoker.GetBufferedStarts() {
		projected := projectBufferedStart(start, index)
		snapshot.Starts = append(snapshot.Starts, projected)
		if (projected.RunID != "" || projected.Attempt > 0) && !projected.Completed && schedulerinternal.TracksExecution(start) {
			snapshot.RunningExecutions = append(snapshot.RunningExecutions, schedulerinternal.ExecutionSnapshot{
				TargetID: projected.TargetID,
				Kind:     projected.Kind,
				RunID:    projected.RunID,
			})
		}
	}
	return snapshot
}

func projectBufferedStart(start *schedulespb.BufferedStart, occurrence int) schedulerinternal.BufferedStartSnapshot {
	return schedulerinternal.BufferedStartSnapshot{
		Occurrence:          occurrence,
		RequestID:           start.GetRequestId(),
		TargetID:            schedulerinternal.TargetID(start),
		Kind:                schedulerinternal.Execution(start).GetType(),
		RunID:               schedulerinternal.RunID(start),
		Attempt:             start.GetAttempt(),
		Manual:              start.GetManual(),
		OverlapPolicy:       start.GetOverlapPolicy(),
		CustomOverlapPolicy: start.GetCustomOverlapPolicy().GetName(),
		ActualTime:          start.GetActualTime().AsTime(),
		DesiredTime:         start.GetDesiredTime().AsTime(),
		Completed:           schedulerinternal.IsCompleted(start),
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
		applied.result.overlapSkippedByCustomPolicy = maps.Clone(plan.OverlapSkippedByCustomPolicy)
	}

	for _, target := range plan.TerminateExecutions {
		if currentRunningExecution(invoker, target) {
			applied.result.terminateExecutions = append(applied.result.terminateExecutions, executionFromSnapshot(target))
		}
	}
	for _, target := range plan.CancelExecutions {
		if currentRunningExecution(invoker, target) {
			applied.result.cancelExecutions = append(applied.result.cancelExecutions, executionFromSnapshot(target))
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
		result.startActions = append(result.startActions, start)
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
		left.DefaultCustomOverlapPolicy == right.DefaultCustomOverlapPolicy &&
		left.CatchupWindow == right.CatchupWindow &&
		left.MinimumCatchupWindow == right.MinimumCatchupWindow &&
		left.Paused == right.Paused &&
		left.LimitedActions == right.LimitedActions &&
		left.RemainingActions == right.RemainingActions &&
		slices.Equal(left.Starts, right.Starts) &&
		slices.Equal(left.RunningExecutions, right.RunningExecutions)
}

func currentRunningExecution(invoker *Invoker, target schedulerinternal.ExecutionSnapshot) bool {
	if target.RunID == "" {
		return false
	}
	for _, start := range invoker.GetBufferedStarts() {
		if schedulerinternal.TargetID(start) == target.TargetID && schedulerinternal.RunID(start) == target.RunID && !schedulerinternal.IsCompleted(start) {
			return true
		}
	}
	return false
}

func executionFromSnapshot(execution schedulerinternal.ExecutionSnapshot) *commonpb.Execution {
	kind := execution.Kind
	if kind == 0 {
		kind = enumspb.EXECUTION_TYPE_WORKFLOW
	}
	return &commonpb.Execution{Type: kind, BusinessId: execution.TargetID, RunId: execution.RunID}
}
