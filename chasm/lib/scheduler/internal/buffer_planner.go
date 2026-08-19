package internal

import (
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

type BufferDecisionAction int

const (
	BufferDecisionRetain BufferDecisionAction = iota
	BufferDecisionExecute
	BufferDecisionDiscard
	BufferDecisionDefer
	BufferDecisionRetry
	BufferDecisionRunning
	BufferDecisionCompleted
)

type BufferDecisionReason int

const (
	BufferDecisionReasonNone BufferDecisionReason = iota
	BufferDecisionReasonAlreadyProcessed
	BufferDecisionReasonOverlapPolicy
	BufferDecisionReasonCancelOrTerminate
	BufferDecisionReasonMissedCatchupWindow
	BufferDecisionReasonPausedOrLimited
	BufferDecisionReasonStale
)

type BufferedStartSnapshot struct {
	RequestID     string
	WorkflowID    string
	RunID         string
	Attempt       int64
	Manual        bool
	OverlapPolicy enumspb.ScheduleOverlapPolicy
	ActualTime    time.Time
	DesiredTime   time.Time
	Completed     bool
}

func (s BufferedStartSnapshot) GetOverlapPolicy() enumspb.ScheduleOverlapPolicy {
	return s.OverlapPolicy
}

type WorkflowExecutionSnapshot struct {
	WorkflowID string
	RunID      string
}

type BufferProcessingSnapshot struct {
	Starts               []BufferedStartSnapshot
	RunningWorkflows     []WorkflowExecutionSnapshot
	DefaultOverlapPolicy enumspb.ScheduleOverlapPolicy
	CatchupWindow        time.Duration
	MinimumCatchupWindow time.Duration
	Paused               bool
	LimitedActions       bool
	RemainingActions     int64
}

type BufferDecision struct {
	RequestID                  string
	Expected                   BufferedStartSnapshot
	Action                     BufferDecisionAction
	Reason                     BufferDecisionReason
	OverlapPolicy              enumspb.ScheduleOverlapPolicy
	OverlapSkipped             bool
	MissedCatchupMetric        bool
	MissedCatchupActionRunning bool
}

func (d BufferDecision) MutatesState() bool {
	return d.Action == BufferDecisionExecute || d.Action == BufferDecisionDiscard || d.Action == BufferDecisionDefer
}

type BufferPlan struct {
	Snapshot                     BufferProcessingSnapshot
	Decisions                    []BufferDecision
	CancelWorkflows              []WorkflowExecutionSnapshot
	TerminateWorkflows           []WorkflowExecutionSnapshot
	OverlapSkipped               int64
	OverlapSkippedByPolicy       map[enumspb.ScheduleOverlapPolicy]int64
	MissedCatchupByActionRunning map[bool]int64
}

type overlapAction struct {
	OverlappingStarts      []BufferedStartSnapshot
	NonOverlappingStart    BufferedStartSnapshot
	NewBuffer              []BufferedStartSnapshot
	NeedCancel             bool
	NeedTerminate          bool
	OverlapSkipped         int64
	OverlapSkippedByPolicy map[enumspb.ScheduleOverlapPolicy]int64
}

func PlanBufferProcessing(snapshot BufferProcessingSnapshot, now time.Time) BufferPlan {
	plan := BufferPlan{
		Snapshot:                     cloneBufferProcessingSnapshot(snapshot),
		OverlapSkippedByPolicy:       make(map[enumspb.ScheduleOverlapPolicy]int64),
		MissedCatchupByActionRunning: make(map[bool]int64),
	}
	resolveOverlapPolicy := func(policy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
		if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
			return snapshot.DefaultOverlapPolicy
		}
		return policy
	}

	pending := make([]BufferedStartSnapshot, 0, len(snapshot.Starts))
	for _, start := range snapshot.Starts {
		if start.Attempt == 0 ||
			(start.Attempt == -1 && resolveOverlapPolicy(start.OverlapPolicy) == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE) {
			pending = append(pending, start)
		}
	}

	action := planOverlapActions(pending, len(snapshot.RunningWorkflows) > 0, resolveOverlapPolicy)
	plan.OverlapSkipped = action.OverlapSkipped
	plan.OverlapSkippedByPolicy = action.OverlapSkippedByPolicy
	if action.NeedTerminate {
		plan.TerminateWorkflows = append(plan.TerminateWorkflows, snapshot.RunningWorkflows...)
	} else if action.NeedCancel {
		plan.CancelWorkflows = append(plan.CancelWorkflows, snapshot.RunningWorkflows...)
	}

	deferred := make(map[string]struct{}, len(action.NewBuffer))
	for _, start := range action.NewBuffer {
		deferred[start.RequestID] = struct{}{}
	}
	ready := make(map[string]struct{}, len(action.OverlappingStarts)+1)
	for _, start := range action.OverlappingStarts {
		ready[start.RequestID] = struct{}{}
	}
	if action.NonOverlappingStart.RequestID != "" {
		ready[action.NonOverlappingStart.RequestID] = struct{}{}
	}

	remainingActions := snapshot.RemainingActions
	for _, start := range snapshot.Starts {
		isPending := start.Attempt == 0 ||
			(start.Attempt == -1 && resolveOverlapPolicy(start.OverlapPolicy) == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
		if !isPending {
			plan.Decisions = append(plan.Decisions, alreadyProcessedBufferDecision(start))
			continue
		}
		decision := planPendingBufferDecision(
			start,
			snapshot,
			now,
			deferred,
			ready,
			action.NeedCancel || action.NeedTerminate,
			resolveOverlapPolicy,
			&remainingActions,
		)
		if decision.MissedCatchupMetric {
			plan.MissedCatchupByActionRunning[decision.MissedCatchupActionRunning]++
		}
		plan.Decisions = append(plan.Decisions, decision)
	}
	return plan
}

func planOverlapActions(
	buffer []BufferedStartSnapshot,
	isRunning bool,
	resolve func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy,
) overlapAction {
	action := overlapAction{OverlapSkippedByPolicy: make(map[enumspb.ScheduleOverlapPolicy]int64)}
	for _, start := range buffer {
		overlapPolicy := resolve(start.OverlapPolicy)
		if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
			action.OverlappingStarts = append(action.OverlappingStarts, start)
			continue
		}
		if !isRunning && action.NonOverlappingStart.RequestID == "" {
			action.NonOverlappingStart = start
			continue
		}
		switch overlapPolicy {
		case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
			action.OverlapSkipped++
			action.OverlapSkippedByPolicy[overlapPolicy]++
		case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
			if len(action.NewBuffer) == 0 {
				action.NewBuffer = append(action.NewBuffer, start)
			} else {
				action.OverlapSkipped++
				action.OverlapSkippedByPolicy[overlapPolicy]++
			}
		case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
			action.NewBuffer = append(action.NewBuffer, start)
		case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
			if isRunning {
				action.NeedCancel = true
				action.NewBuffer = append(action.NewBuffer, start)
			} else {
				action.NonOverlappingStart = start
			}
		case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
			if isRunning {
				action.NeedTerminate = true
				action.NewBuffer = append(action.NewBuffer, start)
			} else {
				action.NonOverlappingStart = start
			}
		default:
		}
	}
	if action.NeedCancel || action.NeedTerminate {
		action.OverlappingStarts = nil
	}
	return action
}

func alreadyProcessedBufferDecision(start BufferedStartSnapshot) BufferDecision {
	decision := BufferDecision{
		RequestID: start.RequestID,
		Expected:  start,
		Action:    BufferDecisionRetain,
		Reason:    BufferDecisionReasonAlreadyProcessed,
	}
	switch {
	case start.Completed:
		decision.Action = BufferDecisionCompleted
	case start.RunID != "":
		decision.Action = BufferDecisionRunning
	case start.Attempt > 1:
		decision.Action = BufferDecisionRetry
	default:
	}
	return decision
}

func planPendingBufferDecision(
	start BufferedStartSnapshot,
	snapshot BufferProcessingSnapshot,
	now time.Time,
	deferred map[string]struct{},
	ready map[string]struct{},
	needCancelOrTerminate bool,
	resolveOverlapPolicy func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy,
	remainingActions *int64,
) BufferDecision {
	decision := BufferDecision{RequestID: start.RequestID, Expected: start}
	if _, ok := deferred[start.RequestID]; ok {
		decision.Action = BufferDecisionDefer
		decision.Reason = BufferDecisionReasonOverlapPolicy
		return decision
	}
	if _, ok := ready[start.RequestID]; !ok {
		decision.Action = BufferDecisionDiscard
		decision.Reason = BufferDecisionReasonOverlapPolicy
		if needCancelOrTerminate {
			decision.Reason = BufferDecisionReasonCancelOrTerminate
			return decision
		}
		decision.OverlapPolicy = resolveOverlapPolicy(start.OverlapPolicy)
		decision.OverlapSkipped = decision.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP ||
			decision.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
		return decision
	}

	deadline := start.ActualTime.Add(max(snapshot.CatchupWindow, snapshot.MinimumCatchupWindow))
	canTakeScheduledAction := !snapshot.Paused && (!snapshot.LimitedActions || *remainingActions > 0)
	if !start.Manual && now.After(deadline) {
		decision.Action = BufferDecisionDiscard
		decision.Reason = BufferDecisionReasonMissedCatchupWindow
		if canTakeScheduledAction {
			decision.MissedCatchupMetric = true
			decision.MissedCatchupActionRunning = len(snapshot.RunningWorkflows) > 0 || start.DesiredTime.After(deadline)
		}
		return decision
	}
	if !start.Manual && !canTakeScheduledAction {
		decision.Action = BufferDecisionDiscard
		decision.Reason = BufferDecisionReasonPausedOrLimited
		return decision
	}
	if !start.Manual && snapshot.LimitedActions {
		*remainingActions--
	}
	decision.Action = BufferDecisionExecute
	return decision
}

func cloneBufferProcessingSnapshot(snapshot BufferProcessingSnapshot) BufferProcessingSnapshot {
	snapshot.Starts = slices.Clone(snapshot.Starts)
	snapshot.RunningWorkflows = slices.Clone(snapshot.RunningWorkflows)
	return snapshot
}
