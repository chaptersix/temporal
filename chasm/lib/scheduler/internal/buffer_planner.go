package internal

import (
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// BufferDecisionAction describes how the apply phase should treat one buffered start.
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

// BufferDecisionReason records why the planner selected an action.
type BufferDecisionReason int

const (
	BufferDecisionReasonNone BufferDecisionReason = iota
	BufferDecisionReasonAlreadyProcessed
	BufferDecisionReasonOverlapPolicy
	BufferDecisionReasonCancelOrTerminate
	BufferDecisionReasonMissedCatchupWindow
	BufferDecisionReasonPausedOrLimited
)

// BufferedStartSnapshot is the value-only planner projection of a persisted BufferedStart.
// Keeping protobuf pointers out of the planner prevents planning from mutating live CHASM state.
type BufferedStartSnapshot struct {
	Occurrence    int
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

// WorkflowExecutionSnapshot identifies a running workflow without retaining a protobuf pointer.
type WorkflowExecutionSnapshot struct {
	WorkflowID string
	RunID      string
}

// BufferProcessingSnapshot contains all state read by one buffer-planning pass.
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

// BufferDecision describes the planned outcome for one exact BufferedStart snapshot.
type BufferDecision struct {
	RequestID string
	// Expected is matched against current persisted state before any mutation is applied.
	Expected BufferedStartSnapshot
	Action   BufferDecisionAction
	Reason   BufferDecisionReason
	// ConsumesScheduledAction defers the live capacity decrement until after revalidation.
	ConsumesScheduledAction    bool
	MissedCatchupMetric        bool
	MissedCatchupActionRunning bool
}

// MutatesState reports whether applying the decision changes the persisted buffer.
func (d BufferDecision) MutatesState() bool {
	return d.Action == BufferDecisionExecute || d.Action == BufferDecisionDiscard || d.Action == BufferDecisionDefer
}

// BufferPlan is an ordered, value-based description of a buffer-processing pass.
// Snapshot anchors whole-plan validation; each decision also carries its exact expected start.
type BufferPlan struct {
	Snapshot               BufferProcessingSnapshot
	Decisions              []BufferDecision
	CancelWorkflows        []WorkflowExecutionSnapshot
	TerminateWorkflows     []WorkflowExecutionSnapshot
	OverlapSkipped         int64
	OverlapSkippedByPolicy map[enumspb.ScheduleOverlapPolicy]int64
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

// PlanBufferProcessing computes buffer outcomes without mutating snapshot or live scheduler state.
func PlanBufferProcessing(snapshot BufferProcessingSnapshot, now time.Time) BufferPlan {
	snapshot.Starts = slices.Clone(snapshot.Starts)
	for index := range snapshot.Starts {
		snapshot.Starts[index].Occurrence = index
	}
	plan := BufferPlan{
		Snapshot:               cloneBufferProcessingSnapshot(snapshot),
		OverlapSkippedByPolicy: make(map[enumspb.ScheduleOverlapPolicy]int64),
	}
	resolveOverlapPolicy := func(policy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
		if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
			return snapshot.DefaultOverlapPolicy
		}
		return policy
	}

	pending := pendingBufferedStarts(snapshot.Starts, resolveOverlapPolicy)
	action := planOverlapActions(pending, len(snapshot.RunningWorkflows) > 0, resolveOverlapPolicy)
	plan.OverlapSkipped = action.OverlapSkipped
	plan.OverlapSkippedByPolicy = action.OverlapSkippedByPolicy
	if action.NeedTerminate {
		plan.TerminateWorkflows = append(plan.TerminateWorkflows, snapshot.RunningWorkflows...)
	} else if action.NeedCancel {
		plan.CancelWorkflows = append(plan.CancelWorkflows, snapshot.RunningWorkflows...)
	}

	keep := make(map[BufferedStartSnapshot]struct{}, len(action.NewBuffer)+len(action.OverlappingStarts)+1)
	for _, start := range action.NewBuffer {
		keep[start] = struct{}{}
	}
	readyStarts := slices.Clone(action.OverlappingStarts)
	if action.NonOverlappingStart.RequestID != "" {
		readyStarts = append(readyStarts, action.NonOverlappingStart)
	}

	// All ready decisions share this task-wide budget. Passing its address preserves
	// legacy ready-start ordering while keeping the input snapshot immutable.
	remainingActions := snapshot.RemainingActions
	ready := make(map[BufferedStartSnapshot]struct{}, len(readyStarts))
	readyDecisions := make([]BufferDecision, 0, len(readyStarts))
	readyOccurrences := make(map[BufferedStartSnapshot]int, len(readyStarts))
	for _, start := range readyStarts {
		decision := planReadyBufferDecision(start, snapshot, now, &remainingActions)
		if decision.Action == BufferDecisionExecute {
			keep[start] = struct{}{}
			ready[start] = struct{}{}
		}
		readyDecisions = append(readyDecisions, decision)
		readyOccurrences[start]++
	}

	for _, decision := range readyDecisions {
		decision.Action = finalPendingBufferAction(decision.Expected, keep, ready)
		plan.Decisions = append(plan.Decisions, decision)
	}
	for _, start := range snapshot.Starts {
		if !isPendingBufferedStart(start, resolveOverlapPolicy) {
			plan.Decisions = append(plan.Decisions, alreadyProcessedBufferDecision(start))
			continue
		}
		if readyOccurrences[start] > 0 {
			readyOccurrences[start]--
			continue
		}
		decision := BufferDecision{
			RequestID: start.RequestID,
			Expected:  start,
			Action:    finalPendingBufferAction(start, keep, ready),
			Reason:    BufferDecisionReasonOverlapPolicy,
		}
		if decision.Action == BufferDecisionDiscard && (action.NeedCancel || action.NeedTerminate) {
			decision.Reason = BufferDecisionReasonCancelOrTerminate
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

func pendingBufferedStarts(
	starts []BufferedStartSnapshot,
	resolve func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy,
) []BufferedStartSnapshot {
	pending := make([]BufferedStartSnapshot, 0, len(starts))
	for _, start := range starts {
		if isPendingBufferedStart(start, resolve) {
			pending = append(pending, start)
		}
	}
	return pending
}

func isPendingBufferedStart(
	start BufferedStartSnapshot,
	resolve func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy,
) bool {
	return start.Attempt == bufferedStartUnprocessedAttempt ||
		(start.Attempt == bufferedStartDeferredAttempt &&
			resolve(start.OverlapPolicy) == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
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
	case start.Attempt > bufferedStartFirstExecutionAttempt:
		decision.Action = BufferDecisionRetry
	default:
	}
	return decision
}

func planReadyBufferDecision(
	start BufferedStartSnapshot,
	snapshot BufferProcessingSnapshot,
	now time.Time,
	remainingActions *int64,
) BufferDecision {
	decision := BufferDecision{RequestID: start.RequestID, Expected: start}
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
	decision.ConsumesScheduledAction = !start.Manual
	return decision
}

func finalPendingBufferAction(
	start BufferedStartSnapshot,
	keep map[BufferedStartSnapshot]struct{},
	ready map[BufferedStartSnapshot]struct{},
) BufferDecisionAction {
	if _, ok := ready[start]; ok {
		return BufferDecisionExecute
	}
	if _, ok := keep[start]; ok {
		return BufferDecisionDefer
	}
	return BufferDecisionDiscard
}

func cloneBufferProcessingSnapshot(snapshot BufferProcessingSnapshot) BufferProcessingSnapshot {
	snapshot.Starts = slices.Clone(snapshot.Starts)
	snapshot.RunningWorkflows = slices.Clone(snapshot.RunningWorkflows)
	return snapshot
}
