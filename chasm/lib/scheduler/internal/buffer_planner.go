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
	Occurrence          int
	RequestID           string
	TargetID            string
	Kind                enumspb.ExecutionType
	RunID               string
	Attempt             int64
	Manual              bool
	OverlapPolicy       enumspb.ScheduleOverlapPolicy
	CustomOverlapPolicy string
	ActualTime          time.Time
	DesiredTime         time.Time
	Completed           bool
}

// ExecutionSnapshot identifies a running workflow without retaining a protobuf pointer.
type ExecutionSnapshot struct {
	TargetID string
	Kind     enumspb.ExecutionType
	RunID    string
}

// BufferProcessingSnapshot contains all state read by one buffer-planning pass.
type BufferProcessingSnapshot struct {
	Starts                     []BufferedStartSnapshot
	RunningExecutions          []ExecutionSnapshot
	DefaultOverlapPolicy       enumspb.ScheduleOverlapPolicy
	DefaultCustomOverlapPolicy string
	Policies                   *PolicyRegistry
	CustomOverlapPolicy        string
	CatchupWindow              time.Duration
	MinimumCatchupWindow       time.Duration
	Paused                     bool
	LimitedActions             bool
	RemainingActions           int64
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
	Snapshot                     BufferProcessingSnapshot
	Decisions                    []BufferDecision
	CancelExecutions             []ExecutionSnapshot
	TerminateExecutions          []ExecutionSnapshot
	OverlapSkipped               int64
	OverlapSkippedByPolicy       map[enumspb.ScheduleOverlapPolicy]int64
	OverlapSkippedByCustomPolicy map[string]int64
}

type overlapAction struct {
	OverlappingStarts            []BufferedStartSnapshot
	NonOverlappingStart          BufferedStartSnapshot
	NewBuffer                    []BufferedStartSnapshot
	NeedCancel                   bool
	NeedTerminate                bool
	OverlapSkipped               int64
	OverlapSkippedByPolicy       map[enumspb.ScheduleOverlapPolicy]int64
	OverlapSkippedByCustomPolicy map[string]int64
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
	action := planRegisteredOverlapActions(pending, snapshot, now)
	plan.OverlapSkipped = action.OverlapSkipped
	plan.OverlapSkippedByPolicy = action.OverlapSkippedByPolicy
	plan.OverlapSkippedByCustomPolicy = action.OverlapSkippedByCustomPolicy
	if action.NeedTerminate {
		plan.TerminateExecutions = append(plan.TerminateExecutions, snapshot.RunningExecutions...)
	} else if action.NeedCancel {
		plan.CancelExecutions = append(plan.CancelExecutions, snapshot.RunningExecutions...)
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
			(resolve(start.OverlapPolicy) == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || start.CustomOverlapPolicy != ""))
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
			decision.MissedCatchupActionRunning = len(snapshot.RunningExecutions) > 0 || start.DesiredTime.After(deadline)
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
	if snapshot.Policies != nil {
		snapshot.Policies = snapshot.Policies.clone()
	}
	snapshot.RunningExecutions = slices.Clone(snapshot.RunningExecutions)
	return snapshot
}

func planRegisteredOverlapActions(buffer []BufferedStartSnapshot, snapshot BufferProcessingSnapshot, now time.Time) overlapAction {
	registry := snapshot.Policies
	if registry == nil {
		registry = WorkflowPolicies()
	}
	action := overlapAction{OverlapSkippedByPolicy: make(map[enumspb.ScheduleOverlapPolicy]int64), OverlapSkippedByCustomPolicy: make(map[string]int64)}
	recordSkip := func(start BufferedStartSnapshot) {
		action.OverlapSkipped++
		if start.CustomOverlapPolicy != "" {
			action.OverlapSkippedByCustomPolicy[start.CustomOverlapPolicy]++
		} else {
			action.OverlapSkippedByPolicy[ResolveOverlapPolicy(start.OverlapPolicy, snapshot.DefaultOverlapPolicy)]++
		}
	}
	for _, start := range buffer {
		id, err := registry.Resolve(PolicyIdentity{Builtin: start.OverlapPolicy, Custom: start.CustomOverlapPolicy}, PolicyIdentity{Builtin: snapshot.DefaultOverlapPolicy, Custom: snapshot.DefaultCustomOverlapPolicy})
		if err != nil {
			recordSkip(start)
			continue
		}
		var selected *BufferedStartSnapshot
		if action.NonOverlappingStart.RequestID != "" {
			value := action.NonOverlappingStart
			selected = &value
		}
		decision := registry.plan(id, PolicySnapshot{Occurrence: start, Running: snapshot.RunningExecutions, Waiting: action.NewBuffer, Selected: selected, Now: now})
		for _, replaced := range decision.Replace {
			action.NewBuffer = slices.DeleteFunc(action.NewBuffer, func(waiting BufferedStartSnapshot) bool { return waiting == replaced })
			recordSkip(replaced)
		}
		switch {
		case decision.Start && decision.Overlap:
			action.OverlappingStarts = append(action.OverlappingStarts, start)
		case decision.Start:
			action.NonOverlappingStart = start
		case decision.Wait:
			action.NewBuffer = append(action.NewBuffer, start)
		default:
			recordSkip(start)
		}
		action.NeedCancel = action.NeedCancel || decision.Cancel
		action.NeedTerminate = action.NeedTerminate || decision.Terminate
	}
	if action.NeedCancel || action.NeedTerminate {
		action.OverlappingStarts = nil
	}
	return action
}
