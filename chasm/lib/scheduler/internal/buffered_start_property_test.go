//go:build property_test

package internal

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

var propertyNow = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

func TestPropertyBufferedStartClassificationAndTransitions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		start := drawBufferedStart(t, propertyNow)
		require.Equal(t, classifyBufferedStartIndependently(start, propertyNow), ClassifyBufferedStart(start, propertyNow))
	})

	for _, transition := range []struct {
		name  string
		apply func(*schedulespb.BufferedStart)
		want  BufferedStartState
	}{
		{"unprocessed", MarkStartUnprocessed, BufferedStartStateUnprocessed},
		{"deferred", MarkStartDeferred, BufferedStartStateDeferred},
		{"ready", MarkStartReady, BufferedStartStateReady},
		{"retrying", func(start *schedulespb.BufferedStart) {
			MarkStartRetrying(start, 2, timestamppb.New(propertyNow.Add(time.Second)))
		}, BufferedStartStateBackingOff},
		{"started", func(start *schedulespb.BufferedStart) { MarkStartStarted(start, "run", timestamppb.New(propertyNow)) }, BufferedStartStateStarted},
		{"completed", func(start *schedulespb.BufferedStart) {
			MarkStartStarted(start, "run", timestamppb.New(propertyNow))
			MarkStartCompleted(start, &schedulespb.CompletedResult{})
		}, BufferedStartStateCompleted},
	} {
		t.Run(transition.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				start := validStart(t, BufferedStartStateReady, propertyNow)
				before := proto.Clone(start).(*schedulespb.BufferedStart)
				transition.apply(start)
				require.Equal(t, transition.want, ClassifyBufferedStart(start, propertyNow))
				require.NoError(t, assertTransitionOwnership(before, start, transition.name))
			})
		})
	}
}

func TestPropertyRetryBackoffBoundary(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		attempt := rapid.SampledFrom([]int64{1, 2, 3, 10}).Draw(t, "attempt")
		start := validStart(t, BufferedStartStateReady, propertyNow)
		MarkStartRetrying(start, attempt, timestamppb.New(propertyNow))
		require.Equal(t, BufferedStartStateReady, ClassifyBufferedStart(start, propertyNow))
		MarkStartRetrying(start, attempt+1, timestamppb.New(propertyNow.Add(time.Nanosecond)))
		require.Equal(t, BufferedStartStateBackingOff, ClassifyBufferedStart(start, propertyNow))
		require.NoError(t, assertExactRetry(start, attempt+1, propertyNow.Add(time.Nanosecond)))
	})
}

func TestPropertyBufferPlannerInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		snapshot := drawSnapshot(t)
		before := cloneSnapshot(snapshot)
		plan := PlanBufferProcessing(snapshot, propertyNow)
		require.Equal(t, before, snapshot)
		require.Equal(t, plan, PlanBufferProcessing(snapshot, propertyNow))
		require.NoError(t, assertPlanOccurrenceConservation(snapshot, plan))
		require.NoError(t, assertPlanAccounting(snapshot, plan))
		require.NoError(t, assertEffectOrdering(plan))
		require.NoError(t, assertBufferOneOccupancy(snapshot, plan))

		if len(plan.Snapshot.Starts) > 0 {
			plan.Snapshot.Starts[0].RequestID = "changed"
			require.NotEqual(t, "changed", snapshot.Starts[0].RequestID)
		}
		if len(plan.Decisions) > 0 {
			plan.Decisions[0].Expected.RequestID = "changed"
			require.NotEqual(t, "changed", snapshot.Starts[0].RequestID)
		}
	})
}

func TestPropertyDirectedDeferredBufferOneOccupancy(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		policy := rapid.SampledFrom([]enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED}).Draw(t, "policy")
		snapshot := BufferProcessingSnapshot{
			Starts: []BufferedStartSnapshot{
				{RequestID: "deferred", WorkflowID: "deferred", Attempt: -1, OverlapPolicy: policy, ActualTime: propertyNow},
				{RequestID: "new", WorkflowID: "new", OverlapPolicy: policy, ActualTime: propertyNow},
			},
			RunningWorkflows:     []WorkflowExecutionSnapshot{{WorkflowID: "running", RunID: "run"}},
			DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			CatchupWindow:        time.Hour,
			MinimumCatchupWindow: time.Second,
		}
		plan := PlanBufferProcessing(snapshot, propertyNow)
		require.NoError(t, assertBufferOneOccupancy(snapshot, plan))
	})
}

func TestPropertyInvariantHelpersRejectInvalidSnapshots(t *testing.T) {
	snapshot := BufferProcessingSnapshot{Starts: []BufferedStartSnapshot{{RequestID: "one"}}}
	plan := BufferPlan{Decisions: []BufferDecision{{RequestID: "one", Expected: snapshot.Starts[0]}, {RequestID: "one", Expected: snapshot.Starts[0]}}}
	require.Error(t, assertPlanOccurrenceConservation(snapshot, plan))
	require.Error(t, assertExactRetry(&schedulespb.BufferedStart{Attempt: 1}, 2, propertyNow))
	require.Error(t, assertTransitionOwnership(&schedulespb.BufferedStart{RequestId: "one"}, &schedulespb.BufferedStart{RequestId: "two"}, "ready"))
	require.Error(t, assertEffectOrdering(BufferPlan{TerminateWorkflows: []WorkflowExecutionSnapshot{{WorkflowID: "same"}}, CancelWorkflows: []WorkflowExecutionSnapshot{{WorkflowID: "same"}}}))
	require.Error(t, assertBufferOneOccupancy(BufferProcessingSnapshot{Starts: []BufferedStartSnapshot{{RequestID: "one", Attempt: -1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}, {RequestID: "two", Attempt: -1, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}}}, BufferPlan{Decisions: []BufferDecision{{Action: BufferDecisionDefer, Expected: BufferedStartSnapshot{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}}, {Action: BufferDecisionDefer, Expected: BufferedStartSnapshot{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE}}}}))
}

func drawBufferedStart(t *rapid.T, now time.Time) *schedulespb.BufferedStart {
	states := []BufferedStartState{BufferedStartStateInvalid, BufferedStartStateUnprocessed, BufferedStartStateDeferred, BufferedStartStateReady, BufferedStartStateBackingOff, BufferedStartStateStarted, BufferedStartStateCompleted}
	state := rapid.SampledFrom(states).Draw(t, "state")
	if state != BufferedStartStateInvalid {
		return validStart(t, state, now)
	}
	return &schedulespb.BufferedStart{
		RequestId: rapid.StringN(0, 4, 4).Draw(t, "request"),
		Attempt:   rapid.SampledFrom([]int64{-2, -1, 0, 1, 2}).Draw(t, "attempt"),
		RunId:     rapid.StringN(0, 1, 4).Draw(t, "run"),
		BackoffTime: func() *timestamppb.Timestamp {
			if rapid.Bool().Draw(t, "backoff") {
				return timestamppb.New(now.Add(time.Second))
			}
			return nil
		}(),
		Completed: func() *schedulespb.CompletedResult {
			if rapid.Bool().Draw(t, "completed") {
				return &schedulespb.CompletedResult{}
			}
			return nil
		}(),
	}
}

func validStart(t *rapid.T, state BufferedStartState, now time.Time) *schedulespb.BufferedStart {
	start := &schedulespb.BufferedStart{RequestId: rapid.StringN(1, 1, 5).Draw(t, "request"), WorkflowId: "workflow", NominalTime: timestamppb.New(now), ActualTime: timestamppb.New(now), DesiredTime: timestamppb.New(now)}
	switch state {
	case BufferedStartStateDeferred:
		start.Attempt = -1
	case BufferedStartStateReady:
		start.Attempt = 1
	case BufferedStartStateBackingOff:
		start.Attempt, start.BackoffTime = 2, timestamppb.New(now.Add(time.Second))
	case BufferedStartStateStarted:
		start.Attempt, start.RunId, start.StartTime = 1, "run", timestamppb.New(now)
	case BufferedStartStateCompleted:
		start.Attempt, start.RunId, start.StartTime, start.Completed = 1, "run", timestamppb.New(now), &schedulespb.CompletedResult{}
	}
	return start
}

func drawSnapshot(t *rapid.T) BufferProcessingSnapshot {
	policies := []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}
	starts := make([]BufferedStartSnapshot, rapid.IntRange(0, 6).Draw(t, "count"))
	for index := range starts {
		starts[index] = BufferedStartSnapshot{RequestID: rapid.SampledFrom([]string{"duplicate", "one", "two", "three"}).Draw(t, fmt.Sprintf("request-%d", index)), WorkflowID: fmt.Sprintf("workflow-%d", index), Attempt: rapid.SampledFrom([]int64{-1, 0, 1, 2}).Draw(t, fmt.Sprintf("attempt-%d", index)), Manual: rapid.Bool().Draw(t, fmt.Sprintf("manual-%d", index)), OverlapPolicy: rapid.SampledFrom(policies).Draw(t, fmt.Sprintf("policy-%d", index)), ActualTime: propertyNow.Add(time.Duration(rapid.SampledFrom([]int{-2, -1, 0, 1}).Draw(t, fmt.Sprintf("actual-%d", index))) * time.Hour), DesiredTime: propertyNow}
	}
	return BufferProcessingSnapshot{Starts: starts, DefaultOverlapPolicy: rapid.SampledFrom(policies).Draw(t, "default-policy"), CatchupWindow: time.Hour, MinimumCatchupWindow: time.Second, Paused: rapid.Bool().Draw(t, "paused"), LimitedActions: rapid.Bool().Draw(t, "limited"), RemainingActions: rapid.SampledFrom([]int64{0, 1, 2, 7}).Draw(t, "remaining")}
}

func cloneSnapshot(snapshot BufferProcessingSnapshot) BufferProcessingSnapshot {
	if snapshot.Starts != nil {
		starts := make([]BufferedStartSnapshot, len(snapshot.Starts))
		copy(starts, snapshot.Starts)
		snapshot.Starts = starts
	}
	if snapshot.RunningWorkflows != nil {
		running := make([]WorkflowExecutionSnapshot, len(snapshot.RunningWorkflows))
		copy(running, snapshot.RunningWorkflows)
		snapshot.RunningWorkflows = running
	}
	return snapshot
}

func classifyBufferedStartIndependently(start *schedulespb.BufferedStart, now time.Time) BufferedStartState {
	if start == nil || start.GetAttempt() < -1 {
		return BufferedStartStateInvalid
	}
	hasRun, hasCompleted, hasBackoff := start.GetRunId() != "", start.GetCompleted() != nil, start.GetBackoffTime() != nil
	switch start.GetAttempt() {
	case -1:
		if hasRun || hasCompleted || hasBackoff {
			return BufferedStartStateInvalid
		}
		return BufferedStartStateDeferred
	case 0:
		if hasRun || hasCompleted || hasBackoff {
			return BufferedStartStateInvalid
		}
		return BufferedStartStateUnprocessed
	}
	if hasRun {
		if hasCompleted {
			return BufferedStartStateCompleted
		}
		return BufferedStartStateStarted
	}
	if hasCompleted {
		return BufferedStartStateInvalid
	}
	if hasBackoff && start.GetBackoffTime().AsTime().After(now) {
		return BufferedStartStateBackingOff
	}
	return BufferedStartStateReady
}

func assertTransitionOwnership(before, after *schedulespb.BufferedStart, transition string) error {
	if before.GetRequestId() != after.GetRequestId() || before.GetWorkflowId() != after.GetWorkflowId() || before.GetManual() != after.GetManual() || before.GetOverlapPolicy() != after.GetOverlapPolicy() {
		return fmt.Errorf("%s changed an unowned field", transition)
	}
	return nil
}
func assertExactRetry(start *schedulespb.BufferedStart, attempt int64, deadline time.Time) error {
	if start.GetAttempt() != attempt || start.GetBackoffTime() == nil || !start.GetBackoffTime().AsTime().Equal(deadline) {
		return fmt.Errorf("retry state does not match exact attempt/deadline")
	}
	return nil
}
func assertPlanOccurrenceConservation(snapshot BufferProcessingSnapshot, plan BufferPlan) error {
	if len(snapshot.Starts) != len(plan.Decisions) {
		return fmt.Errorf("starts=%d decisions=%d", len(snapshot.Starts), len(plan.Decisions))
	}
	remaining := append([]BufferedStartSnapshot(nil), snapshot.Starts...)
	for index := range remaining {
		remaining[index].Occurrence = index
	}
	for index, decision := range plan.Decisions {
		match := -1
		for candidate, start := range remaining {
			if reflect.DeepEqual(decision.Expected, start) {
				match = candidate
				break
			}
		}
		if match < 0 {
			return fmt.Errorf("decision %d does not preserve an input occurrence", index)
		}
		remaining = append(remaining[:match], remaining[match+1:]...)
	}
	return nil
}
func assertPlanAccounting(snapshot BufferProcessingSnapshot, plan BufferPlan) error {
	var consumed int64
	for _, decision := range plan.Decisions {
		if decision.ConsumesScheduledAction {
			consumed++
		}
	}
	if snapshot.LimitedActions && consumed > snapshot.RemainingActions {
		return fmt.Errorf("consumed=%d remaining=%d", consumed, snapshot.RemainingActions)
	}
	return nil
}
func assertEffectOrdering(plan BufferPlan) error {
	terminated := make(map[string]struct{}, len(plan.TerminateWorkflows))
	for _, workflow := range plan.TerminateWorkflows {
		terminated[workflow.WorkflowID+workflow.RunID] = struct{}{}
	}
	for _, workflow := range plan.CancelWorkflows {
		if _, ok := terminated[workflow.WorkflowID+workflow.RunID]; ok {
			return fmt.Errorf("workflow has both terminate and cancel effects")
		}
	}
	return nil
}
func assertBufferOneOccupancy(snapshot BufferProcessingSnapshot, plan BufferPlan) error {
	pending := 0
	for _, decision := range plan.Decisions {
		policy := decision.Expected.OverlapPolicy
		if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
			policy = snapshot.DefaultOverlapPolicy
		}
		if decision.Action == BufferDecisionDefer && policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE {
			pending++
		}
	}
	if pending > 1 {
		return fmt.Errorf("buffer one retained %d pending occurrences: %#v", pending, plan.Decisions)
	}
	return nil
}
