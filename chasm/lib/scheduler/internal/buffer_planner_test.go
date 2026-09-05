package internal

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestPlanBufferProcessing_OverlapPolicies(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		policy         enumspb.ScheduleOverlapPolicy
		action         BufferDecisionAction
		reason         BufferDecisionReason
		cancelCount    int
		terminateCount int
		overlapSkipped int64
	}{
		{name: "allow all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, action: BufferDecisionExecute},
		{name: "skip", policy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, action: BufferDecisionDiscard, reason: BufferDecisionReasonOverlapPolicy, overlapSkipped: 1},
		{name: "buffer one", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, action: BufferDecisionDefer, reason: BufferDecisionReasonOverlapPolicy},
		{name: "buffer all", policy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, action: BufferDecisionDefer, reason: BufferDecisionReasonOverlapPolicy},
		{name: "cancel other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, action: BufferDecisionDefer, reason: BufferDecisionReasonOverlapPolicy, cancelCount: 1},
		{name: "terminate other", policy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, action: BufferDecisionDefer, reason: BufferDecisionReasonOverlapPolicy, terminateCount: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := BufferProcessingSnapshot{
				Starts: []BufferedStartSnapshot{{
					RequestID:     "request",
					OverlapPolicy: test.policy,
					ActualTime:    now,
					DesiredTime:   now,
				}},
				RunningExecutions:    []ExecutionSnapshot{{TargetID: "running", RunID: "run"}},
				DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				CatchupWindow:        time.Hour,
				MinimumCatchupWindow: 5 * time.Second,
			}

			plan := PlanBufferProcessing(snapshot, now)

			require.Len(t, plan.Decisions, 1)
			require.Equal(t, test.action, plan.Decisions[0].Action)
			require.Equal(t, test.reason, plan.Decisions[0].Reason)
			require.Len(t, plan.CancelExecutions, test.cancelCount)
			require.Len(t, plan.TerminateExecutions, test.terminateCount)
			require.Equal(t, test.overlapSkipped, plan.OverlapSkipped)
		})
	}
}

func TestPlanBufferProcessing_TimeAndCapacity(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	snapshot := BufferProcessingSnapshot{
		Starts: []BufferedStartSnapshot{
			{RequestID: "expired", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now.Add(-2 * time.Hour), DesiredTime: now.Add(-2 * time.Hour)},
			{RequestID: "automatic", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now, DesiredTime: now},
			{RequestID: "limited", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now, DesiredTime: now},
			{RequestID: "manual", Manual: true, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now.Add(-24 * time.Hour), DesiredTime: now},
		},
		DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow:        time.Hour,
		MinimumCatchupWindow: 5 * time.Second,
		LimitedActions:       true,
		RemainingActions:     1,
	}

	plan := PlanBufferProcessing(snapshot, now)

	require.Equal(t, []BufferDecisionAction{
		BufferDecisionDiscard,
		BufferDecisionExecute,
		BufferDecisionDiscard,
		BufferDecisionExecute,
	}, decisionActions(plan.Decisions))
	require.Equal(t, BufferDecisionReasonMissedCatchupWindow, plan.Decisions[0].Reason)
	require.Equal(t, BufferDecisionReasonPausedOrLimited, plan.Decisions[2].Reason)
	require.True(t, plan.Decisions[0].MissedCatchupMetric)
	require.False(t, plan.Decisions[0].MissedCatchupActionRunning)
}

func TestPlanBufferProcessing_PreservesLegacyReadyOrder(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	snapshot := BufferProcessingSnapshot{
		Starts: []BufferedStartSnapshot{
			{RequestID: "non-overlapping", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, ActualTime: now},
			{RequestID: "allow-all", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now},
		},
		DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow:        time.Hour,
		MinimumCatchupWindow: 5 * time.Second,
		LimitedActions:       true,
		RemainingActions:     1,
	}

	plan := PlanBufferProcessing(snapshot, now)

	require.Len(t, plan.Decisions, 2)
	require.Equal(t, "allow-all", plan.Decisions[0].RequestID)
	require.Equal(t, BufferDecisionExecute, plan.Decisions[0].Action)
	require.True(t, plan.Decisions[0].ConsumesScheduledAction)
	require.Equal(t, "non-overlapping", plan.Decisions[1].RequestID)
	require.Equal(t, BufferDecisionDiscard, plan.Decisions[1].Action)
	require.Equal(t, BufferDecisionReasonPausedOrLimited, plan.Decisions[1].Reason)
}

func TestPlanBufferProcessing_PausedAndProcessedStates(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	snapshot := BufferProcessingSnapshot{
		Starts: []BufferedStartSnapshot{
			{RequestID: "automatic", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now},
			{RequestID: "manual", Manual: true, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now},
			{RequestID: "backing-off", Attempt: 2, ActualTime: now},
			{RequestID: "running", Attempt: 1, RunID: "run", ActualTime: now},
			{RequestID: "completed", Attempt: 1, RunID: "run", Completed: true, ActualTime: now},
		},
		DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow:        time.Hour,
		MinimumCatchupWindow: 5 * time.Second,
		Paused:               true,
	}

	plan := PlanBufferProcessing(snapshot, now)

	require.Equal(t, []BufferDecisionAction{
		BufferDecisionDiscard,
		BufferDecisionExecute,
		BufferDecisionRetry,
		BufferDecisionRunning,
		BufferDecisionCompleted,
	}, decisionActions(plan.Decisions))
}

func TestPlanBufferProcessing_DoesNotMutateOrAliasInput(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	snapshot := BufferProcessingSnapshot{
		Starts: []BufferedStartSnapshot{{
			RequestID: "request", OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, ActualTime: now,
		}},
		DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow:        time.Hour,
		MinimumCatchupWindow: 5 * time.Second,
	}
	want := BufferProcessingSnapshot{
		Starts:               append([]BufferedStartSnapshot(nil), snapshot.Starts...),
		DefaultOverlapPolicy: snapshot.DefaultOverlapPolicy,
		CatchupWindow:        snapshot.CatchupWindow,
		MinimumCatchupWindow: snapshot.MinimumCatchupWindow,
	}

	plan := PlanBufferProcessing(snapshot, now)

	require.True(t, reflect.DeepEqual(want, snapshot))
	plan.Decisions[0].Expected.RequestID = "changed"
	plan.Snapshot.Starts[0].RequestID = "changed-again"
	require.Equal(t, "request", snapshot.Starts[0].RequestID)
}

func TestPlanBufferProcessing_BufferOneDoesNotMergeDuplicateRequestIDs(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	snapshot := BufferProcessingSnapshot{
		Starts: []BufferedStartSnapshot{
			{RequestID: "duplicate", TargetID: "first", Attempt: bufferedStartDeferredAttempt, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, ActualTime: now},
			{RequestID: "duplicate", TargetID: "second", Attempt: bufferedStartDeferredAttempt, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, ActualTime: now},
		},
		RunningExecutions:    []ExecutionSnapshot{{TargetID: "running", RunID: "run"}},
		DefaultOverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		CatchupWindow:        time.Hour,
		MinimumCatchupWindow: time.Second,
	}

	plan := PlanBufferProcessing(snapshot, now)

	require.Equal(t, []BufferDecisionAction{BufferDecisionDefer, BufferDecisionDiscard}, decisionActions(plan.Decisions))
}

func decisionActions(decisions []BufferDecision) []BufferDecisionAction {
	actions := make([]BufferDecisionAction, 0, len(decisions))
	for _, decision := range decisions {
		actions = append(actions, decision.Action)
	}
	return actions
}
