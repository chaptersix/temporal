package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestPolicyRegistrySubsetWithoutDefault(t *testing.T) {
	only := PolicyIdentity{Builtin: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}
	registry, err := NewPolicyRegistry([]PolicyDefinition{BuiltinPolicy(only.Builtin)}, PolicyIdentity{}, ExecutionOperations{})
	require.NoError(t, err)
	_, err = registry.Resolve(PolicyIdentity{}, PolicyIdentity{})
	require.Error(t, err)
	selected, err := registry.Resolve(PolicyIdentity{}, only)
	require.NoError(t, err)
	require.Equal(t, only, selected)
	_, err = registry.Resolve(PolicyIdentity{Builtin: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}, only)
	require.Error(t, err)
	_, err = NewPolicyRegistry([]PolicyDefinition{BuiltinPolicy(enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER)}, PolicyIdentity{}, ExecutionOperations{})
	require.Error(t, err)
	_, err = NewPolicyRegistry([]PolicyDefinition{BuiltinPolicy(only.Builtin)}, PolicyIdentity{Custom: "unknown"}, ExecutionOperations{})
	require.Error(t, err)
}

func TestActivityPolicySelections(t *testing.T) {
	registry := ActivityPolicies()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER} {
		_, err := registry.Resolve(PolicyIdentity{}, PolicyIdentity{Builtin: policy})
		require.NoError(t, err)
	}
	for _, selection := range []PolicyIdentity{{}, {Builtin: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER}, {Custom: "unknown"}, {Builtin: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, Custom: BufferLatestPolicyName}} {
		_, err := registry.Resolve(PolicyIdentity{}, selection)
		require.Error(t, err)
	}
	_, err := registry.Resolve(PolicyIdentity{}, PolicyIdentity{Custom: BufferLatestPolicyName})
	require.NoError(t, err)
	_, err = WorkflowPolicies().Resolve(PolicyIdentity{}, PolicyIdentity{Custom: BufferLatestPolicyName})
	require.Error(t, err)
}

func TestBufferLatestReplacesOnlyOlderWaitingOccurrences(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	latest := func(id string, at time.Time) BufferedStartSnapshot {
		return BufferedStartSnapshot{RequestID: id, ActualTime: at, CustomOverlapPolicy: BufferLatestPolicyName}
	}
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected", true: "running"}[active], func(t *testing.T) {
			starts := []BufferedStartSnapshot{latest("first", now), latest("second", now.Add(time.Second)), {RequestID: "other-policy", ActualTime: now, OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL}, latest("older", now.Add(-time.Second)), latest("tie-newer-insertion", now.Add(time.Second))}
			snapshot := BufferProcessingSnapshot{Starts: starts, Policies: ActivityPolicies(), CatchupWindow: time.Hour, LimitedActions: true, RemainingActions: 3}
			if active {
				snapshot.RunningExecutions = []ExecutionSnapshot{{TargetID: "active", RunID: "run"}}
			}
			plan := PlanBufferProcessing(snapshot, now)
			require.EqualValues(t, map[bool]int{false: 2, true: 3}[active], plan.OverlapSkipped)
			require.Equal(t, plan.OverlapSkipped, plan.OverlapSkippedByCustomPolicy[BufferLatestPolicyName])
			decisions := make(map[string]BufferDecision)
			for _, decision := range plan.Decisions {
				decisions[decision.RequestID] = decision
			}
			require.Equal(t, BufferDecisionDefer, decisions["tie-newer-insertion"].Action)
			require.Equal(t, BufferDecisionDefer, decisions["other-policy"].Action)
			require.Equal(t, BufferDecisionDiscard, decisions["older"].Action)
			require.False(t, decisions["second"].ConsumesScheduledAction)
			require.EqualValues(t, 3, snapshot.RemainingActions)
			require.Equal(t, starts, snapshot.Starts)
		})
	}
}

func TestPolicyPlannerReceivesDetachedSnapshots(t *testing.T) {
	id := PolicyIdentity{Custom: "test.only"}
	registry, err := NewPolicyRegistry([]PolicyDefinition{{Identity: id, Plan: func(snapshot PolicySnapshot) PolicyDecision {
		snapshot.Running[0].RunID = "changed"
		return PolicyDecision{Wait: true}
	}}}, id, ExecutionOperations{})
	require.NoError(t, err)
	snapshot := BufferProcessingSnapshot{Policies: registry, Starts: []BufferedStartSnapshot{{RequestID: "pending"}}, RunningExecutions: []ExecutionSnapshot{{RunID: "original"}}}
	plan := PlanBufferProcessing(snapshot, time.Time{})
	require.Equal(t, "original", snapshot.RunningExecutions[0].RunID)
	require.Equal(t, "original", plan.Snapshot.RunningExecutions[0].RunID)
	require.Equal(t, BufferDecisionDefer, plan.Decisions[0].Action)
}
