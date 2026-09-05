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
