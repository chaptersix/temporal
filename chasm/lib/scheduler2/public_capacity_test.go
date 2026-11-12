package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestPublicConfiguredAutomaticCapacity(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{2, 5, 1000, 0, -1} {
		for _, policy := range []enumspb.ScheduleOverlapPolicy{enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL} {
			t.Run(fmt.Sprintf("limit%d/%s", limit, policy), func(t *testing.T) {
				t.Parallel()
				h := publicCapacityHarness(t, &limit, policy)
				const count = 7
				for range count {
					publicProjectionAdmission(h)
				}
				s, _ := h.root(false)
				accepted := count
				if limit > 0 {
					accepted = min(count, limit)
				}
				require.EqualValues(t, accepted, int64(len(s.Queue))+s.UnconfirmedStarts, "retained intent follows configured capacity")
				require.EqualValues(t, count-accepted, s.BufferDropped+s.CapacityDropped, "every excess automatic candidate is accounted")
			})
		}
	}
}

func publicCapacityHarness(t *testing.T, limit *int, policy enumspb.ScheduleOverlapPolicy) *treeHarness {
	t.Helper()
	h := executingPublicTree(t, nil)
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	library := NewNilLibrary()
	library.tweaks = func(string) schedulerconfig.Tweakables {
		tweaks := schedulerconfig.DefaultTweakables
		tweaks.MaxBufferSize = *limit
		return tweaks
	}
	require.NoError(t, registry.Register(library))
	h.registry = registry
	h.reload()
	s, _ := h.root(true)
	s.Policy = policy
	h.commit(true)
	return h
}

func TestPublicCapacityAcknowledgmentAndLiveReduction(t *testing.T) {
	t.Parallel()
	limit := 5
	h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = publicProjectionAdmission(h)
	}
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: ids[0], ChainID: "chain"}, true)
	s, ctx := h.root(false)
	require.EqualValues(t, 4, s.capacity(ctx).BufferedOperations, "acknowledged ALLOW_ALL does not retain buffer occupancy")
	limit = 2
	publicProjectionAdmission(h)
	s, _ = h.root(false)
	require.EqualValues(t, 4, s.UnconfirmedStarts, "live reduction does not discard previously accepted intents")
	require.EqualValues(t, 1, s.BufferDropped)
	limit = 0
	publicProjectionAdmission(h)
	s, _ = h.root(false)
	require.EqualValues(t, 5, s.UnconfirmedStarts, "disabled limit resumes admission")
	h.reload()
	s, ctx = h.root(false)
	require.EqualValues(t, 5, s.capacity(ctx).BufferedOperations)
}

func TestPublicCapacityTrackedCompletionAndFullBufferDrain(t *testing.T) {
	t.Parallel()
	limit := 2
	h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	id := publicProjectionAdmission(h)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: id, ChainID: "chain"}, true)
	publicProjectionAdmission(h)
	s, ctx := h.root(false)
	require.Len(t, s.Queue, 1)
	require.EqualValues(t, 1, s.capacity(ctx).BufferedOperations, "running tracked action remains buffered")
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, PublicCloseTime: time.Unix(123, 0)}, true)
	s, ctx = h.root(false)
	require.EqualValues(t, 1, s.capacity(ctx).BufferedOperations, "completed tracked result remains buffered")
	before := s.Remaining
	h.apply(decision.Event{Kind: decision.Drain, Incarnation: "incarnation", CatchupWindow: int64(time.Hour)}, true)
	s, ctx = h.root(false)
	require.Empty(t, s.Queue, "transferring an accepted waiter requires no additional capacity")
	require.EqualValues(t, 2, s.capacity(ctx).BufferedOperations)
	require.Equal(t, before, s.Remaining, "queued admission already consumed its action budget only when applicable")
}

func TestPublicCapacityCompletionRetentionUsesCanonicalCloseTime(t *testing.T) {
	t.Parallel()
	limit := 0
	h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	ids := make([]string, 11)
	for i := range ids {
		ids[i] = publicProjectionAdmission(h)
		// First arrival has latest close time; the second has the earliest.
		closeAt := time.Unix(int64(i+1), 0)
		if i == 0 {
			closeAt = time.Unix(100, 0)
		}
		h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: ids[i], ChainID: fmt.Sprintf("chain/%d", i), Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, PublicCloseTime: closeAt}, true)
	}
	h.reload()
	s, ctx := h.root(false)
	runtime := s.PublicRuntime.Get(ctx)
	require.Len(t, runtime.CompletedOperations, 10)
	require.Contains(t, s.Operations, ids[0], "late workflow close wins over callback arrival order")
	require.NotContains(t, s.Operations, ids[1], "oldest canonical close is evicted")
	require.Equal(t, ids[0], runtime.CompletedOperations[9].OperationId)
	require.EqualValues(t, 10, s.capacity(ctx).BufferedOperations)
}

func TestPublicCapacityLateOldestCompletionEvictsItself(t *testing.T) {
	t.Parallel()
	limit := 0
	h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
	for i := range 10 {
		id := publicProjectionAdmission(h)
		h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, PublicCloseTime: time.Unix(int64(i+100), 0)}, true)
	}
	late := publicProjectionAdmission(h)
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: "incarnation", OperationID: late, ChainID: "late-chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, PublicCloseTime: time.Unix(1, 0)}, true)
	s, ctx := h.root(false)
	require.Equal(t, late, s.OperationId)
	require.NotContains(t, s.Operations, late)
	require.Len(t, s.PublicRuntime.Get(ctx).CompletedOperations, 10)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: "incarnation", OperationID: late, ChainID: "late-chain", PublicRunID: "late-run"}, true)
	next := publicProjectionAdmission(h)
	require.NotEqual(t, late, next, "evicting completed latest record cannot block subsequent admission")
	s, _ = h.root(false)
	require.EqualValues(t, 1, s.Unfinished)
}

func TestPublicCapacityWaitRetainsManualAndRetriesConfiguration(t *testing.T) {
	t.Parallel()
	limit := 2
	h := publicCapacityHarness(t, &limit, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
	publicProjectionAdmission(h)
	publicProjectionAdmission(h)
	root, cc := h.root(true)
	root.Generator.Get(cc).Armed = false // Isolate accepted ingress from unrelated automatic scan work.
	h.commit(true)
	h.apply(decision.Event{Kind: decision.AcceptManual, PublicManualIngress: true, Incarnation: "incarnation", Nominal: h.now, Policy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}, true)
	s, ctx := h.root(false)
	require.Len(t, s.Manuals, 1)
	before := cloneNodes(h.durable)
	_, ready, wake := s.planWork(ctx)
	require.Nil(t, ready)
	require.True(t, wake.After(time.Unix(0, h.now)))
	require.Equal(t, before, h.durable)
	limit = 5
	plan, ready, _ := s.planWork(ctx)
	require.NotNil(t, ready)
	require.Equal(t, plan.OwnerKey, ready.key)
	require.Equal(t, "manual/1", ready.key)
	h.apply(decision.Event{Kind: decision.DispatchManual, Incarnation: "incarnation"}, false)
	require.Equal(t, before, h.durable, "failed publication retains command and all capacity accounting")
	h.apply(decision.Event{Kind: decision.DispatchManual, Incarnation: "incarnation"}, true)
	s, ctx = h.root(false)
	require.Empty(t, s.Manuals)
	require.EqualValues(t, 3, s.capacity(ctx).BufferedOperations)
}
