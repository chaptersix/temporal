//go:build test_dep

package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func TestSelectedListTransitionAvoidsDefinitionAndPreservesPreview(t *testing.T) {
	t.Parallel()
	h, _ := newBoundedRuntimeTree(t, 0, true)
	s, raw := h.root(true)
	s.PauseOnFailure = true
	require.NoError(t, s.AcceptManual(raw.(chasm.MutableContext), &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}))
	h.commit(true)
	s, _ = h.root(false)
	for step := 0; step < 8 && s.OperationStore.OperationCount == 0; step++ {
		executed, err := deliverGeneration(h, time.Unix(0, h.now))
		require.NoError(t, err)
		require.Positive(t, executed)
		commitGeneration(h, true)
		s, _ = h.root(false)
	}
	require.EqualValues(t, 1, s.OperationStore.OperationCount)
	s, read := h.root(false)
	ready, err := s.readOperationReady(read)
	require.NoError(t, err)
	require.NotNil(t, ready)
	id := ready.Operation.Id
	owner := [32]byte{1}
	notes := "unchanged"
	stamp := stageListTest(t, h, owner, listTestStatic(t, "workflow", 2), 2, &notes)
	finishListTest(t, h, owner, stamp, true)
	pending := stageListTest(t, h, owner, nil, 2, nil)
	s, _ = h.root(true)
	s.PauseOnFailure = true
	s.Paused = false
	h.commit(true)
	for _, path := range []string{"PublicSchedule", "PublicList", "ListStaticA", "ListNotes"} {
		h.durable[path].Data.Data = []byte{0xff}
	}
	h.reload()
	for _, event := range []decision.Event{
		{Kind: decision.Acknowledged, OperationID: id, ChainID: "chain", PublicRunID: "run", StartObserved: true, StartObservedAt: h.now},
		{Kind: decision.Callback, OperationID: id, ChainID: "chain", Outcome: decision.Failure, Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED},
	} {
		s, raw := h.root(true)
		version := s.MemoVersion(raw)
		event.Incarnation = s.Incarnation
		require.NoError(t, s.applyTransition(raw.(chasm.MutableContext), event))
		require.Greater(t, s.MemoVersion(raw), version)
		control, err := s.listControl(raw)
		require.NoError(t, err)
		require.Equal(t, pending, control.PendingStamp)
		require.Equal(t, stamp, control.PublishedStamp)
		require.Len(t, control.RecentActions, 1)
		require.Equal(t, "run", control.RecentActions[0].StartWorkflowResult.RunId)
		for _, path := range []string{"PublicSchedule", "PublicList", "ListStaticA"} {
			require.NotContains(t, h.node.MaterializedPathsForTest(), path)
		}
		h.commit(true)
	}
	s, _ = h.root(false)
	require.True(t, s.Paused)
}
