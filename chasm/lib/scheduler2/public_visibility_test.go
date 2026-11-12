package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func TestPublicVisibilityCountsFollowStartAcknowledgment(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	check := func(running, buffered int64) {
		t.Helper()
		s, ctx := h.root(false)
		values := s.SearchAttributes(ctx)
		require.Contains(t, values, publicRunningWorkflowCount.Value(running))
		require.Contains(t, values, publicBufferedStartsCount.Value(buffered))
	}
	check(0, 0)
	id := publicProjectionAdmission(h)
	check(0, 1)
	s, _ := h.root(false)
	h.apply(decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", PublicRunID: "actual-run", StartObserved: true, StartObservedAt: h.now}, true)
	check(1, 0)
	h.apply(decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: id, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, true)
	check(0, 0)
	s, ctx := h.root(false)
	s.Closed = true
	require.Equal(t, []chasm.SearchAttributeKeyValue{chasm.SearchAttributeTemporalSchedulePaused.Value(s.Paused)}, s.SearchAttributes(ctx))
}

func TestPublicVisibilityIdleDeadlineRequiresArmedOpenSchedule(t *testing.T) {
	t.Parallel()
	h := executingPublicTree(t, nil)
	s, ctx := h.root(false)
	s.IdleAt = h.now
	value := publicIdleCloseTime.Value(time.Unix(0, h.now))
	s.IdleArmed = false
	require.NotContains(t, s.SearchAttributes(ctx), value)
	s.IdleArmed = true
	require.Contains(t, s.SearchAttributes(ctx), value)
	s.Closed = true
	require.NotContains(t, s.SearchAttributes(ctx), value)
}
