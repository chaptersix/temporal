package scheduler2

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func TestPublicRevisionAdmissionAndCompletion(t *testing.T) {
	t.Parallel()
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "pause_on_failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newTreeHarness(t)
			s, _ := h.root(true)
			s.PublicRevision = 1
			s.PauseOnFailure = true
			h.commit(true)
			request := h.admit()
			s, _ = h.root(false)
			require.EqualValues(t, 2, s.PublicRevision)
			event := decision.Event{Kind: decision.Callback, Incarnation: s.Incarnation, OperationID: request.RequestId, ChainID: "chain", Outcome: decision.Success, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}
			want := uint64(2)
			if failure {
				event.Outcome, event.Status = decision.Failure, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
				want = 3
			}
			h.apply(event, true)
			s, _ = h.root(false)
			require.Equal(t, want, s.PublicRevision)
			h.apply(event, true)
			s, _ = h.root(false)
			require.Equal(t, want, s.PublicRevision)
		})
	}
}

func TestPublicRevisionExhaustionRejectsAdmission(t *testing.T) {
	t.Parallel()
	h := newTreeHarness(t)
	s, _ := h.root(true)
	s.PublicRevision = math.MaxUint64
	h.commit(true)
	before, physical := cloneNodes(h.durable), len(h.physical)
	_, encoded := frozenRequest(t)
	s, ctx := h.root(true)
	err := s.applyTransition(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Due, Incarnation: s.Incarnation, Nominal: h.now, CatchupDeadline: h.now, Request: encoded})
	require.ErrorContains(t, err, "public mutation counter exhausted")
	requireRejectedCoordinatorUnchanged(h, before, physical)
}
