package scheduler2

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
)

func TestIdleSuccessorTokenPreflightPreservesTransitions(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"automatic exhaustion", "backfill exhaustion", "start acknowledgment"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var h *treeHarness
			if kind == "automatic exhaustion" {
				h = coordinatorHarness(t, &schedulepb.ScheduleSpec{}, generationStart(), generationStart())
			} else {
				h = manualHarness(t)
				if kind == "backfill exhaustion" {
					coordinatorBackfill(h, generationStart(), generationStart(), enumspb.SCHEDULE_OVERLAP_POLICY_SKIP)
				} else {
					acceptManual(h, &schedulepb.TriggerImmediatelyRequest{}, true)
					coordinatorStep(h, true)
				}
			}
			s, _ := h.root(true)
			// Inject the representability boundary before an event makes idle eligible.
			// Current work/intent remains durable; no idle successor can be allocated.
			s.IdleDuration = int64(time.Minute)
			s.IdleToken = math.MaxUint64
			s.IdleArmed = false
			commitGeneration(h, true)
			before, physical := cloneNodes(h.durable), len(h.physical)
			s, ctx := h.root(true)
			var err error
			if kind == "start acknowledgment" {
				err = s.Apply(ctx.(chasm.MutableContext), decision.Event{Kind: decision.Acknowledged, Incarnation: s.Incarnation, OperationID: s.OperationId, ChainID: "chain", StartObserved: true, StartObservedAt: h.now})
			} else {
				err = (&WorkHandler{}).Execute(ctx.(chasm.MutableContext), s, chasm.TaskAttributes{ScheduledTime: time.Unix(0, s.Coordination.At)}, &scheduler2pb.WorkTask{Token: s.Coordination.Token})
			}
			require.ErrorContains(t, err, "idle token exhausted")
			requireRejectedCoordinatorUnchanged(h, before, physical)
		})
	}
}
