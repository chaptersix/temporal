package scheduler2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

func TestTimeSkippingRuntimeGate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Scheduler, *Generator)
		want   bool
	}{
		{name: "future timer", want: true},
		{name: "closed", change: func(s *Scheduler, _ *Generator) { s.Closed = true }},
		{name: "paused", change: func(s *Scheduler, _ *Generator) { s.Paused = true }},
		{name: "no generator", change: func(s *Scheduler, _ *Generator) { s.HasGenerator = false }},
		{name: "tracked workflow", change: func(s *Scheduler, _ *Generator) { s.Unfinished = 1 }},
		{name: "unconfirmed start", change: func(s *Scheduler, _ *Generator) { s.UnconfirmedStarts = 1 }},
		{name: "pending start", change: func(s *Scheduler, _ *Generator) { s.PendingStarts = map[string]int64{"start": 200} }},
		{name: "effect", change: func(s *Scheduler, _ *Generator) { s.Effects = []*scheduler2pb.EffectState{{}} }},
		{name: "queue", change: func(s *Scheduler, _ *Generator) { s.Queue = []*scheduler2pb.QueuedOccurrence{{}} }},
		{name: "manual", change: func(s *Scheduler, _ *Generator) { s.Manuals = []*scheduler2pb.ManualCommand{{}} }},
		{name: "backfill", change: func(s *Scheduler, _ *Generator) { s.Backfills = []*scheduler2pb.BackfillState{{}} }},
		{name: "due coordinator", change: func(s *Scheduler, _ *Generator) { s.Coordination.At = 100 }},
		{name: "reserved work", change: func(s *Scheduler, _ *Generator) { s.Coordination.OwnerKey = "owner" }},
		{name: "due generator", change: func(_ *Scheduler, g *Generator) { g.WakeAt = 100 }},
		{name: "rearmed future pending", change: func(_ *Scheduler, g *Generator) {
			g.WakeAt = 100
			g.Continuation = &scheduler2pb.OccurrenceContinuation{Pending: &scheduler2pb.OccurrenceCandidate{Actual: 200}}
		}, want: true},
		{name: "rearmed due pending", change: func(_ *Scheduler, g *Generator) {
			g.WakeAt = 100
			g.Continuation = &scheduler2pb.OccurrenceContinuation{Pending: &scheduler2pb.OccurrenceCandidate{Actual: 100}}
		}},
		{name: "rearmed excluded pending", change: func(_ *Scheduler, g *Generator) {
			g.WakeAt = 100
			g.Continuation = &scheduler2pb.OccurrenceContinuation{Pending: &scheduler2pb.OccurrenceCandidate{Actual: 200, Excluded: true}}
		}},
		{name: "source exhausted", change: func(s *Scheduler, g *Generator) { g.Armed = false; s.Coordination.Armed = false }, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &Generator{GeneratorState: &scheduler2pb.GeneratorState{Armed: true, WakeAt: 200}}
			s := New("incarnation", 0)
			s.HasGenerator = true
			s.Generator = chasm.NewComponentField[*Generator](nil, g)
			s.Coordination = &scheduler2pb.CoordinationState{Armed: true, At: 200}
			if tc.change != nil {
				tc.change(s, g)
			}
			ctx := &chasm.MockContext{HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 100) }}
			require.Equal(t, tc.want, s.IsExecutionSkippable(ctx))
		})
	}
}
