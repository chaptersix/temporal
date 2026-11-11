package scheduler2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

// Seven exclusions leave room for the exhaustion probe in the first quantum;
// eight and nine must persist a continuation before discovering exhaustion.
func TestCoordinatorScanBudgetNeighbors(t *testing.T) {
	t.Parallel()
	for _, historical := range []bool{false, true} {
		for _, count := range []int{7, 8, 9} {
			t.Run(fmt.Sprintf("historical_%t/exclusions_%d", historical, count), func(t *testing.T) {
				t.Parallel()
				base := generationStart()
				end := base.Add(time.Duration(count-1) * time.Second)
				now := end.Add(time.Hour)
				spec := generationSpec(base, end, time.Second)
				ranges := func(start, end int32) []*schedulepb.Range {
					return []*schedulepb.Range{{Start: start, End: end, Step: 1}}
				}
				spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{{Second: ranges(0, 59), Minute: ranges(0, 59), Hour: ranges(0, 23), DayOfMonth: ranges(1, 31), Month: ranges(1, 12), DayOfWeek: ranges(0, 6)}}
				after := base.Add(-time.Nanosecond)
				if historical {
					after = now
				}
				h := coordinatorHarness(t, spec, after, now)
				if historical {
					coordinatorStep(h, true) // Exhaust automatic work before accepting the range.
					coordinatorBackfill(h, base, end, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)
				}
				before := cloneNodes(h.durable)
				coordinatorStep(h, false)
				require.Equal(t, before, h.durable)
				coordinatorStep(h, true)
				s, _ := h.root(false)
				if count >= 8 {
					var continuation *scheduler2pb.OccurrenceContinuation
					if historical {
						require.Len(t, s.Backfills, 1)
						continuation = s.Backfills[0].Continuation
					} else {
						g := generationState(h)
						require.Equal(t, scheduler2pb.ENUMERATION_STATUS_YIELD, g.LastStatus)
						continuation = g.Continuation
					}
					require.Equal(t, base.Add(7*time.Second).UnixNano(), continuation.DisposedThrough)
					require.Nil(t, continuation.Pending)
					require.True(t, s.Coordination.Armed)
					h.now = max(h.now, s.Coordination.At)
					h.reload() // The successor must resume exclusively from committed state.
					coordinatorStep(h, true)
					s, _ = h.root(false)
				}
				require.False(t, s.Coordination.Armed)
				require.Empty(t, s.Backfills)
				require.Empty(t, s.Operations)
				require.Empty(t, s.Queue)
				require.Equal(t, int64(100), s.Remaining)
				require.Zero(t, pureObligations(h))
				if historical {
					require.Equal(t, now.UnixNano(), s.Cursor, "historical exhaustion cannot move the automatic cursor")
				} else {
					require.Equal(t, end.UnixNano(), s.Cursor)
					require.Equal(t, scheduler2pb.ENUMERATION_STATUS_IMPOSSIBLE, generationState(h).LastStatus)
				}
			})
		}
	}
}
