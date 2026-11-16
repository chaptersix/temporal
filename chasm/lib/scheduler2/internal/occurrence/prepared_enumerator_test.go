package occurrence

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func preparedEnumerationFixture(t *testing.T, spec *schedulepb.ScheduleSpec, revision int64) (*PreparedEnumerator, *Enumerator) {
	t.Helper()
	seed := "long immutable jitter seed crossing several arithmetic blocks............"
	h, r, z, compiled := specFixture(t, spec, seed)
	h.ReturnExcluded = true
	reference, err := NewPublic(compiled.CanonicalForm(), seed, revision)
	require.NoError(t, err)
	raw, err := hex.DecodeString(reference.identity)
	require.NoError(t, err)
	var identity [32]byte
	copy(identity[:], raw)
	prepared, err := NewPreparedEnumerator(identity, revision, h, z, r)
	require.NoError(t, err)
	return prepared, reference
}

func TestPreparedEnumeratorReference(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		spec *schedulepb.ScheduleSpec
	}{
		{"empty", &schedulepb.ScheduleSpec{}},
		{"jitter", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(7 * time.Second)}, {Interval: durationpb.New(11 * time.Second)}}, Jitter: durationpb.New(9 * time.Second)}},
		{"excluded", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0, End: 10}}, Minute: []*schedulepb.Range{{End: 59}}, Hour: []*schedulepb.Range{{End: 23}}}}}},
		{"finite", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, StartTime: timestamppb.New(start.Add(500 * time.Millisecond)), EndTime: timestamppb.New(start.Add(3 * time.Second))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, budget := range []int{1, 7, 64} {
				t.Run(fmt.Sprint(budget), func(t *testing.T) {
					t.Parallel()
					prepared, reference := preparedEnumerationFixture(t, tc.spec, 1)
					state, err := prepared.Begin(start)
					require.NoError(t, err)
					wantState, err := reference.Begin(start)
					require.NoError(t, err)
					require.Equal(t, wantState, state)
					var work PreparedSpecState
					end := start.Add(20 * time.Second)
					for probe := 0; probe < 24; probe++ {
						want, err := reference.Peek(wantState, end, 1)
						require.NoError(t, err)
						var got PreparedResult
						for quantum := 0; quantum < 10000; quantum++ {
							before := clone(state)
							got, err = prepared.Peek(state, work, end, budget)
							require.NoError(t, err)
							require.LessOrEqual(t, got.Stats.Steps, budget)
							if got.Work != (PreparedSpecState{}) {
								require.Equal(t, before, got.State, "unfinished work cannot move the occurrence cursor")
							}
							state, work = got.State, got.Work
							if work != (PreparedSpecState{}) {
								wire, err := EncodePreparedSpecState(work)
								require.NoError(t, err)
								work, err = DecodePreparedSpecState(wire, prepared.header, prepared.zone, state.SearchAfter)
								require.NoError(t, err)
							}
							if got.Probes > 0 || got.Status != Yield {
								break
							}
							require.Less(t, quantum, 9999, "lookup must finish within fixture bound")
						}
						require.Equal(t, want.State, got.State)
						require.Equal(t, want.Status, got.Status)
						wantState = want.State
						if got.Status == Active {
							state, err = prepared.DisposeRange(state)
							require.NoError(t, err)
							wantState, err = reference.DisposeRange(wantState)
							require.NoError(t, err)
							require.Equal(t, wantState, state)
						} else if got.Status == Waiting {
							end = end.Add(20 * time.Second)
						} else if got.Status != Yield {
							break
						}
					}
				})
			}
		})
	}
}

func TestPreparedEnumeratorFaultAndRevisionFences(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}
	e, _ := preparedEnumerationFixture(t, spec, 1)
	state, err := e.Begin(start)
	require.NoError(t, err)
	partial, err := e.Peek(state, PreparedSpecState{}, start.Add(time.Hour), 1)
	require.NoError(t, err)
	require.Equal(t, state, partial.State)
	require.NotEqual(t, PreparedSpecState{}, partial.Work)
	original := partial
	e.reader.Term = func(bool, uint64) (PreparedSpecTerm, error) {
		return PreparedSpecTerm{}, errors.New("addressed read fault")
	}
	_, err = e.Peek(partial.State, partial.Work, start.Add(time.Hour), 64)
	require.ErrorContains(t, err, "addressed read fault")
	require.Equal(t, original, partial)
	newer, _ := preparedEnumerationFixture(t, spec, 2)
	_, err = newer.Peek(state, partial.Work, start.Add(time.Hour), 64)
	require.ErrorContains(t, err, "another definition revision")
	rebased, old, err := newer.RebaseRange(state)
	require.NoError(t, err)
	require.Nil(t, old)
	require.Equal(t, state.DisposedThrough, rebased.Origin)
	_, err = newer.DisposeRange(rebased)
	require.ErrorContains(t, err, "eligible pending")
}
