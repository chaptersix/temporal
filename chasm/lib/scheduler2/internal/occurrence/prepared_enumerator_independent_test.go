package occurrence

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func independentPreparedPeek(t *testing.T, e *PreparedEnumerator, state State, work PreparedSpecState, end time.Time) PreparedResult {
	t.Helper()
	for n := 0; n < 1000; n++ {
		result, err := e.Peek(state, work, end, 1)
		require.NoError(t, err)
		if result.Probes != 0 || result.Status != Yield {
			return result
		}
		state, work = result.State, result.Work
	}
	t.Fatal("fixture failed to complete raw probe")
	return PreparedResult{}
}

func TestPreparedEnumeratorIndependentExcludedWait(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{End: 59}}, Hour: []*schedulepb.Range{{End: 23}}}}}
	prepared, reference := preparedEnumerationFixture(t, spec, 1)
	state, err := prepared.Begin(start)
	require.NoError(t, err)
	initial := independentPreparedPeek(t, prepared, state, PreparedSpecState{}, start)
	require.Equal(t, Waiting, initial.Status)
	require.True(t, initial.State.Pending.Excluded)
	want, err := reference.Peek(state, start, 1)
	require.NoError(t, err)
	require.Equal(t, want.State, initial.State)
	due := start.Add(2 * time.Second)
	want, err = reference.Peek(want.State, due, 1)
	require.NoError(t, err)
	got := independentPreparedPeek(t, prepared, initial.State, initial.Work, due)
	require.Equal(t, want.State, got.State)
	require.Equal(t, want.Status, got.Status)
	require.Equal(t, start.Add(time.Second), got.State.DisposedThrough)
	require.Equal(t, due, got.State.Pending.Nominal)
}

func TestPreparedEnumeratorIndependentZoneRefresh(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	prepared, reference := preparedEnumerationFixture(t, &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}, 1)
	state, err := prepared.Begin(start)
	require.NoError(t, err)
	partial, err := prepared.Peek(state, PreparedSpecState{}, start, 1)
	require.NoError(t, err)
	require.NotEqual(t, PreparedSpecState{}, partial.Work)
	fresh := *prepared.zone
	fresh.fingerprint[0] ^= 1
	prepared.zone = &fresh
	restarted, err := prepared.Peek(partial.State, partial.Work, start, 1)
	require.NoError(t, err)
	require.Equal(t, state, restarted.State)
	require.Equal(t, fresh.fingerprint, restarted.Work.ZoneID)
	require.Equal(t, state.SearchAfter, restarted.Work.Boundary)
	got := independentPreparedPeek(t, prepared, restarted.State, restarted.Work, start)
	want, err := reference.Peek(state, start, 1)
	require.NoError(t, err)
	require.Equal(t, want.State, got.State)
	require.Equal(t, Waiting, got.Status)
	fresh.fingerprint[1] ^= 1
	prepared.reader.Term = func(bool, uint64) (PreparedSpecTerm, error) {
		t.Fatal("completed pending must not read source")
		return PreparedSpecTerm{}, nil
	}
	retained, err := prepared.Peek(got.State, PreparedSpecState{}, start.Add(time.Second), 1)
	require.NoError(t, err)
	require.Equal(t, Active, retained.Status)
	require.Equal(t, got.State, retained.State)
}
