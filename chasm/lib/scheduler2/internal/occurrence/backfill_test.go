package occurrence_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

func collectRange(t *testing.T, e *occurrence.Enumerator, state occurrence.State, end time.Time, probes int) ([]occurrence.Candidate, occurrence.State, occurrence.Status) {
	t.Helper()
	var output []occurrence.Candidate
	for step := 0; step < 10000; step++ {
		peek, err := e.PeekRange(state, end, probes)
		require.NoError(t, err)
		require.LessOrEqual(t, peek.Probes, probes)
		require.Empty(t, peek.Occurrences)
		state = peek.State
		switch peek.Status {
		case occurrence.Active:
			require.NotNil(t, state.Pending)
			output = append(output, *state.Pending)
			state, err = e.DisposeRange(state)
			require.NoError(t, err)
		case occurrence.Yield:
		default:
			return output, state, peek.Status
		}
	}
	t.Fatal("bounded range did not finish")
	return nil, state, occurrence.Active
}

func TestBackfillRangeInclusiveFractionalAndMinimumBounds(t *testing.T) {
	t.Parallel()
	minimum := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		start, end time.Time
		seconds    []int
	}{
		{"equal", instant(), instant(), []int{0}},
		{"fractional", instant().Add(time.Nanosecond), instant().Add(3*time.Second - time.Nanosecond), []int{1, 2}},
		{"minimum", minimum, minimum, []int{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}
			e, err := occurrence.New(spec, "", 1)
			require.NoError(t, err)
			state, err := e.BeginRange(tc.start, tc.end)
			require.NoError(t, err)
			require.Equal(t, tc.start.Add(-time.Nanosecond), state.DisposedThrough)
			got, _, status := collectRange(t, e, state, tc.end, 1)
			require.Equal(t, occurrence.Exhausted, status)
			require.Len(t, got, len(tc.seconds))
			base := tc.start.Truncate(time.Second)
			for i, seconds := range tc.seconds {
				want := base.Add(time.Duration(seconds) * time.Second)
				require.Equal(t, want, got[i].Nominal)
				require.Equal(t, want, got[i].Actual)
			}
		})
	}
}

func TestBackfillRangeCapacityWaitOwnershipAndRebase(t *testing.T) {
	t.Parallel()
	e, err := occurrence.New(intervalSpec(instant(), 1), "old", 1)
	require.NoError(t, err)
	end := instant().Add(5 * time.Second)
	state, err := e.BeginRange(instant(), end)
	require.NoError(t, err)
	first, err := e.PeekRange(state, end, 1)
	require.NoError(t, err)
	require.Equal(t, occurrence.Active, first.Status)
	disposed, err := e.DisposeRange(first.State)
	require.NoError(t, err)
	waiting, err := e.PeekRange(disposed, end, 1)
	require.NoError(t, err)
	require.Equal(t, instant().Add(time.Second), waiting.State.Pending.Nominal)
	require.Equal(t, instant(), waiting.State.DisposedThrough)
	encoded, err := json.Marshal(waiting.State)
	require.NoError(t, err)
	var persisted occurrence.State
	require.NoError(t, json.Unmarshal(encoded, &persisted))
	for i := 0; i < 4; i++ {
		repeated, err := e.PeekRange(persisted, end, 1)
		require.NoError(t, err)
		require.Zero(t, repeated.Probes)
		require.Equal(t, persisted, repeated.State)
		repeated.State.Pending.Nominal = end
		require.Equal(t, instant().Add(time.Second), persisted.Pending.Nominal)
	}
	updated, err := occurrence.New(intervalSpec(instant(), 2), "new", 2)
	require.NoError(t, err)
	next, superseded, err := updated.RebaseRange(persisted)
	require.NoError(t, err)
	require.Equal(t, persisted.Pending, superseded)
	superseded.Nominal = end
	require.Equal(t, instant().Add(time.Second), persisted.Pending.Nominal)
	require.Equal(t, instant(), next.Origin)
	require.Equal(t, instant(), next.DisposedThrough)
	candidate, err := updated.PeekRange(next, end, 1)
	require.NoError(t, err)
	require.Equal(t, instant().Add(2*time.Second), candidate.State.Pending.Nominal)
	_, _, err = e.RebaseRange(persisted)
	require.Error(t, err)
	_, err = updated.PeekRange(persisted, end, 1)
	require.Error(t, err)
	// Rebasing after handing off the inclusive endpoint remains a valid empty range.
	endpoint := candidate.State
	endpointEnd := endpoint.Pending.Nominal
	endpoint, err = updated.DisposeRange(endpoint)
	require.NoError(t, err)
	newer, err := occurrence.New(intervalSpec(instant(), 1), "", 3)
	require.NoError(t, err)
	rebased, _, err := newer.RebaseRange(endpoint)
	require.NoError(t, err)
	terminal, err := newer.PeekRange(rebased, endpointEnd, 1)
	require.NoError(t, err)
	require.Equal(t, occurrence.Exhausted, terminal.Status)
}

func TestBackfillRangeJitterWitness(t *testing.T) {
	t.Parallel()
	nominal := time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC)
	actual := time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC)
	cal := calendar()
	cal.Year = span(2022, 2022)
	cal.Month = span(4, 4)
	cal.DayOfMonth = span(7, 7)
	cal.Hour = span(13, 13)
	cal.Minute = span(55, 55)
	cal.Second = span(0, 0)
	e, err := occurrence.New(&schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, Jitter: durationpb.New(time.Hour)}, "", 1)
	require.NoError(t, err)
	for _, end := range []time.Time{actual.Add(-time.Nanosecond), actual} {
		state, err := e.BeginRange(nominal, end)
		require.NoError(t, err)
		peek, err := e.PeekRange(state, end, 1)
		require.NoError(t, err)
		require.Equal(t, actual, peek.State.Pending.Actual)
		require.Equal(t, state.DisposedThrough, peek.State.DisposedThrough)
		require.Equal(t, occurrence.Active, peek.State.Terminal)
		if end.Before(actual) {
			require.Equal(t, occurrence.Exhausted, peek.Status)
			again, err := e.PeekRange(peek.State, end, 1)
			require.NoError(t, err)
			require.Equal(t, peek.State, again.State)
			require.Zero(t, again.Probes)
		} else {
			require.Equal(t, occurrence.Active, peek.Status)
		}
	}
}

func TestBackfillRangeExclusionsAndTerminalDistinctions(t *testing.T) {
	t.Parallel()
	start, end := instant(), instant().Add(3*time.Second)
	excluded := intervalSpec(start, 1)
	excluded.EndTime = timestamppb.New(end)
	excluded.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{calendar()}
	e, err := occurrence.New(excluded, "", 1)
	require.NoError(t, err)
	state, err := e.BeginRange(start, end)
	require.NoError(t, err)
	peek, err := e.PeekRange(state, end, 1)
	require.NoError(t, err)
	require.Equal(t, occurrence.Yield, peek.Status)
	require.Nil(t, peek.State.Pending)
	require.Equal(t, start, peek.State.DisposedThrough)
	output, _, status := collectRange(t, e, peek.State, end, 1)
	require.Empty(t, output)
	require.Equal(t, occurrence.Impossible, status)
	empty, err := occurrence.New(&schedulepb.ScheduleSpec{}, "", 1)
	require.NoError(t, err)
	state, err = empty.BeginRange(start, end)
	require.NoError(t, err)
	peek, err = empty.PeekRange(state, end, 1)
	require.NoError(t, err)
	require.Equal(t, occurrence.Empty, peek.Status)
	require.Zero(t, peek.Probes)
	for _, probes := range []int{0, 1025} {
		_, err = empty.PeekRange(state, end, probes)
		require.Error(t, err)
	}
	_, err = empty.BeginRange(end, start)
	require.Error(t, err)
	_, err = empty.BeginRange(time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC), end)
	require.Error(t, err)
	_, err = empty.DisposeRange(state)
	require.Error(t, err)
}

func TestBackfillRangeIndependentIntervalProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		period := rapid.IntRange(1, 15).Draw(t, "period")
		offset := rapid.IntRange(0, period-1).Draw(t, "phase")
		startSec := rapid.IntRange(0, 30).Draw(t, "start")
		fraction := rapid.SampledFrom([]int64{0, 1, 500000000, 999999999}).Draw(t, "fraction")
		width := rapid.IntRange(0, 60).Draw(t, "width")
		start := instant().Add(time.Duration(startSec)*time.Second + time.Duration(fraction))
		end := start.Add(time.Duration(width) * time.Second)
		spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Duration(period) * time.Second), Phase: durationpb.New(time.Duration(offset) * time.Second)}}}
		e, err := occurrence.New(spec, "", 1)
		require.NoError(t, err)
		var expected []time.Time
		for sec := start.Unix(); sec <= end.Unix(); sec++ {
			at := time.Unix(sec, 0).UTC()
			if !at.Before(start) && !at.After(end) && (sec-int64(offset))%int64(period) == 0 {
				expected = append(expected, at)
			}
		}
		for _, probes := range []int{1, 2, 8} {
			state, err := e.BeginRange(start, end)
			require.NoError(t, err)
			var got []time.Time
			for step := 0; step < 100; step++ {
				peek, err := e.PeekRange(state, end, probes)
				require.NoError(t, err)
				require.LessOrEqual(t, peek.Probes, probes)
				state = peek.State
				if peek.Status == occurrence.Exhausted {
					break
				}
				require.Equal(t, occurrence.Active, peek.Status)
				require.NotNil(t, state.Pending)
				got = append(got, state.Pending.Nominal)
				state, err = e.DisposeRange(state)
				require.NoError(t, err)
			}
			require.Equal(t, expected, got)
		}
	})
}

func TestBackfillRangeStructuredCalendarFractionalExclusion(t *testing.T) {
	t.Parallel()
	cal := calendar()
	cal.Second = []*schedulepb.Range{{Start: 0, End: 4, Step: 2}}
	excluded := calendar()
	excluded.Second = span(2, 2)
	spec := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{excluded}}
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	start, end := instant().Add(time.Nanosecond), instant().Add(4*time.Second)
	state, err := e.BeginRange(start, end)
	require.NoError(t, err)
	excludedStep, err := e.PeekRange(state, end, 1)
	require.NoError(t, err)
	require.Equal(t, occurrence.Yield, excludedStep.Status)
	require.Equal(t, instant().Add(2*time.Second), excludedStep.State.DisposedThrough)
	candidate, err := e.PeekRange(excludedStep.State, end, 1)
	require.NoError(t, err)
	require.Equal(t, occurrence.Active, candidate.Status)
	require.Equal(t, end, candidate.State.Pending.Nominal)
	require.Equal(t, excludedStep.State.DisposedThrough, candidate.State.DisposedThrough)
	wrongSeed, err := occurrence.New(spec, "different", 1)
	require.NoError(t, err)
	_, err = wrongSeed.PeekRange(candidate.State, end, 1)
	require.Error(t, err)
	_, err = wrongSeed.DisposeRange(candidate.State)
	require.Error(t, err)
}
