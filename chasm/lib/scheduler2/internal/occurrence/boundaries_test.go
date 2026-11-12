package occurrence_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFractionalStartAndInclusiveEnd(t *testing.T) {
	t.Parallel()
	spec := intervalSpec(instant().Add(500*time.Millisecond), 1)
	spec.EndTime = timestamppb.New(instant().Add(3 * time.Second))
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	state, err := e.Begin(instant().Add(-time.Nanosecond))
	require.NoError(t, err)
	actual, _, status, err := collect(e, state, instant().Add(time.Minute), occurrence.Limits{Probes: 2, Outputs: 1})
	require.NoError(t, err)
	var expected []occurrence.Candidate
	for sec := 1; sec <= 3; sec++ {
		at := instant().Add(time.Duration(sec) * time.Second)
		expected = append(expected, occurrence.Candidate{Nominal: at, Actual: at, Revision: 1})
	}
	require.Equal(t, expected, actual)
	require.Equal(t, occurrence.Exhausted, status)
}
func TestPendingJitterBeyondNominalEnd(t *testing.T) {
	t.Parallel()
	// Literal golden from legacy TestSpecJitterSingleRun; expected time is not
	// derived by invoking either the new candidate or legacy next-time search.
	nominal := time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC)
	actual := time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC)
	cal := calendar()
	cal.Year = span(2022, 2022)
	cal.Month = span(4, 4)
	cal.DayOfMonth = span(7, 7)
	cal.Hour = span(13, 13)
	cal.Minute = span(55, 55)
	cal.Second = span(0, 0)
	spec := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, StartTime: timestamppb.New(nominal), EndTime: timestamppb.New(nominal), Jitter: durationpb.New(time.Hour)}
	original := proto.Clone(spec)
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, spec))
	state, err := e.Begin(nominal.Add(-time.Nanosecond))
	require.NoError(t, err)
	result, err := e.Run(state, nominal, occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	candidate := occurrence.Candidate{Nominal: nominal, Actual: actual, Revision: 1}
	require.Equal(t, occurrence.Waiting, result.Status)
	require.Equal(t, &candidate, result.State.Pending)
	require.Equal(t, state.DisposedThrough, result.State.DisposedThrough)
	// Compilation owns its definition, and each result owns its pending candidate.
	spec.StructuredCalendar[0].Year[0].Start = 2099
	saved := result.State
	result2, err := e.Run(saved, nominal, occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	result2.State.Pending.Actual = nominal
	require.Equal(t, &candidate, saved.Pending)
	occurrences, _, _, err := collect(e, saved, actual, occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, []occurrence.Candidate{candidate}, occurrences)
}
func TestUpdateSupersedesFutureCandidates(t *testing.T) {
	t.Parallel()
	for _, excluded := range []bool{false, true} {
		t.Run(fmt.Sprintf("excluded_%t", excluded), func(t *testing.T) {
			t.Parallel()
			spec := intervalSpec(instant().Add(10*time.Second), 10)
			if excluded {
				spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{calendar()}
			}
			e, err := occurrence.New(spec, "old", 1)
			require.NoError(t, err)
			state, err := e.Begin(instant())
			require.NoError(t, err)
			waiting, err := e.Run(state, instant().Add(5*time.Second), occurrence.Limits{Probes: 1, Outputs: 1})
			require.NoError(t, err)
			require.Equal(t, occurrence.Waiting, waiting.Status)
			require.Equal(t, excluded, waiting.State.Pending.Excluded)
			updated, err := occurrence.New(intervalSpec(instant(), 1), "new", 2)
			require.NoError(t, err)
			require.Equal(t, state.DisposedThrough, waiting.State.DisposedThrough)
			next, superseded, err := updated.Update(waiting.State, instant().Add(5*time.Second))
			require.NoError(t, err)
			require.Equal(t, waiting.State.Pending, superseded)
			require.Nil(t, next.Pending)
			superseded.Nominal = instant()
			require.Equal(t, instant().Add(10*time.Second), waiting.State.Pending.Nominal)
			output, _, _, err := collect(updated, next, instant().Add(6*time.Second), occurrence.Limits{Probes: 1, Outputs: 1})
			require.NoError(t, err)
			require.Equal(t, []occurrence.Candidate{{Nominal: instant().Add(6 * time.Second), Actual: instant().Add(6 * time.Second), Revision: 2}}, output)
		})
	}
}
func TestContinuationDefinitionBinding(t *testing.T) {
	t.Parallel()
	spec := intervalSpec(instant(), 1)
	e, err := occurrence.New(spec, "seed", 1)
	require.NoError(t, err)
	state, err := e.Begin(instant())
	require.NoError(t, err)
	for _, tc := range []struct {
		name, seed string
		revision   int64
		change     bool
	}{
		{"revision", "seed", 2, false}, {"seed", "other", 1, false}, {"definition", "seed", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := proto.Clone(spec).(*schedulepb.ScheduleSpec)
			if tc.change {
				candidate.Interval[0].Interval = durationpb.New(2 * time.Second)
			}
			different, err := occurrence.New(candidate, tc.seed, tc.revision)
			require.NoError(t, err)
			_, err = different.Run(state, instant().Add(time.Second), occurrence.Limits{Probes: 1, Outputs: 1})
			require.Error(t, err)
		})
	}
	_, _, err = e.Update(state, instant())
	require.Error(t, err)
}
func TestPrototypeBounds(t *testing.T) {
	t.Parallel()
	lower := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	upper := time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC)
	// Definition timestamps can extend beyond the bounded execution domain.
	// Cursors/windows still enforce that separate representation boundary.
	for _, outside := range []time.Time{time.Unix(0, 0), lower.Add(-time.Nanosecond), upper, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)} {
		for _, spec := range []*schedulepb.ScheduleSpec{{StartTime: timestamppb.New(outside)}, {EndTime: timestamppb.New(outside)}} {
			_, err := occurrence.New(spec, "", 1)
			require.NoError(t, err)
		}
	}
	for _, spec := range []*schedulepb.ScheduleSpec{{StartTime: &timestamppb.Timestamp{Nanos: 1000000000}}, {EndTime: &timestamppb.Timestamp{Seconds: 253402300800}}} {
		_, err := occurrence.New(spec, "", 1)
		require.Error(t, err)
	}
	e, err := occurrence.New(intervalSpec(instant(), 1), "", 1)
	require.NoError(t, err)
	_, err = e.Begin(lower.Add(-2 * time.Nanosecond))
	require.Error(t, err)
	_, err = e.Begin(upper)
	require.Error(t, err)
	_, err = e.Begin(lower.Add(-time.Nanosecond))
	require.NoError(t, err)
	state, err := e.Begin(instant())
	require.NoError(t, err)
	for _, limits := range []occurrence.Limits{{Probes: 0, Outputs: 1}, {Probes: 1025, Outputs: 1}, {Probes: 1, Outputs: 0}, {Probes: 1, Outputs: 129}} {
		_, err = e.Run(state, instant(), limits)
		require.Error(t, err)
	}
	_, err = e.Run(state, upper, occurrence.Limits{Probes: 1, Outputs: 1})
	require.Error(t, err)
	_, err = occurrence.New(&schedulepb.ScheduleSpec{Jitter: durationpb.New(-time.Second)}, "", 1)
	require.Error(t, err)
	oversized := intervalSpec(instant(), 1)
	for len(oversized.Interval) < 65 {
		oversized.Interval = append(oversized.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Second)})
	}
	_, err = occurrence.New(oversized, "", 1)
	require.Error(t, err)
}
func TestPinnedTimezoneGapAndFold(t *testing.T) {
	t.Parallel()
	require.Equal(t, "e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95", fmt.Sprintf("%x", sha256.Sum256(newYorkTZ)))
	location, err := time.LoadLocationFromTZData("America/New_York", newYorkTZ)
	require.NoError(t, err)
	for _, tc := range []struct {
		name             string
		month, day, hour int32
		start            time.Time
		expected         []time.Time
	}{
		{"gap", 3, 10, 2, time.Date(2024, 3, 10, 4, 0, 0, 0, time.UTC), nil},
		{"fold", 11, 3, 1, time.Date(2024, 11, 3, 4, 0, 0, 0, time.UTC), []time.Time{time.Date(2024, 11, 3, 5, 30, 0, 0, time.UTC), time.Date(2024, 11, 3, 6, 30, 0, 0, time.UTC)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cal := calendar()
			cal.Year = span(2024, 2024)
			cal.Month = span(tc.month, tc.month)
			cal.DayOfMonth = span(tc.day, tc.day)
			cal.Hour = span(tc.hour, tc.hour)
			cal.Minute = span(30, 30)
			cal.Second = span(0, 0)
			spec := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneName: "America/New_York", TimezoneData: bytes.Clone(newYorkTZ), StartTime: timestamppb.New(tc.start), EndTime: timestamppb.New(tc.start.Add(6 * time.Hour))}
			e, err := occurrence.New(spec, "", 1)
			require.NoError(t, err)
			state, err := e.Begin(tc.start.Add(-time.Nanosecond))
			require.NoError(t, err)
			output, _, _, err := collect(e, state, spec.EndTime.AsTime(), occurrence.Limits{Probes: 1, Outputs: 1})
			require.NoError(t, err)
			expected := scanOracle(spec, "", state.SearchAfter, spec.EndTime.AsTime(), location)
			require.Equal(t, expected, output)
			var times []time.Time
			for _, candidate := range output {
				times = append(times, candidate.Nominal)
			}
			require.Equal(t, tc.expected, times)
		})
	}
}

func TestExcludedWorkYieldsWithoutExhaustion(t *testing.T) {
	t.Parallel()
	spec := intervalSpec(instant(), 1)
	spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{calendar()}
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	state, err := e.Begin(instant().Add(-time.Nanosecond))
	require.NoError(t, err)
	result, err := e.Run(state, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	expected := state
	expected.SearchAfter = instant()
	expected.DisposedThrough = instant()
	require.Equal(t, occurrence.Result{State: expected, Status: occurrence.Yield, Probes: 1}, result)
}

func TestJitterOutsidePrototypeRange(t *testing.T) {
	t.Parallel()
	nominal := time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC)
	cal := calendar()
	cal.Year = span(2100, 2100)
	cal.Month = span(12, 12)
	cal.DayOfMonth = span(31, 31)
	cal.Hour = span(23, 23)
	cal.Minute = span(59, 59)
	cal.Second = span(59, 59)
	spec := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, Jitter: durationpb.New(time.Hour)}
	e, err := occurrence.New(spec, "range", 1)
	require.NoError(t, err)
	state, err := e.Begin(nominal.Add(-time.Second))
	require.NoError(t, err)
	_, err = e.Run(state, nominal, occurrence.Limits{Probes: 1, Outputs: 1})
	require.ErrorContains(t, err, "jittered time is outside")
}

func TestScanOracleDetectsDroppedPendingCandidate(t *testing.T) {
	t.Parallel()
	spec := intervalSpec(instant(), 2)
	spec.Jitter = durationpb.New(time.Second)
	e, err := occurrence.New(spec, "pending", 1)
	require.NoError(t, err)
	initial, err := e.Begin(instant().Add(-time.Nanosecond))
	require.NoError(t, err)
	waiting, err := e.Run(initial, instant().Add(-time.Nanosecond), occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, occurrence.Waiting, waiting.Status)
	require.NotNil(t, waiting.State.Pending)
	expected := scanOracle(spec, "pending", initial.SearchAfter, instant().Add(time.Minute), time.UTC)
	baseline, _, _, err := collect(e, waiting.State, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, expected, baseline)
	// This isolated persisted-state mutation simulates losing the candidate while
	// retaining advanced search progress; the independent oracle must distinguish it.
	mutated := waiting.State
	mutated.Pending = nil
	missing, _, _, err := collect(e, mutated, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 1})
	if err != nil {
		require.ErrorContains(t, err, "invalid enumeration continuation")
		return
	}
	require.NotEqual(t, expected, missing)
	require.Equal(t, expected[1:], missing)
}

func TestMalformedDefinitionsRejected(t *testing.T) {
	t.Parallel()
	negativeStep := calendar()
	negativeStep.Second = []*schedulepb.Range{{Start: 0, End: 59, Step: -1}}
	hugeYear := calendar()
	hugeYear.Year = []*schedulepb.Range{{Start: 2000, End: 2147483647, Step: 1}}
	invalidPhase := intervalSpec(instant(), 1)
	invalidPhase.Interval[0].Phase = durationpb.New(time.Second)
	for _, tc := range []struct {
		name string
		spec *schedulepb.ScheduleSpec
	}{
		{"negative_exclusion_step", &schedulepb.ScheduleSpec{ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{negativeStep}}},
		{"huge_exclusion_year", &schedulepb.ScheduleSpec{ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{hugeYear}}},
		{"nil_structured_calendar", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{nil}}},
		{"nil_exclusion", &schedulepb.ScheduleSpec{ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{nil}}},
		{"nil_calendar", &schedulepb.ScheduleSpec{Calendar: []*schedulepb.CalendarSpec{nil}}},
		{"nil_interval", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{nil}}},
		{"invalid_duration_nanos", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: &durationpb.Duration{Seconds: 1, Nanos: 1000000000}}}}},
		{"invalid_phase_nanos", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second), Phase: &durationpb.Duration{Nanos: 1000000000}}}}},
		{"phase_equals_period", invalidPhase},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); _, err := occurrence.New(tc.spec, "", 1); require.Error(t, err) })
	}
}

func TestForgedContinuationsRejected(t *testing.T) {
	t.Parallel()
	e, err := occurrence.New(intervalSpec(instant(), 1), "", 1)
	require.NoError(t, err)
	state, err := e.Begin(instant().Add(-time.Nanosecond))
	require.NoError(t, err)
	waiting, err := e.Run(state, instant().Add(-time.Nanosecond), occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		mutate func(*occurrence.State)
	}{
		{"cursor_outside_range", func(s *occurrence.State) { s.SearchAfter = time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"pending_revision", func(s *occurrence.State) { s.Pending.Revision++ }},
		{"waiting_not_terminal", func(s *occurrence.State) { s.Terminal = occurrence.Waiting }},
		{"disposed_ahead_of_search", func(s *occurrence.State) { s.DisposedThrough = s.SearchAfter.Add(time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mutated := waiting.State
			pending := *mutated.Pending
			mutated.Pending = &pending
			tc.mutate(&mutated)
			_, err := e.Run(mutated, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 1})
			require.Error(t, err)
		})
	}
}

func TestEpochDefinitionPreservesBoundedEnumeration(t *testing.T) {
	t.Parallel()
	spec := intervalSpec(instant(), 1)
	spec.StartTime = &timestamppb.Timestamp{}
	spec.EndTime = timestamppb.New(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	before := proto.Clone(spec)
	e, err := occurrence.New(spec, "", 1)
	require.NoError(t, err)
	state, err := e.Begin(instant())
	require.NoError(t, err)
	require.False(t, state.FullRange)
	result, err := e.Run(state, instant().Add(time.Second), occurrence.Limits{Probes: 2, Outputs: 1})
	require.NoError(t, err)
	require.Len(t, result.Occurrences, 1)
	require.Equal(t, instant().Add(time.Second), result.Occurrences[0].Nominal)
	require.True(t, proto.Equal(before, spec))
}
