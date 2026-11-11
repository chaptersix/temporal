package occurrence_test

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

//go:embed testdata/America_New_York.tzif
var newYorkTZ []byte

func instant() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }
func span(start, end int32) []*schedulepb.Range {
	return []*schedulepb.Range{{Start: start, End: end, Step: 1}}
}
func calendar() *schedulepb.StructuredCalendarSpec {
	return &schedulepb.StructuredCalendarSpec{Second: span(0, 59), Minute: span(0, 59), Hour: span(0, 23), DayOfMonth: span(1, 31), Month: span(1, 12), Year: span(2000, 2100), DayOfWeek: span(0, 6)}
}
func intervalSpec(start time.Time, seconds int) *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Duration(seconds) * time.Second)}}, StartTime: timestamppb.New(start), EndTime: timestamppb.New(start.Add(30 * time.Second))}
}
func matchesRange(ranges []*schedulepb.Range, value int) bool {
	for _, r := range ranges {
		end := max(r.Start, r.End)
		step := max(r.Step, 1)
		if int(r.Start) <= value && value <= int(end) && (value-int(r.Start))%int(step) == 0 {
			return true
		}
	}
	return false
}
func matchesCalendar(c *schedulepb.StructuredCalendarSpec, at time.Time) bool {
	return matchesRange(c.Year, at.Year()) && matchesRange(c.Month, int(at.Month())) && matchesRange(c.DayOfMonth, at.Day()) && matchesRange(c.DayOfWeek, int(at.Weekday())) && matchesRange(c.Hour, at.Hour()) && matchesRange(c.Minute, at.Minute()) && matchesRange(c.Second, at.Second())
}

// scanOracle enumerates each second and directly evaluates predicates. It never
// calls a production next-time search. UTC/TZif conversion, time.MarshalBinary,
// and Farm32 are shared primitives; their implementations are trusted here.
func scanOracle(spec *schedulepb.ScheduleSpec, seed string, after, through time.Time, location *time.Location) []occurrence.Candidate {
	var raw []time.Time
	end := spec.EndTime.AsTime()
	for at := after.Truncate(time.Second).Add(time.Second); !at.After(end.Add(2 * time.Minute)); at = at.Add(time.Second) {
		included := false
		for _, iv := range spec.Interval {
			period := int64(iv.Interval.AsDuration() / time.Second)
			phase := int64(iv.Phase.AsDuration() / time.Second)
			if (at.Unix()-phase)%period == 0 {
				included = true
			}
		}
		for _, cal := range spec.StructuredCalendar {
			if matchesCalendar(cal, at.In(location)) {
				included = true
			}
		}
		if included {
			raw = append(raw, at)
		}
	}
	var result []occurrence.Candidate
	for i, nominal := range raw {
		if nominal.Before(spec.StartTime.AsTime()) || nominal.After(end) {
			continue
		}
		excluded := false
		for _, cal := range spec.ExcludeStructuredCalendar {
			if matchesCalendar(cal, nominal.In(location)) {
				excluded = true
			}
		}
		if excluded {
			continue
		}
		bound := spec.Jitter.AsDuration()
		if i+1 < len(raw) {
			bound = min(bound, raw[i+1].Sub(nominal))
		}
		encoded, _ := nominal.MarshalBinary() // Oracle instants are restricted to valid years 2000-2100.
		hash := uint64(farm.Fingerprint32(append(encoded, []byte(seed)...)))
		milliseconds := min(uint64(bound/time.Millisecond), uint64(math.MaxUint32))
		actual := nominal.Add(time.Duration(hash*milliseconds>>32) * time.Millisecond)
		if !actual.After(through) {
			result = append(result, occurrence.Candidate{Nominal: nominal, Actual: actual, Revision: 1})
		}
	}
	return result
}

// collect reloads the JSON continuation at every call and detects any mutation
// of the caller's input, in addition to checking per-call resource bounds.
func collect(e *occurrence.Enumerator, state occurrence.State, through time.Time, limits occurrence.Limits) ([]occurrence.Candidate, occurrence.State, occurrence.Status, error) {
	var output []occurrence.Candidate
	for calls := 0; calls < 4096; calls++ {
		before, err := json.Marshal(state)
		if err != nil {
			return nil, state, 0, err
		}
		result, err := e.Run(state, through, limits)
		if err != nil {
			return nil, state, 0, err
		}
		after, err := json.Marshal(state)
		if err != nil {
			return nil, state, 0, err
		}
		if !bytes.Equal(before, after) {
			return nil, state, 0, errors.New("input continuation mutated")
		}
		if result.Probes > limits.Probes || len(result.Occurrences) > limits.Outputs {
			return nil, state, 0, errors.New("work budget exceeded")
		}
		encoded, err := json.Marshal(result.State)
		if err != nil {
			return nil, state, 0, err
		}
		if err = json.Unmarshal(encoded, &state); err != nil {
			return nil, state, 0, err
		}
		output = append(output, result.Occurrences...)
		switch result.Status {
		case occurrence.Yield, occurrence.Limited:
			continue
		default:
			return output, state, result.Status, nil
		}
	}
	return nil, state, 0, errors.New("enumeration did not yield a final window result")
}
func TestBoundedEnumerationAgainstScan(t *testing.T) {
	t.Parallel()
	for _, probes := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("probes_%d", probes), func(t *testing.T) {
			t.Parallel()
			spec := intervalSpec(instant(), 2)
			spec.Interval = append(spec.Interval, proto.Clone(spec.Interval[0]).(*schedulepb.IntervalSpec))
			excluded := calendar()
			excluded.Second = span(4, 10)
			spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{excluded}
			spec.Jitter = durationpb.New(5 * time.Second)
			e, err := occurrence.New(spec, "seed", 1)
			require.NoError(t, err)
			state, err := e.Begin(instant().Add(-time.Nanosecond))
			require.NoError(t, err)
			through := instant().Add(35 * time.Second)
			actual, _, status, err := collect(e, state, through, occurrence.Limits{Probes: probes, Outputs: 2})
			require.NoError(t, err)
			require.Equal(t, occurrence.Exhausted, status)
			require.Equal(t, scanOracle(spec, "seed", state.SearchAfter, through, time.UTC), actual)
		})
	}
}
func TestEnumerationProperties(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		start := instant()
		period := rapid.IntRange(1, 6).Draw(rt, "period")
		spec := intervalSpec(start, period)
		spec.Interval[0].Phase = durationpb.New(time.Duration(rapid.IntRange(0, period-1).Draw(rt, "phase")) * time.Second)
		if rapid.Bool().Draw(rt, "calendar") {
			cal := calendar()
			cal.Second = span(int32(rapid.IntRange(0, 20).Draw(rt, "second")), 25)
			spec.StructuredCalendar = []*schedulepb.StructuredCalendarSpec{cal}
		}
		excluded := calendar()
		excluded.Second = span(int32(rapid.IntRange(0, 15).Draw(rt, "exclude_start")), int32(rapid.IntRange(16, 25).Draw(rt, "exclude_end")))
		spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{excluded}
		spec.Jitter = durationpb.New(time.Duration(rapid.IntRange(0, 6000).Draw(rt, "jitter_ms")) * time.Millisecond)
		e, err := occurrence.New(spec, "property", 1)
		if err != nil {
			rt.Fatal(err)
		}
		state, err := e.Begin(start.Add(-time.Nanosecond))
		if err != nil {
			rt.Fatal(err)
		}
		limits := occurrence.Limits{Probes: rapid.IntRange(1, 8).Draw(rt, "probes"), Outputs: rapid.IntRange(1, 4).Draw(rt, "outputs")}
		cut := start.Add(time.Duration(rapid.IntRange(0, 30).Draw(rt, "cut_seconds"))*time.Second + time.Duration(rapid.IntRange(0, 999).Draw(rt, "cut_ms"))*time.Millisecond)
		first, next, _, err := collect(e, state, cut, limits)
		if err != nil {
			rt.Fatal(err)
		}
		second, _, _, err := collect(e, next, start.Add(40*time.Second), limits)
		if err != nil {
			rt.Fatal(err)
		}
		actual := append(first, second...)
		expected := scanOracle(spec, "property", state.SearchAfter, start.Add(40*time.Second), time.UTC)
		if len(actual) != len(expected) {
			rt.Fatalf("occurrence count: got %d expected %d", len(actual), len(expected))
		}
		for i := range expected {
			if actual[i] != expected[i] {
				rt.Fatalf("occurrence %d got %+v expected %+v", i, actual[i], expected[i])
			}
		}
	})
}
func TestTerminalAndWindowStatuses(t *testing.T) {
	t.Parallel()
	impossible := calendar()
	impossible.Month = span(2, 2)
	impossible.DayOfMonth = span(30, 30)
	excluded := intervalSpec(instant(), 1)
	excluded.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{calendar()}
	for _, tc := range []struct {
		name     string
		spec     *schedulepb.ScheduleSpec
		after    time.Time
		expected occurrence.Status
	}{
		{"empty", &schedulepb.ScheduleSpec{}, instant(), occurrence.Empty},
		{"impossible", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{impossible}}, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond), occurrence.Impossible},
		{"partial_impossible", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{impossible}}, instant(), occurrence.Exhausted},
		{"all_excluded", excluded, instant().Add(-time.Nanosecond), occurrence.Impossible},
		{"partial_excluded", excluded, instant(), occurrence.Exhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, err := occurrence.New(tc.spec, "", 1)
			require.NoError(t, err)
			state, err := e.Begin(tc.after)
			require.NoError(t, err)
			_, terminal, status, err := collect(e, state, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 1})
			require.NoError(t, err)
			require.Equal(t, tc.expected, status)
			again, err := e.Run(terminal, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 1})
			require.NoError(t, err)
			require.Equal(t, occurrence.Result{State: terminal, Status: tc.expected}, again)
		})
	}
	e, err := occurrence.New(intervalSpec(instant(), 1), "", 1)
	require.NoError(t, err)
	state, err := e.Begin(instant().Add(-time.Nanosecond))
	require.NoError(t, err)
	yielded, err := e.Run(state, instant().Add(time.Minute), occurrence.Limits{Probes: 1, Outputs: 2})
	require.NoError(t, err)
	require.Equal(t, occurrence.Yield, yielded.Status)
	require.Len(t, yielded.Occurrences, 1)
	limited, err := e.Run(state, instant().Add(time.Minute), occurrence.Limits{Probes: 2, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, occurrence.Limited, limited.Status)
	waiting, err := e.Run(state, instant().Add(-time.Nanosecond), occurrence.Limits{Probes: 1, Outputs: 1})
	require.NoError(t, err)
	require.Equal(t, occurrence.Waiting, waiting.Status)
	require.NotNil(t, waiting.State.Pending)
}
