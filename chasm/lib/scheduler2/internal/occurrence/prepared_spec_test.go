package occurrence

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func specFixture(t *testing.T, spec *schedulepb.ScheduleSpec, seed string) (PreparedSpecHeader, PreparedSpecReader, *PreparedLocation, *legacy.CompiledSpec) {
	t.Helper()
	spec = proto.Clone(spec).(*schedulepb.ScheduleSpec)
	for _, cal := range append(spec.StructuredCalendar, spec.ExcludeStructuredCalendar...) {
		fields := []struct {
			p      *[]*schedulepb.Range
			lo, hi int32
		}{{&cal.Second, 0, 0}, {&cal.Minute, 0, 0}, {&cal.Hour, 0, 0}, {&cal.DayOfMonth, 1, 31}, {&cal.Month, 1, 12}, {&cal.DayOfWeek, 0, 6}}
		for _, f := range fields {
			if len(*f.p) == 0 {
				*f.p = []*schedulepb.Range{{Start: f.lo, End: f.hi, Step: 1}}
			}
		}
	}
	compiled, err := legacy.NewSpecBuilder(func() int { return 2 }, func() int { return 1000 }).NewCompiledSpec(spec)
	require.NoError(t, err)
	canonical := compiled.CanonicalForm()
	h := PreparedSpecHeader{SourceID: [32]byte{1}, SeedBytes: uint64(len(seed)), Start: canonical.StartTime.AsTime(), HasStart: canonical.StartTime != nil, HasEnd: canonical.EndTime != nil, End: canonical.EndTime.AsTime(), Jitter: canonical.Jitter.AsDuration(), WarnIterations: 2, MaxIterations: 1000}
	var terms, exclusions []PreparedSpecTerm
	makeCal := func(cal *schedulepb.StructuredCalendarSpec) PreparedSpecTerm {
		d, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}})
		return PreparedSpecTerm{Calendar: d}
	}
	for _, cal := range canonical.StructuredCalendar {
		terms = append(terms, makeCal(cal))
	}
	for _, iv := range canonical.Interval {
		terms = append(terms, PreparedSpecTerm{IsInterval: true, Interval: max(int64(iv.Interval.AsDuration()/time.Second), 1), Phase: max(int64(iv.Phase.AsDuration()/time.Second), 0)})
	}
	for _, cal := range canonical.ExcludeStructuredCalendar {
		exclusions = append(exclusions, makeCal(cal))
	}
	h.Terms = uint64(len(terms))
	h.Exclusions = uint64(len(exclusions))
	reader := PreparedSpecReader{Seed: seedReader([]byte(seed)), Term: func(exclusion bool, index uint64) (PreparedSpecTerm, error) {
		if exclusion {
			return exclusions[index], nil
		}
		return terms[index], nil
	}}
	zone := PreparedUTC()
	if canonical.TimezoneData != nil {
		zone, err = PrepareTimezone(canonical.TimezoneData, time.Unix(1704067200, 0))
		require.NoError(t, err)
	}
	z, err := NewResolvedLocation(zone)
	require.NoError(t, err)
	return h, reader, z, compiled
}

func runPreparedSpec(t *testing.T, h PreparedSpecHeader, r PreparedSpecReader, z *PreparedLocation, after time.Time, budget int) PreparedSpecState {
	t.Helper()
	var state PreparedSpecState
	for n := 0; n < 100000; n++ {
		next, stats, err := AdvancePreparedSpec(state, h, z, after, r, budget)
		require.NoError(t, err)
		require.LessOrEqual(t, stats.Steps, budget)
		require.LessOrEqual(t, stats.TermReads, budget)
		require.LessOrEqual(t, stats.SeedReads, budget)
		wire, err := json.Marshal(next)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(wire, &state))
		if state.Phase == specComplete {
			return state
		}
	}
	t.Fatal("prepared spec did not finish")
	return state
}

func TestPreparedSpecReference(t *testing.T) {
	t.Parallel()
	cal := &schedulepb.StructuredCalendarSpec{Hour: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{Start: 30}}}
	every := &schedulepb.StructuredCalendarSpec{Second: []*schedulepb.Range{{End: 59}}, Minute: []*schedulepb.Range{{End: 59}}, Hour: []*schedulepb.Range{{End: 23}}}
	cases := []struct {
		name  string
		spec  *schedulepb.ScheduleSpec
		after time.Time
	}{
		{"union exclusion following", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(7 * time.Second)}, {Interval: durationpb.New(11 * time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0, End: 15}}, Minute: every.Minute, Hour: every.Hour}}, Jitter: durationpb.New(10 * time.Second)}, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"mixed union", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal, cal}, Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}, Jitter: durationpb.New(time.Hour)}, time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)},
		{"fold first", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneData: preparedNewYorkTZ, Jitter: durationpb.New(2 * time.Hour)}, time.Date(2024, 11, 3, 5, 0, 0, 0, time.UTC)},
		{"fold second", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneData: preparedNewYorkTZ}, time.Date(2024, 11, 3, 5, 30, 0, 0, time.UTC)},
		{"spring", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: 2}}, Minute: cal.Minute}}, TimezoneData: preparedNewYorkTZ}, time.Date(2024, 3, 10, 6, 0, 0, 0, time.UTC)},
		{"before2000", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}}, time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year1", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, StartTime: timestamppb.New(time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC))}, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"utc2101", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneData: preparedNewYorkTZ}, time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"year9999", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"end exhaustion", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}, EndTime: timestamppb.New(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))}, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed := "seed with a long tail crossing several hash blocks........................"
			h, r, z, compiled := specFixture(t, tc.spec, seed)
			want, err := compiled.GetNextTime(seed, tc.after)
			require.NoError(t, err)
			for _, budget := range []int{1, 7, 64} {
				t.Run(fmt.Sprint(budget), func(t *testing.T) {
					got := runPreparedSpec(t, h, r, z, tc.after, budget)
					require.Equal(t, want.Nominal, got.Nominal)
					require.Equal(t, want.Next, got.Actual)
					require.Equal(t, want.ComputeLimitWarning, got.Warned)
				})
			}
		})
	}
}

func TestPreparedSpecReadFaultRollback(t *testing.T) {
	h, r, z, _ := specFixture(t, &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, Jitter: durationpb.New(time.Second)}, "abcdefghijklmnopqrstuvwxyz0123456789")
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var state PreparedSpecState
	for state.Phase != specComplete {
		fail := r
		fail.Term = func(bool, uint64) (PreparedSpecTerm, error) { return PreparedSpecTerm{}, errors.New("fault") }
		fail.Seed = func(uint64) (SeedWindow, error) { return SeedWindow{}, errors.New("fault") }
		got, _, err := AdvancePreparedSpec(state, h, z, after, fail, 64)
		if err != nil {
			require.Equal(t, state, got)
		}
		state, _, err = AdvancePreparedSpec(state, h, z, after, r, 1)
		require.NoError(t, err)
	}
}

func TestPreparedSpecKnownCandidates(t *testing.T) {
	t.Parallel()
	cal := &schedulepb.StructuredCalendarSpec{Hour: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{Start: 30}}}
	h, r, z, _ := specFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneData: preparedNewYorkTZ}, "")
	first := time.Date(2024, 11, 3, 5, 30, 0, 0, time.UTC)
	second := time.Date(2024, 11, 3, 6, 30, 0, 0, time.UTC)
	require.Equal(t, first, runPreparedSpec(t, h, r, z, first.Add(-time.Minute), 1).Actual)
	require.Equal(t, second, runPreparedSpec(t, h, r, z, first, 1).Actual)
	cal.Hour = []*schedulepb.Range{{Start: 2}}
	h, r, z, _ = specFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, TimezoneData: preparedNewYorkTZ}, "")
	require.Equal(t, time.Date(2024, 3, 11, 6, 30, 0, 0, time.UTC), runPreparedSpec(t, h, r, z, time.Date(2024, 3, 10, 6, 0, 0, 0, time.UTC), 1).Actual)
}

func TestPreparedSpecAutomaticProbe(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 0, 0, 0, 500000000, time.UTC)
	h, r, z, _ := specFixture(t, &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, StartTime: timestamppb.New(start), ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 1}}}}}, "")
	after := start.Add(-time.Hour)
	// GetNextTime preserves its historical StartTime-1s wrapper; automatic probes
	// preserve their distinct StartTime-1ns wrapper and disposition of exclusions.
	require.Equal(t, start.Truncate(time.Second), runPreparedSpec(t, h, r, z, after, 1).Nominal)
	h.ReturnExcluded = true
	got := runPreparedSpec(t, h, r, z, after, 1)
	require.True(t, got.Excluded)
	require.Equal(t, start.Truncate(time.Second).Add(time.Second), got.Nominal)
	require.Equal(t, got.Nominal, got.Actual)
}

func TestPreparedSpecLimitAndRefresh(t *testing.T) {
	t.Parallel()
	full := &schedulepb.StructuredCalendarSpec{Second: []*schedulepb.Range{{End: 59}}, Minute: []*schedulepb.Range{{End: 59}}, Hour: []*schedulepb.Range{{End: 23}}}
	h, r, z, _ := specFixture(t, &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{full}}, "seed")
	h.MaxIterations = 3
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var state PreparedSpecState
	for i := 0; i < 100; i++ {
		next, _, err := AdvancePreparedSpec(state, h, z, after, r, 1)
		if err != nil {
			require.ErrorIs(t, err, legacy.ErrComputeLimitExceeded)
			require.Equal(t, state, next)
			require.Equal(t, 3, state.Iterations)
			break
		}
		state = next
		require.Less(t, i, 99)
	}
	other, err := NewResolvedLocation(PreparedUTC())
	require.NoError(t, err)
	other.fingerprint[0] ^= 1
	got, _, err := AdvancePreparedSpec(state, h, other, after, r, 64)
	require.ErrorIs(t, err, ErrPreparedZoneChanged)
	require.Equal(t, state, got)
	corrupt := state
	corrupt.Index = h.Terms + 1
	corrupt.Phase = specUnion
	got, _, err = AdvancePreparedSpec(corrupt, h, z, after, r, 64)
	require.Error(t, err)
	require.Equal(t, corrupt, got)
}

func TestPreparedSpecFollowingIgnoresEndAndExclusions(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(10 * time.Second)}}, Jitter: durationpb.New(time.Hour), EndTime: timestamppb.New(base.Add(15 * time.Second)), ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 20}}}}}
	h, r, z, compiled := specFixture(t, spec, "following seed")
	got := runPreparedSpec(t, h, r, z, base, 1)
	want, err := compiled.GetNextTime("following seed", base)
	require.NoError(t, err)
	require.Equal(t, base.Add(10*time.Second), got.Nominal)
	require.Equal(t, want.Next, got.Actual)
	require.Less(t, got.Actual.Sub(got.Nominal), 10*time.Second)
	require.Equal(t, 10*time.Second, got.MaxJitter)
}

func TestPreparedSpecFixedReadBudget(t *testing.T) {
	t.Parallel()
	for _, count := range []uint64{4, 5, 1000, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			h := PreparedSpecHeader{SourceID: [32]byte{1}, Terms: count}
			r := PreparedSpecReader{Term: func(bool, uint64) (PreparedSpecTerm, error) {
				return PreparedSpecTerm{IsInterval: true, Interval: 60}, nil
			}, Seed: seedReader(nil)}
			z, err := NewResolvedLocation(PreparedUTC())
			require.NoError(t, err)
			after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			state, stats, err := AdvancePreparedSpec(PreparedSpecState{}, h, z, after, r, 4)
			require.NoError(t, err)
			require.Equal(t, 4, stats.Steps)
			require.Equal(t, 3, stats.TermReads)
			require.Equal(t, PreparedSpecResult{}, state.Result())
			done := runPreparedSpec(t, h, r, z, after, 64)
			require.Equal(t, after.Add(time.Minute), done.Actual)
		})
	}
}

func TestPreparedSpecExcludedFinalExhaustion(t *testing.T) {
	t.Parallel()
	cal := &schedulepb.StructuredCalendarSpec{Year: []*schedulepb.Range{{Start: 2024}}, Month: []*schedulepb.Range{{Start: 1}}, DayOfMonth: []*schedulepb.Range{{Start: 1}}}
	h, r, z, compiled := specFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}, ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{cal}}, "")
	after := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	want, err := compiled.GetNextTime("", after)
	require.NoError(t, err)
	for _, budget := range []int{1, 7, 64} {
		got := runPreparedSpec(t, h, r, z, after, budget)
		require.Equal(t, PreparedSpecResult{Done: true, ComputeLimitWarning: want.ComputeLimitWarning}, got.Result())
	}
}

func TestPreparedSpecPublicYearOneSentinel(t *testing.T) {
	t.Parallel()
	start := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	after := start.Add(-time.Nanosecond)
	for _, tc := range []struct {
		name string
		spec *schedulepb.ScheduleSpec
	}{
		{"interval zero exhaustion", &schedulepb.ScheduleSpec{StartTime: timestamppb.New(start), Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}},
		{"calendar zero ignored", &schedulepb.ScheduleSpec{StartTime: timestamppb.New(start), StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{}, {Second: []*schedulepb.Range{{Start: 1}}}}}},
		{"default epoch clamp", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, r, z, compiled := specFixture(t, tc.spec, "")
			h.ReturnExcluded = true
			boundary := after
			if !h.HasStart {
				boundary = time.Unix(0, 0).Add(-time.Nanosecond)
			}
			want, excluded := compiled.NextTimeCandidate("", boundary)
			got := runPreparedSpec(t, h, r, z, after, 1)
			require.Equal(t, want.Nominal, got.Nominal)
			require.Equal(t, want.Next, got.Actual)
			require.Equal(t, excluded, got.Excluded)
			require.Equal(t, !want.Nominal.IsZero(), got.Found)
			encoded, err := EncodePreparedSpecState(got)
			require.NoError(t, err)
			decoded, err := DecodePreparedSpecState(encoded, h, z, after)
			require.NoError(t, err)
			require.Equal(t, got, decoded)
		})
	}
}

// A zero-jitter lookup must not pay for a second union. Keep this assertion
// against observed reader calls and the legacy result, across wire restarts.
func TestPreparedSpecZeroJitterSingleUnion(t *testing.T) {
	t.Parallel()
	const terms = 1000
	spec := &schedulepb.ScheduleSpec{}
	for range terms {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{Interval: durationpb.New(time.Minute)})
	}
	h, reader, zone, reference := specFixture(t, spec, "seed")
	after := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	want, err := reference.GetNextTime("seed", after)
	require.NoError(t, err)
	reads := 0
	original := reader.Term
	reader.Term = func(exclusion bool, index uint64) (PreparedSpecTerm, error) {
		reads++
		return original(exclusion, index)
	}
	var state PreparedSpecState
	for n := 0; n < terms+20; n++ {
		next, stats, err := AdvancePreparedSpec(state, h, zone, after, reader, 1)
		require.NoError(t, err)
		require.LessOrEqual(t, stats.Steps, 1)
		wire, err := EncodePreparedSpecState(next)
		require.NoError(t, err)
		state, err = DecodePreparedSpecState(wire, h, zone, after)
		require.NoError(t, err)
		if state.Result().Done {
			require.Equal(t, want.Nominal, state.Nominal)
			require.Equal(t, want.Next, state.Actual)
			require.Equal(t, terms, reads)
			return
		}
	}
	t.Fatal("zero-jitter lookup did not finish after one union")
}
