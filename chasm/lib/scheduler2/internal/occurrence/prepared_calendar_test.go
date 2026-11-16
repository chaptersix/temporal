package occurrence

import (
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

//go:embed testdata/America_New_York.tzif
var preparedNewYorkTZ []byte

//go:embed testdata/Europe_London.tzif
var preparedLondonTZ []byte

func preparedCalendarFixture(t *testing.T, spec *schedulepb.ScheduleSpec) (PreparedCalendar, *legacy.CompiledSpec) {
	t.Helper()
	spec = proto.Clone(spec).(*schedulepb.ScheduleSpec)
	for _, cal := range spec.StructuredCalendar {
		fields := []struct {
			target      *[]*schedulepb.Range
			first, last int32
		}{
			{&cal.Second, 0, 0}, {&cal.Minute, 0, 0}, {&cal.Hour, 0, 0},
			{&cal.DayOfMonth, 1, 31}, {&cal.Month, 1, 12}, {&cal.DayOfWeek, 0, 6},
		}
		for _, field := range fields {
			if len(*field.target) == 0 {
				*field.target = []*schedulepb.Range{{Start: field.first, End: field.last, Step: 1}}
			}
		}
	}
	compiled, err := legacy.NewSpecBuilder(func() int { return 0 }, func() int { return 0 }).NewCompiledSpec(spec)
	require.NoError(t, err)
	cal := compiled.CanonicalForm().StructuredCalendar[0]
	var ranges [7][]CalendarRange
	fields := [][]*schedulepb.Range{cal.Second, cal.Minute, cal.Hour, cal.DayOfMonth, cal.Month, cal.DayOfWeek, cal.Year}
	for f, values := range fields {
		for _, r := range values {
			ranges[f] = append(ranges[f], CalendarRange{Start: r.Start, End: r.End, Step: r.Step})
		}
	}
	header, reader := preparationSource(ranges)
	var state PreparationState
	for !state.Done {
		state, _, err = PrepareCalendar(state, header, reader, 64)
		require.NoError(t, err)
	}
	return PreparedCalendar{SourceID: header.SourceID, Masks: state.Partial}, compiled
}
func runPreparedCalendar(t *testing.T, d PreparedCalendar, z *PreparedLocation, after time.Time, budgets []int) (CalendarNextState, CalendarNextStats) {
	t.Helper()
	var state CalendarNextState
	var total CalendarNextStats
	for i := 0; i < 200000; i++ {
		next, stats, err := AdvancePreparedCalendar(state, d, z, after, budgets[i%len(budgets)])
		require.NoError(t, err)
		require.LessOrEqual(t, stats.Steps, budgets[i%len(budgets)])
		require.Positive(t, stats.Steps)
		require.LessOrEqual(t, stats.ZoneConversions, 6*stats.Steps)
		total.Steps += stats.Steps
		total.ZoneConversions += stats.ZoneConversions
		total.TimezoneLookups += stats.TimezoneLookups
		total.TimezonePageReads += stats.TimezonePageReads
		require.LessOrEqual(t, stats.TimezoneLookups, 8*stats.Steps)
		require.LessOrEqual(t, stats.TimezonePageReads, 66*stats.TimezoneLookups)
		wire, err := json.Marshal(next)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(wire, &state))
		if state.Phase == CalendarNextComplete {
			return state, total
		}
	}
	t.Fatal("calendar did not complete within fixture bound")
	return state, total
}
func TestPreparedCalendarReference(t *testing.T) {
	t.Parallel()
	zones := []struct {
		name string
		data []byte
	}{{"UTC", nil}, {"America/New_York", preparedNewYorkTZ}, {"Europe/London", preparedLondonTZ}}
	calendars := []struct {
		name string
		cal  *schedulepb.StructuredCalendarSpec
	}{
		{"every second", &schedulepb.StructuredCalendarSpec{Second: []*schedulepb.Range{{Start: 0, End: 59}}, Minute: []*schedulepb.Range{{Start: 0, End: 59}}, Hour: []*schedulepb.Range{{Start: 0, End: 23}}}},
		{"fold hour", &schedulepb.StructuredCalendarSpec{Second: []*schedulepb.Range{{Start: 0, End: 59, Step: 17}}, Minute: []*schedulepb.Range{{Start: 15, End: 45, Step: 15}}, Hour: []*schedulepb.Range{{Start: 1}}}},
		{"leap", &schedulepb.StructuredCalendarSpec{DayOfMonth: []*schedulepb.Range{{Start: 29}}, Month: []*schedulepb.Range{{Start: 2}}}},
		{"impossible", &schedulepb.StructuredCalendarSpec{DayOfMonth: []*schedulepb.Range{{Start: 30}}, Month: []*schedulepb.Range{{Start: 2}}}},
		{"sparse year", &schedulepb.StructuredCalendarSpec{Year: []*schedulepb.Range{{Start: 2099}}}},
	}
	boundaries := []string{"2000-01-01T00:00:00Z", "2024-02-28T23:59:59.5Z", "2024-03-10T06:59:59Z", "2024-03-31T00:59:59Z", "2024-10-27T00:14:59Z", "2024-10-27T01:14:59Z", "2024-11-03T05:14:59Z", "2024-11-03T06:14:59Z", "2100-12-31T23:59:59Z"}
	for _, zone := range zones {
		for _, calendar := range calendars {
			t.Run(zone.name+"/"+calendar.name, func(t *testing.T) {
				t.Parallel()
				var location *PreparedLocation
				var err error
				if zone.data == nil {
					location, err = NewResolvedLocation(PreparedUTC())
				} else {
					location, err = PreparedLocationFromTZData(zone.name, zone.data, time.Now())
				}
				require.NoError(t, err)
				definition, compiled := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{calendar.cal}, TimezoneName: zone.name, TimezoneData: zone.data})
				for _, boundary := range boundaries {
					after, err := time.Parse(time.RFC3339Nano, boundary)
					require.NoError(t, err)
					expected, err := compiled.GetNextTime("", after)
					require.NoError(t, err)
					if calendar.name != "impossible" && after.Year() < 2099 {
						require.False(t, expected.Nominal.IsZero(), "candidate fixture must exercise a result")
					}
					single, steps := runPreparedCalendar(t, definition, location, after, []int{1})
					mixed, mixedSteps := runPreparedCalendar(t, definition, location, after, []int{64, 3, 17, 1})
					require.Equal(t, single, mixed)
					require.Equal(t, steps, mixedSteps)
					result, found, done := single.Result()
					require.True(t, done)
					// CompiledSpec adds a UTC horizon after the calendar's local-year horizon.
					if found && result.Year() > 2100 {
						result, found = time.Time{}, false
					}
					require.Equal(t, !expected.Nominal.IsZero(), found)
					require.True(t, result.Equal(expected.Nominal), "%s: got %s want %s", boundary, result, expected.Nominal)
				}
			})
		}
	}
}
func TestPreparedCalendarIdentityAndRefresh(t *testing.T) {
	t.Parallel()
	definition, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: 1}}}}})
	first, err := PreparedLocationFromTZData("named", preparedNewYorkTZ, time.Now())
	require.NoError(t, err)
	changed, err := PreparedLocationFromTZData("named", preparedLondonTZ, time.Now())
	require.NoError(t, err)
	after := time.Date(2024, 10, 27, 0, 0, 0, 0, time.UTC)
	partial, _, err := AdvancePreparedCalendar(CalendarNextState{}, definition, first, after, 1)
	require.NoError(t, err)
	next, stats, err := AdvancePreparedCalendar(partial, definition, changed, after, 64)
	require.ErrorIs(t, err, ErrPreparedZoneChanged)
	require.Equal(t, partial, next)
	require.Zero(t, stats)
	require.Equal(t, after, partial.SearchBoundary())
	restarted, _ := runPreparedCalendar(t, definition, changed, partial.SearchBoundary(), []int{1})
	completed, _ := runPreparedCalendar(t, definition, first, after, []int{64})
	preserved, stats, err := AdvancePreparedCalendar(completed, definition, changed, after, 64)
	require.NoError(t, err)
	require.Equal(t, completed, preserved)
	require.Zero(t, stats)
	require.NotEqual(t, completed.ResolvedZoneID, restarted.ResolvedZoneID)
	for _, kind := range []string{"nil zone", "source", "after", "budget", "cursor", "mask"} {
		t.Run(kind, func(t *testing.T) {
			d, z, b, state, limit := definition, first, after, partial, 1
			switch kind {
			case "nil zone":
				z = nil
			case "source":
				d.SourceID[0]++
			case "after":
				b = b.Add(time.Second)
			case "budget":
				limit = 65
			case "cursor":
				state.Month = 14
			case "mask":
				d.Masks.Fields[0] = 1 << 63
			default:
				t.Fatal("unexpected case")
			}
			next, _, err := AdvancePreparedCalendar(state, d, z, b, limit)
			require.Error(t, err)
			require.Equal(t, state, next)
		})
	}
}
func TestPreparedLocationInput(t *testing.T) {
	t.Parallel()
	for _, data := range [][]byte{nil, {}, []byte("bad TZif")} {
		_, err := PreparedLocationFromTZData("custom", data, time.Now())
		require.Error(t, err)
	}
	data := append([]byte(nil), preparedNewYorkTZ...)
	first, err := PreparedLocationFromTZData("custom", data, time.Now())
	require.NoError(t, err)
	for i := range data {
		data[i] = 0
	}
	same, err := PreparedLocationFromTZData("custom", preparedNewYorkTZ, time.Now())
	require.NoError(t, err)
	require.Equal(t, first.fingerprint, same.fingerprint)
	for _, at := range []time.Time{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)} {
		var work timezoneWork
		before, err := first.location.wall(at.Unix(), &work)
		require.NoError(t, err)
		after, err := same.location.wall(at.Unix(), &work)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	_, err = NewResolvedLocation(nil)
	require.Error(t, err)
	_, err = NewResolvedLocation(&PreparedTimezone{})
	require.Error(t, err)
}

func TestPreparedCalendarZoneOperationCounts(t *testing.T) {
	t.Parallel()
	definition, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0, End: 59}}, Minute: []*schedulepb.Range{{Start: 0, End: 59}}, Hour: []*schedulepb.Range{{Start: 0, End: 23}}}}})
	zone, err := NewResolvedLocation(PreparedUTC())
	require.NoError(t, err)
	for _, tc := range []struct {
		name                        string
		minute, second, conversions int
	}{
		{"no carry", 0, 0, 8},
		{"hour carry", 59, 59, 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			after := time.Date(2024, 1, 1, 12, tc.minute, tc.second, 0, time.UTC)
			state, stats := runPreparedCalendar(t, definition, zone, after, []int{1})
			candidate, found, done := state.Result()
			require.True(t, found && done)
			require.Equal(t, after.Add(time.Second), candidate)
			require.Equal(t, CalendarNextStats{Steps: 9, ZoneConversions: tc.conversions, TimezoneLookups: tc.conversions}, stats)
		})
	}
}

func TestPreparedCalendarKnownDSTInstants(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		data         []byte
		hour, minute int32
		after, want  string
	}{
		{"New York first fold", preparedNewYorkTZ, 1, 15, "2024-11-03T05:14:59Z", "2024-11-03T05:15:00Z"},
		{"New York second fold", preparedNewYorkTZ, 1, 15, "2024-11-03T05:15:00Z", "2024-11-03T06:15:00Z"},
		{"London first fold", preparedLondonTZ, 1, 15, "2024-10-27T00:14:59Z", "2024-10-27T00:15:00Z"},
		{"London second fold", preparedLondonTZ, 1, 15, "2024-10-27T00:15:00Z", "2024-10-27T01:15:00Z"},
		{"New York skipped hour", preparedNewYorkTZ, 2, 30, "2024-03-10T06:59:59Z", "2024-03-11T06:30:00Z"},
		{"London skipped hour", preparedLondonTZ, 1, 30, "2024-03-31T00:59:59Z", "2024-04-01T00:30:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			definition, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: tc.hour}}, Minute: []*schedulepb.Range{{Start: tc.minute}}}}, TimezoneName: tc.name, TimezoneData: tc.data})
			zone, err := PreparedLocationFromTZData(tc.name, tc.data, time.Now())
			require.NoError(t, err)
			after, err := time.Parse(time.RFC3339, tc.after)
			require.NoError(t, err)
			want, err := time.Parse(time.RFC3339, tc.want)
			require.NoError(t, err)
			state, _ := runPreparedCalendar(t, definition, zone, after, []int{1, 17})
			candidate, found, done := state.Result()
			require.True(t, found && done)
			require.Equal(t, want, candidate)
		})
	}
}

func TestPreparedCalendarLocalAndUTCHorizon(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		data  []byte
		found bool
	}{{"UTC", nil, false}, {"New York", preparedNewYorkTZ, true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, reference := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0, End: 59}}, Minute: []*schedulepb.Range{{Start: 0, End: 59}}, Hour: []*schedulepb.Range{{Start: 0, End: 23}}}}, TimezoneName: tc.name, TimezoneData: tc.data})
			var zone *PreparedLocation
			var err error
			if tc.data == nil {
				zone, err = NewResolvedLocation(PreparedUTC())
			} else {
				zone, err = PreparedLocationFromTZData(tc.name, tc.data, time.Now())
			}
			require.NoError(t, err)
			after := time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC)
			state, _ := runPreparedCalendar(t, d, zone, after, []int{1})
			candidate, found, done := state.Result()
			require.True(t, done)
			require.Equal(t, tc.found, found)
			if found {
				require.Equal(t, after.Add(time.Second), candidate)
			}
			wrapped, err := reference.GetNextTime("", after)
			require.NoError(t, err)
			require.True(t, wrapped.Nominal.IsZero())
		})
	}
}
