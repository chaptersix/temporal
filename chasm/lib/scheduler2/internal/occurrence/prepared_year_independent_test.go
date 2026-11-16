package occurrence

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func TestPreparedYearIndependentMaskSuccessor(t *testing.T) {
	t.Parallel()
	masks := []CalendarMasks{{AllYears: true}, {}, {Years: [2]uint64{1, 0}}, {Years: [2]uint64{1 << 63, 1}}, {Years: [2]uint64{0, 1 << 36}}, {Years: [2]uint64{^uint64(0), (1 << 37) - 1}}}
	for year := 2000; year <= 2100; year++ {
		m := CalendarMasks{}
		m.Years[(year-2000)/64] = 1 << uint((year-2000)%64)
		masks = append(masks, m)
	}
	for _, m := range masks {
		for _, after := range []int{1, 1969, 1999, 2000, 2001, 2062, 2063, 2064, 2065, 2099, 2100, 2101, 9999} {
			expected, found := 0, false
			for year := after; year <= 2100; year++ {
				if calendarYearMatches(m, year) {
					expected, found = year, true
					break
				}
			}
			actual, ok := nextPreparedCalendarYear(m, after)
			require.Equal(t, found, ok)
			require.Equal(t, expected, actual, "after=%d mask=%v", after, m)
		}
	}
}
func TestPreparedYearIndependentReferenceAndResume(t *testing.T) {
	t.Parallel()
	calendars := []*schedulepb.StructuredCalendarSpec{
		{},
		{Year: []*schedulepb.Range{{Start: 2064}}},
		{Year: []*schedulepb.Range{{Start: 2063}, {Start: 2064}, {Start: 2100}}},
		{Year: []*schedulepb.Range{{Start: 2024}}, Month: []*schedulepb.Range{{Start: 11}}, DayOfMonth: []*schedulepb.Range{{Start: 3}}, Hour: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{Start: 15}}},
		{Year: []*schedulepb.Range{{Start: 2024}}, Month: []*schedulepb.Range{{Start: 3}}, DayOfMonth: []*schedulepb.Range{{Start: 10}}, Hour: []*schedulepb.Range{{Start: 2}}, Minute: []*schedulepb.Range{{Start: 30}}},
		{Year: []*schedulepb.Range{{Start: 2100}}, Month: []*schedulepb.Range{{Start: 2}}, DayOfMonth: []*schedulepb.Range{{Start: 29}}},
	}
	for zi, data := range [][]byte{nil, preparedNewYorkTZ, preparedLondonTZ} {
		for ci, calendar := range calendars {
			t.Run(fmt.Sprintf("zone%d/calendar%d", zi, ci), func(t *testing.T) {
				var zone *PreparedLocation
				var err error
				name := "UTC"
				if data == nil {
					zone, err = NewResolvedLocation(PreparedUTC())
				} else {
					name = "custom"
					zone, err = PreparedLocationFromTZData(name, data, time.Unix(0, 0))
				}
				require.NoError(t, err)
				definition, reference := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{calendar}, TimezoneName: name, TimezoneData: data})
				for _, boundary := range []string{"1999-12-30T23:59:59Z", "2023-12-31T23:59:59Z", "2024-11-03T05:15:00Z", "2063-12-31T23:59:59Z", "2064-01-01T00:00:00Z", "2099-12-31T23:59:59Z", "2100-12-31T23:59:59Z"} {
					after, err := time.Parse(time.RFC3339, boundary)
					require.NoError(t, err)
					expected, err := reference.GetNextTime("", after)
					require.NoError(t, err)
					single, stats := runPreparedCalendar(t, definition, zone, after, []int{1})
					mixed, mixedStats := runPreparedCalendar(t, definition, zone, after, []int{64, 1, 7})
					require.Equal(t, single, mixed)
					require.Equal(t, stats, mixedStats)
					candidate, found, done := single.Result()
					require.True(t, done)
					if found && candidate.Year() > 2100 {
						candidate, found = time.Time{}, false
					}
					require.Equal(t, !expected.Nominal.IsZero(), found)
					require.True(t, candidate.Equal(expected.Nominal), "after %s got %s want %s", boundary, candidate, expected.Nominal)
				}
			})
		}
	}
}
func TestPreparedYearIndependentPersistedYearPhase(t *testing.T) {
	t.Parallel()
	d, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Year: []*schedulepb.Range{{Start: 2064}}}}})
	z, err := NewResolvedLocation(PreparedUTC())
	require.NoError(t, err)
	after := time.Date(1999, 11, 3, 1, 15, 0, 0, time.UTC)
	state := CalendarNextState{}
	for state.Phase != CalendarNextYear {
		state, _, err = AdvancePreparedCalendar(state, d, z, after, 1)
		require.NoError(t, err)
	}
	// Existing durable Year-phase cursors retain the same field/schema meaning.
	wire, err := json.Marshal(state)
	require.NoError(t, err)
	var restored CalendarNextState
	require.NoError(t, json.Unmarshal(wire, &restored))
	next, stats, err := AdvancePreparedCalendar(restored, d, z, after, 1)
	require.NoError(t, err)
	require.Equal(t, CalendarNextMonth, next.Phase)
	require.Equal(t, 2064, next.Year)
	require.Equal(t, 1, next.Month)
	require.Equal(t, 1, next.Day)
	require.Zero(t, next.Hour)
	require.Zero(t, next.Minute)
	require.Zero(t, next.Second)
	require.False(t, next.SecondCopy)
	require.Equal(t, 1, stats.Steps)
	require.Zero(t, stats.ZoneConversions)
}
