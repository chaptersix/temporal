package occurrence

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func nativePacific() WindowsTimezoneSource {
	return WindowsTimezoneSource{WordBits: 64, InitializationYear: 2024, Bias: 480, DaylightBias: -60, StandardDate: WindowsSystemTime{Month: 11, Day: 1, Hour: 2}, DaylightDate: WindowsSystemTime{Month: 3, Day: 2, Hour: 2}}
}
func TestPreparedWindowsKnownTransitions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		source                 WindowsTimezoneSource
		first, second          string
		before, between, after int64
	}{
		{"north", nativePacific(), "2024-03-10T10:00:00Z", "2024-11-03T09:00:00Z", -28800, -25200, -28800},
		{"south", WindowsTimezoneSource{WordBits: 64, InitializationYear: 2024, Bias: -600, DaylightBias: -60, StandardDate: WindowsSystemTime{Month: 4, Day: 1, Hour: 3}, DaylightDate: WindowsSystemTime{Month: 10, Day: 1, Hour: 2}}, "2024-04-06T16:00:00Z", "2024-10-05T16:00:00Z", 39600, 36000, 39600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			zone, err := PrepareWindowsTimezone(tc.source)
			require.NoError(t, err)
			require.Equal(t, 25, int(zone.PageCount()))
			require.False(t, zone.cache.present)
			first, err := time.Parse(time.RFC3339, tc.first)
			require.NoError(t, err)
			second, err := time.Parse(time.RFC3339, tc.second)
			require.NoError(t, err)
			for _, sample := range []struct {
				at   int64
				want int64
			}{{first.Unix() - 1, tc.before}, {first.Unix(), tc.between}, {second.Unix() - 1, tc.between}, {second.Unix(), tc.after}} {
				var work timezoneWork
				offset, _, _, err := zone.lookup(sample.at, &work)
				require.NoError(t, err)
				require.Equal(t, sample.want, offset)
				require.LessOrEqual(t, work.pages, 66)
			}
			var work timezoneWork
			earliest, err := zone.transition(0, &work)
			require.NoError(t, err)
			latest, err := zone.transition(399, &work)
			require.NoError(t, err)
			offset, _, _, err := zone.lookup(earliest.when-1, &work)
			require.NoError(t, err)
			require.Equal(t, zone.first, offset)
			offset, _, _, err = zone.lookup(latest.when+100*365*86400, &work)
			require.NoError(t, err)
			require.Equal(t, latest.offset, offset)
		})
	}
}
func TestPreparedWindowsNativeArithmetic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		bits           uint8
		bias, standard int32
		dst            bool
		want           int64
	}{
		{"fixed ignores standard", 64, 60, math.MaxInt32, false, -3600},
		{"wide positive", 64, math.MinInt32, 0, false, 128849018880},
		{"wide negative", 64, math.MaxInt32, 0, false, -128849018820},
		{"native32 wraps product", 32, math.MaxInt32, 0, false, 60},
		{"native32 negation wraps", 32, math.MinInt32, 0, false, 0},
		{"sum before cast64", 64, math.MaxInt32, 1, true, 128849018880},
		{"sum before cast32", 32, math.MaxInt32, 1, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := nativePacific()
			source.WordBits = tc.bits
			source.Bias = tc.bias
			source.StandardBias = tc.standard
			if !tc.dst {
				source.StandardDate.Month = 0
			}
			zone, err := PrepareWindowsTimezone(source)
			require.NoError(t, err)
			require.Equal(t, tc.want, zone.first)
			if !tc.dst {
				require.Equal(t, preparedTimezoneCache{present: true, start: preparedTZAlpha, end: preparedTZOmega, offset: tc.want}, zone.cache)
			}
			encoded := source.Bytes()
			require.Len(t, encoded, 182)
			roundTrip, err := WindowsTimezoneSourceFromBytes(encoded)
			require.NoError(t, err)
			require.Equal(t, source, roundTrip)
			other, err := PrepareWindowsTimezone(roundTrip)
			require.NoError(t, err)
			require.Equal(t, zone.Fingerprint(), other.Fingerprint())
		})
	}
}

// nativeReferenceTransitions is a test-only source transcription of Go1.27.3
// time/zoneinfo_windows.go initLocalFromTZI/pseudoUnix on the 64-bit target.
// It retains source order and explicitly models the 64-bit source arithmetic.
func nativeReferenceTransitions(source WindowsTimezoneSource) []preparedTZTransition {
	offsets := [2]int64{-int64(source.Bias+source.StandardBias) * 60, -int64(source.Bias+source.DaylightBias) * 60}
	dates := [2]WindowsSystemTime{source.StandardDate, source.DaylightDate}
	indices := [2]int{0, 1}
	if dates[0].Month > dates[1].Month {
		dates[0], dates[1] = dates[1], dates[0]
		indices[0], indices[1] = indices[1], indices[0]
	}
	result := make([]preparedTZTransition, 400)
	position := 0
	for y := int(source.InitializationYear) - 100; y < int(source.InitializationYear)+100; y++ {
		for j, date := range dates {
			base := time.Date(y, time.Month(date.Month), 1, int(date.Hour), int(date.Minute), int(date.Second), 0, time.UTC)
			weekday := int(date.DayOfWeek) - int(base.Weekday())
			if weekday < 0 {
				weekday += 7
			}
			day := 1 + weekday
			if week := int(date.Day) - 1; week < 4 {
				day += week * 7
			} else {
				day += 28
				if day > time.Date(y, time.Month(date.Month)+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1).Day() {
					day -= 7
				}
			}
			result[position] = preparedTZTransition{when: base.Unix() + int64(day-1)*86400 - offsets[indices[1-j]], offset: offsets[indices[j]]}
			position++
		}
	}
	return result
}

// nativeReferenceLookup transcribes Go Location.lookup with its native no-cache,
// no-extension inputs. In particular, it must not sort its transition array.
func nativeReferenceLookup(tx []preparedTZTransition, first, sec int64) (offset, start, end int64) {
	if sec < tx[0].when {
		return first, math.MinInt64, tx[0].when
	}
	end = math.MaxInt64
	lo, hi := 0, len(tx)
	for hi-lo > 1 {
		middle := int(uint(lo+hi) >> 1)
		if sec < tx[middle].when {
			end = tx[middle].when
			hi = middle
		} else {
			lo = middle
		}
	}
	return tx[lo].offset, tx[lo].when, end
}
func TestPreparedWindowsUnsortedSourceComparison(t *testing.T) {
	t.Parallel()
	source := nativePacific()
	source.Bias = 0
	source.StandardBias = math.MaxInt32
	source.DaylightBias = math.MinInt32
	zone, err := PrepareWindowsTimezone(source)
	require.NoError(t, err)
	expected := nativeReferenceTransitions(source)
	require.Greater(t, expected[0].when, expected[1].when)
	for i, want := range expected {
		var work timezoneWork
		got, err := zone.transition(i, &work)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	queries := []int64{0, math.MinInt64, math.MaxInt64}
	for _, tx := range expected {
		queries = append(queries, tx.when-1, tx.when, tx.when+1)
	}
	for _, sec := range queries {
		wantOffset, wantStart, wantEnd := nativeReferenceLookup(expected, zone.first, sec)
		var work timezoneWork
		offset, start, end, err := zone.lookup(sec, &work)
		require.NoError(t, err)
		require.Equal(t, [3]int64{wantOffset, wantStart, wantEnd}, [3]int64{offset, start, end}, "instant %d", sec)
		require.LessOrEqual(t, work.pages, 66)
	}
}
func TestPreparedWindowsCodecAndReadFailures(t *testing.T) {
	t.Parallel()
	source := nativePacific()
	source.StandardBias = math.MaxInt32
	zone, err := PrepareWindowsTimezone(source)
	require.NoError(t, err)
	header := zone.Header()
	require.Len(t, header, 70)
	pages := make(map[uint32][]byte)
	for i := uint32(0); i < zone.PageCount(); i++ {
		page, err := zone.Page(i)
		require.NoError(t, err)
		require.Len(t, page, 294)
		pages[i] = page
	}
	reads := 0
	restored, err := RestorePreparedTimezone(header, func(i uint32) ([]byte, error) { reads++; return pages[i], nil })
	require.NoError(t, err)
	require.Zero(t, reads)
	require.Equal(t, header, restored.Header())
	for i := uint32(0); i < zone.PageCount(); i++ {
		page, err := restored.Page(i)
		require.NoError(t, err)
		require.Equal(t, pages[i], page)
	}
	for n := 0; n < len(header); n++ {
		_, err := RestorePreparedTimezone(header[:n], func(uint32) ([]byte, error) { return nil, nil })
		require.Error(t, err)
	}
	for ordinal := uint32(0); ordinal < zone.PageCount(); ordinal++ {
		broken, err := RestorePreparedTimezone(header, func(i uint32) ([]byte, error) {
			if i == ordinal {
				return nil, errors.New("injected")
			}
			return pages[i], nil
		})
		require.NoError(t, err)
		var work timezoneWork
		_, err = broken.transition(int(ordinal)*16, &work)
		require.ErrorContains(t, err, "injected")
		bad := bytes.Clone(pages[ordinal])
		bad[0] = 1
		broken, err = RestorePreparedTimezone(header, func(uint32) ([]byte, error) { return bad, nil })
		require.NoError(t, err)
		_, err = broken.Page(ordinal)
		require.Error(t, err)
	}
}
func TestPreparedWindowsSourceValidationAndLastWeek(t *testing.T) {
	t.Parallel()
	source := nativePacific()
	source.DaylightDate = WindowsSystemTime{Month: 3, Day: 5, DayOfWeek: 0, Hour: 2}
	zone, err := PrepareWindowsTimezone(source)
	require.NoError(t, err)
	var work timezoneWork
	tx, err := zone.transition(200, &work)
	require.NoError(t, err)
	require.Equal(t, time.Date(2024, 3, 31, 10, 0, 0, 0, time.UTC).Unix(), tx.when)
	source.DaylightDate.Month = 13
	_, err = PrepareWindowsTimezone(source)
	require.ErrorContains(t, err, "last-week month")
	// Go normalizes non-last-week civil fields, so do not broadly reject them.
	source.DaylightDate.Day = 1
	_, err = PrepareWindowsTimezone(source)
	require.NoError(t, err)
	source.WordBits = 16
	_, err = PrepareWindowsTimezone(source)
	require.Error(t, err)
	for n := 0; n < WindowsTimezoneSourceBytes; n++ {
		_, err := WindowsTimezoneSourceFromBytes(make([]byte, n))
		require.Error(t, err)
	}
}

func TestPreparedWindowsDateAndCalendarParity(t *testing.T) {
	t.Parallel()
	source := nativePacific()
	zone, err := PrepareWindowsTimezone(source)
	require.NoError(t, err)
	transitions := nativeReferenceTransitions(source)
	times := make([]int64, len(transitions))
	indices := make([]byte, len(transitions))
	for i, tx := range transitions {
		times[i] = tx.when
		if tx.offset == -25200 {
			indices[i] = 1
		}
	}
	data := timezoneFixture(times, indices, []preparedTZZone{{offset: -28800}, {offset: -25200, daylight: true}}, "", true)
	reference, err := time.LoadLocationFromTZData("native-reference", data)
	require.NoError(t, err)
	for _, date := range [][6]int{{2024, 3, 10, 2, 30, 0}, {2024, 11, 3, 1, 15, 0}, {1923, 1, 1, 0, 0, 0}, {2124, 7, 1, 0, 0, 0}} {
		var work timezoneWork
		got, err := zone.date(date[0], date[1], date[2], date[3], date[4], date[5], &work)
		require.NoError(t, err)
		want := time.Date(date[0], time.Month(date[1]), date[2], date[3], date[4], date[5], 0, reference)
		require.Equal(t, want.Unix(), got.Unix())
	}
	for _, bias := range []int32{math.MinInt32, math.MaxInt32} {
		wide, err := PrepareWindowsTimezone(WindowsTimezoneSource{WordBits: 64, InitializationYear: 2024, Bias: bias})
		require.NoError(t, err)
		var work timezoneWork
		got, err := wide.date(2024, 7, 1, 1, 2, 3, &work)
		require.NoError(t, err)
		want := time.Date(2024, 7, 1, 1, 2, 3, 0, time.FixedZone("wide-native", int(wide.first)))
		require.Equal(t, want.Unix(), got.Unix())
	}
	for _, tc := range []struct {
		hour, minute int32
		after, want  string
	}{
		{1, 15, "2024-11-03T08:14:59Z", "2024-11-03T08:15:00Z"},
		{1, 15, "2024-11-03T08:15:00Z", "2024-11-03T09:15:00Z"},
		{2, 30, "2024-03-10T09:59:59Z", "2024-03-11T09:30:00Z"},
	} {
		definition, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: tc.hour}}, Minute: []*schedulepb.Range{{Start: tc.minute}}}}})
		location, err := NewResolvedLocation(zone)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339, tc.after)
		require.NoError(t, err)
		state, _ := runPreparedCalendar(t, definition, location, after, []int{1, 16, 64})
		candidate, found, done := state.Result()
		require.True(t, found && done)
		require.Equal(t, tc.want, candidate.Format(time.RFC3339))
	}
}
func TestPreparedWindowsCalendarReaderRollback(t *testing.T) {
	t.Parallel()
	zone, err := PrepareWindowsTimezone(nativePacific())
	require.NoError(t, err)
	definition, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: 1}}, Minute: []*schedulepb.Range{{Start: 15}}}}})
	location, err := NewResolvedLocation(zone)
	require.NoError(t, err)
	after := time.Date(2024, 11, 3, 8, 15, 0, 0, time.UTC)
	original, _, err := AdvancePreparedCalendar(CalendarNextState{}, definition, location, after, 1)
	require.NoError(t, err)
	require.NotEqual(t, CalendarNextState{}, original)
	reads := 0
	restored, err := RestorePreparedTimezone(zone.Header(), func(i uint32) ([]byte, error) { reads++; return zone.Page(i) })
	require.NoError(t, err)
	location, err = NewResolvedLocation(restored)
	require.NoError(t, err)
	_, _, err = AdvancePreparedCalendar(original, definition, location, after, 64)
	require.NoError(t, err)
	require.Positive(t, reads)
	for failed := 1; failed <= reads; failed++ {
		calls := 0
		broken, err := RestorePreparedTimezone(zone.Header(), func(i uint32) ([]byte, error) {
			calls++
			if calls == failed {
				return nil, errors.New("native addressed fault")
			}
			return zone.Page(i)
		})
		require.NoError(t, err)
		location, err := NewResolvedLocation(broken)
		require.NoError(t, err)
		got, _, err := AdvancePreparedCalendar(original, definition, location, after, 64)
		require.ErrorContains(t, err, "native addressed fault")
		require.Equal(t, original, got)
	}
}
