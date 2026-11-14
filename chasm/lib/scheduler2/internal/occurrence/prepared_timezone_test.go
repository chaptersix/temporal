package occurrence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func timezoneFixture(transitions []int64, indices []byte, zones []preparedTZZone, extension string, wide bool) []byte {
	block := func(wide bool) []byte {
		data := make([]byte, 44)
		copy(data, "TZif")
		if wide {
			data[4] = '2'
		}
		binary.BigEndian.PutUint32(data[32:36], uint32(len(transitions)))
		binary.BigEndian.PutUint32(data[36:40], uint32(len(zones)))
		binary.BigEndian.PutUint32(data[40:44], 4)
		for _, at := range transitions {
			if wide {
				data = binary.BigEndian.AppendUint64(data, uint64(at))
			} else {
				data = binary.BigEndian.AppendUint32(data, uint32(at))
			}
		}
		data = append(data, indices...)
		for _, zone := range zones {
			data = binary.BigEndian.AppendUint32(data, uint32(zone.offset))
			dst := byte(0)
			if zone.daylight {
				dst = 1
			}
			data = append(data, dst, 0)
		}
		return append(data, []byte("TZ0\x00")...)
	}
	var data []byte
	if wide {
		data = block(false)
		data[4] = '2'
	}
	data = append(data, block(wide)...)
	if extension != "" {
		data = append(data, []byte("\n"+extension+"\n")...)
	}
	return data
}

func TestPreparedTimezoneReference(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data []byte
	}{{"New York", preparedNewYorkTZ}, {"London", preparedLondonTZ}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Real fixture transitions are far from the two loader clock samples.
			prepared, err := PrepareTimezone(tc.data, time.Now())
			require.NoError(t, err)
			reference, err := time.LoadLocationFromTZData(tc.name, tc.data)
			require.NoError(t, err)
			for _, year := range []int{1900, 1931, 1969, 1970, 2000, 2024, 2050, 2100, 2168} {
				for month := 1; month <= 12; month++ {
					for _, day := range []int{1, 10, 27, 31} {
						for _, hour := range []int{0, 1, 2, 3, 23, 24} {
							var work timezoneWork
							got, err := prepared.date(year, month, day, hour, 30, 17, &work)
							require.NoError(t, err)
							want := time.Date(year, time.Month(month), day, hour, 30, 17, 0, reference)
							require.True(t, got.Equal(want), "%d/%d/%d %d got%s want%s", year, month, day, hour, got, want)
							wall, err := prepared.wall(got.Unix(), &work)
							require.NoError(t, err)
							require.Equal(t, want.Format("2006-01-02 15:04:05"), wall.Format("2006-01-02 15:04:05"))
							require.LessOrEqual(t, work.lookups, 3)
							require.LessOrEqual(t, work.pages, 66*work.lookups)
						}
					}
				}
			}
		})
	}
}

func TestPreparedTimezoneCustomAcceptance(t *testing.T) {
	t.Parallel()
	for _, wide := range []bool{false, true} {
		t.Run(fmt.Sprint(wide), func(t *testing.T) {
			t.Parallel()
			zones := []preparedTZZone{{offset: math.MinInt32, daylight: true}, {offset: math.MaxInt32}, {offset: 1234, daylight: true}}
			for _, indices := range [][]byte{{1, 1, 1}, {2, 1, 0}, {0, 2, 0}} {
				data := timezoneFixture([]int64{-100000, 0, 100000}, indices, zones, "", wide)
				got, err := PrepareTimezone(data, time.Now())
				require.NoError(t, err)
				want, err := time.LoadLocationFromTZData("fixture", data)
				require.NoError(t, err)
				for _, sec := range []int64{-200000, -100000, -99999, -1, 0, 1, 99999, 100000, 100001} {
					var work timezoneWork
					offset, _, _, err := got.lookup(sec, &work)
					require.NoError(t, err)
					_, expected := time.Unix(sec, 0).In(want).Zone()
					require.Equal(t, expected, offset)
				}
			}
		})
	}
	for _, extension := range []string{"STD0", "STD0,ignored", "STD0DST", "STD0DST,M3.2.0,M11.1.0", "STD0DST;J60/-2,J300/168:59:59", "<+03>-3<+04>-4,60/0,300/0", "STD-10DST-11,M10.1.0,M4.1.0", "invalid", "STD169", "STD0DST,M13.1.0,M4.1.0"} {
		t.Run(extension, func(t *testing.T) {
			t.Parallel()
			data := timezoneFixture([]int64{-1 << 40}, []byte{0}, []preparedTZZone{{offset: 1234}}, extension, true)
			got, err := PrepareTimezone(data, time.Now())
			require.NoError(t, err)
			want, err := time.LoadLocationFromTZData("extension", data)
			require.NoError(t, err)
			for _, year := range []int{1931, 1969, 1970, 2024, 2050, 2100} {
				for month := 1; month <= 12; month++ {
					at := time.Date(year, time.Month(month), 15, 12, 0, 0, 0, time.UTC)
					var work timezoneWork
					offset, start, end, err := got.lookup(at.Unix(), &work)
					require.NoError(t, err)
					_, expected := at.In(want).Zone()
					require.Equal(t, expected, offset)
					left, right := at.In(want).ZoneBounds()
					if !left.IsZero() {
						require.Equal(t, left.Unix(), start)
					}
					if !right.IsZero() {
						require.Equal(t, right.Unix(), end)
					}
				}
			}
		})
	}
}

func TestPreparedTimezoneUnsortedCache(t *testing.T) {
	t.Parallel()
	nowSec := time.Now().Unix()
	data := timezoneFixture([]int64{nowSec - 300, nowSec + 300, nowSec - 100, nowSec + 100}, []byte{0, 1, 2, 3}, []preparedTZZone{{offset: 0}, {offset: 3600}, {offset: 7200}, {offset: 10800}}, "", true)
	got, err := PrepareTimezone(data, time.Unix(nowSec, 0))
	require.NoError(t, err)
	require.Equal(t, preparedTimezoneCache{present: true, start: nowSec - 300, end: nowSec + 300, offset: 0}, got.cache)
	var work timezoneWork
	offset, _, _, err := got.lookup(nowSec, &work)
	require.NoError(t, err)
	require.Zero(t, offset)
	withoutCache := *got
	withoutCache.cache.present = false
	offset, _, _, err = withoutCache.lookup(nowSec, &work)
	require.NoError(t, err)
	require.Equal(t, 7200, offset)
	reference, err := time.LoadLocationFromTZData("unsorted", data)
	require.NoError(t, err)
	_, expected := time.Unix(nowSec, 0).In(reference).Zone()
	require.Zero(t, expected)
	shifted, err := PrepareTimezone(data, time.Unix(nowSec+301, 0))
	require.NoError(t, err)
	require.NotEqual(t, got.Fingerprint(), shifted.Fingerprint())
}

func TestPreparedTimezoneRejectsTruncationBeforeAllocation(t *testing.T) {
	t.Parallel()
	good := timezoneFixture([]int64{0}, []byte{0}, []preparedTZZone{{}}, "", true)
	for n := range len(good) {
		_, got := PrepareTimezone(good[:n], time.Time{})
		_, want := time.LoadLocationFromTZData("truncated", good[:n])
		require.Equal(t, want != nil, got != nil, "prefix%d", n)
	}
	bad := make([]byte, 44)
	copy(bad, "TZif")
	for i := 20; i < 44; i++ {
		bad[i] = 255
	}
	_, err := PrepareTimezone(bad, time.Time{})
	require.Error(t, err)
}

func TestPreparedTimezoneIndexedWorkAndFaults(t *testing.T) {
	t.Parallel()
	for _, n := range []int{4, 5, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			transitions := make([]int64, n)
			indices := make([]byte, n)
			for i := range transitions {
				transitions[i] = int64(i) * 100
				indices[i] = byte(i % 2)
			}
			data := timezoneFixture(transitions, indices, []preparedTZZone{{offset: 0}, {offset: 3600}}, "", true)
			z, err := PrepareTimezone(data, time.Unix(-1000, 0))
			require.NoError(t, err)
			for _, sec := range []int64{-1, 0, int64(n) * 50, int64(n) * 100} {
				var work timezoneWork
				_, _, _, err := z.lookup(sec, &work)
				require.NoError(t, err)
				require.Equal(t, 1, work.lookups)
				require.LessOrEqual(t, work.pages, 66)
				t.Logf("N=%d second=%d page_reads=%d fixed_page_slots=%d", n, sec, work.pages, preparedTimezonePageSize)
			}
			for _, kind := range []string{"missing", "identity", "ordinal", "length"} {
				broken := *z
				broken.readPage = func(ordinal int) (preparedTimezonePage, error) {
					page := z.pages[ordinal]
					switch kind {
					case "missing":
						return preparedTimezonePage{}, errors.New("injected missing page")
					case "identity":
						page.identity[0]++
					case "ordinal":
						page.ordinal++
					case "length":
						page.length--
					default:
						t.Fatal("unexpected corruption")
					}
					return page, nil
				}
				var work timezoneWork
				_, _, _, err := broken.lookup(int64(n)*50, &work)
				require.Error(t, err)
			}
		})
	}
}

func TestPreparedCalendarTimezoneReadFailurePreservesState(t *testing.T) {
	t.Parallel()
	d, _ := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Hour: []*schedulepb.Range{{Start: 2}}, Minute: []*schedulepb.Range{{Start: 30}}}}, TimezoneName: "New York", TimezoneData: preparedNewYorkTZ})
	z, err := PrepareTimezone(preparedNewYorkTZ, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	capability, err := NewResolvedLocation(z)
	require.NoError(t, err)
	after := time.Date(2024, 3, 10, 6, 59, 59, 0, time.UTC)
	partial, _, err := AdvancePreparedCalendar(CalendarNextState{}, d, capability, after, 1)
	require.NoError(t, err)
	_, stats, err := AdvancePreparedCalendar(partial, d, capability, after, 64)
	require.NoError(t, err)
	require.Positive(t, stats.TimezonePageReads)
	for fail := 1; fail <= stats.TimezonePageReads; fail++ {
		broken := *z
		reads := 0
		broken.readPage = func(ordinal int) (preparedTimezonePage, error) {
			reads++
			if reads == fail {
				return preparedTimezonePage{}, errors.New("injected timezone page failure")
			}
			return z.pages[ordinal], nil
		}
		supplied, err := NewResolvedLocation(&broken)
		require.NoError(t, err)
		next, _, err := AdvancePreparedCalendar(partial, d, supplied, after, 64)
		require.ErrorContains(t, err, "injected timezone page failure")
		require.Equal(t, partial, next)
	}
	var work timezoneWork
	_, err = z.date(2024, 3, 10, 2, 30, 0, &work)
	require.NoError(t, err)
	require.Equal(t, 2, work.lookups, "nonexistent wall time must exercise Date's second lookup")
}

func TestPreparedTimezoneFirstZoneAndDuplicates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		zones   []preparedTZZone
		indices []byte
		want    int
	}{
		{"unused zero", []preparedTZZone{{offset: 11, daylight: true}, {offset: 22}}, []byte{1, 1}, 11},
		{"nearest standard", []preparedTZZone{{offset: 11}, {offset: 22}, {offset: 33, daylight: true}}, []byte{2, 0}, 22},
		{"first standard", []preparedTZZone{{offset: 11, daylight: true}, {offset: 22}}, []byte{0, 0}, 22},
		{"all daylight", []preparedTZZone{{offset: 11, daylight: true}, {offset: 22, daylight: true}}, []byte{0, 1}, 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := timezoneFixture([]int64{100, 200}, tc.indices, tc.zones, "", true)
			z, err := PrepareTimezone(data, time.Unix(-1, 0))
			require.NoError(t, err)
			reference, err := time.LoadLocationFromTZData(tc.name, data)
			require.NoError(t, err)
			var work timezoneWork
			offset, _, _, err := z.lookup(0, &work)
			require.NoError(t, err)
			_, want := time.Unix(0, 0).In(reference).Zone()
			require.Equal(t, tc.want, offset)
			require.Equal(t, want, offset)
		})
	}
	data := timezoneFixture([]int64{0, 0, 10}, []byte{0, 1, 2}, []preparedTZZone{{offset: 11}, {offset: 22}, {offset: 33}}, "", true)
	z, err := PrepareTimezone(data, time.Unix(-1, 0))
	require.NoError(t, err)
	var work timezoneWork
	offset, _, _, err := z.lookup(0, &work)
	require.NoError(t, err)
	require.Equal(t, 22, offset)
	for _, version := range []byte{0, '2', '3'} {
		empty := timezoneFixture(nil, nil, []preparedTZZone{{offset: 1234}}, "", version != 0)
		empty[4] = version
		z, err := PrepareTimezone(empty, time.Unix(0, 0))
		require.NoError(t, err)
		offset, _, _, err := z.lookup(-1<<62, &work)
		require.NoError(t, err)
		require.Equal(t, 1234, offset)
	}
}

func TestPreparedCalendarExtremeOffset(t *testing.T) {
	t.Parallel()
	for _, offset := range []int{math.MinInt32, math.MaxInt32} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			t.Parallel()
			data := timezoneFixture(nil, nil, []preparedTZZone{{offset: offset}}, "", true)
			definition, reference := preparedCalendarFixture(t, &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 0, End: 59}}, Minute: []*schedulepb.Range{{Start: 0, End: 59}}, Hour: []*schedulepb.Range{{Start: 0, End: 23}}}}, TimezoneData: data})
			zone, err := PreparedLocationFromTZData("extreme", data, time.Unix(0, 0))
			require.NoError(t, err)
			after := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			state, _ := runPreparedCalendar(t, definition, zone, after, []int{1})
			candidate, found, done := state.Result()
			require.True(t, found && done)
			require.Equal(t, after.Add(time.Second), candidate)
			legacy, err := reference.GetNextTime("", after)
			require.NoError(t, err)
			require.Equal(t, legacy.Nominal, candidate)
		})
	}
}
