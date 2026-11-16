package occurrence

import (
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func preparationSource(ranges [7][]CalendarRange) (CalendarHeader, func(uint8, uint32) (RangePage, error)) {
	header := CalendarHeader{SourceID: [32]byte{1}}
	for i := range ranges {
		header.Ranges[i] = uint32(len(ranges[i]))
	}
	return header, func(field uint8, index uint32) (RangePage, error) {
		var page RangePage
		start := int(index) * CalendarRangePageSize
		for _, r := range ranges[field][start:min(start+CalendarRangePageSize, len(ranges[field]))] {
			page.Ranges[page.Count] = r
			page.Count++
		}
		return page, nil
	}
}

func expandedMasks(ranges [7][]CalendarRange) (CalendarMasks, int) {
	masks := CalendarMasks{AllYears: len(ranges[6]) == 0}
	steps := 7
	for field, list := range ranges {
		for _, r := range list {
			step := max(int64(r.Step), 1)
			for value := int64(r.Start); value <= max(int64(r.Start), int64(r.End)); value += step {
				steps++
				if field == 6 {
					masks.Years[(value-2000)/64] |= 1 << ((value - 2000) % 64)
				} else {
					masks.Fields[field] |= 1 << value
				}
			}
		}
	}
	return masks, steps
}

func TestCalendarPreparationBudgetPartitions(t *testing.T) {
	t.Parallel()
	ranges := [7][]CalendarRange{
		{{0, 59, 7}, {59, 0, 0}}, {{0, 59, 1}}, {{0, 23, 2}},
		{{1, 31, 3}}, {{1, 12, 1}}, {{0, 6, math.MaxInt32}}, {{2000, 2100, 3}, {2064, 2100, 2}},
	}
	for range 1025 {
		ranges[0] = append(ranges[0], CalendarRange{0, 59, 1})
	}
	header, pages := preparationSource(ranges)
	want, total := expandedMasks(ranges)
	for _, budget := range []int{0, 1, 7, MaxPreparationSteps} {
		t.Run("budget"+strconv.Itoa(budget), func(t *testing.T) {
			t.Parallel()
			var state PreparationState
			steps := 0
			for !state.Done {
				before := state
				actualBudget := budget
				if actualBudget == 0 {
					actualBudget = 1 + steps%MaxPreparationSteps
				}
				next, stats, err := PrepareCalendar(state, header, pages, actualBudget)
				require.NoError(t, err)
				require.Equal(t, before, state, "input continuation must be detached")
				require.Positive(t, stats.Steps)
				require.LessOrEqual(t, stats.Steps, actualBudget)
				require.LessOrEqual(t, stats.Pages, stats.Steps)
				steps += stats.Steps
				state = next
			}
			require.Equal(t, want, state.Partial)
			require.Equal(t, total, steps)
			next, stats, err := PrepareCalendar(state, header, pages, max(budget, 1))
			require.NoError(t, err)
			require.Equal(t, state, next)
			require.Equal(t, PreparationStats{}, stats)
		})
	}
}

func TestCalendarPreparationEmptyAndExplicitYears(t *testing.T) {
	t.Parallel()
	for _, explicit := range []bool{false, true} {
		ranges := [7][]CalendarRange{}
		if explicit {
			ranges[6] = []CalendarRange{{2000, 2100, 1}}
		}
		header, pages := preparationSource(ranges)
		var state PreparationState
		for !state.Done {
			var err error
			state, _, err = PrepareCalendar(state, header, pages, MaxPreparationSteps)
			require.NoError(t, err)
		}
		want, _ := expandedMasks(ranges)
		require.Equal(t, want, state.Partial)
		require.Equal(t, !explicit, state.Partial.AllYears)
	}
}

func TestCalendarPreparationCorruptionAndFailedRead(t *testing.T) {
	t.Parallel()
	header, pages := preparationSource([7][]CalendarRange{{{1, 5, 2}}})
	state, _, err := PrepareCalendar(PreparationState{}, header, pages, 1)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		edit func(*PreparationState)
	}{
		{"source", func(s *PreparationState) { s.SourceID[0]++ }},
		{"field", func(s *PreparationState) { s.Field = 8 }},
		{"range", func(s *PreparationState) { s.Range = 99 }},
		{"value", func(s *PreparationState) { s.Next = 2 }},
		{"completed", func(s *PreparationState) { s.Done = true }},
		{"field_mask", func(s *PreparationState) { s.Partial.Fields[0] = 1 << 63 }},
		{"future_mask", func(s *PreparationState) { s.Partial.Fields[1] = 1 }},
		{"year_mask", func(s *PreparationState) { s.Partial.Years[1] = 1 << 63 }},
		{"wildcard_year_mask", func(s *PreparationState) { s.Partial.Years[0] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bad := state
			tc.edit(&bad)
			after, _, err := PrepareCalendar(bad, header, pages, MaxPreparationSteps)
			require.Error(t, err)
			require.Equal(t, bad, after)
		})
	}
	for _, budget := range []int{-1, 0, MaxPreparationSteps + 1} {
		after, _, err := PrepareCalendar(state, header, pages, budget)
		require.Error(t, err)
		require.Equal(t, state, after)
	}
	for _, read := range []func(uint8, uint32) (RangePage, error){
		func(uint8, uint32) (RangePage, error) { return RangePage{}, errors.New("page unavailable") },
		func(uint8, uint32) (RangePage, error) { return RangePage{Count: 17}, nil },
		func(uint8, uint32) (RangePage, error) {
			return RangePage{Count: 1, Ranges: [16]CalendarRange{{-1, 59, 1}}}, nil
		},
		func(uint8, uint32) (RangePage, error) {
			return RangePage{Count: 1, Ranges: [16]CalendarRange{{5, 2, 1}}}, nil
		},
		func(uint8, uint32) (RangePage, error) {
			return RangePage{Count: 1, Ranges: [16]CalendarRange{{5, -1, 1}}}, nil
		},
	} {
		after, stats, err := PrepareCalendar(state, header, read, MaxPreparationSteps)
		require.Error(t, err)
		require.Equal(t, state, after)
		require.Equal(t, PreparationStats{Steps: 1, Pages: 1}, stats)
	}
}

func TestCalendarPreparationMatchesReferenceCandidates(t *testing.T) {
	t.Parallel()
	ranges := [7][]CalendarRange{
		{{0, 59, 7}, {59, 0, 0}}, {{0, 59, 3}}, {{0, 23, 1}},
		{{1, 31, 1}}, {{1, 12, 1}}, {{0, 6, 1}}, {{2000, 2100, 1}},
	}
	header, pages := preparationSource(ranges)
	var state PreparationState
	for !state.Done {
		var err error
		state, _, err = PrepareCalendar(state, header, pages, 7)
		require.NoError(t, err)
	}
	specs := [2]*schedulepb.StructuredCalendarSpec{{}, {}}
	for i, spec := range specs {
		fields := []*[]*schedulepb.Range{&spec.Second, &spec.Minute, &spec.Hour, &spec.DayOfMonth, &spec.Month, &spec.DayOfWeek, &spec.Year}
		for field := range fields {
			if i == 0 {
				for _, r := range ranges[field] {
					*fields[field] = append(*fields[field], &schedulepb.Range{Start: r.Start, End: r.End, Step: r.Step})
				}
				continue
			}
			for value := 0; value <= 2100; value++ {
				matches := false
				if field < 6 && value < 64 {
					matches = state.Partial.Fields[field]&(uint64(1)<<value) != 0
				} else if field == 6 && value >= 2000 {
					matches = state.Partial.Years[(value-2000)/64]&(uint64(1)<<((value-2000)%64)) != 0
				}
				if matches {
					*fields[field] = append(*fields[field], &schedulepb.Range{Start: int32(value)})
				}
			}
		}
	}
	for _, after := range []time.Time{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 2, 29, 23, 59, 59, 0, time.UTC)} {
		var results [2]Result
		for i, spec := range specs {
			enumerator, err := NewPublic(&schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{spec}}, "seed", 1)
			require.NoError(t, err)
			initial, err := enumerator.Begin(after)
			require.NoError(t, err)
			results[i], err = enumerator.Run(initial, after.Add(24*time.Hour), Limits{Probes: 8, Outputs: 8})
			require.NoError(t, err)
		}
		require.Equal(t, results[0].Occurrences, results[1].Occurrences)
		require.NotEmpty(t, results[0].Occurrences)
	}
}

func TestCalendarPreparationFailureAfterProgress(t *testing.T) {
	t.Parallel()
	header, pages := preparationSource([7][]CalendarRange{{{1, 5, 1}}})
	input := PreparationState{}
	calls := 0
	failure := errors.New("second page fetch failed")
	output, stats, err := PrepareCalendar(input, header, func(field uint8, page uint32) (RangePage, error) {
		calls++
		if calls == 2 {
			return RangePage{}, failure
		}
		return pages(field, page)
	}, MaxPreparationSteps)
	require.ErrorIs(t, err, failure)
	require.Equal(t, input, output)
	require.Equal(t, PreparationStats{Steps: 2, Pages: 2}, stats)
	// Retrying from the unchanged continuation must compile the full range.
	output, _, err = PrepareCalendar(output, header, pages, MaxPreparationSteps)
	require.NoError(t, err)
	require.True(t, output.Done)
	require.Equal(t, uint64(62), output.Partial.Fields[0])
}
