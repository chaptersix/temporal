package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func TestCalendarFoldOrdering(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, zone, month, day string
		first                  time.Time
	}{
		{"London", "Europe/London", "10", "27", time.Date(2024, 10, 27, 0, 0, 0, 0, time.UTC)},
		{"NewYork", "America/New_York", "11", "3", time.Date(2024, 11, 3, 5, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			zone, err := time.LoadLocation(tc.zone)
			require.NoError(t, err)
			cal, err := parseCalendarToStructured(&schedulepb.CalendarSpec{
				Year: "2024", Month: tc.month, DayOfMonth: tc.day,
				Hour: "1", Minute: "0,30", Second: "0,30",
			})
			require.NoError(t, err)
			cc := newCompiledCalendar(cal, zone)
			// Literal physical offsets enumerate both copies of the repeated hour.
			// Do not construct expectations with ambiguous local time.Date calls.
			offsets := []time.Duration{0, 30 * time.Second, 30 * time.Minute, 30*time.Minute + 30*time.Second,
				time.Hour, time.Hour + 30*time.Second, 90 * time.Minute, 90*time.Minute + 30*time.Second}
			for _, offset := range offsets {
				for _, neighbor := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
					after := tc.first.Add(offset + neighbor)
					var want time.Time
					for _, expectedOffset := range offsets {
						candidate := tc.first.Add(expectedOffset)
						if candidate.After(after) {
							want = candidate
							break
						}
					}
					got := cc.next(after)
					require.True(t, want.Equal(got), "after=%s want=%s got=%s", after, want, got)
					if !got.IsZero() {
						require.True(t, got.After(after), "next must strictly advance")
						require.True(t, cc.matches(got), "next must satisfy the local calendar")
					}
				}
			}
		})
	}
}

func TestCalendarFoldNormalizationPreservesGaps(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		zone, hour  string
		after, want time.Time
	}{
		{"Europe/London", "1", time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC), time.Date(2024, 4, 1, 0, 30, 0, 0, time.UTC)},
		{"America/New_York", "2", time.Date(2024, 3, 10, 6, 0, 0, 0, time.UTC), time.Date(2024, 3, 11, 6, 30, 0, 0, time.UTC)},
	} {
		t.Run(tc.zone, func(t *testing.T) {
			t.Parallel()
			zone, err := time.LoadLocation(tc.zone)
			require.NoError(t, err)
			cal, err := parseCalendarToStructured(&schedulepb.CalendarSpec{Year: "2024", Hour: tc.hour, Minute: "30"})
			require.NoError(t, err)
			cc := newCompiledCalendar(cal, zone)
			got := cc.next(tc.after)
			require.True(t, tc.want.Equal(got), "want=%s got=%s", tc.want, got)
			require.True(t, cc.matches(got))
		})
	}
}
