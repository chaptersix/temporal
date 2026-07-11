package propertytest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	productionscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCopiedCalculatorParity(t *testing.T) {
	t.Parallel()

	start := time.Date(2022, 3, 23, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		spec *schedulepb.ScheduleSpec
		seed string
		end  time.Time
	}{
		{
			name: "interval",
			spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(90 * time.Minute),
			}}},
			end: start.Add(24 * time.Hour),
		},
		{
			name: "calendar",
			spec: &schedulepb.ScheduleSpec{Calendar: []*schedulepb.CalendarSpec{{
				Hour: "5,7", Minute: "23",
			}}},
			end: start.Add(72 * time.Hour),
		},
		{
			name: "mixed with exclusion",
			spec: &schedulepb.ScheduleSpec{
				Calendar: []*schedulepb.CalendarSpec{{Hour: "11,13", Minute: "55"}},
				Interval: []*schedulepb.IntervalSpec{{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(7 * time.Minute),
				}},
				ExcludeCalendar: []*schedulepb.CalendarSpec{{
					Hour: "12-14", Minute: "*", Second: "*",
				}},
			},
			end: start.Add(72 * time.Hour),
		},
		{
			name: "jitter and schedule bounds",
			spec: &schedulepb.ScheduleSpec{
				Interval:  []*schedulepb.IntervalSpec{{Interval: durationpb.New(90 * time.Minute)}},
				StartTime: timestamppb.New(start.Add(4 * time.Hour)),
				EndTime:   timestamppb.New(start.Add(20 * time.Hour)),
				Jitter:    durationpb.New(time.Hour),
			},
			seed: "parity-seed",
			end:  start.Add(48 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			productionSpec, err := productionscheduler.NewSpecBuilder().NewCompiledSpec(tt.spec)
			require.NoError(t, err)
			expected := productionMatchingTimes(productionSpec, start, tt.end, tt.seed, 100)

			actual, err := ComputeMatchingTimes(tt.spec, start, tt.end, tt.seed, ComputeOptions{
				MaxResults:    100,
				MaxIterations: 1_000_000,
			})
			require.NoError(t, err)
			require.Equal(t, expected, actual.Times)
		})
	}
}

func productionMatchingTimes(
	spec *productionscheduler.CompiledSpec,
	start time.Time,
	end time.Time,
	seed string,
	maxResults int,
) []time.Time {
	times := make([]time.Time, 0, maxResults)
	after := start
	for range maxResults {
		after = spec.GetNextTime(seed, after).Next
		if after.IsZero() || after.After(end) {
			break
		}
		times = append(times, after)
	}
	return times
}
