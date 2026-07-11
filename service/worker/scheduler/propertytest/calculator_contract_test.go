package propertytest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestComputeMatchingTimesErrors(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	valid := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
		Interval: durationpb.New(time.Hour),
	}}}

	_, err := ComputeMatchingTimes(valid, start, start.Add(-time.Second), "", ComputeOptions{1, 100})
	require.ErrorIs(t, err, ErrInvalidQueryRange)

	_, err = ComputeMatchingTimes(valid, start, start.Add(time.Hour), "", ComputeOptions{0, 100})
	require.ErrorIs(t, err, ErrInvalidOptions)

	invalid := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
		Month: []*schedulepb.Range{{Start: 13}},
	}}}
	_, err = ComputeMatchingTimes(invalid, start, start.Add(time.Hour), "", ComputeOptions{1, 100})
	require.ErrorIs(t, err, ErrInvalidSpec)
}

func TestValidEmptyScheduleShapesReturnEmptySuccess(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		spec *schedulepb.ScheduleSpec
	}{
		{name: "empty schedule", spec: &schedulepb.ScheduleSpec{}},
		{name: "empty structured calendar", spec: &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{}},
		}},
		{name: "inverted schedule bounds describe empty set", spec: &schedulepb.ScheduleSpec{
			Interval:  []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			StartTime: timestamppb.New(start.Add(2 * time.Hour)),
			EndTime:   timestamppb.New(start.Add(time.Hour)),
		}},
		{name: "impossible civil date", spec: &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
				Second:     []*schedulepb.Range{{Start: 0}},
				Minute:     []*schedulepb.Range{{Start: 0}},
				Hour:       []*schedulepb.Range{{Start: 0}},
				DayOfMonth: []*schedulepb.Range{{Start: 30}},
				Month:      []*schedulepb.Range{{Start: 2}},
				DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
			}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ComputeMatchingTimes(tt.spec, start, start.Add(365*24*time.Hour), "", ComputeOptions{
				MaxResults:    10,
				MaxIterations: 100_000,
			})
			require.NoError(t, err)
			require.Empty(t, result.Times)
		})
	}
}

func TestComputeMatchingTimesBudgetBoundary(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
		Interval: durationpb.New(time.Hour),
	}}}
	options := ComputeOptions{MaxResults: 5, MaxIterations: 1_000_000}

	baseline, err := ComputeMatchingTimes(spec, start, start.Add(5*time.Hour), "", options)
	require.NoError(t, err)
	require.Positive(t, baseline.Work.Total)
	requireWorkConsistent(t, baseline.Work)

	exact, err := ComputeMatchingTimes(spec, start, start.Add(5*time.Hour), "", ComputeOptions{
		MaxResults:    options.MaxResults,
		MaxIterations: baseline.Work.Total,
	})
	require.NoError(t, err)
	require.Equal(t, baseline, exact)

	partial, err := ComputeMatchingTimes(spec, start, start.Add(5*time.Hour), "", ComputeOptions{
		MaxResults:    options.MaxResults,
		MaxIterations: baseline.Work.Total - 1,
	})
	require.ErrorIs(t, err, ErrIterationLimit)
	require.Equal(t, baseline.Work.Total-1, partial.Work.Total)
	var limitErr *IterationLimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, partial.Work, limitErr.Work)
}

func requireWorkConsistent(t *testing.T, work WorkBreakdown) {
	t.Helper()
	require.Equal(t, work.Total,
		work.NextTimeCalls+
			work.InclusionSourceChecks+
			work.CalendarSearchSteps+
			work.IntervalChecks+
			work.ExclusionChecks+
			work.ExcludedCandidateRetries+
			work.ResultLoopSteps,
	)
}
