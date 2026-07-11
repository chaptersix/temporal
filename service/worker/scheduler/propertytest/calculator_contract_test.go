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

	_, err := ComputeMatchingTimes(backgroundContext, valid, start, start.Add(-time.Second), "", ComputeOptions{MaxResults: 1, MaxIterations: 100})
	require.ErrorIs(t, err, ErrInvalidQueryRange)

	_, err = ComputeMatchingTimes(backgroundContext, valid, start, start.Add(time.Hour), "", ComputeOptions{MaxResults: 0, MaxIterations: 100})
	require.ErrorIs(t, err, ErrInvalidOptions)

	invalid := &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
		Month: []*schedulepb.Range{{Start: 13}},
	}}}
	_, err = ComputeMatchingTimes(backgroundContext, invalid, start, start.Add(time.Hour), "", ComputeOptions{MaxResults: 1, MaxIterations: 100})
	require.ErrorIs(t, err, ErrInvalidSpec)
}

func TestInvalidEmptyInclusionSet(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := ComputeMatchingTimes(backgroundContext, &schedulepb.ScheduleSpec{}, start, start.Add(time.Hour), "", ComputeOptions{
		MaxResults: 10, MaxIterations: 100_000,
	})

	require.ErrorIs(t, err, ErrUnsatisfiableSpec)
	require.ErrorIs(t, err, ErrInvalidSpec)
	require.Equal(t, ValidationInvalidComponentUnsatisfiable, result.Validation.Status)
}

func TestInvalidEmptyStructuredInclusion(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := ComputeMatchingTimes(backgroundContext, &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{}},
	}, start, start.Add(time.Hour), "", ComputeOptions{MaxResults: 10, MaxIterations: 100_000})

	require.ErrorIs(t, err, ErrUnsatisfiableSpec)
	require.Equal(t, ValidationInvalidComponentUnsatisfiable, result.Validation.Status)
}

func TestInvalidImpossibleCivilDate(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := ComputeMatchingTimes(backgroundContext, &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
			Second:     []*schedulepb.Range{{Start: 0}},
			Minute:     []*schedulepb.Range{{Start: 0}},
			Hour:       []*schedulepb.Range{{Start: 0}},
			DayOfMonth: []*schedulepb.Range{{Start: 30}},
			Month:      []*schedulepb.Range{{Start: 2}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
		}},
	}, start, start.Add(365*24*time.Hour), "", ComputeOptions{MaxResults: 10, MaxIterations: 100_000})

	require.ErrorIs(t, err, ErrUnsatisfiableSpec)
	require.Equal(t, ValidationInvalidComponentUnsatisfiable, result.Validation.Status)
}

func TestInvalidInvertedScheduleBounds(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := ComputeMatchingTimes(backgroundContext, &schedulepb.ScheduleSpec{
		Interval:  []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
		StartTime: timestamppb.New(start.Add(2 * time.Hour)),
		EndTime:   timestamppb.New(start.Add(time.Hour)),
	}, start, start.Add(365*24*time.Hour), "", ComputeOptions{MaxResults: 10, MaxIterations: 100_000})

	require.ErrorIs(t, err, ErrUnsatisfiableSpec)
	require.Equal(t, ValidationInvalidStructural, result.Validation.Status)
}

func TestValidScheduleCanHaveEmptyQueryResult(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	result, err := ComputeMatchingTimes(backgroundContext, &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(24 * time.Hour)}},
	}, start, start.Add(time.Hour), "", ComputeOptions{MaxResults: 10, MaxIterations: 100_000})

	require.NoError(t, err)
	require.Empty(t, result.Times)
	require.Equal(t, ValidationValid, result.Validation.Status)
	require.False(t, result.Validation.Witness.IsZero())
}

func TestValidationAcceptsTimezoneCalendarAtScheduleEnd(t *testing.T) {
	witness := time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC)
	location, err := time.LoadLocation("Pacific/Apia")
	require.NoError(t, err)
	model := scheduleModel{
		calendars: []calendarModel{exactCalendarModel(witness.In(location))},
		endTime:   &witness,
		timezone:  "Pacific/Apia",
	}
	spec, err := canonicalizeSpec(model.renderStructured())
	require.NoError(t, err)
	CleanSpec(spec)
	horizonStart, horizonEnd := validationHorizon(spec, location)
	calendar := newCompiledCalendar(spec.StructuredCalendar[0], location)
	budget := &validationBudget{limit: defaultValidationIterations}
	componentWitness, componentErr := calendarComponentWitness(
		calendar,
		spec.StructuredCalendar[0],
		horizonStart,
		horizonEnd,
		budget,
		"inclusion calendar 0",
		analysisFaults{},
	)
	require.NoError(t, componentErr)
	require.Equal(t, witness, componentWitness)
}

func TestComputeMatchingTimesBudgetBoundary(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
		Interval: durationpb.New(time.Hour),
	}}}
	options := ComputeOptions{MaxResults: 5, MaxIterations: 1_000_000}

	baseline, err := ComputeMatchingTimes(backgroundContext, spec, start, start.Add(5*time.Hour), "", options)
	require.NoError(t, err)
	require.Positive(t, baseline.Work.Total)
	requireWorkConsistent(t, baseline.Work)

	exact, err := ComputeMatchingTimes(backgroundContext, spec, start, start.Add(5*time.Hour), "", ComputeOptions{
		MaxResults:    options.MaxResults,
		MaxIterations: baseline.Work.Total,
	})

	require.NoError(t, err)
	require.Equal(t, baseline, exact)

	partial, err := ComputeMatchingTimes(backgroundContext, spec, start, start.Add(5*time.Hour), "", ComputeOptions{
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
