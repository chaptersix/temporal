package scheduler

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newSpecBuilderForTest builds a SpecBuilder with the given warn/max compute-limit bounds. A value
// of 0 means "use the default" (GetNextTime treats a non-positive bound as its default).
func newSpecBuilderForTest(warnIter, maxIter int) *SpecBuilder {
	return NewSpecBuilder(func() int { return warnIter }, func() int { return maxIter })
}

func bruteForceNextTimeWithUpperBound(
	cs *CompiledSpec,
	jitterSeed string,
	after time.Time,
	upperBound time.Time,
) GetNextTimeResult {
	var nominal time.Time
	for {
		nominal = cs.rawNextTime(after)
		if nominal.IsZero() || nominal.After(upperBound) || nominal.Year() > maxCalendarYear {
			return GetNextTimeResult{}
		}
		excluded := false
		for _, exclusion := range cs.excludes {
			if exclusion.calendar.matches(nominal) {
				excluded = true
				break
			}
		}
		if !excluded {
			break
		}
		after = nominal
	}

	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	if following := cs.rawNextTime(nominal); !following.IsZero() {
		maxJitter = min(maxJitter, following.Sub(nominal))
	}
	next := cs.addJitter(jitterSeed, nominal, maxJitter)
	if next.After(upperBound) {
		return GetNextTimeResult{}
	}
	return GetNextTimeResult{Nominal: nominal, Next: next}
}

// legacyRawNextTime is the pre-optimization union search kept as a test oracle. It deliberately
// compiles and evaluates every source from the canonical spec on every call.
func legacyRawNextTime(spec *schedulepb.ScheduleSpec, tz *time.Location, after time.Time) time.Time {
	var minTimestamp int64 = math.MaxInt64
	for _, structured := range spec.StructuredCalendar {
		if next := newCompiledCalendar(structured, tz).next(after); !next.IsZero() {
			minTimestamp = min(minTimestamp, next.Unix())
		}
	}
	for _, intervalSpec := range spec.Interval {
		interval := max(int64(timestamp.DurationValue(intervalSpec.Interval)/time.Second), 1)
		phase := max(int64(timestamp.DurationValue(intervalSpec.Phase)/time.Second), 0)
		next := (((after.Unix()-phase)/interval)+1)*interval + phase
		minTimestamp = min(minTimestamp, next)
	}
	if minTimestamp == math.MaxInt64 {
		return time.Time{}
	}
	return time.Unix(minTimestamp, 0).UTC()
}

type specSuite struct {
	suite.Suite
	protorequire.ProtoAssertions

	specBuilder *SpecBuilder
}

func TestSpec(t *testing.T) {
	suite.Run(t, new(specSuite))
}

func (s *specSuite) SetupTest() {
	s.ProtoAssertions = protorequire.New(s.T())
	s.specBuilder = newSpecBuilderForTest(0, 0)
}

func (s *specSuite) checkSequenceRaw(spec *schedulepb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := s.specBuilder.NewCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		next := cs.rawNextTime(start)
		s.Equal(exp, next)
		start = next
	}
}

func (s *specSuite) checkSequenceFull(jitterSeed string, spec *schedulepb.ScheduleSpec, start time.Time, seq ...time.Time) {
	s.T().Helper()
	cs, err := s.specBuilder.NewCompiledSpec(spec)
	s.NoError(err)
	for _, exp := range seq {
		result, err := cs.GetNextTime(jitterSeed, start)
		s.Require().NoError(err)
		if exp.IsZero() {
			s.Require().True(
				result.Nominal.IsZero(),
				"exp %v nominal should be zero, got %v", exp, result.Nominal,
			)
			s.Require().True(result.Next.IsZero(), "next should be zero")
			break
		}
		s.Require().False(result.Nominal.IsZero())
		s.Require().False(result.Next.IsZero())
		s.Equal(exp, result.Next)
		start = result.Next
	}
}

func (s *specSuite) TestCanonicalize() {
	canonical, err := canonicalizeSpec(&schedulepb.ScheduleSpec{})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{}, canonical)

	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"@every 43h",
		},
	})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(43 * time.Hour),
		}},
	}, canonical)

	// negative interval
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(-43 * time.Hour),
		}},
	})
	s.Error(err)

	// phase exceeds interval
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{
			Interval: durationpb.New(3 * time.Hour),
			Phase:    durationpb.New(4 * time.Hour),
		}},
	})
	s.Error(err)

	// various errors in ranges
	for _, scs := range []*schedulepb.StructuredCalendarSpec{
		{Second: []*schedulepb.Range{{Start: 100}}},
		{Second: []*schedulepb.Range{{Start: -3}}},
		{Second: []*schedulepb.Range{{Start: 30, End: 60}}},
		{Second: []*schedulepb.Range{{Start: 30, End: 40, Step: -3}}},
		{Minute: []*schedulepb.Range{{Start: 60}}},
		{Hour: []*schedulepb.Range{{Start: 0, End: 24}}},
		{Hour: []*schedulepb.Range{{Start: 24, End: 26}}},
		{Hour: []*schedulepb.Range{{Start: 16, End: 12}}},
		{DayOfMonth: []*schedulepb.Range{{Start: 0}}},
		{DayOfMonth: []*schedulepb.Range{{End: 33}}},
		{Month: []*schedulepb.Range{{Start: 0}}},
		{Month: []*schedulepb.Range{{End: 13}}},
		{DayOfWeek: []*schedulepb.Range{{Start: 7}}},
		{DayOfWeek: []*schedulepb.Range{{Start: 6, End: 7}}},
		{Year: []*schedulepb.Range{{Start: 1999}}},
		{Year: []*schedulepb.Range{{Start: 2112}}},
	} {
		_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{scs},
		})
		s.Error(err)
	}

	// check parsing and filling in defaults
	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{
			{Hour: "5,7", Minute: "23"},
		},
	})
	s.NoError(err)
	structured := []*schedulepb.StructuredCalendarSpec{{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 23}},
		Hour:       []*schedulepb.Range{{Start: 5}, {Start: 7}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6}},
	}}
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		StructuredCalendar: structured,
	}, canonical)

	// no tz in cron string, leave spec alone
	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"23 5,7 * * *",
		},
		Jitter:       durationpb.New(5 * time.Minute),
		StartTime:    timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName: "Europe/London",
	})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		StructuredCalendar: structured,
		Jitter:             durationpb.New(5 * time.Minute),
		StartTime:          timestamppb.New(time.Date(2022, 3, 23, 0, 0, 0, 0, time.UTC)),
		TimezoneName:       "Europe/London",
	}, canonical)

	// tz matches, ok
	canonical, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	})
	s.NoError(err)
	s.ProtoEqual(&schedulepb.ScheduleSpec{
		StructuredCalendar: structured,
		TimezoneName:       "Europe/London",
	}, canonical)

	// tz mismatch, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
		TimezoneName: "Europe/London",
	})
	s.Error(err)

	// tz mismatch between cron strings, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"CRON_TZ=America/New_York 23 5,7 * * *",
		},
	})
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"CRON_TZ=Europe/London 23 5,7 * * *",
			"23 5,7 * * *",
		},
	})
	s.Error(err)

	// all cron strings don't agree, error
	_, err = canonicalizeSpec(&schedulepb.ScheduleSpec{
		CronString: []string{
			"23 5,7 * * *",
			"CRON_TZ=Europe/London 23 5,7 * * *",
		},
	})
	s.Error(err)
}

func (s *specSuite) TestCompiledSpecDeduplicatesInclusionSources() {
	duplicate := &schedulepb.CalendarSpec{
		Second: "0,10-20/2", Minute: "*", Hour: "*", Comment: "first representation",
	}
	equivalent := &schedulepb.CalendarSpec{
		Second: "10,12,14,16,18,20,0", Minute: "*", Hour: "*", Comment: "same matches",
	}
	cs, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{duplicate, duplicate, equivalent},
		Interval: []*schedulepb.IntervalSpec{
			{Interval: durationpb.New(4 * time.Second), Phase: durationpb.New(time.Second)},
			{Interval: durationpb.New(2 * time.Second), Phase: durationpb.New(time.Second)},
			{Interval: durationpb.New(4 * time.Second), Phase: durationpb.New(3 * time.Second)},
		},
	})
	s.Require().NoError(err)
	s.Len(cs.calendar, 1)
	s.Equal([]compiledInterval{{interval: 2, phase: 1}}, cs.intervals)
	// Compiled-source optimization must not rewrite the canonical user-visible spec.
	s.Len(cs.CanonicalForm().StructuredCalendar, 3)
	s.Len(cs.CanonicalForm().Interval, 3)
}

func (s *specSuite) TestEverySecondIntervalSubsumesCalendarSources() {
	cs, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{{Second: "0", Minute: "0", Hour: "0"}},
		Interval: []*schedulepb.IntervalSpec{
			{Interval: durationpb.New(24 * time.Hour)},
			{Interval: durationpb.New(time.Second)},
		},
	})
	s.Require().NoError(err)
	s.Empty(cs.calendar)
	s.Equal([]compiledInterval{{interval: 1, phase: 0}}, cs.intervals)
}

func TestCompiledInclusionOptimizationProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(73621))
	zNames := []string{"UTC", "America/New_York", "Europe/London", "Australia/Lord_Howe"}
	secondExpressions := []string{"*", "0", "1", "*/2", "1-59/2", "0-30", "15-45/3"}
	minuteExpressions := []string{"*", "0", "*/5", "1-59/7"}
	hourExpressions := []string{"*", "0", "6-18/3", "23"}
	pick := func(values []string) string { return values[rng.Intn(len(values))] }

	for testCase := range 500 {
		spec := &schedulepb.ScheduleSpec{TimezoneName: tzNames[rng.Intn(len(tzNames))]}
		for range rng.Intn(6) {
			calendar := &schedulepb.CalendarSpec{
				Second: pick(secondExpressions), Minute: pick(minuteExpressions), Hour: pick(hourExpressions),
			}
			spec.Calendar = append(spec.Calendar, calendar)
			if rng.Intn(3) == 0 {
				copy := *calendar
				copy.Comment = "semantic duplicate"
				spec.Calendar = append(spec.Calendar, &copy)
			}
		}
		for range rng.Intn(6) {
			intervalSeconds := int64(rng.Intn(30) + 1)
			phaseSeconds := rng.Int63n(intervalSeconds)
			spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{
				Interval: durationpb.New(time.Duration(intervalSeconds) * time.Second),
				Phase:    durationpb.New(time.Duration(phaseSeconds) * time.Second),
			})
			if rng.Intn(3) == 0 {
				spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{
					Interval: durationpb.New(time.Duration(intervalSeconds) * time.Second),
					Phase:    durationpb.New(time.Duration(phaseSeconds) * time.Second),
				})
			}
		}
		if len(spec.Calendar) == 0 && len(spec.Interval) == 0 {
			spec.Interval = []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}}
		}

		cs, err := newSpecBuilderForTest(0, 0).NewCompiledSpec(spec)
		require.NoError(t, err, "case %d", testCase)
		after := time.Unix(
			time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()+rng.Int63n(3*365*24*60*60),
			0,
		)
		for result := range 100 {
			expected := legacyRawNextTime(cs.spec, cs.tz, after)
			actual := cs.rawNextTime(after)
			require.Equal(t, expected, actual, "case %d result %d spec=%v", testCase, result, spec)
			if actual.IsZero() {
				break
			}
			after = actual
		}
	}
}

func (s *specSuite) TestSpecIntervalBasic() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
		},
		time.Date(2022, 3, 23, 12, 53, 2, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 30, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 00, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecIntervalPhase() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(5*time.Minute + 44*time.Second),
				},
			},
		},
		time.Date(2022, 3, 23, 12, 53, 02, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 05, 44, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecIntervalMultiple() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(5*time.Minute + 44*time.Second),
				},
				{
					Interval: durationpb.New(157 * time.Minute),
					Phase:    durationpb.New(22*time.Minute + 11*time.Second),
				},
			},
		},
		time.Date(2022, 3, 23, 12, 53, 02, 9, time.UTC),
		time.Date(2022, 3, 23, 13, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 26, 11, 0, time.UTC),
		time.Date(2022, 3, 23, 16, 35, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 03, 11, 0, time.UTC),
		time.Date(2022, 3, 23, 18, 05, 44, 0, time.UTC),
		time.Date(2022, 3, 23, 19, 35, 44, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecCalendarBasic() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "5,7", Minute: "23"},
			},
		},
		time.Date(2022, 3, 23, 3, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 7, 23, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecCalendarMultiple() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "5,7", Minute: "23"},
				{Hour: "11,13", Minute: "55"},
			},
		},
		time.Date(2022, 3, 23, 3, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 13, 55, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecCalendarAndCron() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "5,7", Minute: "23"},
			},
			CronString: []string{
				"55 11,13 * * *",
			},
		},
		time.Date(2022, 3, 23, 3, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 5, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 7, 23, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 13, 55, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecMixedCalendarInterval() {
	s.checkSequenceRaw(
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "11,13", Minute: "55"},
			},
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(90 * time.Minute),
					Phase:    durationpb.New(7 * time.Minute),
				},
			},
		},
		time.Date(2022, 3, 23, 10, 0, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 10, 37, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 11, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 07, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 37, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 13, 55, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 15, 07, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecExclude() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			ExcludeCalendar: []*schedulepb.CalendarSpec{
				{
					Hour:   "12-14",
					Minute: "*",
					Second: "*",
				},
			},
			Jitter: durationpb.New(1 * time.Second),
		},
		time.Date(2022, 3, 23, 8, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 9, 00, 0, 235000000, time.UTC),
		time.Date(2022, 3, 23, 10, 30, 0, 139000000, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 89000000, time.UTC),
		time.Date(2022, 3, 23, 16, 30, 0, 687000000, time.UTC),
	)
}

func (s *specSuite) TestSpecExcludeOverlappingRanges() {
	cs, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			{Second: "*", Minute: "*", Hour: "0-2"},
			{Second: "*", Minute: "*", Hour: "1-3"},
		},
	})
	s.Require().NoError(err)

	start := time.Date(2024, time.January, 2, 1, 30, 0, 0, time.UTC)
	result, err := cs.GetNextTime("", start)
	s.Require().NoError(err)
	s.Equal(time.Date(2024, time.January, 2, 4, 0, 0, 0, time.UTC), result.Nominal)
}

func (s *specSuite) TestGetNextTimeWithUpperBound() {
	cs, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			{Second: "*", Minute: "*", Hour: "0-11"},
		},
	})
	s.Require().NoError(err)

	start := time.Date(2024, time.January, 2, 1, 0, 0, 0, time.UTC)
	result, err := cs.GetNextTimeWithUpperBound("", start, start.Add(time.Minute))
	s.Require().NoError(err)
	s.Zero(result)

	result, err = cs.GetNextTimeWithUpperBound("", start, time.Date(2024, time.January, 2, 12, 0, 0, 0, time.UTC))
	s.Require().NoError(err)
	s.Equal(time.Date(2024, time.January, 2, 12, 0, 0, 0, time.UTC), result.Nominal)
}

func (s *specSuite) TestDenseExclusionDoesNotCrossComputeWarning() {
	builder := newSpecBuilderForTest(5, 10)
	cs, err := builder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			{Second: "*", Minute: "*", Hour: "0-11"},
		},
	})
	s.Require().NoError(err)

	result, err := cs.GetNextTime("", time.Date(2024, time.January, 2, 1, 0, 0, 0, time.UTC))
	s.Require().NoError(err)
	s.False(result.ComputeLimitWarning)
	s.Equal(time.Date(2024, time.January, 2, 12, 0, 0, 0, time.UTC), result.Nominal)
}

func TestGetNextTimeOptimizationProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(99173))
	zNames := []string{"UTC", "America/Los_Angeles", "America/New_York", "Europe/London", "Australia/Lord_Howe"}
	secondExpressions := []string{"*", "0-14", "15-44", "45-59", "*/2", "1-59/2", "0-58"}
	minuteExpressions := []string{"*", "0-14", "15-44", "45-59", "*/2", "1-59/2"}
	hourExpressions := []string{"*", "0-5", "6-11", "12-17", "18-23", "*/2", "1-23/2"}
	dayOfWeekExpressions := []string{"*", "0-1", "1-5", "5-6", "*/2"}
	pick := func(values []string) string { return values[rng.Intn(len(values))] }

	for testCase := range 500 {
		spec := &schedulepb.ScheduleSpec{
			TimezoneName: tzNames[rng.Intn(len(tzNames))],
			Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(time.Duration(rng.Intn(30)+1) * time.Second),
			}},
			Jitter: durationpb.New(time.Duration(rng.Intn(10)) * time.Second),
		}
		if rng.Intn(2) == 0 {
			spec.Calendar = []*schedulepb.CalendarSpec{{
				Second: pick(secondExpressions), Minute: "*", Hour: "*",
			}}
		}
		for range rng.Intn(4) {
			spec.ExcludeCalendar = append(spec.ExcludeCalendar, &schedulepb.CalendarSpec{
				Second:     pick(secondExpressions),
				Minute:     pick(minuteExpressions),
				Hour:       pick(hourExpressions),
				DayOfMonth: "*",
				Month:      "*",
				Year:       "*",
				DayOfWeek:  pick(dayOfWeekExpressions),
			})
		}

		cs, err := newSpecBuilderForTest(0, 0).NewCompiledSpec(spec)
		require.NoError(t, err, "case %d", testCase)
		start := time.Unix(
			time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()+rng.Int63n(365*24*60*60),
			0,
		)
		upperBound := start.Add(2 * time.Hour)
		expected := bruteForceNextTimeWithUpperBound(cs, "property", start, upperBound)
		actual, err := cs.GetNextTimeWithUpperBound("property", start, upperBound)
		require.NoError(t, err, "case %d", testCase)
		require.Equal(t, expected, actual, "case %d, spec: %v", testCase, spec)
	}
}

func BenchmarkGetNextTimeDenseExclusion(b *testing.B) {
	cs, err := newSpecBuilderForTest(0, 0).NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			{Second: "*", Minute: "*", Hour: "0-11"},
		},
	})
	require.NoError(b, err)
	start := time.Date(2024, time.January, 2, 1, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for range b.N {
		result, err := cs.GetNextTime("", start)
		if err != nil || result.Nominal.Hour() != 12 {
			b.Fatalf("result=%v err=%v", result, err)
		}
	}
}

func BenchmarkGetNextTimeFullyExcluded(b *testing.B) {
	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	cs, err := newSpecBuilderForTest(0, 0).NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar:        []*schedulepb.CalendarSpec{everySecond},
		ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
	})
	require.NoError(b, err)
	start := time.Date(2024, time.January, 2, 1, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for range b.N {
		result, err := cs.GetNextTime("", start)
		if err != nil || !result.Next.IsZero() {
			b.Fatalf("result=%v err=%v", result, err)
		}
	}
}

func (s *specSuite) TestExcludeAll() {
	cs, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{
			{Interval: durationpb.New(7 * 24 * time.Hour)},
		},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			&schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"},
		},
	})
	s.NoError(err)
	result, err := cs.GetNextTime("", time.Date(2022, 3, 23, 12, 53, 2, 9, time.UTC))
	s.Require().NoError(err)
	s.Zero(result)
}

func (s *specSuite) TestGetNextTimeComputeLimitExceeded() {
	// The inclusion matches only isolated excluded seconds, so range skipping cannot collapse
	// the search into a single jump and the hard compute bound is still enforced.
	const iterations = 10_000
	builder := newSpecBuilderForTest(0, iterations)
	cs, err := builder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval:        []*schedulepb.IntervalSpec{{Interval: durationpb.New(2 * time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{{Second: "*/2", Minute: "*", Hour: "*"}},
	})
	s.Require().NoError(err)

	after := time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)
	_, err = cs.GetNextTime("", after)

	s.Require().ErrorIs(err, ErrComputeLimitExceeded)
}

func (s *specSuite) TestGetNextTimeComputeLimitWarning() {
	// Isolated excluded candidates cannot be collapsed into one range jump. With the hard limit
	// disabled, the search crosses the warning threshold and continues to the spec's end time.
	start := time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)
	builder := newSpecBuilderForTest(5, 0)
	cs, err := builder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval:        []*schedulepb.IntervalSpec{{Interval: durationpb.New(2 * time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{{Second: "*/2", Minute: "*", Hour: "*"}},
		EndTime:         timestamppb.New(start.Add(30 * time.Second)),
	})
	s.Require().NoError(err)

	result, err := cs.GetNextTime("", start)
	s.Require().NoError(err, "crossing the warn threshold must not error; the hard limit is disabled")
	s.True(result.ComputeLimitWarning, "search should flag that it crossed the warn threshold")
	s.True(result.Next.IsZero(), "an over-excluded spec resolves to no next time")
}

func (s *specSuite) TestGetNextTimeComputeLimitWarningThenResolves() {
	// The interval matches isolated even seconds and the exclusion covers the first few of them.
	// The search crosses the small warn threshold and then resolves to a real time.
	// This exercises the success return path carrying ComputeLimitWarning=true (as opposed to the
	// no-match path in TestGetNextTimeComputeLimitWarning).
	start := time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)
	builder := newSpecBuilderForTest(5, 0)
	cs, err := builder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval:        []*schedulepb.IntervalSpec{{Interval: durationpb.New(2 * time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{{Second: "0-8/2", Minute: "*", Hour: "*"}},
	})
	s.Require().NoError(err)

	result, err := cs.GetNextTime("", start)
	s.Require().NoError(err)
	s.True(result.ComputeLimitWarning, "crossing the warn threshold should be flagged even when the search resolves")
	s.Require().False(result.Next.IsZero(), "the search should resolve to a real next time")
	s.Equal(10, result.Nominal.Second(), "the first non-excluded interval time is :10")
}

func (s *specSuite) TestSpecStartTime() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			StartTime: timestamppb.New(time.Date(2022, 3, 23, 12, 0, 0, 0, time.UTC)),
			Jitter:    durationpb.New(1 * time.Second),
		},
		time.Date(2022, 3, 23, 8, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Date(2022, 3, 23, 15, 00, 0, 89000000, time.UTC),
	)
}

func (s *specSuite) TestSpecStartTimeMinusOneSecond() {
	// This checks the bug fixed by FixStartTimeBug.
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(time.Hour)},
			},
			StartTime: timestamppb.New(time.Date(2022, 3, 23, 12, 0, 0, 456000000, time.UTC)),
		},
		time.Date(2022, 3, 23, 12, 00, 0, 123000000, time.UTC),
		time.Date(2022, 3, 23, 13, 00, 0, 0, time.UTC),
	)
}

func (s *specSuite) TestSpecEndTime() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			EndTime: timestamppb.New(time.Date(2022, 3, 23, 14, 0, 0, 0, time.UTC)),
			Jitter:  durationpb.New(1 * time.Second),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 00, 0, 162000000, time.UTC),
		time.Date(2022, 3, 23, 13, 30, 0, 587000000, time.UTC),
		time.Time{}, // end of sequence
	)
}

func (s *specSuite) TestSpecBoundedJitter() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(90 * time.Minute)},
			},
			Jitter: durationpb.New(24 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 23, 12, 14, 36, 524000000, time.UTC),
		time.Date(2022, 3, 23, 14, 22, 54, 562000000, time.UTC),
		time.Date(2022, 3, 23, 15, 8, 3, 724000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSingleRun() {
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 55, 0, 0, time.UTC),
	)
	s.checkSequenceFull(
		"",
		&schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{
				{Hour: "13", Minute: "55", DayOfMonth: "7", Month: "4", Year: "2022"},
			},
			Jitter: durationpb.New(1 * time.Hour),
		},
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 4, 7, 13, 57, 26, 927000000, time.UTC),
	)
}

func (s *specSuite) TestSpecJitterSeed() {
	spec := &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{
			{Interval: durationpb.New(24 * time.Hour)},
		},
		Jitter: durationpb.New(1 * time.Hour),
	}
	s.checkSequenceFull(
		"",
		spec,
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 0, 35, 24, 276000000, time.UTC),
	)
	s.checkSequenceFull(
		"seed-1",
		spec,
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 0, 6, 12, 519000000, time.UTC),
	)
	s.checkSequenceFull(
		"seed-2",
		spec,
		time.Date(2022, 3, 23, 11, 00, 0, 0, time.UTC),
		time.Date(2022, 3, 24, 0, 39, 16, 922000000, time.UTC),
	)
}
