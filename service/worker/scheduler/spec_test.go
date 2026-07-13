package scheduler

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	s.specBuilder = NewSpecBuilder()
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

func (s *specSuite) TestRejectsSpecsWithNoMatchingTime() {
	second := durationpb.New(time.Second)
	tests := map[string]*schedulepb.ScheduleSpec{
		"impossible civil date": {
			Calendar: []*schedulepb.CalendarSpec{{Month: "2", DayOfMonth: "30"}},
		},
		"inverted bounds": {
			Interval:  []*schedulepb.IntervalSpec{{Interval: second}},
			StartTime: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			EndTime:   timestamppb.New(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		"universal exclusion": {
			Interval:        []*schedulepb.IntervalSpec{{Interval: second}},
			ExcludeCalendar: []*schedulepb.CalendarSpec{{Second: "*", Minute: "*", Hour: "*"}},
		},
		"partitioned universal exclusion": {
			Interval: []*schedulepb.IntervalSpec{{Interval: second}},
			ExcludeCalendar: []*schedulepb.CalendarSpec{
				{Second: "0-58", Minute: "*", Hour: "*"},
				{Second: "59", Minute: "*", Hour: "*"},
			},
		},
	}
	for name, spec := range tests {
		s.Run(name, func() {
			_, err := s.specBuilder.NewCompiledSpec(spec)
			s.Require().ErrorIs(err, ErrNoMatchingTimes)
		})
	}

	_, err := s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{})
	s.Require().NoError(err, "an empty inclusion set is a supported manual-only schedule")

	_, err = s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{{Month: "2", DayOfMonth: "30"}},
		Interval: []*schedulepb.IntervalSpec{{Interval: second}},
	})
	s.Require().NoError(err, "one impossible union member does not make the effective set empty")

	_, err = s.specBuilder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{{
			Second: "0", Minute: "30", Hour: "1", DayOfMonth: "5", Month: "4", Year: "2020",
		}},
		StartTime:    timestamppb.New(time.Date(2020, 4, 4, 14, 29, 59, 0, time.UTC)),
		EndTime:      timestamppb.New(time.Date(2020, 4, 4, 14, 30, 0, 0, time.UTC)),
		TimezoneName: "Australia/Lord_Howe",
	})
	s.Require().NoError(err, "validation must not inherit next-time search false negatives")
}

func (s *specSuite) TestGetNextTimeComputeLimitExceeded() {
	// The exclusions cover a finite year, so a result exists after that year even though this
	// search cannot reach it before exhausting the fallback bound.
	const iterations = 10_000
	builder := NewSpecBuilder()
	builder.SetMaxIterations(func() int { return iterations })
	cs, err := builder.NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar: []*schedulepb.CalendarSpec{{Second: "*", Minute: "*", Hour: "*"}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			{Second: "0-58", Minute: "*", Hour: "*", Year: "2025"},
			{Second: "59", Minute: "*", Hour: "*", Year: "2025"},
		},
	})
	s.Require().NoError(err)

	after := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := cs.GetNextTime("", after)

	s.Require().ErrorIs(err, ErrComputeLimitExceeded)
	s.True(result.Nominal.IsZero())
	s.True(result.Next.IsZero())
	s.Equal(iterations, result.ComputeIterations)
}

func (s *specSuite) TestUniversalExclusionValidationShortCircuits() {
	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	started := time.Now()
	_, err := NewSpecBuilder().NewCompiledSpec(&schedulepb.ScheduleSpec{
		Calendar:        []*schedulepb.CalendarSpec{everySecond},
		ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
	})
	elapsed := time.Since(started)
	s.T().Logf("universal exclusion was rejected without walking the default %d-candidate limit in %s", DefaultMaxIterations, elapsed)

	s.Require().ErrorIs(err, ErrNoMatchingTimes)
}

func (s *specSuite) TestGetNextTimeYearBoundedExclusionIsNotUniversal() {
	cs, err := NewSpecBuilder().NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{{
			Second: "*", Minute: "*", Hour: "*", DayOfMonth: "*", Month: "*", Year: "2025",
		}},
	})
	s.Require().NoError(err)

	result, err := cs.GetNextTime("", time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC))
	s.Require().NoError(err)
	s.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), result.Next)
}

func (s *specSuite) TestGetNextTimeDefaultComputeLimitCanRejectValidBlackout() {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	boundary := start.Add(time.Duration(DefaultMaxIterations) * time.Second)
	expected := boundary.Add(time.Second)
	cs, err := NewSpecBuilder().NewCompiledSpec(&schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		ExcludeCalendar: []*schedulepb.CalendarSpec{
			{
				Second: "*", Minute: "*", Hour: "*", DayOfMonth: "1-14", Month: "1", Year: "2025",
			},
			{
				Second: "0", Minute: "0", Hour: "0", DayOfMonth: "15", Month: "1", Year: "2025",
			},
		},
		EndTime: timestamppb.New(time.Date(2025, 1, 16, 0, 0, 0, 0, time.UTC)),
	})
	s.Require().NoError(err)

	started := time.Now()
	result, err := cs.GetNextTime("", start)
	elapsed := time.Since(started)
	s.T().Logf("valid finite-blackout search reached the default %d-candidate limit in %s", DefaultMaxIterations, elapsed)

	s.Require().ErrorIs(err, ErrComputeLimitExceeded)
	s.True(result.Nominal.IsZero())
	s.True(result.Next.IsZero())
	s.Equal(DefaultMaxIterations, result.ComputeIterations)
	s.Equal(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC), boundary)
	s.Equal(expected, cs.rawNextTime(boundary))
	s.False(cs.excluded(expected), "the schedule has an allowed occurrence one second beyond the default limit")
}

func (s *specSuite) TestGetNextTimeDenseIntervalWithManyExhaustedCalendars() {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := &schedulepb.ScheduleSpec{
		Interval:     []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Second)}},
		TimezoneName: "Test/UTC",
		TimezoneData: syntheticUTCZone(30_000),
	}
	s.Len(spec.TimezoneData, 150_054)
	for range 24 {
		spec.Calendar = append(spec.Calendar, &schedulepb.CalendarSpec{
			Second: "0", Minute: "0", Hour: "0", DayOfMonth: "1", Month: "1", Year: "2025",
		})
	}
	compileStarted := time.Now()
	cs, err := NewSpecBuilder().NewCompiledSpec(spec)
	compileElapsed := time.Since(compileStarted)
	s.Require().NoError(err)

	started := time.Now()
	after := start
	for range 1_000 {
		result, err := cs.GetNextTime("", after)
		s.Require().NoError(err)
		s.Require().False(result.Next.IsZero())
		after = result.Next
	}
	elapsed := time.Since(started)
	s.T().Logf(
		"dense interval, 24 exhausted calendars, and a %d-byte TZif compiled in %s and produced 1,000 results in %s",
		len(spec.TimezoneData),
		compileElapsed,
		elapsed,
	)

	s.Equal(start.Add(1_000*time.Second), after)
}

func syntheticUTCZone(transitionCount int) []byte {
	var data bytes.Buffer
	data.WriteString("TZif")
	data.WriteByte(0)
	data.Write(make([]byte, 15))
	counts := []uint32{0, 0, 0, uint32(transitionCount), 1, 4}
	var word [4]byte
	for _, count := range counts {
		binary.BigEndian.PutUint32(word[:], count)
		data.Write(word[:])
	}
	for index := range transitionCount {
		transition := int32(-1_800_000_000 + index*60)
		binary.BigEndian.PutUint32(word[:], uint32(transition))
		data.Write(word[:])
	}
	data.Write(make([]byte, transitionCount))
	binary.BigEndian.PutUint32(word[:], 0)
	data.Write(word[:])
	data.WriteByte(0)
	data.WriteByte(0)
	data.WriteString("UTC\x00")
	return data.Bytes()
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
