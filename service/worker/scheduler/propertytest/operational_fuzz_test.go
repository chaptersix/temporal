package propertytest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	operationalFuzzMaxInput       = 256 << 10
	operationalFuzzValidationWork = 100_000
)

func FuzzOperationalRawScheduleDecodeAndValidate(f *testing.F) {
	seed, err := proto.Marshal(plan3SimpleIntervalSpec())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0xff, 0x01, 0x7f})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > operationalFuzzMaxInput {
			return
		}
		var spec schedulepb.ScheduleSpec
		if err := proto.Unmarshal(data, &spec); err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := ValidateSchedule(ctx, &spec, ValidationOptions{MaxIterations: operationalFuzzValidationWork})
		if err != nil && !isExpectedOperationalFuzzError(err) {
			t.Fatalf("unexpected validation error: %v", err)
		}
	})
}

func FuzzOperationalMalformedTimezoneData(f *testing.F) {
	f.Add("Fuzz/UTC", redTeamSyntheticTZif(1))
	f.Add("", []byte("TZif"))
	f.Fuzz(func(t *testing.T, name string, data []byte) {
		if len(name) > 128 || len(data) > 192<<10 {
			return
		}
		spec := plan3SimpleIntervalSpec()
		spec.TimezoneName = name
		spec.TimezoneData = append([]byte(nil), data...)
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := ValidateSchedule(ctx, spec, ValidationOptions{MaxIterations: operationalFuzzValidationWork})
		if err != nil && !isExpectedOperationalFuzzError(err) {
			t.Fatalf("unexpected timezone error: %v", err)
		}
	})
}

func FuzzOperationalExtremeTimestampsAndDurations(f *testing.F) {
	f.Add(int64(0), int32(0), int64(1), int32(0))
	f.Add(int64(253402300799), int32(999999999), int64(315576000000), int32(0))
	f.Fuzz(func(t *testing.T, seconds int64, nanos int32, durationSeconds int64, durationNanos int32) {
		spec := &schedulepb.ScheduleSpec{
			StartTime: timestamppb.New(time.Unix(seconds, int64(nanos)).UTC()),
			Interval:  []*schedulepb.IntervalSpec{{Interval: &durationpb.Duration{Seconds: durationSeconds, Nanos: durationNanos}}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := ValidateSchedule(ctx, spec, ValidationOptions{MaxIterations: operationalFuzzValidationWork})
		if err != nil && !isExpectedOperationalFuzzError(err) {
			t.Fatalf("unexpected timestamp/duration error: %v", err)
		}
	})
}

func FuzzOperationalLargeRepeatedFields(f *testing.F) {
	f.Add(uint8(1), uint8(1))
	f.Add(uint8(64), uint8(64))
	f.Fuzz(func(t *testing.T, componentByte uint8, rangeByte uint8) {
		components := 1 + int(componentByte)%64
		ranges := 1 + int(rangeByte)%64
		spec := &schedulepb.ScheduleSpec{}
		for range components {
			seconds := make([]*schedulepb.Range, 0, ranges)
			for index := range ranges {
				seconds = append(seconds, &schedulepb.Range{Start: int32(index % 60)})
			}
			calendar := &schedulepb.StructuredCalendarSpec{
				Second: seconds, Minute: allRanges(0, 59), Hour: allRanges(0, 23),
				DayOfMonth: allRanges(1, 31), Month: allRanges(1, 12), DayOfWeek: allRanges(0, 6),
			}
			spec.StructuredCalendar = append(spec.StructuredCalendar, calendar)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := ValidateSchedule(ctx, spec, ValidationOptions{MaxIterations: operationalFuzzValidationWork})
		if err != nil && !isExpectedOperationalFuzzError(err) {
			t.Fatalf("unexpected repeated-field error: %v", err)
		}
	})
}

func FuzzOperationalUnicodeCronAndComments(f *testing.F) {
	f.Add("0 0 * * * # hello")
	f.Add("0 0 * * * # 世界")
	f.Add("☃ 0 * * *")
	f.Fuzz(func(t *testing.T, cron string) {
		if len(cron) > 1_024 || !utf8.ValidString(cron) {
			return
		}
		spec := &schedulepb.ScheduleSpec{CronString: []string{cron}}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := ValidateSchedule(ctx, spec, ValidationOptions{MaxIterations: operationalFuzzValidationWork})
		if err != nil && !isExpectedOperationalFuzzError(err) {
			t.Fatalf("unexpected cron error: %v", err)
		}
	})
}

func FuzzOperationalCancellationRacingParsingAndValidation(f *testing.F) {
	f.Add("0 0 * * *", uint16(1))
	f.Add("0 0 30 2 *", uint16(100))
	f.Fuzz(func(t *testing.T, cron string, cancelAt uint16) {
		if len(cron) > 1_024 {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var once sync.Once
		_, err := ValidateSchedule(ctx, &schedulepb.ScheduleSpec{CronString: []string{cron}}, ValidationOptions{
			MaxIterations: operationalFuzzValidationWork,
			WorkTickHook: func(tick WorkTick) {
				if tick.Total >= int(cancelAt)%1_000 {
					once.Do(cancel)
				}
			},
		})
		if err != nil && !isExpectedOperationalFuzzError(err) {
			t.Fatalf("unexpected cancellation/parse error: %v", err)
		}
	})
}

func isExpectedOperationalFuzzError(err error) bool {
	return errors.Is(err, ErrInvalidSpec) || errors.Is(err, ErrUnsatisfiableSpec) ||
		errors.Is(err, ErrValidationLimit) || errors.Is(err, ErrInvalidOptions) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
