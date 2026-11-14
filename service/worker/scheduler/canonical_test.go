package scheduler

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCanonicalizeScheduleSpecReference(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec *schedulepb.ScheduleSpec
	}{
		{"cron", &schedulepb.ScheduleSpec{CronString: []string{"TZ=UTC 0 12 * * *"}}},
		{"malformed cron", &schedulepb.ScheduleSpec{CronString: []string{"invalid"}}},
		{"conflicting cron zones", &schedulepb.ScheduleSpec{CronString: []string{"TZ=UTC 0 12 * * *", "TZ=America/New_York 0 12 * * *"}}},
		{"conflicting explicit zone", &schedulepb.ScheduleSpec{CronString: []string{"TZ=UTC 0 12 * * *"}, TimezoneName: "Europe/London"}},
		{"conflicting custom bytes", &schedulepb.ScheduleSpec{CronString: []string{"TZ=UTC 0 12 * * *"}, TimezoneData: []byte{}}},
		{"nil calendar", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{nil}}},
		{"nil interval", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{nil}}},
		{"invalid duration", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: &durationpb.Duration{Nanos: 1000000000}}}}},
		{"nil range", &schedulepb.ScheduleSpec{StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{nil}}}}},
		{"invalid exclusion", &schedulepb.ScheduleSpec{ExcludeStructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Second: []*schedulepb.Range{{Start: 60}}}}}},
		{"interval", &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := proto.Clone(tc.spec)
			got, err := CanonicalizeScheduleSpec(tc.spec)
			reference, referenceErr := NewSpecBuilder(func() int { return 0 }, func() int { return 0 }).NewBoundedCompiledSpec(tc.spec, math.MaxInt, math.MaxInt)
			if referenceErr != nil {
				require.EqualError(t, err, referenceErr.Error())
			} else {
				require.NoError(t, err)
				require.True(t, proto.Equal(reference.CanonicalForm(), got))
			}
			require.True(t, proto.Equal(before, tc.spec))
		})
	}
}

func TestCanonicalizeScheduleSpecDoesNotResolve(t *testing.T) {
	t.Parallel()
	for _, data := range [][]byte{nil, {}, {1, 2, 3}} {
		source := &schedulepb.ScheduleSpec{TimezoneName: "missing/preparation-only", TimezoneData: data}
		got, err := CanonicalizeScheduleSpec(source)
		require.NoError(t, err)
		require.Equal(t, source.TimezoneName, got.TimezoneName)
		// The existing canonicalizer protobuf clone normalizes present-empty bytes to nil.
		require.Equal(t, proto.Clone(source).(*schedulepb.ScheduleSpec).TimezoneData, got.TimezoneData)
	}
}
