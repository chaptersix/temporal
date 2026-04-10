package schedutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	schedulepb "go.temporal.io/api/schedule/v1"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestDeduplicateCalendars(t *testing.T) {
	hour5 := sdkclient.ScheduleCalendarSpec{
		Hour:   []sdkclient.ScheduleRange{{Start: 5}},
		Minute: []sdkclient.ScheduleRange{{Start: 0}},
	}
	hour9 := sdkclient.ScheduleCalendarSpec{
		Hour:   []sdkclient.ScheduleRange{{Start: 9}},
		Minute: []sdkclient.ScheduleRange{{Start: 30}},
	}

	tests := []struct {
		name  string
		input []sdkclient.ScheduleCalendarSpec
		want  []sdkclient.ScheduleCalendarSpec
	}{
		{
			name:  "empty",
			input: nil,
			want:  nil,
		},
		{
			name:  "no duplicates preserved in order",
			input: []sdkclient.ScheduleCalendarSpec{hour5, hour9},
			want:  []sdkclient.ScheduleCalendarSpec{hour5, hour9},
		},
		{
			name:  "all duplicates collapsed to one",
			input: []sdkclient.ScheduleCalendarSpec{hour5, hour5, hour5},
			want:  []sdkclient.ScheduleCalendarSpec{hour5},
		},
		{
			name:  "first occurrence wins",
			input: []sdkclient.ScheduleCalendarSpec{hour5, hour9, hour5},
			want:  []sdkclient.ScheduleCalendarSpec{hour5, hour9},
		},
		{
			name:  "single entry unchanged",
			input: []sdkclient.ScheduleCalendarSpec{hour9},
			want:  []sdkclient.ScheduleCalendarSpec{hour9},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deduplicateCalendars(tc.input)
			if len(tc.want) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestDeduplicateIntervals(t *testing.T) {
	hourly := sdkclient.ScheduleIntervalSpec{Every: time.Hour}
	daily := sdkclient.ScheduleIntervalSpec{Every: 24 * time.Hour}
	hourlyOffset := sdkclient.ScheduleIntervalSpec{Every: time.Hour, Offset: 30 * time.Minute}

	tests := []struct {
		name  string
		input []sdkclient.ScheduleIntervalSpec
		want  []sdkclient.ScheduleIntervalSpec
	}{
		{
			name:  "empty",
			input: nil,
			want:  nil,
		},
		{
			name:  "no duplicates preserved",
			input: []sdkclient.ScheduleIntervalSpec{hourly, daily},
			want:  []sdkclient.ScheduleIntervalSpec{hourly, daily},
		},
		{
			name:  "duplicates collapsed",
			input: []sdkclient.ScheduleIntervalSpec{hourly, hourly, hourly},
			want:  []sdkclient.ScheduleIntervalSpec{hourly},
		},
		{
			name:  "same duration different offset not deduplicated",
			input: []sdkclient.ScheduleIntervalSpec{hourly, hourlyOffset},
			want:  []sdkclient.ScheduleIntervalSpec{hourly, hourlyOffset},
		},
		{
			name:  "first occurrence wins",
			input: []sdkclient.ScheduleIntervalSpec{hourly, daily, hourly},
			want:  []sdkclient.ScheduleIntervalSpec{hourly, daily},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deduplicateIntervals(tc.input)
			if len(tc.want) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestDeduplicateStructuredCalendarsProto(t *testing.T) {
	hourly := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
	}
	daily := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 9}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
	}

	tests := []struct {
		name  string
		input []*schedulepb.StructuredCalendarSpec
		want  int
	}{
		{name: "empty", input: nil, want: 0},
		{name: "no duplicates preserved", input: []*schedulepb.StructuredCalendarSpec{hourly, daily}, want: 2},
		{name: "all duplicates collapsed to one", input: []*schedulepb.StructuredCalendarSpec{hourly, hourly, hourly}, want: 1},
		{name: "first occurrence wins", input: []*schedulepb.StructuredCalendarSpec{hourly, daily, hourly}, want: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Len(t, deduplicateStructuredCalendarsProto(tc.input), tc.want)
		})
	}
}

func TestDeduplicateIntervalsProto(t *testing.T) {
	hourly := &schedulepb.IntervalSpec{Interval: durationpb.New(time.Hour)}
	daily := &schedulepb.IntervalSpec{Interval: durationpb.New(24 * time.Hour)}

	tests := []struct {
		name  string
		input []*schedulepb.IntervalSpec
		want  int
	}{
		{name: "empty", input: nil, want: 0},
		{name: "no duplicates preserved", input: []*schedulepb.IntervalSpec{hourly, daily}, want: 2},
		{name: "all duplicates collapsed to one", input: []*schedulepb.IntervalSpec{hourly, hourly, hourly}, want: 1},
		{name: "first occurrence wins", input: []*schedulepb.IntervalSpec{hourly, daily, hourly}, want: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Len(t, deduplicateIntervalsProto(tc.input), tc.want)
		})
	}
}
