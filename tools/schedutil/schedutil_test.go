package schedutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	schedulepb "go.temporal.io/api/schedule/v1"
)

func TestDeduplicateStructuredCalendarsProto(t *testing.T) {
	hourly := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
	}
	hourlyCommented := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
		Comment:    "same schedule, different comment",
	}
	hourlyDefaulted := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
		Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23, Step: 1}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31, Step: 1}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
		DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
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
		{name: "preserves comment differences", input: []*schedulepb.StructuredCalendarSpec{hourly, hourlyCommented}, want: 2},
		{name: "normalizes defaulted ranges", input: []*schedulepb.StructuredCalendarSpec{hourly, hourlyDefaulted}, want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Len(t, deduplicateStructuredCalendarsProto(tc.input), tc.want)
		})
	}
}
