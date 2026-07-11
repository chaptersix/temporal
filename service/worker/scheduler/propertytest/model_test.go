package propertytest

import (
	"fmt"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type rangeModel struct {
	start int
	end   int
	step  int
}

func (r rangeModel) matches(value int) bool {
	return value >= r.start && value <= r.end && (value-r.start)%r.step == 0
}

func (r rangeModel) render() *schedulepb.Range {
	return &schedulepb.Range{Start: int32(r.start), End: int32(r.end), Step: int32(r.step)}
}

func (r rangeModel) calendarString() string {
	if r.start == r.end {
		return fmt.Sprintf("%d", r.start)
	}
	if r.step == 1 {
		return fmt.Sprintf("%d-%d", r.start, r.end)
	}
	return fmt.Sprintf("%d-%d/%d", r.start, r.end, r.step)
}

type calendarModel struct {
	second    []rangeModel
	minute    []rangeModel
	hour      []rangeModel
	day       []rangeModel
	month     []rangeModel
	year      []rangeModel
	dayOfWeek []rangeModel
}

func (c calendarModel) matches(ts time.Time, location *time.Location) bool {
	local := ts.In(location)
	year, month, day := local.Date()
	hour, minute, second := local.Clock()
	return rangesMatch(c.second, second, false) &&
		rangesMatch(c.minute, minute, false) &&
		rangesMatch(c.hour, hour, false) &&
		rangesMatch(c.day, day, false) &&
		rangesMatch(c.month, int(month), false) &&
		rangesMatch(c.year, year, true) &&
		rangesMatch(c.dayOfWeek, int(local.Weekday()), false)
}

func (c calendarModel) render() *schedulepb.StructuredCalendarSpec {
	return &schedulepb.StructuredCalendarSpec{
		Second:     renderRanges(c.second),
		Minute:     renderRanges(c.minute),
		Hour:       renderRanges(c.hour),
		DayOfMonth: renderRanges(c.day),
		Month:      renderRanges(c.month),
		Year:       renderRanges(c.year),
		DayOfWeek:  renderRanges(c.dayOfWeek),
	}
}

func (c calendarModel) renderCalendar() *schedulepb.CalendarSpec {
	return &schedulepb.CalendarSpec{
		Second:     rangesString(c.second, false),
		Minute:     rangesString(c.minute, false),
		Hour:       rangesString(c.hour, false),
		DayOfMonth: rangesString(c.day, false),
		Month:      rangesString(c.month, false),
		Year:       rangesString(c.year, true),
		DayOfWeek:  rangesString(c.dayOfWeek, false),
	}
}

func (c calendarModel) renderCron() string {
	return fmt.Sprintf(
		"%s %s %s %s %s %s %s",
		rangesString(c.second, false),
		rangesString(c.minute, false),
		rangesString(c.hour, false),
		rangesString(c.day, false),
		rangesString(c.month, false),
		rangesString(c.dayOfWeek, false),
		rangesString(c.year, true),
	)
}

type intervalModel struct {
	intervalSeconds int64
	phaseSeconds    int64
}

func (i intervalModel) matches(ts time.Time) bool {
	seconds := ts.Unix() - i.phaseSeconds
	return seconds%i.intervalSeconds == 0
}

func (i intervalModel) render() *schedulepb.IntervalSpec {
	return &schedulepb.IntervalSpec{
		Interval: durationpb.New(time.Duration(i.intervalSeconds) * time.Second),
		Phase:    durationpb.New(time.Duration(i.phaseSeconds) * time.Second),
	}
}

type scheduleModel struct {
	calendars  []calendarModel
	intervals  []intervalModel
	exclusions []calendarModel
	witness    time.Time
	startTime  *time.Time
	endTime    *time.Time
	jitter     time.Duration
	timezone   string
}

func (m scheduleModel) renderStructured() *schedulepb.ScheduleSpec {
	spec := &schedulepb.ScheduleSpec{
		TimezoneName: m.timezone,
		Jitter:       durationpb.New(m.jitter),
	}
	for _, calendar := range m.calendars {
		spec.StructuredCalendar = append(spec.StructuredCalendar, calendar.render())
	}
	for _, interval := range m.intervals {
		spec.Interval = append(spec.Interval, interval.render())
	}
	for _, exclusion := range m.exclusions {
		spec.ExcludeStructuredCalendar = append(spec.ExcludeStructuredCalendar, exclusion.render())
	}
	if m.startTime != nil {
		spec.StartTime = timestamppb.New(*m.startTime)
	}
	if m.endTime != nil {
		spec.EndTime = timestamppb.New(*m.endTime)
	}
	return spec
}

func (m scheduleModel) location() (*time.Location, error) {
	if m.timezone == "" {
		return time.UTC, nil
	}
	return time.LoadLocation(m.timezone)
}

func (m scheduleModel) matchesNominal(ts time.Time) bool {
	if m.startTime != nil && ts.Before(*m.startTime) {
		return false
	}
	if m.endTime != nil && ts.After(*m.endTime) {
		return false
	}
	location, err := m.location()
	if err != nil {
		return false
	}
	included := false
	for _, calendar := range m.calendars {
		included = included || calendar.matches(ts, location)
	}
	for _, interval := range m.intervals {
		included = included || interval.matches(ts)
	}
	if !included {
		return false
	}
	for _, exclusion := range m.exclusions {
		if exclusion.matches(ts, location) {
			return false
		}
	}
	return true
}

func bruteForceMatchingTimes(
	model scheduleModel,
	start time.Time,
	end time.Time,
	maxResults int,
) []time.Time {
	times := make([]time.Time, 0, maxResults)
	for candidate := time.Unix(start.Unix()+1, 0).UTC(); !candidate.After(end); candidate = candidate.Add(time.Second) {
		if model.matchesNominal(candidate) {
			times = append(times, candidate)
			if len(times) == maxResults {
				break
			}
		}
	}
	return times
}

func bruteForceValidationWitness(model scheduleModel, start time.Time, end time.Time) time.Time {
	for candidate := start; !candidate.After(end); candidate = candidate.Add(time.Second) {
		if model.matchesNominal(candidate) {
			return candidate
		}
	}
	return time.Time{}
}

func rangesMatch(ranges []rangeModel, value int, emptyMatchesAll bool) bool {
	if len(ranges) == 0 {
		return emptyMatchesAll
	}
	for _, r := range ranges {
		if r.matches(value) {
			return true
		}
	}
	return false
}

func renderRanges(ranges []rangeModel) []*schedulepb.Range {
	out := make([]*schedulepb.Range, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, r.render())
	}
	return out
}

func rangesString(ranges []rangeModel, emptyMeansAll bool) string {
	if len(ranges) == 0 && emptyMeansAll {
		return "*"
	}
	result := ""
	for i, r := range ranges {
		if i > 0 {
			result += ","
		}
		result += r.calendarString()
	}
	return result
}
