package propertytest

import (
	"fmt"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

var analysisTimezones = []string{
	"",
	"UTC",
	"America/Chicago",
	"America/Los_Angeles",
	"Europe/London",
	"Australia/Lord_Howe",
	"Pacific/Apia",
}

type scheduleCase struct {
	model scheduleModel
	spec  *schedulepb.ScheduleSpec
	start time.Time
	end   time.Time
	seed  string
}

type invalidScheduleCase struct {
	spec  *schedulepb.ScheduleSpec
	label string
}

type queryRangeProfile struct {
	name          string
	earliestStart time.Time
	earliestEnd   time.Time
	latestEnd     time.Time
	minWindow     time.Duration
	maxWindow     time.Duration
	windowBuckets []time.Duration
}

var (
	realisticQueryRangeProfile = queryRangeProfile{
		name:          "realistic",
		earliestStart: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		earliestEnd:   time.Date(2020, 1, 1, 0, 1, 0, 0, time.UTC),
		latestEnd:     time.Date(2035, 12, 31, 23, 59, 59, 0, time.UTC),
		minWindow:     time.Minute,
		maxWindow:     365 * 24 * time.Hour,
		windowBuckets: []time.Duration{time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour, 90 * 24 * time.Hour, 365 * 24 * time.Hour},
	}
	shortQueryRangeProfile = queryRangeProfile{
		name:          "short",
		earliestStart: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		earliestEnd:   time.Date(2020, 1, 1, 0, 0, 1, 0, time.UTC),
		latestEnd:     time.Date(2035, 12, 31, 23, 59, 59, 0, time.UTC),
		minWindow:     time.Second,
		maxWindow:     7 * 24 * time.Hour,
		windowBuckets: []time.Duration{time.Minute, time.Hour, 24 * time.Hour, 7 * 24 * time.Hour},
	}
	longQueryRangeProfile = queryRangeProfile{
		name:          "long-horizon",
		earliestStart: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		earliestEnd:   time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
		latestEnd:     time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC),
		minWindow:     365 * 24 * time.Hour,
		maxWindow:     50 * 365 * 24 * time.Hour,
		windowBuckets: []time.Duration{365 * 24 * time.Hour, 5 * 365 * 24 * time.Hour, 10 * 365 * 24 * time.Hour, 25 * 365 * 24 * time.Hour, 50 * 365 * 24 * time.Hour},
	}
)

func scheduleCaseGenerator(maxWindow time.Duration, allowJitter bool) *rapid.Generator[scheduleCase] {
	profile := realisticQueryRangeProfile
	profile.maxWindow = maxWindow
	profile.windowBuckets = nil
	for _, window := range realisticQueryRangeProfile.windowBuckets {
		if window <= maxWindow {
			profile.windowBuckets = append(profile.windowBuckets, window)
		}
	}
	if len(profile.windowBuckets) == 0 {
		profile.windowBuckets = []time.Duration{maxWindow}
		profile.minWindow = min(profile.minWindow, maxWindow)
	}
	return scheduleCaseGeneratorForProfile(profile, allowJitter)
}

func scheduleCaseGeneratorForProfile(profile queryRangeProfile, allowJitter bool) *rapid.Generator[scheduleCase] {
	return rapid.Custom(func(t *rapid.T) scheduleCase {
		if profile.earliestStart.After(profile.latestEnd) ||
			profile.earliestEnd.After(profile.latestEnd) ||
			profile.minWindow <= 0 ||
			profile.maxWindow < profile.minWindow {
			panic("invalid query range profile")
		}
		latestStart := profile.latestEnd.Add(-profile.minWindow)
		startUnix := rapid.Int64Range(
			profile.earliestStart.Unix(),
			latestStart.Unix(),
		).Draw(t, "query-start")
		start := time.Unix(startUnix, 0).UTC()
		availableWindows := make([]time.Duration, 0, len(profile.windowBuckets))
		for _, window := range profile.windowBuckets {
			end := start.Add(window)
			if window >= profile.minWindow && window <= profile.maxWindow &&
				!end.Before(profile.earliestEnd) && !end.After(profile.latestEnd) {
				availableWindows = append(availableWindows, window)
			}
		}
		if len(availableWindows) == 0 {
			minimumEnd := maxTime(profile.earliestEnd, start.Add(profile.minWindow))
			maximumEnd := minTime(profile.latestEnd, start.Add(profile.maxWindow))
			if minimumEnd.After(maximumEnd) {
				panic("query range profile cannot produce an end for generated start")
			}
			windowSeconds := rapid.Int64Range(
				int64(minimumEnd.Sub(start)/time.Second),
				int64(maximumEnd.Sub(start)/time.Second),
			).Draw(t, "query-window-seconds")
			availableWindows = append(availableWindows, time.Duration(windowSeconds)*time.Second)
		}
		window := rapid.SampledFrom(availableWindows).Draw(t, "query-window")

		model := drawScheduleModel(t, start, window, allowJitter)
		return scheduleCase{
			model: model,
			spec:  model.renderStructured(),
			start: start,
			end:   start.Add(window),
			seed:  rapid.StringN(0, 16, 16).Draw(t, "jitter-seed"),
		}
	})
}

func maxTime(a time.Time, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a time.Time, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func drawScheduleModel(t *rapid.T, queryStart time.Time, window time.Duration, allowJitter bool) scheduleModel {
	kind := rapid.IntRange(0, 2).Draw(t, "schedule-kind")
	model := scheduleModel{
		timezone: rapid.SampledFrom(analysisTimezones).Draw(t, "timezone"),
	}
	if kind != 1 {
		count := rapid.IntRange(1, 3).Draw(t, "calendar-count")
		for i := range count {
			model.calendars = append(model.calendars, drawCalendarModel(t, fmt.Sprintf("calendar-%d", i), false))
		}
	}
	if kind != 0 {
		count := rapid.IntRange(1, 3).Draw(t, "interval-count")
		for i := range count {
			intervalSeconds := rapid.Int64Range(1, int64(30*24*time.Hour/time.Second)).Draw(t, fmt.Sprintf("interval-%d", i))
			phaseSeconds := rapid.Int64Range(0, intervalSeconds-1).Draw(t, fmt.Sprintf("phase-%d", i))
			model.intervals = append(model.intervals, intervalModel{
				intervalSeconds: intervalSeconds,
				phaseSeconds:    phaseSeconds,
			})
		}
	}
	exclusionCount := rapid.IntRange(0, 2).Draw(t, "exclusion-count")
	for i := range exclusionCount {
		model.exclusions = append(model.exclusions, drawCalendarModel(t, fmt.Sprintf("exclusion-%d", i), false))
	}
	if allowJitter {
		model.jitter = rapid.SampledFrom([]time.Duration{0, time.Second, time.Minute, time.Hour}).Draw(t, "jitter")
	}
	if rapid.Bool().Draw(t, "has-schedule-start") {
		scheduleStart := queryStart.Add(time.Duration(rapid.Int64Range(0, int64(window/time.Second)).Draw(t, "schedule-start-offset")) * time.Second)
		model.startTime = &scheduleStart
	}
	if rapid.Bool().Draw(t, "has-schedule-end") {
		scheduleEnd := queryStart.Add(time.Duration(rapid.Int64Range(0, int64(window/time.Second)).Draw(t, "schedule-end-offset")) * time.Second)
		model.endTime = &scheduleEnd
	}
	return model
}

func drawCalendarModel(t *rapid.T, label string, sparseYear bool) calendarModel {
	calendar := calendarModel{
		second:    drawRanges(t, label+"-second", 0, 59, 1, 2),
		minute:    drawRanges(t, label+"-minute", 0, 59, 1, 2),
		hour:      drawRanges(t, label+"-hour", 0, 23, 1, 2),
		day:       drawRanges(t, label+"-day", 1, 31, 1, 2),
		month:     drawRanges(t, label+"-month", 1, 12, 1, 2),
		dayOfWeek: drawRanges(t, label+"-dow", 0, 6, 1, 2),
	}
	if sparseYear || rapid.Bool().Draw(t, label+"-has-year") {
		calendar.year = drawRanges(t, label+"-year", 2020, 2035, 1, 2)
	}
	return calendar
}

func drawRanges(
	t *rapid.T,
	label string,
	minimum int,
	maximum int,
	minCount int,
	maxCount int,
) []rangeModel {
	count := rapid.IntRange(minCount, maxCount).Draw(t, label+"-count")
	ranges := make([]rangeModel, 0, count)
	for i := range count {
		start := rapid.IntRange(minimum, maximum).Draw(t, fmt.Sprintf("%s-%d-start", label, i))
		end := rapid.IntRange(start, maximum).Draw(t, fmt.Sprintf("%s-%d-end", label, i))
		step := rapid.IntRange(1, max(1, end-start+1)).Draw(t, fmt.Sprintf("%s-%d-step", label, i))
		ranges = append(ranges, rangeModel{start: start, end: end, step: step})
	}
	return ranges
}

func simpleCalendarCaseGenerator() *rapid.Generator[scheduleCase] {
	return rapid.Custom(func(t *rapid.T) scheduleCase {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		calendar := calendarModel{
			second:    []rangeModel{{start: rapid.IntRange(0, 59).Draw(t, "second"), end: 0, step: 1}},
			minute:    []rangeModel{{start: rapid.IntRange(0, 59).Draw(t, "minute"), end: 0, step: 1}},
			hour:      []rangeModel{{start: rapid.IntRange(0, 23).Draw(t, "hour"), end: 0, step: 1}},
			day:       []rangeModel{{start: rapid.IntRange(1, 28).Draw(t, "day"), end: 0, step: 1}},
			month:     []rangeModel{{start: rapid.IntRange(1, 12).Draw(t, "month"), end: 0, step: 1}},
			year:      []rangeModel{{start: 2024, end: 2024, step: 1}},
			dayOfWeek: []rangeModel{{start: rapid.IntRange(0, 6).Draw(t, "dow"), end: 0, step: 1}},
		}
		for _, ranges := range [][]rangeModel{calendar.second, calendar.minute, calendar.hour, calendar.day, calendar.month, calendar.dayOfWeek} {
			ranges[0].end = ranges[0].start
		}
		model := scheduleModel{calendars: []calendarModel{calendar}, timezone: "UTC"}
		return scheduleCase{model: model, spec: model.renderStructured(), start: start, end: start.Add(366 * 24 * time.Hour)}
	})
}

func invalidScheduleCaseGenerator() *rapid.Generator[invalidScheduleCase] {
	return rapid.Custom(func(t *rapid.T) invalidScheduleCase {
		base := scheduleCaseGenerator(24*time.Hour, false).Draw(t, "valid-base")
		spec := proto.Clone(base.spec).(*schedulepb.ScheduleSpec)
		mutation := rapid.IntRange(0, 8).Draw(t, "invalid-mutation")
		switch mutation {
		case 0:
			spec.StructuredCalendar = []*schedulepb.StructuredCalendarSpec{{
				Month: []*schedulepb.Range{{Start: 13}},
			}}
			return invalidScheduleCase{spec: spec, label: "month-out-of-range"}
		case 1:
			spec.Interval = []*schedulepb.IntervalSpec{{Interval: durationpb.New(500 * time.Millisecond)}}
			spec.StructuredCalendar = nil
			return invalidScheduleCase{spec: spec, label: "interval-too-small"}
		case 2:
			spec.Interval = []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(time.Hour),
				Phase:    durationpb.New(time.Hour),
			}}
			spec.StructuredCalendar = nil
			return invalidScheduleCase{spec: spec, label: "phase-not-less-than-interval"}
		case 3:
			spec.TimezoneName = "Not/A_Timezone"
			return invalidScheduleCase{spec: spec, label: "invalid-timezone"}
		case 4:
			spec.Interval = []*schedulepb.IntervalSpec{nil}
			spec.StructuredCalendar = nil
			return invalidScheduleCase{spec: spec, label: "nil-interval"}
		case 5:
			spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{{
				Month: []*schedulepb.Range{{Start: 13}},
			}}
			return invalidScheduleCase{spec: spec, label: "invalid-exclusion-month"}
		case 6:
			spec.Jitter = durationpb.New(-time.Second)
			return invalidScheduleCase{spec: spec, label: "negative-jitter"}
		case 7:
			spec.Jitter = &durationpb.Duration{Seconds: 1, Nanos: 1_000_000_000}
			return invalidScheduleCase{spec: spec, label: "invalid-jitter-protobuf"}
		default:
			spec.StartTime = &timestamppb.Timestamp{Seconds: 253_402_300_800}
			return invalidScheduleCase{spec: spec, label: "invalid-start-timestamp"}
		}
	})
}
