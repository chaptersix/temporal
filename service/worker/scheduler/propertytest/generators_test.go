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

type validationCase struct {
	model    scheduleModel
	spec     *schedulepb.ScheduleSpec
	expected ValidationStatus
	witness  time.Time
	label    string
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

func validSatisfiableScheduleCaseGenerator(maxWindow time.Duration, allowJitter bool) *rapid.Generator[scheduleCase] {
	return scheduleCaseGenerator(maxWindow, allowJitter)
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
	witnessOffset := rapid.Int64Range(1, max(1, int64(window/time.Second))).Draw(t, "witness-offset")
	witness := queryStart.Add(time.Duration(witnessOffset) * time.Second).UTC()
	model := scheduleModel{
		timezone: rapid.SampledFrom(analysisTimezones).Draw(t, "timezone"),
		witness:  witness,
	}
	location := time.UTC
	if model.timezone != "" {
		var err error
		location, err = time.LoadLocation(model.timezone)
		if err != nil {
			panic(err)
		}
	}
	localWitness := witness.In(location)
	preferredOccurrence := time.Date(
		localWitness.Year(),
		localWitness.Month(),
		localWitness.Day(),
		localWitness.Hour(),
		localWitness.Minute(),
		localWitness.Second(),
		0,
		location,
	).UTC()
	if preferredOccurrence != witness {
		model.timezone = "UTC"
		location = time.UTC
	}
	if kind != 1 {
		count := rapid.IntRange(1, 3).Draw(t, "calendar-count")
		for i := range count {
			model.calendars = append(model.calendars, drawWitnessCalendarModel(
				t,
				fmt.Sprintf("calendar-%d", i),
				witness.In(location),
			))
		}
	}
	if kind != 0 {
		count := rapid.IntRange(1, 3).Draw(t, "interval-count")
		for i := range count {
			intervalSeconds := rapid.Int64Range(1, int64(30*24*time.Hour/time.Second)).Draw(t, fmt.Sprintf("interval-%d", i))
			phaseSeconds := witness.Unix() % intervalSeconds
			model.intervals = append(model.intervals, intervalModel{
				intervalSeconds: intervalSeconds,
				phaseSeconds:    phaseSeconds,
			})
		}
	}
	exclusionCount := rapid.IntRange(0, 2).Draw(t, "exclusion-count")
	for i := range exclusionCount {
		exclusionWitness := witness.In(location).Add(time.Duration(i+1) * time.Second)
		model.exclusions = append(model.exclusions, exactCalendarModel(exclusionWitness))
	}
	if allowJitter {
		model.jitter = rapid.SampledFrom([]time.Duration{0, time.Second, time.Minute, time.Hour}).Draw(t, "jitter")
	}
	if rapid.Bool().Draw(t, "has-schedule-start") {
		scheduleStart := witness.Add(-time.Duration(rapid.Int64Range(0, max(0, int64(witness.Sub(queryStart)/time.Second))).Draw(t, "schedule-start-before-witness")) * time.Second)
		model.startTime = &scheduleStart
	}
	if rapid.Bool().Draw(t, "has-schedule-end") {
		minimumAfterWitness := int64(exclusionCount)
		maximumAfterWitness := max(minimumAfterWitness, int64(queryStart.Add(window).Sub(witness)/time.Second))
		scheduleEnd := witness.Add(time.Duration(rapid.Int64Range(minimumAfterWitness, maximumAfterWitness).Draw(t, "schedule-end-after-witness")) * time.Second)
		model.endTime = &scheduleEnd
	}
	return model
}

func drawWitnessCalendarModel(t *rapid.T, label string, witness time.Time) calendarModel {
	exactOrWide := func(value int, minimum int, maximum int, field string) []rangeModel {
		if rapid.Bool().Draw(t, label+"-"+field+"-wide") {
			return []rangeModel{{start: minimum, end: maximum, step: 1}}
		}
		return []rangeModel{{start: value, end: value, step: 1}}
	}
	calendar := calendarModel{
		second:    exactOrWide(witness.Second(), 0, 59, "second"),
		minute:    exactOrWide(witness.Minute(), 0, 59, "minute"),
		hour:      exactOrWide(witness.Hour(), 0, 23, "hour"),
		day:       exactOrWide(witness.Day(), 1, 31, "day"),
		month:     exactOrWide(int(witness.Month()), 1, 12, "month"),
		dayOfWeek: exactOrWide(int(witness.Weekday()), 0, 6, "dow"),
	}
	if rapid.Bool().Draw(t, label+"-has-year") {
		calendar.year = []rangeModel{{start: witness.Year(), end: witness.Year(), step: 1}}
	}
	return calendar
}

func exactCalendarModel(witness time.Time) calendarModel {
	return calendarModel{
		second:    []rangeModel{{start: witness.Second(), end: witness.Second(), step: 1}},
		minute:    []rangeModel{{start: witness.Minute(), end: witness.Minute(), step: 1}},
		hour:      []rangeModel{{start: witness.Hour(), end: witness.Hour(), step: 1}},
		day:       []rangeModel{{start: witness.Day(), end: witness.Day(), step: 1}},
		month:     []rangeModel{{start: int(witness.Month()), end: int(witness.Month()), step: 1}},
		year:      []rangeModel{{start: witness.Year(), end: witness.Year(), step: 1}},
		dayOfWeek: []rangeModel{{start: int(witness.Weekday()), end: int(witness.Weekday()), step: 1}},
	}
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
			dayOfWeek: []rangeModel{{start: 0, end: 0, step: 1}},
		}
		calendar.dayOfWeek[0].start = int(time.Date(2024, time.Month(calendar.month[0].start), calendar.day[0].start, 0, 0, 0, 0, time.UTC).Weekday())
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
		mutation := rapid.IntRange(0, 10).Draw(t, "invalid-mutation")
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
		case 8:
			spec.StartTime = &timestamppb.Timestamp{Seconds: 253_402_300_800}
			return invalidScheduleCase{spec: spec, label: "invalid-start-timestamp"}
		case 9:
			spec.Calendar = []*schedulepb.CalendarSpec{nil}
			spec.StructuredCalendar = nil
			spec.Interval = nil
			return invalidScheduleCase{spec: spec, label: "nil-calendar"}
		default:
			spec.ExcludeCalendar = []*schedulepb.CalendarSpec{nil}
			return invalidScheduleCase{spec: spec, label: "nil-exclusion-calendar"}
		}
	})
}

func invalidStructuralScheduleCaseGenerator() *rapid.Generator[invalidScheduleCase] {
	return invalidScheduleCaseGenerator()
}

func invalidComponentScheduleCaseGenerator() *rapid.Generator[validationCase] {
	return rapid.Custom(func(t *rapid.T) validationCase {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := start.Add(365*24*time.Hour - time.Second)
		all := []*schedulepb.Range{{Start: 0, End: 6, Step: 1}}
		calendar := exactCalendarModel(start).render()
		label := ""
		switch rapid.IntRange(0, 5).Draw(t, "component-mutation") {
		case 0:
			calendar.Minute = nil
			label = "missing-required-field"
		case 1:
			calendar.Month = []*schedulepb.Range{{Start: 2}}
			calendar.DayOfMonth = []*schedulepb.Range{{Start: 30}}
			calendar.DayOfWeek = all
			calendar.Year = nil
			label = "february-30"
		case 2:
			calendar.Month = []*schedulepb.Range{{Start: 4}}
			calendar.DayOfMonth = []*schedulepb.Range{{Start: 31}}
			calendar.DayOfWeek = all
			calendar.Year = nil
			label = "april-31"
		case 3:
			calendar.Month = []*schedulepb.Range{{Start: 2}}
			calendar.DayOfMonth = []*schedulepb.Range{{Start: 29}}
			calendar.DayOfWeek = all
			calendar.Year = []*schedulepb.Range{{Start: 2023}}
			label = "non-leap-february-29"
		case 4:
			calendar.Year = []*schedulepb.Range{{Start: 2024}}
			calendar.DayOfWeek = []*schedulepb.Range{{Start: int32((start.Weekday() + 1) % 7)}}
			label = "impossible-day-of-week-conjunction"
		default:
			calendar = exactCalendarModel(start.Add(-24 * time.Hour)).render()
			label = "component-outside-schedule-bounds"
		}
		spec := &schedulepb.ScheduleSpec{
			StructuredCalendar: []*schedulepb.StructuredCalendarSpec{calendar},
			StartTime:          timestamppb.New(start),
			EndTime:            timestamppb.New(end),
		}
		return validationCase{spec: spec, expected: ValidationInvalidComponentUnsatisfiable, label: label}
	})
}

func invalidEffectiveSetScheduleCaseGenerator() *rapid.Generator[validationCase] {
	return rapid.Custom(func(t *rapid.T) validationCase {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		seconds := rapid.Int64Range(1, 300).Draw(t, "horizon-seconds")
		end := start.Add(time.Duration(seconds) * time.Second)
		full := calendarModel{
			second:    []rangeModel{{start: 0, end: 59, step: 1}},
			minute:    []rangeModel{{start: 0, end: 59, step: 1}},
			hour:      []rangeModel{{start: 0, end: 23, step: 1}},
			day:       []rangeModel{{start: 1, end: 31, step: 1}},
			month:     []rangeModel{{start: 1, end: 12, step: 1}},
			dayOfWeek: []rangeModel{{start: 0, end: 6, step: 1}},
		}
		model := scheduleModel{
			exclusions: []calendarModel{full},
			startTime:  &start,
			endTime:    &end,
			timezone:   "UTC",
		}
		switch rapid.IntRange(0, 2).Draw(t, "inclusion-kind") {
		case 0:
			model.intervals = []intervalModel{{intervalSeconds: 1}}
		case 1:
			model.calendars = []calendarModel{exactCalendarModel(start)}
		default:
			model.intervals = []intervalModel{{intervalSeconds: 1}}
			model.calendars = []calendarModel{exactCalendarModel(start)}
		}
		return validationCase{
			model:    model,
			spec:     model.renderStructured(),
			expected: ValidationInvalidEffectiveSetEmpty,
			label:    "fully-excluded-interval",
		}
	})
}

func reducedValidationCaseGenerator() *rapid.Generator[validationCase] {
	return rapid.Custom(func(t *rapid.T) validationCase {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		horizonSeconds := rapid.Int64Range(1, 120).Draw(t, "horizon-seconds")
		end := start.Add(time.Duration(horizonSeconds) * time.Second)
		if rapid.Bool().Draw(t, "empty-effective-set") {
			full := calendarModel{
				second:    []rangeModel{{start: 0, end: 59, step: 1}},
				minute:    []rangeModel{{start: 0, end: 59, step: 1}},
				hour:      []rangeModel{{start: 0, end: 23, step: 1}},
				day:       []rangeModel{{start: 1, end: 31, step: 1}},
				month:     []rangeModel{{start: 1, end: 12, step: 1}},
				dayOfWeek: []rangeModel{{start: 0, end: 6, step: 1}},
			}
			model := scheduleModel{
				intervals:  []intervalModel{{intervalSeconds: 1}},
				exclusions: []calendarModel{full},
				startTime:  &start,
				endTime:    &end,
				timezone:   "UTC",
			}
			return validationCase{model: model, spec: model.renderStructured(), expected: ValidationInvalidEffectiveSetEmpty, label: "reduced-empty"}
		}
		witnessOffset := rapid.Int64Range(0, horizonSeconds).Draw(t, "witness-offset")
		witness := start.Add(time.Duration(witnessOffset) * time.Second)
		period := rapid.Int64Range(1, max(1, horizonSeconds)).Draw(t, "interval-seconds")
		model := scheduleModel{
			intervals: []intervalModel{{intervalSeconds: period, phaseSeconds: witness.Unix() % period}},
			witness:   witness,
			startTime: &start,
			endTime:   &end,
			timezone:  "UTC",
		}
		return validationCase{model: model, spec: model.renderStructured(), expected: ValidationValid, witness: witness, label: "reduced-valid"}
	})
}
