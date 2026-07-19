package scheduler

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
)

const DefaultWarnIterations = 24 * 60 * 60

type (
	CompiledSpec struct {
		spec           *schedulepb.ScheduleSpec
		tz             *time.Location
		calendar       []*compiledCalendar
		intervals      []compiledInterval
		excludes       []*compiledExclusion
		warnIterations dynamicconfig.IntPropertyFn
		maxIterations  dynamicconfig.IntPropertyFn
	}

	compiledInterval struct {
		interval int64
		phase    int64
	}

	GetNextTimeResult struct {
		Nominal time.Time // scheduled time before adding jitter
		Next    time.Time // scheduled time after adding jitter
		// ComputeLimitWarning is set when the search crossed the (non-fatal) warn threshold
		// before returning. It carries no scheduling meaning; callers surface it as a metric +
		// log so an over-excluded spec is observable without stopping the schedule.
		ComputeLimitWarning bool
	}

	SpecBuilder struct {
		// locationCache is a cache for the results of time.LoadLocation. That function accesses
		// the filesystem and is relatively slow. We assume that it returns a semantically
		// equivalent value for the same location name. This isn't strictly true, for example if
		// the time zone database is changed while the process is running. To handle that, we
		// expire entries after a day. Note that we cache negative results also.
		locationCache  cache.Cache
		warnIterations dynamicconfig.IntPropertyFn
		maxIterations  dynamicconfig.IntPropertyFn
	}

	locationAndError struct {
		loc *time.Location
		err error
	}
)

// ErrComputeLimitExceeded is returned by GetNextTime when the search for the next matching time
// hits the hard compute iteration bound before finding a non-excluded time.
var ErrComputeLimitExceeded = errors.New("schedule spec next-time search exceeded the compute iteration limit")
var ErrScheduleSpecLimitHit = serviceerror.NewInvalidArgument("the schedule calendar specification requires too much exclusion processing. Please modify the specification.")

// NewSpecBuilder takes the compute-limit getters directly (rather than a *dynamicconfig.Collection)
// so the dynamic-config plumbing stays in the wiring layer, per the common codebase pattern.
func NewSpecBuilder(warnIterations, maxIterations dynamicconfig.IntPropertyFn) *SpecBuilder {
	return &SpecBuilder{
		warnIterations: warnIterations,
		maxIterations:  maxIterations,
		locationCache: cache.New(1000,
			&cache.Options{
				TTL: 24 * time.Hour,
			},
		),
	}
}

func (b *SpecBuilder) NewCompiledSpec(spec *schedulepb.ScheduleSpec) (*CompiledSpec, error) {
	spec, err := canonicalizeSpec(spec)
	if err != nil {
		return nil, err
	}

	// load timezone
	tz, err := b.loadTimezone(spec)
	if err != nil {
		return nil, err
	}

	intervals := compileIntervals(spec.Interval)

	// Compile only semantically distinct calendar sources. Comments and range representation do
	// not affect matching, so equivalent calendars can share one search. A one-second interval
	// already contains every calendar occurrence and makes all calendar sources redundant.
	var ccs []*compiledCalendar
	if !containsEverySecond(intervals) {
		seen := make(map[compiledCalendarKey]struct{}, len(spec.StructuredCalendar))
		for _, structured := range spec.StructuredCalendar {
			calendar := newCompiledCalendar(structured, tz)
			key := calendar.key()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ccs = append(ccs, calendar)
		}
	}

	// compile excludes
	excludes := make([]*compiledExclusion, len(spec.ExcludeStructuredCalendar))
	for i, excal := range spec.ExcludeStructuredCalendar {
		excludes[i] = newCompiledExclusion(excal, tz)
	}

	cspec := &CompiledSpec{
		spec:           spec,
		tz:             tz,
		calendar:       ccs,
		intervals:      intervals,
		excludes:       excludes,
		warnIterations: b.warnIterations,
		maxIterations:  b.maxIterations,
	}

	return cspec, nil
}

// CleanSpec sets default values in ranges.
func CleanSpec(spec *schedulepb.ScheduleSpec) {
	cleanRanges := func(ranges []*schedulepb.Range) {
		for _, r := range ranges {
			if r.End < r.Start {
				r.End = r.Start
			}
			if r.Step == 0 {
				r.Step = 1
			}
		}
	}
	cleanCal := func(structured *schedulepb.StructuredCalendarSpec) {
		cleanRanges(structured.Second)
		cleanRanges(structured.Minute)
		cleanRanges(structured.Hour)
		cleanRanges(structured.DayOfMonth)
		cleanRanges(structured.Month)
		cleanRanges(structured.Year)
		cleanRanges(structured.DayOfWeek)
	}
	for _, structured := range spec.StructuredCalendar {
		cleanCal(structured)
	}
	for _, structured := range spec.ExcludeStructuredCalendar {
		cleanCal(structured)
	}
}

//revive:disable-next-line:cognitive-complexity
func canonicalizeSpec(spec *schedulepb.ScheduleSpec) (*schedulepb.ScheduleSpec, error) {
	// make copy so we can change some fields
	spec = common.CloneProto(spec)

	// parse CalendarSpecs to StructuredCalendarSpecs
	for _, cal := range spec.Calendar {
		structured, err := parseCalendarToStructured(cal)
		if err != nil {
			return nil, err
		}
		spec.StructuredCalendar = append(spec.StructuredCalendar, structured)
	}
	spec.Calendar = nil

	// parse ExcludeCalendars
	for _, cal := range spec.ExcludeCalendar {
		structured, err := parseCalendarToStructured(cal)
		if err != nil {
			return nil, err
		}
		spec.ExcludeStructuredCalendar = append(spec.ExcludeStructuredCalendar, structured)
	}
	spec.ExcludeCalendar = nil

	// parse CronStrings
	const unset = "__unset__"
	cronTZ := unset
	for _, cs := range spec.CronString {
		structured, interval, tz, err := parseCronString(cs)
		if err != nil {
			return nil, err
		}
		if cronTZ != unset && tz != cronTZ {
			// all cron strings must agree on timezone (whether present or not)
			return nil, errConflictingTimezoneNames
		}
		cronTZ = tz
		if structured != nil {
			spec.StructuredCalendar = append(spec.StructuredCalendar, structured)
		}
		if interval != nil {
			spec.Interval = append(spec.Interval, interval)
		}
	}
	spec.CronString = nil

	// if we have cron string(s), copy the timezone to spec, checking for conflict first.
	// if cron string timezone is empty string, don't copy, let the one in spec be used.
	if cronTZ != unset && cronTZ != "" {
		if spec.TimezoneName != "" && spec.TimezoneName != cronTZ || spec.TimezoneData != nil {
			return nil, errConflictingTimezoneNames
		} else if spec.TimezoneName == "" {
			spec.TimezoneName = cronTZ
		}
	}

	// validate structured calendar
	for _, structured := range spec.StructuredCalendar {
		if err := validateStructuredCalendar(structured); err != nil {
			return nil, err
		}
	}

	// validate intervals
	for _, interval := range spec.Interval {
		if err := validateInterval(interval); err != nil {
			return nil, err
		}
	}

	return spec, nil
}

func validateStructuredCalendar(scs *schedulepb.StructuredCalendarSpec) error {
	var errs []string

	checkRanges := func(ranges []*schedulepb.Range, field string, minVal, maxVal int32) {
		for _, r := range ranges {
			if r == nil { // shouldn't happen
				errs = append(errs, "range is nil")
				continue
			}
			if r.Start < minVal || r.Start > maxVal {
				errs = append(errs, fmt.Sprintf("%s Start is not in range [%d-%d]", field, minVal, maxVal))
			}
			if r.End != 0 && (r.End < r.Start || r.End > maxVal) {
				errs = append(errs, fmt.Sprintf("%s End is before Start or not in range [%d-%d]", field, minVal, maxVal))
			}
			if r.Step < 0 {
				errs = append(errs, fmt.Sprintf("%s has invalid Step", field))
			}
		}
	}

	checkRanges(scs.Second, "Second", 0, 59)
	checkRanges(scs.Minute, "Minute", 0, 59)
	checkRanges(scs.Hour, "Hour", 0, 23)
	checkRanges(scs.DayOfMonth, "DayOfMonth", 1, 31)
	checkRanges(scs.Month, "Month", 1, 12)
	checkRanges(scs.Year, "Year", minCalendarYear, maxCalendarYear)
	checkRanges(scs.DayOfWeek, "DayOfWeek", 0, 6)

	if len(scs.Comment) > maxCommentLen {
		errs = append(errs, "comment is too long")
	}

	if len(errs) > 0 {
		return errors.New("invalid calendar spec: " + strings.Join(errs, ", "))
	}
	return nil
}

func validateInterval(i *schedulepb.IntervalSpec) error {
	if i == nil {
		return errors.New("interval is nil")
	}
	// TODO: use timestamp.ValidateAndCapProtoDuration after switching to state machine based implementation.
	// 	Not adding it to workflow based implementation to avoid potential non-determinism errors.
	iv, phase := timestamp.DurationValue(i.Interval), timestamp.DurationValue(i.Phase)
	if iv < time.Second {
		return errors.New("interval is too small")
	} else if phase < 0 {
		return errors.New("phase is negative")
	} else if phase >= iv {
		return errors.New("phase cannot be greater than Interval")
	}
	return nil
}

func (b *SpecBuilder) loadTimezone(spec *schedulepb.ScheduleSpec) (*time.Location, error) {
	if spec.TimezoneData != nil {
		return time.LoadLocationFromTZData(spec.TimezoneName, spec.TimezoneData)
	}

	if cached, ok := b.locationCache.Get(spec.TimezoneName).(*locationAndError); ok {
		return cached.loc, cached.err
	}
	loc, err := time.LoadLocation(spec.TimezoneName)
	b.locationCache.Put(spec.TimezoneName, &locationAndError{
		loc: loc,
		err: err,
	})
	return loc, err
}

func (cs *CompiledSpec) CanonicalForm() *schedulepb.ScheduleSpec {
	return cs.spec
}

// Returns the earliest time that matches the schedule spec that is after the given time.
// Returns: Nominal is the time that matches, pre-jitter. Next is the nominal time with
// jitter applied. If there is no matching time, Nominal and Next will be the zero time.
func (cs *CompiledSpec) GetNextTime(jitterSeed string, after time.Time) (GetNextTimeResult, error) {
	return cs.getNextTime(jitterSeed, after, time.Time{})
}

// GetNextTimeWithUpperBound is equivalent to GetNextTime, but returns no match if the next
// jittered time would be after the inclusive upper bound.
func (cs *CompiledSpec) GetNextTimeWithUpperBound(
	jitterSeed string,
	after time.Time,
	upperBound time.Time,
) (GetNextTimeResult, error) {
	return cs.getNextTime(jitterSeed, after, upperBound)
}

func (cs *CompiledSpec) getNextTime(
	jitterSeed string,
	after time.Time,
	upperBound time.Time,
) (GetNextTimeResult, error) {
	// If we're starting before the schedule's allowed time range, jump up to right before
	// it (so that we can still return the first second of the range if it happens to match).
	// note: AsTime returns unix epoch on nil StartTime
	after = util.MaxTime(after, cs.spec.StartTime.AsTime().Add(-time.Second))

	pastEndTime := func(t time.Time) bool {
		return (!upperBound.IsZero() && t.After(upperBound)) ||
			(cs.spec.EndTime != nil && t.After(cs.spec.EndTime.AsTime())) ||
			t.Year() > maxCalendarYear
	}
	warnIterations := cs.warnIterations()
	if warnIterations <= 0 {
		warnIterations = DefaultWarnIterations
	}
	maxIterations := cs.maxIterations()
	if maxIterations <= 0 {
		maxIterations = math.MaxInt // disabled: effectively unlimited
	}

	var warned bool
	var nominal time.Time
	for iterations := 0; ; iterations++ {
		// Hard bound: stop an over-excluded / adversarial spec from spinning toward
		// maxCalendarYear. Disabled by default (maxIterations == math.MaxInt); an operator can
		// lower it to re-enable enforcement. Well-formed specs resolve in a handful of iterations.
		if iterations >= maxIterations {
			return GetNextTimeResult{ComputeLimitWarning: true}, ErrComputeLimitExceeded
		}
		if iterations >= warnIterations {
			warned = true
		}
		nominal = cs.rawNextTime(after)

		if nominal.IsZero() || pastEndTime(nominal) {
			return GetNextTimeResult{ComputeLimitWarning: warned}, nil
		}

		excludedUntil, excluded := cs.excludedUntil(nominal)
		if !excluded {
			break
		}
		if excludedUntil.IsZero() || pastEndTime(excludedUntil) {
			return GetNextTimeResult{ComputeLimitWarning: warned}, nil
		}
		// rawNextTime returns a time strictly after its argument. Search from one second before
		// the boundary so an inclusion exactly at the first non-excluded second is retained.
		after = excludedUntil.Add(-time.Second)
	}

	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	// Ensure that jitter doesn't push this time past the _next_ nominal start time
	if following := cs.rawNextTime(nominal); !following.IsZero() {
		maxJitter = min(maxJitter, following.Sub(nominal))
	}
	next := cs.addJitter(jitterSeed, nominal, maxJitter)
	if !upperBound.IsZero() && next.After(upperBound) {
		return GetNextTimeResult{ComputeLimitWarning: warned}, nil
	}

	return GetNextTimeResult{Nominal: nominal, Next: next, ComputeLimitWarning: warned}, nil
}

// Returns the next matching time (without jitter), or the zero value if no time matches.
func (cs *CompiledSpec) rawNextTime(after time.Time) (nominal time.Time) {
	var minTimestamp int64 = math.MaxInt64 // unix seconds-since-epoch as int64

	for _, cal := range cs.calendar {
		if next := cal.next(after); !next.IsZero() {
			nextTs := next.Unix()
			if nextTs < minTimestamp {
				minTimestamp = nextTs
			}
		}
	}

	ts := after.Unix()
	for _, iv := range cs.intervals {
		next := iv.next(ts)
		if next < minTimestamp {
			minTimestamp = next
		}
	}

	if minTimestamp == math.MaxInt64 {
		return time.Time{}
	}
	return time.Unix(minTimestamp, 0).UTC()
}

// Returns the next matching time for a single compiled interval source.
func (iv compiledInterval) next(ts int64) int64 {
	return (((ts-iv.phase)/iv.interval)+1)*iv.interval + iv.phase
}

// contains returns true when every occurrence in other is also an occurrence in iv.
func (iv compiledInterval) contains(other compiledInterval) bool {
	return other.interval%iv.interval == 0 && (other.phase-iv.phase)%iv.interval == 0
}

func compileIntervals(intervals []*schedulepb.IntervalSpec) []compiledInterval {
	compiled := make([]compiledInterval, 0, len(intervals))
	for _, intervalSpec := range intervals {
		candidate := compiledInterval{
			interval: max(int64(timestamp.DurationValue(intervalSpec.Interval)/time.Second), 1),
			phase:    max(int64(timestamp.DurationValue(intervalSpec.Phase)/time.Second), 0),
		}

		redundant := false
		for _, existing := range compiled {
			if existing.contains(candidate) {
				redundant = true
				break
			}
		}
		if redundant {
			continue
		}

		// If the new source is denser than an earlier source with a compatible phase, the earlier
		// source is redundant. Preserve order for all remaining sources to keep debugging stable.
		kept := compiled[:0]
		for _, existing := range compiled {
			if !candidate.contains(existing) {
				kept = append(kept, existing)
			}
		}
		compiled = append(kept, candidate)
	}
	return compiled
}

func containsEverySecond(intervals []compiledInterval) bool {
	for _, interval := range intervals {
		if interval.interval == 1 {
			return true
		}
	}
	return false
}

// Returns the end of the contiguous excluded range containing nominal. If any matching
// exclusion continues through the supported calendar range, the returned time is zero.
func (cs *CompiledSpec) excludedUntil(nominal time.Time) (time.Time, bool) {
	var until time.Time
	var excluded bool
	for _, exclusion := range cs.excludes {
		if exclusion.calendar.matches(nominal) {
			excluded = true
			next := exclusion.nextNonMatch(nominal)
			if next.IsZero() {
				return time.Time{}, true
			}
			until = util.MaxTime(until, next)
		}
	}
	return until, excluded
}

// Adds jitter to a nominal time, deterministically (by hashing the given time and a seed).
func (cs *CompiledSpec) addJitter(seed string, nominal time.Time, maxJitter time.Duration) time.Time {
	if maxJitter < 0 {
		maxJitter = 0
	}

	bin, err := nominal.MarshalBinary()
	if err != nil {
		return nominal
	}

	bin = append(bin, []byte(seed)...)

	// we want to fit the result of a multiply in 64 bits, and use 32 bits of hash, which
	// leaves 32 bits for the range. if we use nanoseconds or microseconds, our range is
	// limited to only a few seconds or hours. using milliseconds supports up to 49 days.
	fp := uint64(farm.Fingerprint32(bin))
	ms := min(uint64(maxJitter.Milliseconds()), math.MaxUint32)
	jitter := time.Duration((fp*ms)>>32) * time.Millisecond
	return nominal.Add(jitter)
}
