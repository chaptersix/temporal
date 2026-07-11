package propertytest

import (
	"context"
	"errors"
	"fmt"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	defaultValidationIterations = 2_000_000
	validatorDefinitionVersion  = "v1"
)

type ValidationStatus int

const (
	ValidationValid ValidationStatus = iota
	ValidationInvalidStructural
	ValidationInvalidComponentUnsatisfiable
	ValidationInvalidEffectiveSetEmpty
	ValidationIndeterminateBudget
)

func (s ValidationStatus) String() string {
	switch s {
	case ValidationValid:
		return "valid"
	case ValidationInvalidStructural:
		return "invalid-structural"
	case ValidationInvalidComponentUnsatisfiable:
		return "invalid-unsatisfiable-component"
	case ValidationInvalidEffectiveSetEmpty:
		return "invalid-empty-effective-set"
	case ValidationIndeterminateBudget:
		return "indeterminate-validation-budget"
	default:
		return "unknown"
	}
}

type ValidationWorkBreakdown struct {
	Total               int
	Canonicalization    int
	ComponentChecks     int
	CivilDays           int
	CalendarTupleChecks int
	IntervalOccurrences int
	EffectiveDays       int
	ExclusionChecks     int
}

type ValidationResult struct {
	Status    ValidationStatus
	Work      ValidationWorkBreakdown
	Witness   time.Time
	Component string
	Reason    string
}

type ValidationOptions struct {
	MaxIterations int
	Context       context.Context
	faults        analysisFaults
}

type ValidationLimitError struct {
	Limit     int
	Consumed  int
	LastTime  time.Time
	Work      ValidationWorkBreakdown
	Component string
}

func (e *ValidationLimitError) Error() string {
	return fmt.Sprintf(
		"%v: limit=%d consumed=%d last_time=%s component=%q definition=%s",
		ErrValidationLimit,
		e.Limit,
		e.Consumed,
		e.LastTime.Format(time.RFC3339Nano),
		e.Component,
		validatorDefinitionVersion,
	)
}

func (e *ValidationLimitError) Unwrap() error {
	return ErrValidationLimit
}

type validationClassificationError struct {
	kind   error
	reason string
}

func newValidationClassificationError(kind error, reason string) error {
	return &validationClassificationError{kind: kind, reason: reason}
}

func (e *validationClassificationError) Error() string {
	return fmt.Sprintf("%v: %s", e.kind, e.reason)
}

func (e *validationClassificationError) Unwrap() []error {
	if errors.Is(e.kind, ErrInvalidSpec) {
		return []error{ErrInvalidSpec}
	}
	return []error{e.kind, ErrInvalidSpec}
}

type analysisFaults struct {
	queryStartInclusive              bool
	ignoreExclusions                 bool
	stopValidatingExclusions         bool
	acceptEmptyStructuredInclusion   bool
	treatFebruary30AsSatisfiable     bool
	ignoreDayOfWeek                  bool
	permitInvertedScheduleBounds     bool
	iterationLimitIsEmpty            bool
	allowDuplicateUnionResults       bool
	jitterCrossesNextNominal         bool
	validationIndeterminateIsInvalid bool
}

type validationWorkKind int

const (
	validationCanonicalization validationWorkKind = iota
	validationComponent
	validationCivilDay
	validationCalendarTuple
	validationIntervalOccurrence
	validationEffectiveDay
	validationExclusion
)

type validationBudget struct {
	limit     int
	lastTime  time.Time
	component string
	work      ValidationWorkBreakdown
	context   context.Context
}

func (b *validationBudget) at(component string, at time.Time) {
	b.component = component
	b.lastTime = at
}

func (b *validationBudget) tick(kind validationWorkKind) error {
	if b.context != nil {
		select {
		case <-b.context.Done():
			return b.context.Err()
		default:
		}
	}
	if b.work.Total >= b.limit {
		return &ValidationLimitError{
			Limit:     b.limit,
			Consumed:  b.work.Total,
			LastTime:  b.lastTime,
			Work:      b.work,
			Component: b.component,
		}
	}
	b.work.Total++
	switch kind {
	case validationCanonicalization:
		b.work.Canonicalization++
	case validationComponent:
		b.work.ComponentChecks++
	case validationCivilDay:
		b.work.CivilDays++
	case validationCalendarTuple:
		b.work.CalendarTupleChecks++
	case validationIntervalOccurrence:
		b.work.IntervalOccurrences++
	case validationEffectiveDay:
		b.work.EffectiveDays++
	case validationExclusion:
		b.work.ExclusionChecks++
	default:
		panic("unknown validation work kind")
	}
	return nil
}

func ValidateSchedule(spec *schedulepb.ScheduleSpec, options ValidationOptions) (ValidationResult, error) {
	if options.MaxIterations <= 0 {
		return ValidationResult{Status: ValidationInvalidStructural, Reason: "validation limit must be positive"},
			fmt.Errorf("%w: validation limit must be positive", ErrInvalidOptions)
	}
	budget := &validationBudget{limit: options.MaxIterations, context: options.Context}
	if spec == nil {
		return invalidValidationResult(budget, ValidationInvalidStructural, "schedule", "schedule spec is nil", ErrInvalidSpec)
	}
	budget.at("schedule", time.Time{})
	if err := budget.tick(validationCanonicalization); err != nil {
		return validationLimitResult(budget, err)
	}
	canonical, err := canonicalizeSpecWithFaults(spec, options.faults)
	if err != nil {
		return invalidValidationResult(budget, ValidationInvalidStructural, "schedule", err.Error(), ErrInvalidSpec)
	}
	if canonical.StartTime != nil && canonical.EndTime != nil &&
		canonical.StartTime.AsTime().After(canonical.EndTime.AsTime()) &&
		!options.faults.permitInvertedScheduleBounds {
		return invalidValidationResult(
			budget,
			ValidationInvalidStructural,
			"schedule bounds",
			"schedule start is after schedule end",
			ErrUnsatisfiableSpec,
		)
	}

	builder := NewSpecBuilder()
	location, err := builder.loadTimezone(canonical)
	if err != nil {
		return invalidValidationResult(budget, ValidationInvalidStructural, "timezone", err.Error(), ErrInvalidSpec)
	}
	horizonStart, horizonEnd := validationHorizon(canonical, location)
	if horizonEnd.Before(horizonStart) {
		return invalidValidationResult(
			budget,
			ValidationInvalidComponentUnsatisfiable,
			"schedule bounds",
			"schedule bounds do not intersect the supported horizon",
			ErrUnsatisfiableSpec,
		)
	}

	if len(canonical.StructuredCalendar)+len(canonical.Interval) == 0 {
		return invalidValidationResult(
			budget,
			ValidationInvalidComponentUnsatisfiable,
			"inclusion set",
			"schedule has no inclusion component",
			ErrUnsatisfiableSpec,
		)
	}

	CleanSpec(canonical)
	calendars := make([]*compiledCalendar, len(canonical.StructuredCalendar))
	inclusionWitnesses := make([]time.Time, 0, len(canonical.StructuredCalendar)+len(canonical.Interval))
	for i, calendar := range canonical.StructuredCalendar {
		component := fmt.Sprintf("inclusion calendar %d", i)
		budget.at(component, horizonStart)
		if err := budget.tick(validationComponent); err != nil {
			return validationLimitResult(budget, err)
		}
		if missingRequiredCalendarField(calendar) && !options.faults.acceptEmptyStructuredInclusion {
			return invalidValidationResult(
				budget,
				ValidationInvalidComponentUnsatisfiable,
				component,
				"structured inclusion lacks a required non-year field",
				ErrUnsatisfiableSpec,
			)
		}
		calendars[i] = newCompiledCalendar(calendar, location)
		witness, err := calendarComponentWitness(calendars[i], calendar, horizonStart, horizonEnd, budget, component, options.faults)
		if err != nil {
			return validationLimitResult(budget, err)
		}
		if witness.IsZero() {
			return invalidValidationResult(
				budget,
				ValidationInvalidComponentUnsatisfiable,
				component,
				"calendar has no civil timestamp in its effective horizon",
				ErrUnsatisfiableSpec,
			)
		}
		inclusionWitnesses = append(inclusionWitnesses, witness)
	}
	for i, interval := range canonical.Interval {
		component := fmt.Sprintf("inclusion interval %d", i)
		budget.at(component, horizonStart)
		if err := budget.tick(validationComponent); err != nil {
			return validationLimitResult(budget, err)
		}
		witness := intervalComponentWitness(interval, horizonStart, horizonEnd)
		if witness.IsZero() {
			return invalidValidationResult(
				budget,
				ValidationInvalidComponentUnsatisfiable,
				component,
				"interval has no timestamp in its effective horizon",
				ErrUnsatisfiableSpec,
			)
		}
		inclusionWitnesses = append(inclusionWitnesses, witness)
	}

	exclusions := make([]*compiledCalendar, len(canonical.ExcludeStructuredCalendar))
	exclusionHorizonStart, exclusionHorizonEnd := validationHorizon(&schedulepb.ScheduleSpec{}, location)
	for i, calendar := range canonical.ExcludeStructuredCalendar {
		component := fmt.Sprintf("exclusion calendar %d", i)
		exclusions[i] = newCompiledCalendar(calendar, location)
		if options.faults.stopValidatingExclusions {
			continue
		}
		budget.at(component, horizonStart)
		if err := budget.tick(validationComponent); err != nil {
			return validationLimitResult(budget, err)
		}
		if missingRequiredCalendarField(calendar) {
			return invalidValidationResult(
				budget,
				ValidationInvalidComponentUnsatisfiable,
				component,
				"structured exclusion lacks a required non-year field",
				ErrUnsatisfiableSpec,
			)
		}
		witness, err := calendarComponentWitness(
			exclusions[i],
			calendar,
			exclusionHorizonStart,
			exclusionHorizonEnd,
			budget,
			component,
			options.faults,
		)
		if err != nil {
			return validationLimitResult(budget, err)
		}
		if witness.IsZero() {
			return invalidValidationResult(
				budget,
				ValidationInvalidComponentUnsatisfiable,
				component,
				"exclusion calendar has no civil timestamp in its effective horizon",
				ErrUnsatisfiableSpec,
			)
		}
	}
	for _, witness := range inclusionWitnesses {
		if options.faults.ignoreExclusions || !matchesAnyCalendar(exclusions, witness) {
			return ValidationResult{
				Status:  ValidationValid,
				Work:    budget.work,
				Witness: witness,
				Reason:  "effective timestamp witness found",
			}, nil
		}
	}
	if !options.faults.ignoreExclusions {
		for _, exclusion := range exclusions {
			if calendarCoversHorizon(exclusion, horizonStart, horizonEnd) {
				return invalidValidationResult(
					budget,
					ValidationInvalidEffectiveSetEmpty,
					"effective set",
					"an exclusion calendar covers the complete supported horizon",
					ErrUnsatisfiableSpec,
				)
			}
		}
	}

	witness, err := effectiveSetWitness(
		canonical,
		calendars,
		exclusions,
		location,
		horizonStart,
		horizonEnd,
		budget,
		options.faults,
	)
	if err != nil {
		return validationLimitResult(budget, err)
	}
	if witness.IsZero() {
		return invalidValidationResult(
			budget,
			ValidationInvalidEffectiveSetEmpty,
			"effective set",
			"exclusions and schedule bounds remove every inclusion timestamp",
			ErrUnsatisfiableSpec,
		)
	}
	return ValidationResult{
		Status:  ValidationValid,
		Work:    budget.work,
		Witness: witness,
		Reason:  "effective timestamp witness found",
	}, nil
}

func validationLimitResult(budget *validationBudget, err error) (ValidationResult, error) {
	return ValidationResult{
		Status:    ValidationIndeterminateBudget,
		Work:      budget.work,
		Component: budget.component,
		Reason:    "validation proof budget exhausted before classification",
	}, err
}

func invalidValidationResult(
	budget *validationBudget,
	status ValidationStatus,
	component string,
	reason string,
	kind error,
) (ValidationResult, error) {
	return ValidationResult{
		Status:    status,
		Work:      budget.work,
		Component: component,
		Reason:    reason,
	}, newValidationClassificationError(kind, reason)
}

func validationHorizon(spec *schedulepb.ScheduleSpec, location *time.Location) (time.Time, time.Time) {
	start := time.Date(minCalendarYear, time.January, 1, 0, 0, 0, 0, location).UTC()
	end := time.Date(maxCalendarYear+1, time.January, 1, 0, 0, 0, 0, location).Add(-time.Second).UTC()
	if spec.StartTime != nil && spec.StartTime.AsTime().After(start) {
		start = spec.StartTime.AsTime()
	}
	if spec.EndTime != nil && spec.EndTime.AsTime().Before(end) {
		end = spec.EndTime.AsTime()
	}
	return start, end
}

func missingRequiredCalendarField(calendar *schedulepb.StructuredCalendarSpec) bool {
	return len(calendar.Second) == 0 ||
		len(calendar.Minute) == 0 ||
		len(calendar.Hour) == 0 ||
		len(calendar.DayOfMonth) == 0 ||
		len(calendar.Month) == 0 ||
		len(calendar.DayOfWeek) == 0
}

func calendarComponentWitness(
	calendar *compiledCalendar,
	raw *schedulepb.StructuredCalendarSpec,
	horizonStart time.Time,
	horizonEnd time.Time,
	budget *validationBudget,
	component string,
	faults analysisFaults,
) (time.Time, error) {
	if missingRequiredCalendarField(raw) && faults.acceptEmptyStructuredInclusion {
		return horizonStart, nil
	}
	hours, err := allowedFieldValues(calendar.hour, 24, budget, component)
	if err != nil {
		return time.Time{}, err
	}
	minutes, err := allowedFieldValues(calendar.minute, 60, budget, component)
	if err != nil {
		return time.Time{}, err
	}
	seconds, err := allowedFieldValues(calendar.second, 60, budget, component)
	if err != nil {
		return time.Time{}, err
	}
	if len(hours) == 0 || len(minutes) == 0 || len(seconds) == 0 {
		return time.Time{}, nil
	}
	if faults.treatFebruary30AsSatisfiable && calendarRequiresFebruary30(raw) {
		return horizonStart, nil
	}
	firstLocal := horizonStart.In(calendar.tz)
	lastLocal := horizonEnd.In(calendar.tz)
	year, month, date := firstLocal.Date()
	lastYear, lastMonth, lastDate := lastLocal.Date()
	for civilDateCompare(year, month, date, lastYear, lastMonth, lastDate) <= 0 {
		day := time.Date(year, month, date, 12, 0, 0, 0, calendar.tz)
		budget.at(component, day)
		if err := budget.tick(validationCivilDay); err != nil {
			return time.Time{}, err
		}
		if calendarDateMatches(calendar, day, faults.ignoreDayOfWeek) {
			for _, hour := range hours {
				for _, minute := range minutes {
					for _, second := range seconds {
						budget.at(component, day)
						if err := budget.tick(validationCalendarTuple); err != nil {
							return time.Time{}, err
						}
						candidate := time.Date(day.Year(), day.Month(), day.Day(), hour, minute, second, 0, calendar.tz)
						local := candidate.In(calendar.tz)
						if local.Year() != day.Year() || local.Month() != day.Month() || local.Day() != day.Day() ||
							local.Hour() != hour || local.Minute() != minute || local.Second() != second {
							continue
						}
						if occurrence := localTupleOccurrenceInRange(
							candidate,
							day.Year(),
							day.Month(),
							day.Day(),
							hour,
							minute,
							second,
							calendar.tz,
							horizonStart,
							horizonEnd,
						); !occurrence.IsZero() {
							return occurrence, nil
						}
					}
				}
			}
		}
		year, month, date = nextCivilDate(year, month, date)
	}
	return time.Time{}, nil
}

func localTupleOccurrenceInRange(
	candidate time.Time,
	year int,
	month time.Month,
	date int,
	hour int,
	minute int,
	second int,
	location *time.Location,
	start time.Time,
	end time.Time,
) time.Time {
	if !candidate.Before(start) && !candidate.After(end) {
		return candidate.UTC()
	}
	if candidate.Before(start.Add(-3*time.Hour)) || candidate.After(end.Add(3*time.Hour)) {
		return time.Time{}
	}
	_, candidateOffset := candidate.Zone()
	probes := []time.Duration{
		-48 * time.Hour, -24 * time.Hour, -6 * time.Hour, -3 * time.Hour,
		-2 * time.Hour, -time.Hour, time.Hour, 2 * time.Hour,
		3 * time.Hour, 6 * time.Hour, 24 * time.Hour, 48 * time.Hour,
	}
	seenOffsets := map[int]struct{}{candidateOffset: {}}
	for _, probe := range probes {
		_, alternativeOffset := candidate.Add(probe).Zone()
		if _, seen := seenOffsets[alternativeOffset]; seen {
			continue
		}
		seenOffsets[alternativeOffset] = struct{}{}
		alternative := candidate.Add(time.Duration(candidateOffset-alternativeOffset) * time.Second)
		local := alternative.In(location)
		if local.Year() == year && local.Month() == month && local.Day() == date &&
			local.Hour() == hour && local.Minute() == minute && local.Second() == second &&
			!alternative.Before(start) && !alternative.After(end) {
			return alternative.UTC()
		}
	}
	return time.Time{}
}

func allowedFieldValues(
	matcher func(int) bool,
	limit int,
	budget *validationBudget,
	component string,
) ([]int, error) {
	values := make([]int, 0, limit)
	for value := 0; value < limit; value++ {
		budget.at(component, budget.lastTime)
		if err := budget.tick(validationCalendarTuple); err != nil {
			return nil, err
		}
		if matcher(value) {
			values = append(values, value)
		}
	}
	return values, nil
}

func calendarRequiresFebruary30(calendar *schedulepb.StructuredCalendarSpec) bool {
	monthTwo := false
	dayThirty := false
	for _, r := range calendar.Month {
		monthTwo = monthTwo || rangeContains(r, 2)
	}
	for _, r := range calendar.DayOfMonth {
		dayThirty = dayThirty || rangeContains(r, 30)
	}
	return monthTwo && dayThirty
}

func rangeContains(r *schedulepb.Range, value int) bool {
	start := int(r.GetStart())
	end := int(r.GetEnd())
	if end < start {
		end = start
	}
	step := int(r.GetStep())
	if step == 0 {
		step = 1
	}
	return value >= start && value <= end && (value-start)%step == 0
}

func allowedLocalSeconds(
	calendar *compiledCalendar,
	budget *validationBudget,
	component string,
) ([]int, error) {
	allowed := make([]int, 0, 86_400)
	for secondOfDay := 0; secondOfDay < 86_400; secondOfDay++ {
		budget.at(component, budget.lastTime)
		if err := budget.tick(validationCalendarTuple); err != nil {
			return nil, err
		}
		hour := secondOfDay / 3600
		minute := secondOfDay % 3600 / 60
		second := secondOfDay % 60
		if calendar.hour(hour) && calendar.minute(minute) && calendar.second(second) {
			allowed = append(allowed, secondOfDay)
		}
	}
	return allowed, nil
}

func calendarDateMatches(calendar *compiledCalendar, day time.Time, ignoreDayOfWeek bool) bool {
	return calendar.year(day.Year()) &&
		calendar.month(int(day.Month())) &&
		calendar.dayOfMonth(day.Day()) &&
		(ignoreDayOfWeek || calendar.dayOfWeek(int(day.Weekday())))
}

func matchesAnyCalendar(calendars []*compiledCalendar, candidate time.Time) bool {
	for _, calendar := range calendars {
		if calendar.matches(candidate) {
			return true
		}
	}
	return false
}

func calendarCoversHorizon(calendar *compiledCalendar, start time.Time, end time.Time) bool {
	for value := 0; value < 60; value++ {
		if !calendar.second(value) || !calendar.minute(value) {
			return false
		}
	}
	for value := 0; value < 24; value++ {
		if !calendar.hour(value) {
			return false
		}
	}
	for value := 1; value <= 31; value++ {
		if !calendar.dayOfMonth(value) {
			return false
		}
	}
	for value := 1; value <= 12; value++ {
		if !calendar.month(value) {
			return false
		}
	}
	for value := 0; value <= 6; value++ {
		if !calendar.dayOfWeek(value) {
			return false
		}
	}
	firstYear := start.In(calendar.tz).Year()
	lastYear := end.In(calendar.tz).Year()
	for year := firstYear; year <= lastYear; year++ {
		if !calendar.year(year) {
			return false
		}
	}
	return true
}

func intervalComponentWitness(interval *schedulepb.IntervalSpec, start time.Time, end time.Time) time.Time {
	period := int64(timestamp.DurationValue(interval.Interval) / time.Second)
	phase := int64(timestamp.DurationValue(interval.Phase) / time.Second)
	first := firstIntervalAtOrAfter(start.Unix(), period, phase)
	if first > end.Unix() {
		return time.Time{}
	}
	return time.Unix(first, 0).UTC()
}

func firstIntervalAtOrAfter(at int64, period int64, phase int64) int64 {
	quotient := (at - phase) / period
	first := quotient*period + phase
	if first < at {
		first += period
	}
	return first
}

func effectiveSetWitness(
	spec *schedulepb.ScheduleSpec,
	calendars []*compiledCalendar,
	exclusions []*compiledCalendar,
	location *time.Location,
	horizonStart time.Time,
	horizonEnd time.Time,
	budget *validationBudget,
	faults analysisFaults,
) (time.Time, error) {
	calendarTemplates := make([][]uint64, len(calendars))
	for i, calendar := range calendars {
		allowed, err := allowedLocalSeconds(calendar, budget, fmt.Sprintf("effective inclusion calendar %d", i))
		if err != nil {
			return time.Time{}, err
		}
		calendarTemplates[i] = localSecondsBitset(allowed)
	}
	exclusionTemplates := make([][]uint64, len(exclusions))
	for i, exclusion := range exclusions {
		allowed, err := allowedLocalSeconds(exclusion, budget, fmt.Sprintf("effective exclusion calendar %d", i))
		if err != nil {
			return time.Time{}, err
		}
		exclusionTemplates[i] = localSecondsBitset(allowed)
	}

	firstLocal := horizonStart.In(location)
	lastLocal := horizonEnd.In(location)
	year, month, date := firstLocal.Date()
	lastYear, lastMonth, lastDate := lastLocal.Date()
	for civilDateCompare(year, month, date, lastYear, lastMonth, lastDate) <= 0 {
		day := time.Date(year, month, date, 12, 0, 0, 0, location)
		budget.at("effective set", day)
		if err := budget.tick(validationEffectiveDay); err != nil {
			return time.Time{}, err
		}
		dayStart, dayEnd, ok := localDaySpan(year, month, date, location)
		if !ok {
			year, month, date = nextCivilDate(year, month, date)
			continue
		}
		secondsInDay := int(dayEnd.Sub(dayStart) / time.Second)
		if !faults.ignoreExclusions && dayCoveredByExclusion(exclusions, exclusionTemplates, day, secondsInDay, faults) {
			year, month, date = nextCivilDate(year, month, date)
			continue
		}

		included := make([]uint64, bitsetWords(secondsInDay))
		excluded := make([]uint64, bitsetWords(secondsInDay))
		for i, calendar := range calendars {
			if !calendarDateMatches(calendar, day, faults.ignoreDayOfWeek) {
				continue
			}
			if secondsInDay == 86_400 {
				bitsetOr(included, calendarTemplates[i])
			} else {
				populateSpecialCalendarDay(included, calendar, dayStart, secondsInDay)
			}
		}
		lower := max(dayStart.Unix(), horizonStart.Unix())
		upper := min(dayEnd.Add(-time.Second).Unix(), horizonEnd.Unix())
		for i, interval := range spec.Interval {
			period := int64(timestamp.DurationValue(interval.Interval) / time.Second)
			phase := int64(timestamp.DurationValue(interval.Phase) / time.Second)
			for occurrence := firstIntervalAtOrAfter(lower, period, phase); occurrence <= upper; occurrence += period {
				budget.at(fmt.Sprintf("effective inclusion interval %d", i), time.Unix(occurrence, 0))
				if err := budget.tick(validationIntervalOccurrence); err != nil {
					return time.Time{}, err
				}
				bitsetSet(included, int(occurrence-dayStart.Unix()))
			}
		}
		bitsetClearBefore(included, int(lower-dayStart.Unix()))
		bitsetClearAfter(included, int(upper-dayStart.Unix()))

		if !faults.ignoreExclusions {
			for i, exclusion := range exclusions {
				budget.at(fmt.Sprintf("effective exclusion calendar %d", i), day)
				if err := budget.tick(validationExclusion); err != nil {
					return time.Time{}, err
				}
				if !calendarDateMatches(exclusion, day, faults.ignoreDayOfWeek) {
					continue
				}
				if secondsInDay == 86_400 {
					bitsetOr(excluded, exclusionTemplates[i])
				} else {
					populateSpecialCalendarDay(excluded, exclusion, dayStart, secondsInDay)
				}
			}
			bitsetAndNot(included, excluded)
		}
		if offset, ok := bitsetFirst(included, secondsInDay); ok {
			return dayStart.Add(time.Duration(offset) * time.Second), nil
		}
		year, month, date = nextCivilDate(year, month, date)
	}
	return time.Time{}, nil
}

func dayCoveredByExclusion(
	exclusions []*compiledCalendar,
	templates [][]uint64,
	day time.Time,
	secondsInDay int,
	faults analysisFaults,
) bool {
	for i, exclusion := range exclusions {
		if calendarDateMatches(exclusion, day, faults.ignoreDayOfWeek) && bitsetAll(templates[i], 86_400) {
			return true
		}
	}
	return false
}

func populateSpecialCalendarDay(bits []uint64, calendar *compiledCalendar, dayStart time.Time, secondsInDay int) {
	for offset := 0; offset < secondsInDay; offset++ {
		if calendar.matches(dayStart.Add(time.Duration(offset) * time.Second)) {
			bitsetSet(bits, offset)
		}
	}
}

func nextCivilDate(year int, month time.Month, date int) (int, time.Month, int) {
	next := time.Date(year, month, date+1, 12, 0, 0, 0, time.UTC)
	return next.Date()
}

func civilDateCompare(
	year int,
	month time.Month,
	date int,
	otherYear int,
	otherMonth time.Month,
	otherDate int,
) int {
	if year != otherYear {
		return year - otherYear
	}
	if month != otherMonth {
		return int(month - otherMonth)
	}
	return date - otherDate
}

func localDaySpan(year int, month time.Month, date int, location *time.Location) (time.Time, time.Time, bool) {
	start, ok := firstTimeOnCivilDate(year, month, date, location)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	nextYear, nextMonth, nextDate := nextCivilDate(year, month, date)
	for {
		end, found := firstTimeOnCivilDate(nextYear, nextMonth, nextDate, location)
		if found {
			return start.UTC(), end.UTC(), true
		}
		nextYear, nextMonth, nextDate = nextCivilDate(nextYear, nextMonth, nextDate)
	}
}

func firstTimeOnCivilDate(year int, month time.Month, date int, location *time.Location) (time.Time, bool) {
	for hour := 0; hour < 24; hour++ {
		candidate := time.Date(year, month, date, hour, 0, 0, 0, location)
		local := candidate.In(location)
		if local.Year() == year && local.Month() == month && local.Day() == date && local.Hour() == hour {
			return candidate, true
		}
	}
	return time.Time{}, false
}

func bitsetWords(size int) int {
	return (size + 63) / 64
}

func localSecondsBitset(seconds []int) []uint64 {
	bits := make([]uint64, bitsetWords(86_400))
	for _, second := range seconds {
		bitsetSet(bits, second)
	}
	return bits
}

func bitsetSet(bits []uint64, index int) {
	bits[index/64] |= uint64(1) << (index % 64)
}

func bitsetOr(destination []uint64, source []uint64) {
	for i := range destination {
		destination[i] |= source[i]
	}
}

func bitsetAndNot(destination []uint64, removed []uint64) {
	for i := range destination {
		destination[i] &^= removed[i]
	}
}

func bitsetClearBefore(bits []uint64, first int) {
	for i := 0; i < first; i++ {
		bits[i/64] &^= uint64(1) << (i % 64)
	}
}

func bitsetClearAfter(bits []uint64, last int) {
	for i := last + 1; i < len(bits)*64; i++ {
		bits[i/64] &^= uint64(1) << (i % 64)
	}
}

func bitsetFirst(bits []uint64, size int) (int, bool) {
	for i := 0; i < size; i++ {
		if bits[i/64]&(uint64(1)<<(i%64)) != 0 {
			return i, true
		}
	}
	return 0, false
}

func bitsetAll(bits []uint64, size int) bool {
	for i := 0; i < size; i++ {
		if bits[i/64]&(uint64(1)<<(i%64)) == 0 {
			return false
		}
	}
	return true
}

func validationWorkConsistent(work ValidationWorkBreakdown) bool {
	return work.Total == work.Canonicalization+
		work.ComponentChecks+
		work.CivilDays+
		work.CalendarTupleChecks+
		work.IntervalOccurrences+
		work.EffectiveDays+
		work.ExclusionChecks
}
