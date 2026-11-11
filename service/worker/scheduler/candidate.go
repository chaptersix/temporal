package scheduler

import (
	"errors"
	schedulepb "go.temporal.io/api/schedule/v1"
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
)

// NextTimeCandidate examines one raw matching instant, including excluded ones.
// It performs at most two raw calendar lookups, without looping over exclusions.
// Callers enforce spec time bounds and retain the nominal continuation on yield.
// A zero nominal means there are no more raw matches. The boolean reports exclusion.
func (cs *CompiledSpec) NextTimeCandidate(seed string, after time.Time) (GetNextTimeResult, bool) {
	nominal := cs.rawNextTime(after)
	if nominal.IsZero() {
		return GetNextTimeResult{}, false
	}
	if cs.excluded(nominal) {
		return GetNextTimeResult{Nominal: nominal}, true
	}
	maxJitter := timestamp.DurationValue(cs.spec.Jitter)
	if following := cs.rawNextTime(nominal); !following.IsZero() {
		maxJitter = min(maxJitter, following.Sub(nominal))
	}
	return GetNextTimeResult{Nominal: nominal, Next: cs.addJitter(seed, nominal, maxJitter)}, false
}

// NewBoundedCompiledSpec validates all normalized ranges before constructing
// matchers. It leaves the legacy builder's behavior unchanged.
func (b *SpecBuilder) NewBoundedCompiledSpec(spec *schedulepb.ScheduleSpec, maxTerms, maxRanges int) (*CompiledSpec, error) {
	if spec == nil || maxTerms < 1 || maxRanges < 1 {
		return nil, errors.New("invalid bounded specification limits")
	}
	if err := checkSpecSize(spec, maxTerms, maxRanges); err != nil {
		return nil, err
	}
	for _, calendars := range [][]*schedulepb.CalendarSpec{spec.Calendar, spec.ExcludeCalendar} {
		for _, cal := range calendars {
			if cal == nil {
				return nil, errors.New("calendar is nil")
			}
		}
	}
	for _, interval := range spec.Interval {
		if interval == nil || interval.Interval == nil || interval.Interval.CheckValid() != nil {
			return nil, errors.New("interval duration is invalid")
		}
		if interval.Phase != nil && interval.Phase.CheckValid() != nil {
			return nil, errors.New("interval phase is invalid")
		}
	}
	canonical, err := canonicalizeSpec(spec)
	if err != nil {
		return nil, err
	}
	if err = checkSpecSize(canonical, maxTerms, maxRanges); err != nil {
		return nil, err
	}
	for _, cal := range canonical.ExcludeStructuredCalendar {
		if err = validateStructuredCalendar(cal); err != nil {
			return nil, err
		}
	}
	return b.compileCanonicalSpec(canonical)
}

func checkSpecSize(spec *schedulepb.ScheduleSpec, maxTerms, maxRanges int) error {
	if len(spec.Interval)+len(spec.Calendar)+len(spec.ExcludeCalendar)+len(spec.CronString)+len(spec.StructuredCalendar)+len(spec.ExcludeStructuredCalendar) > maxTerms {
		return errors.New("specification exceeds term limit")
	}
	ranges := 0
	for _, calendars := range [][]*schedulepb.StructuredCalendarSpec{spec.StructuredCalendar, spec.ExcludeStructuredCalendar} {
		for _, cal := range calendars {
			if cal == nil {
				return errors.New("structured calendar is nil")
			}
			ranges += len(cal.Second) + len(cal.Minute) + len(cal.Hour) + len(cal.DayOfMonth) + len(cal.Month) + len(cal.Year) + len(cal.DayOfWeek)
		}
	}
	if ranges > maxRanges {
		return errors.New("specification exceeds range limit")
	}
	return nil
}
