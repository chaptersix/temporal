package scheduler

import (
	"errors"
	"math"

	schedulepb "go.temporal.io/api/schedule/v1"
)

// CanonicalizeScheduleSpec validates the unrestricted bounded-builder input and
// returns a detached canonical definition without resolving a timezone or
// allocating matchers. Timezone names and custom bytes remain in the result.
func CanonicalizeScheduleSpec(spec *schedulepb.ScheduleSpec) (*schedulepb.ScheduleSpec, error) {
	if spec == nil {
		return nil, errors.New("invalid bounded specification limits")
	}
	if err := checkSpecSize(spec, math.MaxInt, math.MaxInt); err != nil {
		return nil, err
	}
	//nolint:staticcheck // Legacy exclusion syntax remains supported.
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
	if err = checkSpecSize(canonical, math.MaxInt, math.MaxInt); err != nil {
		return nil, err
	}
	for _, cal := range canonical.ExcludeStructuredCalendar {
		if err = validateStructuredCalendar(cal); err != nil {
			return nil, err
		}
	}
	return canonical, nil
}
