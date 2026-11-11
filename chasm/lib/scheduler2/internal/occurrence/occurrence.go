// Package occurrence enumerates immutable schedule revisions in bounded batches.
package occurrence

import (
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

type Status int

const (
	Active Status = iota
	Yield
	Limited
	Waiting
	Empty
	Impossible
	Exhausted
)

// Candidate keeps the nominal identity distinct from the jittered dispatch time.
type Candidate struct {
	Nominal  time.Time
	Actual   time.Time
	Revision int64
	Excluded bool
}

// State is a serializable continuation owned by its caller.
type State struct {
	Revision        int64
	SearchAfter     time.Time
	DisposedThrough time.Time
	Pending         *Candidate
	FullRange       bool
	EligibleSeen    bool
	Terminal        Status
}

type Limits struct {
	Probes  int
	Outputs int
}

type Result struct {
	State       State
	Occurrences []Candidate
	Status      Status
	Probes      int
}

// Enumerator holds a compiled immutable definition and stable jitter seed.
type Enumerator struct {
	compiled *legacyscheduler.CompiledSpec
	seed     string
	revision int64
	start    time.Time
	end      time.Time
	empty    bool
}

func supported(t time.Time) bool {
	return !t.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) && t.Before(time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC))
}

// New compiles a bounded definition without modifying the caller's protobuf.
func New(spec *schedulepb.ScheduleSpec, seed string, revision int64) (*Enumerator, error) {
	if spec == nil || revision <= 0 {
		return nil, serviceerror.NewInvalidArgument("definition and positive revision are required")
	}
	if proto.Size(spec) > 64*1024 {
		return nil, serviceerror.NewInvalidArgument("prototype definition exceeds 64 KiB")
	}
	if err := validateBounds(spec); err != nil {
		return nil, err
	}
	builder := legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	compiled, err := builder.NewCompiledSpec(spec)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	canonical := compiled.CanonicalForm()
	if err := validateSize(canonical); err != nil {
		return nil, err
	}
	e := &Enumerator{compiled: compiled, seed: seed, revision: revision,
		start: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), end: time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond),
		empty: len(canonical.Interval)+len(canonical.StructuredCalendar) == 0,
	}
	if canonical.StartTime != nil {
		e.start = canonical.StartTime.AsTime()
	}
	if canonical.EndTime != nil {
		e.end = canonical.EndTime.AsTime()
	}
	return e, nil
}

func validateBounds(spec *schedulepb.ScheduleSpec) error {
	if spec.StartTime != nil && (spec.StartTime.CheckValid() != nil || !supported(spec.StartTime.AsTime())) {
		return serviceerror.NewInvalidArgument("start time is outside the prototype range [2000, 2101)")
	}
	if spec.EndTime != nil && (spec.EndTime.CheckValid() != nil || !supported(spec.EndTime.AsTime())) {
		return serviceerror.NewInvalidArgument("end time is outside the prototype range [2000, 2101)")
	}
	if spec.Jitter != nil && (spec.Jitter.CheckValid() != nil || spec.Jitter.AsDuration() < 0) {
		return serviceerror.NewInvalidArgument("jitter must be a nonnegative valid duration")
	}
	return nil
}

func validateSize(spec *schedulepb.ScheduleSpec) error {
	if len(spec.Interval)+len(spec.StructuredCalendar)+len(spec.ExcludeStructuredCalendar) > 64 {
		return serviceerror.NewInvalidArgument("prototype definition exceeds 64 terms")
	}
	ranges := 0
	for _, calendars := range [][]*schedulepb.StructuredCalendarSpec{spec.StructuredCalendar, spec.ExcludeStructuredCalendar} {
		for _, cal := range calendars {
			ranges += len(cal.Second) + len(cal.Minute) + len(cal.Hour) + len(cal.DayOfMonth) + len(cal.Month) + len(cal.Year) + len(cal.DayOfWeek)
		}
	}
	if ranges > 1024 {
		return serviceerror.NewInvalidArgument("prototype definition exceeds 1024 ranges")
	}
	return nil
}

// Begin binds a fresh continuation to this definition revision.
func (e *Enumerator) Begin(after time.Time) (State, error) {
	if !supported(after) && !after.Equal(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)) {
		return State{}, serviceerror.NewInvalidArgument("cursor is outside the prototype range")
	}
	return State{Revision: e.revision, SearchAfter: after, DisposedThrough: after, FullRange: after.Before(e.start)}, nil
}

func clone(state State) State {
	if state.Pending != nil {
		candidate := *state.Pending
		state.Pending = &candidate
	}
	return state
}

// Update explicitly supersedes unadmitted work and resets search at its boundary.
// The returned candidate records the superseded disposition, if any.
func (e *Enumerator) Update(state State, at time.Time) (State, *Candidate, error) {
	if e.revision <= state.Revision || !supported(at) {
		return state, nil, serviceerror.NewInvalidArgument("update requires a newer revision and supported time")
	}
	boundary := at
	if state.DisposedThrough.After(boundary) {
		boundary = state.DisposedThrough
	}
	next, err := e.Begin(boundary)
	if err != nil {
		return state, nil, err
	}
	old := clone(state)
	return next, old.Pending, nil
}

// Run examines at most limits.Probes raw candidates and emits at most limits.Outputs.
// Publishing the returned continuation and outputs together is the caller's obligation.
func (e *Enumerator) Run(state State, through time.Time, limits Limits) (Result, error) {
	if state.Revision != e.revision {
		return Result{}, serviceerror.NewFailedPrecondition("continuation belongs to another definition revision")
	}
	if !supported(through) || limits.Probes < 1 || limits.Probes > 1024 || limits.Outputs < 1 || limits.Outputs > 128 {
		return Result{}, serviceerror.NewInvalidArgument("unsupported window or enumeration limits")
	}
	result := Result{State: clone(state)}
	for {
		if result.State.Terminal != Active {
			result.Status = result.State.Terminal
			return result, nil
		}
		if e.empty {
			result.State.Terminal = Empty
			continue
		}
		if len(result.Occurrences) == limits.Outputs {
			result.Status = Limited
			return result, nil
		}
		if pending := result.State.Pending; pending != nil {
			if pending.Actual.After(through) {
				result.Status = Waiting
				return result, nil
			}
			if !pending.Excluded {
				result.Occurrences = append(result.Occurrences, *pending)
			}
			result.State.DisposedThrough = pending.Nominal
			result.State.Pending = nil
			continue
		}
		if result.Probes == limits.Probes {
			result.Status = Yield
			return result, nil
		}
		after := result.State.SearchAfter
		// Integer-second candidates must be >= a possibly fractional StartTime.
		if lower := e.start.Add(-time.Nanosecond); after.Before(lower) {
			after = lower
		}
		candidate, excluded := e.compiled.NextTimeCandidate(e.seed, after)
		result.Probes++
		if candidate.Nominal.IsZero() || candidate.Nominal.After(e.end) {
			result.State.Terminal = Exhausted
			if result.State.FullRange && !result.State.EligibleSeen {
				result.State.Terminal = Impossible
			}
			continue
		}
		if !candidate.Nominal.After(after) {
			return Result{}, serviceerror.NewInternal("calendar candidate did not advance")
		}
		result.State.SearchAfter = candidate.Nominal
		if excluded {
			result.State.Pending = &Candidate{Nominal: candidate.Nominal, Actual: candidate.Nominal, Revision: e.revision, Excluded: true}
			continue
		}
		if !supported(candidate.Next) {
			return Result{}, serviceerror.NewInvalidArgument("jittered time is outside the prototype range")
		}
		result.State.EligibleSeen = true
		result.State.Pending = &Candidate{Nominal: candidate.Nominal, Actual: candidate.Next, Revision: e.revision}
	}
}
