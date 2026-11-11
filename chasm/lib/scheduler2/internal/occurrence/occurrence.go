// Package occurrence enumerates immutable schedule revisions in bounded batches.
package occurrence

import (
	"crypto/sha256"
	"encoding/hex"
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
	Origin          time.Time
	DefinitionID    string
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
	identity string
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
	compiled, err := builder.NewBoundedCompiledSpec(spec, 64, 1024)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	canonical := compiled.CanonicalForm()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	identity := sha256.Sum256(append(digest[:], []byte(seed)...))
	e := &Enumerator{identity: hex.EncodeToString(identity[:]), compiled: compiled, seed: seed, revision: revision,
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

// Begin binds a fresh continuation to this definition revision.
func (e *Enumerator) Begin(after time.Time) (State, error) {
	if !validCursor(after) {
		return State{}, serviceerror.NewInvalidArgument("cursor is outside the prototype range")
	}
	return State{Origin: after, DefinitionID: e.identity, Revision: e.revision, SearchAfter: after, DisposedThrough: after, FullRange: after.Before(e.start)}, nil
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
	if err := validateState(state); err != nil {
		return state, nil, err
	}
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
	if state.Revision != e.revision || state.DefinitionID != e.identity {
		return Result{}, serviceerror.NewFailedPrecondition("continuation belongs to another definition revision")
	}
	if !supported(through) || limits.Probes < 1 || limits.Probes > 1024 || limits.Outputs < 1 || limits.Outputs > 128 {
		return Result{}, serviceerror.NewInvalidArgument("unsupported window or enumeration limits")
	}
	if err := validateState(state); err != nil {
		return Result{}, err
	}
	if state.FullRange != state.Origin.Before(e.start) || (state.Terminal == Empty && !e.empty) {
		return Result{}, serviceerror.NewFailedPrecondition("invalid enumeration continuation")
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
			result.disposePending()
			continue
		}
		if result.Probes == limits.Probes {
			result.Status = Yield
			return result, nil
		}
		if err := e.probe(&result); err != nil {
			return Result{}, err
		}

	}
}

func (r *Result) disposePending() {
	pending := r.State.Pending
	if !pending.Excluded {
		r.Occurrences = append(r.Occurrences, *pending)
	}
	r.State.DisposedThrough = pending.Nominal
	r.State.Pending = nil
}

func (e *Enumerator) probe(result *Result) error {
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
		return nil
	}
	if !candidate.Nominal.After(after) {
		return serviceerror.NewInternal("calendar candidate did not advance")
	}
	result.State.SearchAfter = candidate.Nominal
	if excluded {
		result.State.Pending = &Candidate{Nominal: candidate.Nominal, Actual: candidate.Nominal, Revision: e.revision, Excluded: true}
		return nil
	}
	if !supported(candidate.Next) {
		return serviceerror.NewInvalidArgument("jittered time is outside the prototype range")
	}
	result.State.EligibleSeen = true
	result.State.Pending = &Candidate{Nominal: candidate.Nominal, Actual: candidate.Next, Revision: e.revision}
	return nil
}

func validCursor(t time.Time) bool {
	return supported(t) || t.Equal(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond))
}

func validateState(state State) error {
	invalid := func() error { return serviceerror.NewFailedPrecondition("invalid enumeration continuation") }
	if state.Revision <= 0 || state.DefinitionID == "" || !validCursor(state.Origin) || !validCursor(state.SearchAfter) || !validCursor(state.DisposedThrough) || state.Origin.After(state.DisposedThrough) || state.DisposedThrough.After(state.SearchAfter) {
		return invalid()
	}
	switch state.Terminal {
	case Active, Empty, Impossible, Exhausted:
	default:
		return invalid()
	}
	if state.Terminal == Impossible && (!state.FullRange || state.EligibleSeen) {
		return invalid()
	}
	if pending := state.Pending; pending != nil {
		if !validPending(state, pending) {
			return invalid()
		}
	} else if !state.SearchAfter.Equal(state.DisposedThrough) {
		return invalid()
	}
	return nil
}

func validPending(state State, pending *Candidate) bool {
	return state.Terminal == Active && pending.Revision == state.Revision && supported(pending.Nominal) && supported(pending.Actual) && pending.Nominal.Nanosecond() == 0 && !pending.Actual.Before(pending.Nominal) && pending.Nominal.Equal(state.SearchAfter) && state.DisposedThrough.Before(pending.Nominal) && (!pending.Excluded || pending.Actual.Equal(pending.Nominal)) && (pending.Excluded || state.EligibleSeen)
}
