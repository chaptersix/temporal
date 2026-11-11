package occurrence

import (
	"time"

	"go.temporal.io/api/serviceerror"
)

// BeginRange starts inclusive historical enumeration. Range bounds belong to the
// caller's durable request and must remain unchanged across subsequent peeks.
// Subtracting exactly one nanosecond preserves fractional lower bounds and also
// supports the first instant of the prototype range via Begin's sentinel cursor.
func (e *Enumerator) BeginRange(start, end time.Time) (State, error) {
	if !supported(start) || !supported(end) || end.Before(start) {
		return State{}, serviceerror.NewInvalidArgument("invalid inclusive backfill range")
	}
	return e.Begin(start.Add(-time.Nanosecond))
}

// PeekRange examines at most probes raw candidates and retains at most one
// eligible candidate in State.Pending. It disposes excluded candidates only.
// The caller must publish DisposeRange and the candidate handoff atomically;
// capacity waits can persist/repeat the returned state without losing identity.
//
// Result.Status describes this fixed range. Exhausted may coexist with an Active
// State.Terminal and a pending out-of-range witness; global enumeration remains
// structurally valid. The caller removes the completed range, rather than polling
// or extending its end. Occurrences is always empty: no candidate was handed off.
func (e *Enumerator) PeekRange(state State, end time.Time, probes int) (Result, error) {
	if end.Before(state.Origin) {
		return Result{}, serviceerror.NewInvalidArgument("backfill end precedes origin")
	}
	result, err := e.Peek(state, end, probes)
	if result.Status == Waiting {
		result.Status = Exhausted
	}
	return result, err
}

// Peek retains a due eligible candidate until the caller atomically disposes it.
// A future actual time returns Waiting without advancing nominal disposition.
func (e *Enumerator) Peek(state State, end time.Time, probes int) (Result, error) {
	if state.Revision != e.revision || state.DefinitionID != e.identity {
		return Result{}, serviceerror.NewFailedPrecondition("continuation belongs to another definition revision")
	}
	if !supported(end) || probes < 1 || probes > 1024 {
		return Result{}, serviceerror.NewInvalidArgument("unsupported backfill range or probe limit")
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
		if pending := result.State.Pending; pending != nil {
			// Jitter is nonnegative and strictly below the following raw nominal.
			// Thus later raw matches (including excluded ones) cannot return into range.
			if pending.Actual.After(end) {
				result.Status = Waiting
				return result, nil
			}
			if pending.Excluded {
				result.disposePending()
				continue
			}
			result.Status = Active
			return result, nil
		}
		if result.Probes == probes {
			result.Status = Yield
			return result, nil
		}
		if err := e.probe(&result); err != nil {
			return Result{}, err
		}
	}
}

// DisposeRange records a successfully handed-off pending occurrence. It never
// mutates the supplied continuation and rejects missing or excluded candidates.
// Call only after PeekRange returned Active for the caller's fixed end; the
// continuation itself does not store or authenticate those range bounds.
func (e *Enumerator) DisposeRange(state State) (State, error) {
	if state.Revision != e.revision || state.DefinitionID != e.identity {
		return State{}, serviceerror.NewFailedPrecondition("continuation belongs to another definition revision")
	}
	if err := validateState(state); err != nil {
		return State{}, err
	}
	if state.Pending == nil || state.Pending.Excluded {
		return State{}, serviceerror.NewFailedPrecondition("backfill handoff requires an eligible pending candidate")
	}
	next := clone(state)
	next.DisposedThrough = next.Pending.Nominal
	next.Pending = nil
	return next, nil
}

// RebaseRange supersedes unadmitted work on a newer definition, searching after
// the last disposed NOMINAL rather than execution time. The caller retains its
// original range end and atomically records the superseded pending candidate.
func (e *Enumerator) RebaseRange(state State) (State, *Candidate, error) {
	if err := validateState(state); err != nil {
		return state, nil, err
	}
	if e.revision <= state.Revision {
		return state, nil, serviceerror.NewInvalidArgument("range rebase requires a newer revision")
	}
	next, err := e.Begin(state.DisposedThrough)
	if err != nil {
		return state, nil, err
	}
	old := clone(state)
	return next, old.Pending, nil
}
