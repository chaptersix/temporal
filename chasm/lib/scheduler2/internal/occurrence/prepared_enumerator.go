package occurrence

import (
	"encoding/hex"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
)

// PreparedEnumerator preserves the public occurrence ownership protocol while
// advancing at most one raw probe in a bounded calendar quantum. Its readers and
// location belong to the caller's current transaction and are never persisted.
type PreparedEnumerator struct {
	control Enumerator
	header  PreparedSpecHeader
	zone    *PreparedLocation
	reader  PreparedSpecReader
}

// PreparedResult separates unfinished lookup work from the occurrence cursor.
// Only a completed raw probe may change SearchAfter or publish a pending candidate.
type PreparedResult struct {
	Result
	Work  PreparedSpecState
	Stats PreparedSpecStats
}

func NewPreparedEnumerator(identity [32]byte, revision int64, header PreparedSpecHeader, zone *PreparedLocation, reader PreparedSpecReader) (*PreparedEnumerator, error) {
	if identity == [32]byte{} || revision <= 0 || !header.ReturnExcluded {
		return nil, serviceerror.NewInvalidArgument("invalid prepared enumeration definition")
	}
	if err := validatePreparedSpec(PreparedSpecState{}, header, zone, time.Unix(0, 0).UTC(), reader, 1); err != nil {
		return nil, err
	}
	cursor, err := NewPreparedCursor(identity, revision, header)
	if err != nil {
		return nil, err
	}
	return &PreparedEnumerator{control: cursor.control, header: header, zone: zone, reader: reader}, nil
}

func (e *PreparedEnumerator) Begin(after time.Time) (State, error) { return e.control.Begin(after) }
func (e *PreparedEnumerator) BeginRange(start, end time.Time) (State, error) {
	return e.control.BeginRange(start, end)
}
func (e *PreparedEnumerator) Update(state State, at time.Time) (State, *Candidate, error) {
	return e.control.Update(state, at)
}
func (e *PreparedEnumerator) RebaseRange(state State) (State, *Candidate, error) {
	return e.control.RebaseRange(state)
}
func (e *PreparedEnumerator) DisposeRange(state State) (State, error) {
	return e.control.DisposeRange(state)
}

// PeekRange uses the caller's unchanged inclusive range. A future pending witness
// ends that range without disposing it or changing global enumeration authority.
func (e *PreparedEnumerator) PeekRange(state State, work PreparedSpecState, end time.Time, budget int) (PreparedResult, error) {
	if end.Before(state.Origin) {
		return PreparedResult{}, serviceerror.NewInvalidArgument("backfill end precedes origin")
	}
	result, err := e.Peek(state, work, end, budget)
	if result.Status == Waiting {
		result.Status = Exhausted
	}
	return result, err
}

func (e *PreparedEnumerator) Peek(state State, work PreparedSpecState, end time.Time, budget int) (PreparedResult, error) {
	if err := e.validate(state, work, end, budget); err != nil {
		return PreparedResult{}, err
	}
	result := PreparedResult{Result: Result{State: clone(state)}, Work: work}
	if state.Terminal != Active {
		result.Status = state.Terminal
		return result, nil
	}
	if e.control.empty {
		result.State.Terminal = Empty
		result.Status = Empty
		return result, nil
	}
	if state.Pending != nil {
		e.finishPending(&result, end)
		return result, nil
	}
	next, stats, err := AdvancePreparedSpec(work, e.header, e.zone, state.SearchAfter, e.reader, budget)
	if errors.Is(err, ErrPreparedZoneChanged) {
		// Only unfinished lookup work is reset. The completed occurrence cursor and
		// its original boundary remain authoritative across source refreshes.
		next, stats, err = AdvancePreparedSpec(PreparedSpecState{}, e.header, e.zone, state.SearchAfter, e.reader, budget)
	}
	if err != nil {
		return PreparedResult{}, err
	}
	result.Work, result.Stats = next, stats
	candidate := next.Result()
	if !candidate.Done {
		result.Status = Yield
		return result, nil
	}
	result.Work = PreparedSpecState{}
	result.Probes = 1
	if !candidate.Found || candidate.Nominal.After(e.control.end) {
		result.State.Terminal = Exhausted
		if state.FullRange && !state.EligibleSeen {
			result.State.Terminal = Impossible
		}
		result.Status = result.State.Terminal
		return result, nil
	}
	after := state.SearchAfter
	if lower := e.control.start.Add(-time.Nanosecond); after.Before(lower) {
		after = lower
	}
	if !candidate.Nominal.After(after) {
		return PreparedResult{}, serviceerror.NewInternal("calendar candidate did not advance")
	}
	if !e.control.supported(candidate.Actual) {
		return PreparedResult{}, serviceerror.NewInvalidArgument("jittered time is outside the prototype range")
	}
	result.State.SearchAfter = candidate.Nominal
	result.State.Pending = &Candidate{Nominal: candidate.Nominal, Actual: candidate.Actual, Revision: state.Revision, Excluded: candidate.Excluded}
	if !candidate.Excluded {
		result.State.EligibleSeen = true
	}
	e.finishPending(&result, end)
	return result, nil
}

func (e *PreparedEnumerator) validate(state State, work PreparedSpecState, end time.Time, budget int) error {
	if state.Revision != e.control.revision || state.DefinitionID != e.control.identity {
		return serviceerror.NewFailedPrecondition("continuation belongs to another definition revision")
	}
	if !e.control.supported(end) || budget < 1 || budget > MaxPreparationSteps {
		return serviceerror.NewInvalidArgument("unsupported window or preparation budget")
	}
	if err := e.control.validateState(state); err != nil {
		return err
	}
	if state.FullRange != state.Origin.Before(e.control.start) || (state.Terminal == Empty && !e.control.empty) {
		return serviceerror.NewFailedPrecondition("invalid enumeration continuation")
	}
	if work != (PreparedSpecState{}) && (state.Pending != nil || state.Terminal != Active || work.SourceID != e.header.SourceID || !work.Boundary.Equal(state.SearchAfter)) {
		return serviceerror.NewFailedPrecondition("lookup work does not belong to the occurrence cursor")
	}
	return nil
}

func (e *PreparedEnumerator) finishPending(result *PreparedResult, end time.Time) {
	pending := result.State.Pending
	if pending.Actual.After(end) {
		result.Status = Waiting
		return
	}
	if pending.Excluded {
		result.State.DisposedThrough = pending.Nominal
		result.State.Pending = nil
		result.Status = Yield
		return
	}
	result.Status = Active
}

// PreparedCursor validates occurrence ownership using fixed definition metadata.
// It deliberately has no source reader or timezone capability, so a completed
// candidate can be disposed while a new source snapshot is being prepared.
type PreparedCursor struct{ control Enumerator }

func NewPreparedCursor(identity [32]byte, revision int64, header PreparedSpecHeader) (*PreparedCursor, error) {
	if identity == [32]byte{} || revision <= 0 {
		return nil, serviceerror.NewInvalidArgument("invalid prepared cursor definition")
	}
	start := time.Unix(0, 0).UTC()
	if header.HasStart {
		start = header.Start
	}
	end := time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)
	if header.HasEnd && header.End.Before(end) {
		end = header.End
	}
	return &PreparedCursor{control: Enumerator{identity: hex.EncodeToString(identity[:]), revision: revision, start: start, end: end, empty: header.Terms == 0, public: true}}, nil
}
func (c *PreparedCursor) Begin(after time.Time) (State, error) { return c.control.Begin(after) }
func (c *PreparedCursor) BeginRange(start, end time.Time) (State, error) {
	return c.control.BeginRange(start, end)
}
func (c *PreparedCursor) Update(state State, at time.Time) (State, *Candidate, error) {
	return c.control.Update(state, at)
}
func (c *PreparedCursor) RebaseRange(state State) (State, *Candidate, error) {
	return c.control.RebaseRange(state)
}
func (c *PreparedCursor) DisposeRange(state State) (State, error) {
	return c.control.DisposeRange(state)
}
