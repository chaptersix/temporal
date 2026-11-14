package occurrence

import (
	"errors"
	"math"
	"time"

	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

// PreparedSpecHeader is immutable and includes the source identity of the seed,
// terms, wrapper bounds and invocation policy. Thresholds are sampled once per
// logical call, not once per continuation quantum. Start defaults to Unix epoch.
type PreparedSpecHeader struct {
	SourceID                      [32]byte
	Terms, Exclusions, SeedBytes  uint64
	Start, End                    time.Time
	HasStart, HasEnd              bool
	Jitter                        time.Duration
	WarnIterations, MaxIterations int
	// ReturnExcluded exposes raw probe dispositions for automatic enumeration.
	ReturnExcluded bool
}

// PreparedSpecTerm is one addressed calendar or interval from canonical input.
// Intervals are already normalized to positive seconds and nonnegative phase.
type PreparedSpecTerm struct {
	Calendar        PreparedCalendar
	Interval, Phase int64
	IsInterval      bool
}

// PreparedSpecReader returns detached fixed-size records from immutable owners.
type PreparedSpecReader struct {
	Term func(exclusion bool, index uint64) (PreparedSpecTerm, error)
	Seed func(offset uint64) (SeedWindow, error)
}

type specPhase uint8

const (
	specInitial specPhase = iota
	specUnion
	specBounds
	specExclusionWall
	specExclusions
	specFollowing
	specHash
	specComplete
)

// PreparedSpecState is fixed-size and value-only. No source slice or caller
// pointer survives a quantum. Completed results have no dependency on the zone.
type PreparedSpecState struct {
	SourceID, ZoneID                       [32]byte
	Boundary, After, Nominal, Actual, Wall time.Time
	Phase                                  specPhase
	Index                                  uint64
	Minimum                                int64
	Calendar                               CalendarNextState
	Hash                                   JitterHashState
	Iterations                             int
	Warned, Found, Excluded, Following     bool
	MaxJitter                              time.Duration
}

// PreparedSpecStats counts elementary phases and all addressed reads/conversions.
type PreparedSpecStats struct {
	Steps, TermReads, SeedReads int
	Calendar                    CalendarNextStats
}

// PreparedSpecResult publishes only complete candidates. Warning state remains
// visible on successful completion; compute-limit errors always imply a warning.
type PreparedSpecResult struct {
	Nominal, Actual                            time.Time
	Excluded, Found, Done, ComputeLimitWarning bool
}

func (s PreparedSpecState) Result() PreparedSpecResult {
	if s.Phase != specComplete {
		return PreparedSpecResult{}
	}
	return PreparedSpecResult{Nominal: s.Nominal, Actual: s.Actual, Excluded: s.Excluded, Found: s.Found, Done: true, ComputeLimitWarning: s.Warned}
}

// AdvancePreparedSpec computes one GetNextTime or raw candidate probe in bounded
// phases. Reader faults return exactly the input state, including after earlier
// successful steps in this quantum. No partial candidate is published.
func AdvancePreparedSpec(state PreparedSpecState, h PreparedSpecHeader, z *PreparedLocation, after time.Time, r PreparedSpecReader, budget int) (PreparedSpecState, PreparedSpecStats, error) {
	var stats PreparedSpecStats
	if err := validatePreparedSpec(state, h, z, after, r, budget); err != nil {
		return state, stats, err
	}
	next := state
	for stats.Steps < budget && next.Phase != specComplete {
		stats.Steps++
		if err := advanceSpecPhase(&next, h, z, after, r, &stats); err != nil {
			return state, stats, err
		}
	}
	return next, stats, nil
}

func validatePreparedSpec(s PreparedSpecState, h PreparedSpecHeader, z *PreparedLocation, after time.Time, r PreparedSpecReader, budget int) error {
	if budget < 1 || budget > MaxPreparationSteps || h.SourceID == [32]byte{} || r.Term == nil || r.Seed == nil || z == nil || z.location == nil {
		return errors.New("invalid prepared spec input")
	}
	if after.Year() < 1 || after.Year() > 9999 || h.SeedBytes > uint64(^uint(0)>>1)-15 {
		return errors.New("prepared spec input outside supported domain")
	}
	if s.Phase == specInitial {
		if s != (PreparedSpecState{}) {
			return errors.New("invalid initial prepared spec state")
		}
		return nil
	}
	if s.SourceID != h.SourceID || !s.Boundary.Equal(after) || s.Phase > specComplete || s.Iterations < 0 {
		return errors.New("prepared spec continuation identity mismatch")
	}
	if s.Phase != specComplete && s.ZoneID != z.fingerprint {
		return ErrPreparedZoneChanged
	}
	if (s.Phase == specUnion && s.Index > h.Terms) || (s.Phase == specExclusions && s.Index > h.Exclusions) {
		return errors.New("invalid prepared spec term cursor")
	}
	return nil
}

func beginSpecUnion(s *PreparedSpecState, after time.Time, following bool) {
	s.After = after
	s.Index = 0
	s.Minimum = math.MaxInt64
	s.Calendar = CalendarNextState{}
	s.Following = following
	s.Phase = specUnion
}

func advanceSpecPhase(s *PreparedSpecState, h PreparedSpecHeader, z *PreparedLocation, after time.Time, r PreparedSpecReader, stats *PreparedSpecStats) error {
	switch s.Phase {
	case specInitial:
		s.SourceID = h.SourceID
		s.ZoneID = z.fingerprint
		s.Boundary = after
		start := h.Start
		if !h.HasStart {
			start = time.Unix(0, 0).UTC()
		}
		delta := time.Second
		if h.ReturnExcluded {
			delta = time.Nanosecond
		}
		if lower := start.Add(-delta); after.Before(lower) {
			after = lower
		}
		beginSpecUnion(s, after, false)
	case specUnion:
		return advanceSpecUnion(s, h, z, r, stats)
	case specBounds:
		if s.Minimum == math.MaxInt64 {
			s.Nominal = time.Time{}
			s.Phase = specComplete
			return nil
		}
		s.Nominal = time.Unix(s.Minimum, 0).UTC()
		if s.Nominal.Year() > 2100 || (h.HasEnd && s.Nominal.After(h.End)) {
			s.Nominal = time.Time{}
			s.Phase = specComplete
			return nil
		}
		s.Index = 0
		s.Phase = specExclusionWall
	case specExclusionWall:
		if h.Exclusions == 0 {
			return finishSpecExclusions(s, h)
		}
		wall, err := calendarWall(z, s.Nominal.Unix(), &stats.Calendar)
		if err != nil {
			return err
		}
		s.Wall = wall
		s.Phase = specExclusions
	case specExclusions:
		return advanceSpecExclusions(s, h, r, stats)
	case specFollowing:
		s.MaxJitter = max(h.Jitter, 0)
		if s.Minimum != math.MaxInt64 {
			s.MaxJitter = min(s.MaxJitter, time.Unix(s.Minimum, 0).UTC().Sub(s.Nominal))
		}
		s.Phase = specHash
	case specHash:
		read := func(offset uint64) (SeedWindow, error) { stats.SeedReads++; return r.Seed(offset) }
		hash, err := advanceJitterHash(s.Hash, s.Nominal, h.SeedBytes, read)
		if err != nil {
			return err
		}
		s.Hash = hash
		if hash.Done {
			ms := min(uint64(max(s.MaxJitter, 0).Milliseconds()), uint64(math.MaxUint32))
			s.Actual = s.Nominal.Add(time.Duration((uint64(hash.Hash)*ms)>>32) * time.Millisecond)
			s.Found = true
			s.Phase = specComplete
		}
	default:
		return errors.New("invalid prepared spec phase")
	}
	return nil
}

func advanceSpecUnion(s *PreparedSpecState, h PreparedSpecHeader, z *PreparedLocation, r PreparedSpecReader, stats *PreparedSpecStats) error {
	if s.Index == 0 && s.Calendar == (CalendarNextState{}) && !s.Following {
		// Iteration policy is applied before starting the first term of a raw lookup.
		if !h.ReturnExcluded && h.MaxIterations > 0 && s.Iterations >= h.MaxIterations {
			return legacyscheduler.ErrComputeLimitExceeded
		}
	}
	if s.Index == h.Terms {
		finishSpecUnion(s, h)
		return nil
	}

	stats.TermReads++
	term, err := r.Term(false, s.Index)
	if err != nil {
		return err
	}
	if term.IsInterval {
		if term.Interval <= 0 || term.Phase < 0 {
			return errors.New("invalid prepared interval")
		}
		next := (((s.After.Unix()-term.Phase)/term.Interval)+1)*term.Interval + term.Phase
		s.Minimum = min(s.Minimum, next)
		s.Index++
		return nil
	}
	cal, work, err := AdvancePreparedCalendar(s.Calendar, term.Calendar, z, s.After, 1)
	stats.Calendar.Steps += work.Steps
	stats.Calendar.ZoneConversions += work.ZoneConversions
	stats.Calendar.TimezoneLookups += work.TimezoneLookups
	stats.Calendar.TimezonePageReads += work.TimezonePageReads
	if err != nil {
		return err
	}
	s.Calendar = cal
	candidate, found, done := cal.Result()
	if done {
		if found {
			s.Minimum = min(s.Minimum, candidate.Unix())
		}
		s.Index++
		s.Calendar = CalendarNextState{}
	}
	return nil
}

func advanceSpecExclusions(s *PreparedSpecState, h PreparedSpecHeader, r PreparedSpecReader, stats *PreparedSpecStats) error {
	if s.Index == h.Exclusions {
		return finishSpecExclusions(s, h)
	}
	stats.TermReads++
	term, err := r.Term(true, s.Index)
	if err != nil {
		return err
	}
	if term.IsInterval || !validPreparationMasks(PreparationState{Field: 7, Partial: term.Calendar.Masks}) {
		return errors.New("invalid prepared exclusion")
	}
	s.Index++
	if preparedMatches(term.Calendar.Masks, s.Wall) {
		s.Excluded = true
		return finishSpecExclusions(s, h)
	}
	return nil
}

func finishSpecExclusions(s *PreparedSpecState, h PreparedSpecHeader) error {
	if s.Excluded {
		if h.ReturnExcluded {
			s.Actual = s.Nominal
			s.Found = true
			s.Phase = specComplete
		} else {
			s.Excluded = false
			beginSpecUnion(s, s.Nominal, false)
		}
	} else {
		beginSpecUnion(s, s.Nominal, true)
	}
	return nil
}

func finishSpecUnion(s *PreparedSpecState, h PreparedSpecHeader) {
	if s.Following {
		s.Phase = specFollowing
	} else {
		warn := h.WarnIterations
		if warn <= 0 {
			warn = legacyscheduler.DefaultWarnIterations
		}
		if !h.ReturnExcluded && s.Iterations >= warn {
			s.Warned = true
		}
		if s.Iterations < math.MaxInt {
			s.Iterations++
		}
		s.Phase = specBounds
	}
}
