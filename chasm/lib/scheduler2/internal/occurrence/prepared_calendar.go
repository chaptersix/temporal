package occurrence

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"
)

// ErrPreparedZoneChanged asks the caller to restart unfinished evaluation from
// SearchBoundary, including all partial union/exclusion results. No disposition
// or admission has occurred. Completed results are unaffected by zone refresh.
var ErrPreparedZoneChanged = errors.New("prepared calendar resolved timezone changed")

// PreparedLocation is an immutable resolved timezone capability supplied outside
// mutation. This prototype still uses Go timezone operations; its phase budget
// does not bound arbitrary TZif lookupFirstZone or POSIX-extension parsing work.
type PreparedLocation struct {
	location    *time.Location
	fingerprint [32]byte
}

// NewResolvedLocation binds a caller-verified semantic fingerprint to a resolved
// location. A named-zone resolver must fingerprint actual resolved rules, not the
// name. The caller keeps the location immutable and performs cache misses outside
// mutation; this constructor does not load named zones or implement that cache.
func NewResolvedLocation(location *time.Location, fingerprint [32]byte) (*PreparedLocation, error) {
	if location == nil || fingerprint == [32]byte{} {
		return nil, errors.New("resolved location and nonzero semantic fingerprint required")
	}
	return &PreparedLocation{location: location, fingerprint: fingerprint}, nil
}

// PreparedLocationFromTZData parses caller-supplied custom bytes outside mutation.
// Nil is not interpreted as a request to load a named zone; present-empty data
// follows LoadLocationFromTZData's error behavior.
func PreparedLocationFromTZData(name string, data []byte) (*PreparedLocation, error) {
	if data == nil {
		return nil, errors.New("custom timezone data is absent")
	}
	location, err := time.LoadLocationFromTZData(name, data)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(name)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(name))
	_, _ = hash.Write(data)
	var id [32]byte
	copy(id[:], hash.Sum(nil))
	return NewResolvedLocation(location, id)
}

// Fingerprint identifies the resolved rules supplied for this capability.
func (p *PreparedLocation) Fingerprint() [32]byte { return p.fingerprint }

// PreparedCalendar holds fixed masks and the immutable source/revision identity.
// SourceID includes the definition's timezone input identity; resolved named-zone
// rules are independently fenced by the supplied capability fingerprint.
type PreparedCalendar struct {
	SourceID [32]byte
	Masks    CalendarMasks
}

type CalendarNextPhase uint8

const (
	CalendarNextInitial CalendarNextPhase = iota
	CalendarNextNormalize
	CalendarNextYear
	CalendarNextMonth
	CalendarNextDay
	CalendarNextHour
	CalendarNextMinute
	CalendarNextSecond
	CalendarNextCandidate
	CalendarNextComplete
)

// CalendarNextState is a value-only durable continuation. Structural validation
// cannot prove the semantic history of a trusted previously committed prefix.
type CalendarNextState struct {
	SourceID, ResolvedZoneID               [32]byte
	AfterSeconds                           int64
	AfterNanos                             int32
	Phase                                  CalendarNextPhase
	Year, Month, Day, Hour, Minute, Second int
	SecondCopy                             bool
	CandidateSeconds                       int64
	Found                                  bool
}

// SearchBoundary is retained when an unfinished timezone evaluation must restart.
func (s CalendarNextState) SearchBoundary() time.Time {
	return time.Unix(s.AfterSeconds, int64(s.AfterNanos)).UTC()
}

// Result distinguishes exhaustion from incomplete evaluation and a found instant.
func (s CalendarNextState) Result() (candidate time.Time, found, done bool) {
	if s.Phase != CalendarNextComplete {
		return time.Time{}, false, false
	}
	if !s.Found {
		return time.Time{}, false, true
	}
	return time.Unix(s.CandidateSeconds, 0).UTC(), true, true
}

// CalendarNextStats counts phase steps and calls to time.Date or zone-dependent
// time accessors (Date, Clock, Hour, Weekday). In and Add do not resolve zone
// offsets. One counted call may perform several lookups; this is not a CPU bound.
type CalendarNextStats struct{ Steps, ZoneConversions int }

// AdvancePreparedCalendar performs at most budget fixed calendar phase steps.
// All errors return the original state. No resolver, range compiler, union scan,
// jitter computation or persistence operation runs here.
func AdvancePreparedCalendar(state CalendarNextState, definition PreparedCalendar, zone *PreparedLocation, after time.Time, budget int) (CalendarNextState, CalendarNextStats, error) {
	var stats CalendarNextStats
	if err := validateCalendarNext(state, definition, zone, after, budget); err != nil {
		return state, stats, err
	}
	if state.Phase == CalendarNextComplete {
		return state, stats, nil
	}
	if state.Phase != CalendarNextInitial && state.ResolvedZoneID != zone.fingerprint {
		return state, stats, ErrPreparedZoneChanged
	}
	next := state
	for stats.Steps < budget && next.Phase != CalendarNextComplete {
		stats.Steps++
		advanceCalendarPhase(&next, definition, zone, after, &stats)
	}
	return next, stats, nil
}

func validateCalendarNext(state CalendarNextState, definition PreparedCalendar, zone *PreparedLocation, after time.Time, budget int) error {
	if budget < 1 || budget > MaxPreparationSteps {
		return errors.New("prepared calendar budget outside supported bounds")
	}
	if definition.SourceID == [32]byte{} || !validPreparationMasks(PreparationState{Field: 7, Partial: definition.Masks}) {
		return errors.New("invalid prepared calendar definition")
	}
	if zone == nil || zone.location == nil || zone.fingerprint == [32]byte{} {
		return errors.New("resolved timezone unavailable")
	}
	if !supported(after) {
		return errors.New("prepared calendar search boundary outside supported domain")
	}
	if state.Phase == CalendarNextInitial {
		if state != (CalendarNextState{}) {
			return errors.New("invalid initial prepared calendar cursor")
		}
		return nil
	}
	if state.SourceID != definition.SourceID || state.AfterSeconds != after.Unix() || state.AfterNanos != int32(after.Nanosecond()) {
		return errors.New("prepared calendar continuation identity mismatch")
	}
	return validateCalendarWallState(state)
}

func validateCalendarWallState(state CalendarNextState) error {
	if state.ResolvedZoneID == [32]byte{} || state.Phase > CalendarNextComplete || state.Month < 1 || state.Month > 13 || state.Day < 1 || state.Day > 32 || state.Hour < 0 || state.Hour > 24 || state.Minute < 0 || state.Minute > 60 || state.Second < 0 || state.Second > 60 {
		return errors.New("invalid prepared calendar cursor")
	}
	return nil
}

func advanceCalendarPhase(s *CalendarNextState, d PreparedCalendar, z *PreparedLocation, after time.Time, stats *CalendarNextStats) {
	switch s.Phase {
	case CalendarNextInitial:
		local := after.In(z.location)
		stats.ZoneConversions += 3 // Date, Clock, previous-hour comparison.
		var month time.Month
		s.Year, month, s.Day = local.Date()
		s.Month = int(month)
		s.Hour, s.Minute, s.Second = local.Clock()
		s.SecondCopy = local.Add(-time.Hour).Hour() == s.Hour
		s.SourceID, s.ResolvedZoneID = d.SourceID, z.fingerprint
		s.AfterSeconds, s.AfterNanos = after.Unix(), int32(after.Nanosecond())
		s.Second++
		s.Phase = CalendarNextNormalize
	case CalendarNextNormalize:
		normalizeCalendarNext(s, z, stats)
	case CalendarNextYear:
		if calendarYearMatches(d.Masks, s.Year) {
			s.Phase = CalendarNextMonth
		} else {
			s.Year++
			s.Month = 1
			s.Day = 1
			s.Hour = 0
			s.Minute = 0
			s.Second = 0
			s.SecondCopy = false
			s.Phase = CalendarNextNormalize
		}
	case CalendarNextMonth:
		if calendarBit(d.Masks.Fields[4], s.Month) {
			s.Phase = CalendarNextDay
		} else {
			s.Month++
			s.Day = 1
			s.Hour = 0
			s.Minute = 0
			s.Second = 0
			s.SecondCopy = false
			if s.Month > 12 {
				s.Phase = CalendarNextNormalize
			}
		}
	case CalendarNextDay:
		advanceCalendarDay(s, d, z, stats)
	case CalendarNextHour, CalendarNextMinute, CalendarNextSecond, CalendarNextCandidate:
		advanceCalendarClock(s, d, z, stats)
	case CalendarNextComplete:
		// Already complete; the driver does not call this phase.
	default:
		return // Invalid phases are rejected before entering the driver.
	}
}

func advanceCalendarClock(s *CalendarNextState, d PreparedCalendar, z *PreparedLocation, stats *CalendarNextStats) {
	switch s.Phase {
	case CalendarNextHour:
		if calendarBit(d.Masks.Fields[2], s.Hour) {
			s.Phase = CalendarNextMinute
		} else {
			s.Hour++
			s.Minute = 0
			s.Second = 0
			s.SecondCopy = false
			if s.Hour >= 24 {
				s.Phase = CalendarNextNormalize
			}
		}
	case CalendarNextMinute:
		if calendarBit(d.Masks.Fields[1], s.Minute) {
			s.Phase = CalendarNextSecond
		} else {
			s.Minute++
			s.Second = 0
			if s.Minute >= 60 {
				s.Phase = CalendarNextNormalize
			}
		}
	case CalendarNextSecond:
		if calendarBit(d.Masks.Fields[0], s.Second) {
			s.Phase = CalendarNextCandidate
		} else {
			s.Second++
			if s.Second >= 60 {
				s.Phase = CalendarNextNormalize
			}
		}
	case CalendarNextCandidate:
		finishCalendarCandidate(s, z, stats)
	default:
		// The phase dispatcher calls this helper only for clock phases.
	}
}

func finishCalendarCandidate(s *CalendarNextState, z *PreparedLocation, stats *CalendarNextStats) {
	candidate := preparedCalendarDate(s.Year, s.Month, s.Day, s.Hour, s.Minute, s.Second, z.location, stats)
	stats.ZoneConversions++
	if candidate.Hour() != s.Hour {
		s.Hour++
		s.Minute = 0
		s.Second = 0
		s.Phase = CalendarNextNormalize
		return
	}
	if s.SecondCopy {
		candidate = candidate.Add(time.Hour)
	}
	s.CandidateSeconds = candidate.Unix()
	s.Found = true
	s.Phase = CalendarNextComplete
}

func normalizeCalendarNext(s *CalendarNextState, z *PreparedLocation, stats *CalendarNextStats) {
	if s.Second >= 60 {
		s.Minute++
		s.Second = 0
	}
	if s.Minute >= 60 {
		previous := preparedCalendarDate(s.Year, s.Month, s.Day, s.Hour, 0, 0, z.location, stats)
		s.Hour++
		s.Minute = 0
		next := preparedCalendarDate(s.Year, s.Month, s.Day, s.Hour, 0, 0, z.location, stats)
		if !s.SecondCopy && next.Sub(previous) > time.Hour {
			s.Hour--
			s.SecondCopy = true
		} else {
			s.SecondCopy = false
		}
	}
	if s.Hour >= 24 {
		s.Day++
		s.Hour = 0
	}
	if s.Day > preparedDaysInMonth(s.Month, s.Year) {
		s.Month++
		s.Day = 1
	}
	if s.Month > 12 {
		s.Year++
		s.Month = 1
	}
	if s.Year > 2100 {
		s.Phase = CalendarNextComplete
		return
	}
	s.Phase = CalendarNextYear
}
func advanceCalendarDay(s *CalendarNextState, d PreparedCalendar, z *PreparedLocation, stats *CalendarNextStats) {
	matches := calendarBit(d.Masks.Fields[3], s.Day)
	if matches {
		stats.ZoneConversions += 2 // Date construction and Weekday.
		weekday := time.Date(s.Year, time.Month(s.Month), s.Day, s.Hour, s.Minute, s.Second, 0, z.location).Weekday()
		matches = calendarBit(d.Masks.Fields[5], int(weekday))
	}
	if matches {
		s.Phase = CalendarNextHour
		return
	}
	s.Day++
	s.Hour = 0
	s.Minute = 0
	s.Second = 0
	s.SecondCopy = false
	if s.Day > preparedDaysInMonth(s.Month, s.Year) {
		s.Phase = CalendarNextNormalize
	}
}
func calendarBit(mask uint64, value int) bool {
	return value >= 0 && value < 64 && mask&(uint64(1)<<value) != 0
}
func calendarYearMatches(m CalendarMasks, year int) bool {
	if m.AllYears {
		return true
	}
	if year < 2000 || year > 2100 {
		return false
	}
	return calendarBit(m.Years[(year-2000)/64], (year-2000)%64)
}
func preparedDaysInMonth(month, year int) int {
	if month == 2 {
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	}
	const bits = 0b1010110101010
	return 30 + (bits>>month)&1
}

// Keep the reference's corrected first-copy choice, including its one-hour scope.
func preparedCalendarDate(year, month, day, hour, minute, second int, zone *time.Location, stats *CalendarNextStats) time.Time {
	stats.ZoneConversions += 5 // Date construction and two Date/Clock pairs.
	result := time.Date(year, time.Month(month), day, hour, minute, second, 0, zone)
	earlier := result.Add(-time.Hour)
	ey, em, ed := earlier.Date()
	ry, rm, rd := result.Date()
	eh, emin, es := earlier.Clock()
	rh, rmin, rs := result.Clock()
	if ey == ry && em == rm && ed == rd && eh == rh && emin == rmin && es == rs {
		return earlier
	}
	return result
}
