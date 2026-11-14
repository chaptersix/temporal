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

// PreparedLocation is an immutable prepared timezone capability supplied outside
// mutation. Its fingerprint includes conversion rules and loader-cache behavior.
type PreparedLocation struct {
	location    *PreparedTimezone
	fingerprint [32]byte
}

// NewResolvedLocation binds a prepared immutable snapshot. Named resolution must
// supply its actual rules outside mutation; no time.Location fallback exists.
func NewResolvedLocation(location *PreparedTimezone) (*PreparedLocation, error) {
	if location == nil || location.identity == [32]byte{} {
		return nil, errors.New("prepared timezone required")
	}
	return &PreparedLocation{location: location, fingerprint: location.identity}, nil
}

// PreparedLocationFromTZData parses caller-supplied custom bytes outside mutation.
// Nil is not interpreted as a request to load a named zone; present-empty data
// follows LoadLocationFromTZData's error behavior.
func PreparedLocationFromTZData(name string, data []byte, resolvedAt time.Time) (*PreparedLocation, error) {
	if data == nil {
		return nil, errors.New("custom timezone data is absent")
	}
	location, err := PrepareTimezone(data, resolvedAt)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(name)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(name))
	_, _ = hash.Write(location.identity[:])
	var id [32]byte
	copy(id[:], hash.Sum(nil))
	return &PreparedLocation{location: location, fingerprint: id}, nil
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

// CalendarNextStats counts phase steps, adapter conversions, offset lookups and
// fixed timezone page reads. UTC-only civil arithmetic does not read zone pages.
type CalendarNextStats struct{ Steps, ZoneConversions, TimezoneLookups, TimezonePageReads int }

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
		if err := advanceCalendarPhase(&next, definition, zone, after, &stats); err != nil {
			return state, stats, err
		}
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
	if after.Year() < 1 || after.Year() > 9999 {
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

func advanceCalendarPhase(s *CalendarNextState, d PreparedCalendar, z *PreparedLocation, after time.Time, stats *CalendarNextStats) error {
	switch s.Phase {
	case CalendarNextInitial:
		local, err := calendarWall(z, after.Unix(), stats)
		if err != nil {
			return err
		}
		previous, err := calendarWall(z, after.Unix()-3600, stats)
		if err != nil {
			return err
		}
		var month time.Month
		s.Year, month, s.Day = local.Date()
		s.Month = int(month)
		s.Hour, s.Minute, s.Second = local.Clock()
		s.SecondCopy = previous.Hour() == s.Hour
		s.SourceID, s.ResolvedZoneID = d.SourceID, z.fingerprint
		s.AfterSeconds, s.AfterNanos = after.Unix(), int32(after.Nanosecond())
		s.Second++
		s.Phase = CalendarNextNormalize
	case CalendarNextNormalize:
		return normalizeCalendarNext(s, z, stats)
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
		return advanceCalendarDay(s, d, z, stats)
	case CalendarNextHour, CalendarNextMinute, CalendarNextSecond, CalendarNextCandidate:
		return advanceCalendarClock(s, d, z, stats)
	case CalendarNextComplete:
		// Already complete; the driver does not call this phase.
	default:
		return nil // Invalid phases are rejected before entering the driver.
	}
	return nil
}

func advanceCalendarClock(s *CalendarNextState, d PreparedCalendar, z *PreparedLocation, stats *CalendarNextStats) error {
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
		return finishCalendarCandidate(s, z, stats)
	default:
		// The phase dispatcher calls this helper only for clock phases.
	}
	return nil
}

func finishCalendarCandidate(s *CalendarNextState, z *PreparedLocation, stats *CalendarNextStats) error {
	candidate, err := preparedCalendarDate(s.Year, s.Month, s.Day, s.Hour, s.Minute, s.Second, z, stats)
	if err != nil {
		return err
	}
	wall, err := calendarWall(z, candidate.Unix(), stats)
	if err != nil {
		return err
	}
	if wall.Hour() != s.Hour {
		s.Hour++
		s.Minute = 0
		s.Second = 0
		s.Phase = CalendarNextNormalize
		return nil
	}
	if s.SecondCopy {
		candidate = candidate.Add(time.Hour)
	}
	s.CandidateSeconds = candidate.Unix()
	s.Found = true
	s.Phase = CalendarNextComplete
	return nil
}

func normalizeCalendarNext(s *CalendarNextState, z *PreparedLocation, stats *CalendarNextStats) error {
	if s.Second >= 60 {
		s.Minute++
		s.Second = 0
	}
	if s.Minute >= 60 {
		previous, err := preparedCalendarDate(s.Year, s.Month, s.Day, s.Hour, 0, 0, z, stats)
		if err != nil {
			return err
		}
		s.Hour++
		s.Minute = 0
		next, err := preparedCalendarDate(s.Year, s.Month, s.Day, s.Hour, 0, 0, z, stats)
		if err != nil {
			return err
		}
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
		return nil
	}
	s.Phase = CalendarNextYear
	return nil
}
func advanceCalendarDay(s *CalendarNextState, d PreparedCalendar, z *PreparedLocation, stats *CalendarNextStats) error {
	matches := calendarBit(d.Masks.Fields[3], s.Day)
	if matches {
		candidate, err := calendarDate(z, s.Year, s.Month, s.Day, s.Hour, s.Minute, s.Second, stats)
		if err != nil {
			return err
		}
		wall, err := calendarWall(z, candidate.Unix(), stats)
		if err != nil {
			return err
		}
		matches = calendarBit(d.Masks.Fields[5], int(wall.Weekday()))
	}
	if matches {
		s.Phase = CalendarNextHour
		return nil
	}
	s.Day++
	s.Hour = 0
	s.Minute = 0
	s.Second = 0
	s.SecondCopy = false
	if s.Day > preparedDaysInMonth(s.Month, s.Year) {
		s.Phase = CalendarNextNormalize
	}
	return nil
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

func calendarWall(z *PreparedLocation, sec int64, stats *CalendarNextStats) (time.Time, error) {
	stats.ZoneConversions++
	var work timezoneWork
	value, err := z.location.wall(sec, &work)
	stats.TimezoneLookups += work.lookups
	stats.TimezonePageReads += work.pages
	return value, err
}
func calendarDate(z *PreparedLocation, year, month, day, hour, minute, second int, stats *CalendarNextStats) (time.Time, error) {
	stats.ZoneConversions++
	var work timezoneWork
	value, err := z.location.date(year, month, day, hour, minute, second, &work)
	stats.TimezoneLookups += work.lookups
	stats.TimezonePageReads += work.pages
	return value, err
}

// Keep the reference's corrected first-copy choice, including its one-hour scope.
func preparedCalendarDate(year, month, day, hour, minute, second int, zone *PreparedLocation, stats *CalendarNextStats) (time.Time, error) {
	result, err := calendarDate(zone, year, month, day, hour, minute, second, stats)
	if err != nil {
		return time.Time{}, err
	}
	earlier := result.Add(-time.Hour)
	before, err := calendarWall(zone, earlier.Unix(), stats)
	if err != nil {
		return time.Time{}, err
	}
	current, err := calendarWall(zone, result.Unix(), stats)
	if err != nil {
		return time.Time{}, err
	}
	by, bm, bd := before.Date()
	cy, cm, cd := current.Date()
	bh, bmin, bs := before.Clock()
	ch, cmin, cs := current.Clock()
	if by == cy && bm == cm && bd == cd && bh == ch && bmin == cmin && bs == cs {
		return earlier, nil
	}
	return result, nil
}
