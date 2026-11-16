// Copyright 2011 The Go Authors. All rights reserved.
// Portions adapted from Go1.27.3 time/zoneinfo.go under the BSD license in
// prepared_timezone_license.txt. Parsing occurs only during preparation.
package occurrence

import "time"

const (
	preparedTZSecondsPerHour   = 3600
	preparedTZSecondsPerMinute = 60
	preparedTZSecondsPerDay    = 86400
)

func preparedTZIsLeap(year int) bool                  { return year%4 == 0 && (year%100 != 0 || year%400 == 0) }
func preparedTZDaysIn(month time.Month, year int) int { return preparedDaysInMonth(int(month), year) }
func preparedTZDaysBefore(month time.Month) int {
	return [...]int{0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}[month]
}

// preparedTZRules contains parsed POSIX rules; no text is read during lookup.
type preparedTZRules struct {
	valid, daylight                bool
	standardOffset, daylightOffset int
	start, end                     preparedTZRule
}

func parsePreparedTZRules(text string) preparedTZRules {
	_, rest, ok := preparedTZTzsetName(text)
	if !ok {
		return preparedTZRules{}
	}
	offset, rest, ok := preparedTZTzsetOffset(rest)
	if !ok {
		return preparedTZRules{}
	}
	rules := preparedTZRules{valid: true, standardOffset: -offset}
	if len(rest) == 0 || rest[0] == ',' {
		return rules
	}
	_, rest, ok = preparedTZTzsetName(rest)
	if !ok {
		return preparedTZRules{}
	}
	rules.daylight = true
	if len(rest) == 0 || rest[0] == ',' {
		rules.daylightOffset = rules.standardOffset + 3600
	} else {
		offset, rest, ok = preparedTZTzsetOffset(rest)
		if !ok {
			return preparedTZRules{}
		}
		rules.daylightOffset = -offset
	}
	if len(rest) == 0 {
		rest = ",M3.2.0,M11.1.0"
	}
	if rest[0] != ',' && rest[0] != ';' {
		return preparedTZRules{}
	}
	rules.start, rest, ok = preparedTZTzsetRule(rest[1:])
	if !ok || len(rest) == 0 || rest[0] != ',' {
		return preparedTZRules{}
	}
	rules.end, rest, ok = preparedTZTzsetRule(rest[1:])
	if !ok || len(rest) > 0 {
		return preparedTZRules{}
	}
	return rules
}

func (r preparedTZRules) lookup(last, sec int64) (offset int, start, end int64) {
	if !r.daylight {
		return r.standardOffset, last, preparedTZOmega
	}
	utc := time.Unix(sec, 0).UTC()
	year, yday := utc.Year(), utc.YearDay()
	// Preserve Go's signed remainder for times before the Unix epoch.
	ysec := int64((yday-1)*86400) + sec%86400
	ystart := sec - ysec
	startSec := int64(preparedTZTzruleTime(year, r.start, r.standardOffset))
	endSec := int64(preparedTZTzruleTime(year, r.end, r.daylightOffset))
	std, dst := r.standardOffset, r.daylightOffset
	if endSec < startSec {
		startSec, endSec = endSec, startSec
		std, dst = dst, std
	}
	if ysec < startSec {
		return std, ystart, startSec + ystart
	}
	if ysec >= endSec {
		return std, endSec + ystart, ystart + 365*86400
	}
	return dst, startSec + ystart, endSec + ystart
}

// preparedTZTzsetName returns the timezone name at the start of the tzset string s,
// and the remainder of s, and reports whether the parsing is OK.
func preparedTZTzsetName(s string) (name, rest string, ok bool) {
	if len(s) == 0 {
		return "", "", false
	}
	if s[0] != '<' {
		for i, r := range s {
			switch r {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ',', '-', '+':
				if i < 3 {
					return "", "", false
				}
				return s[:i], s[i:], true
			}
		}
		if len(s) < 3 {
			return "", "", false
		}
		return s, "", true
	}
	for i, r := range s {
		if r == '>' {
			return s[1:i], s[i+1:], true
		}
	}
	return "", "", false
}

// preparedTZTzsetOffset returns the timezone offset at the start of the tzset string s,
// and the remainder of s, and reports whether the parsing is OK.
// The timezone offset is returned as a number of seconds.
func preparedTZTzsetOffset(s string) (offset int, rest string, ok bool) {
	if len(s) == 0 {
		return 0, "", false
	}
	neg := false
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		s = s[1:]
		neg = true
	default:
	}

	// The tzdata code permits values up to 24 * 7 here,
	// although POSIX does not.
	var hours int
	hours, s, ok = preparedTZTzsetNum(s, 0, 24*7)
	if !ok {
		return 0, "", false
	}
	off := hours * preparedTZSecondsPerHour
	if len(s) == 0 || s[0] != ':' {
		if neg {
			off = -off
		}
		return off, s, true
	}

	var mins int
	mins, s, ok = preparedTZTzsetNum(s[1:], 0, 59)
	if !ok {
		return 0, "", false
	}
	off += mins * preparedTZSecondsPerMinute
	if len(s) == 0 || s[0] != ':' {
		if neg {
			off = -off
		}
		return off, s, true
	}

	var secs int
	secs, s, ok = preparedTZTzsetNum(s[1:], 0, 59)
	if !ok {
		return 0, "", false
	}
	off += secs

	if neg {
		off = -off
	}
	return off, s, true
}

// preparedTZRuleKind is the kinds of rules that can be seen in a tzset string.
type preparedTZRuleKind int

const (
	preparedTZRuleJulian preparedTZRuleKind = iota
	preparedTZRuleDOY
	preparedTZRuleMonthWeekDay
)

// preparedTZRule is a preparedTZRule read from a tzset string.
type preparedTZRule struct {
	kind preparedTZRuleKind
	day  int
	week int
	mon  int
	time int // transition time
}

// preparedTZTzsetRule parses a preparedTZRule from a tzset string.
// It returns the preparedTZRule, and the remainder of the string, and reports success.
func preparedTZTzsetRule(s string) (preparedTZRule, string, bool) {
	var r preparedTZRule
	if len(s) == 0 {
		return preparedTZRule{}, "", false
	}
	ok := false
	switch s[0] {
	case 'J':
		var jday int
		jday, s, ok = preparedTZTzsetNum(s[1:], 1, 365)
		if !ok {
			return preparedTZRule{}, "", false
		}
		r.kind = preparedTZRuleJulian
		r.day = jday
	case 'M':
		var mon int
		mon, s, ok = preparedTZTzsetNum(s[1:], 1, 12)
		if !ok || len(s) == 0 || s[0] != '.' {
			return preparedTZRule{}, "", false

		}
		var week int
		week, s, ok = preparedTZTzsetNum(s[1:], 1, 5)
		if !ok || len(s) == 0 || s[0] != '.' {
			return preparedTZRule{}, "", false
		}
		var day int
		day, s, ok = preparedTZTzsetNum(s[1:], 0, 6)
		if !ok {
			return preparedTZRule{}, "", false
		}
		r.kind = preparedTZRuleMonthWeekDay
		r.day = day
		r.week = week
		r.mon = mon
	default:
		var day int
		day, s, ok = preparedTZTzsetNum(s, 0, 365)
		if !ok {
			return preparedTZRule{}, "", false
		}
		r.kind = preparedTZRuleDOY
		r.day = day
	}

	if len(s) == 0 || s[0] != '/' {
		r.time = 2 * preparedTZSecondsPerHour // 2am is the default
		return r, s, true
	}

	offset, s, ok := preparedTZTzsetOffset(s[1:])
	if !ok {
		return preparedTZRule{}, "", false
	}
	r.time = offset

	return r, s, true
}

// preparedTZTzsetNum parses a number from a tzset string.
// It returns the number, and the remainder of the string, and reports success.
// The number must be between min and max.
func preparedTZTzsetNum(s string, minimum, maximum int) (num int, rest string, ok bool) {
	if len(s) == 0 {
		return 0, "", false
	}
	num = 0
	for i, r := range s {
		if r < '0' || r > '9' {
			if i == 0 || num < minimum {
				return 0, "", false
			}
			return num, s[i:], true
		}
		num *= 10
		num += int(r) - '0'
		if num > maximum {
			return 0, "", false
		}
	}
	if num < minimum {
		return 0, "", false
	}
	return num, "", true
}

// preparedTZTzruleTime takes a year, a preparedTZRule, and a timezone offset,
// and returns the number of seconds since the start of the year
// that the preparedTZRule takes effect.
func preparedTZTzruleTime(year int, r preparedTZRule, off int) int {
	var s int
	switch r.kind {
	case preparedTZRuleJulian:
		s = (r.day - 1) * preparedTZSecondsPerDay
		if preparedTZIsLeap(year) && r.day >= 60 {
			s += preparedTZSecondsPerDay
		}
	case preparedTZRuleDOY:
		s = r.day * preparedTZSecondsPerDay
	case preparedTZRuleMonthWeekDay:
		// Zeller's Congruence.
		m1 := (r.mon+9)%12 + 1
		yy0 := year
		if r.mon <= 2 {
			yy0--
		}
		yy1 := yy0 / 100
		yy2 := yy0 % 100
		dow := ((26*m1-2)/10 + 1 + yy2 + yy2/4 + yy1/4 - 2*yy1) % 7
		if dow < 0 {
			dow += 7
		}
		// Now dow is the day-of-week of the first day of r.mon.
		// Get the day-of-month of the first "dow" day.
		d := r.day - dow
		if d < 0 {
			d += 7
		}
		for i := 1; i < r.week; i++ {
			if d+7 >= preparedTZDaysIn(time.Month(r.mon), year) {
				break
			}
			d += 7
		}
		d += preparedTZDaysBefore(time.Month(r.mon))
		if preparedTZIsLeap(year) && r.mon > 2 {
			d++
		}
		s = d * preparedTZSecondsPerDay
	default:
		// Only parsed rule kinds reach evaluation.
	}

	return s + r.time - off
}
