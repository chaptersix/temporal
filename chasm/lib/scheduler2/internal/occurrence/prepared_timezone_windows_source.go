// Copyright 2009 The Go Authors. All rights reserved.
// Portions adapted from Go1.27.3 time/zoneinfo_windows.go under the BSD license
// in prepared_timezone_license.txt. Native capture is outside this pure package.
package occurrence

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

// WindowsTimezoneSourceBytes is the exact deterministic native input size.
const WindowsTimezoneSourceBytes = 182

// PreparedNativeTimezoneHeaderBytes is the fixed format2 control size.
const PreparedNativeTimezoneHeaderBytes = 70

// WindowsSystemTime retains all native SYSTEMTIME fields without native padding.
type WindowsSystemTime struct{ Year, Month, DayOfWeek, Day, Hour, Minute, Second, Milliseconds uint16 }

// WindowsTimezoneSource records the API input and source-native arithmetic width.
// InitializationYear is sampled once outside mutation; it is never refreshed on replay.
type WindowsTimezoneSource struct {
	WordBits                         uint8
	InitializationYear               int64
	Bias, StandardBias, DaylightBias int32
	StandardName, DaylightName       [32]uint16
	StandardDate, DaylightDate       WindowsSystemTime
}

// Bytes returns a detached, versioned native source frame.
func (s WindowsTimezoneSource) Bytes() []byte {
	out := make([]byte, 0, WindowsTimezoneSourceBytes)
	out = append(out, 1, s.WordBits)
	out = binary.BigEndian.AppendUint64(out, uint64(s.InitializationYear))
	out = binary.BigEndian.AppendUint32(out, uint32(s.Bias))
	for _, v := range s.StandardName {
		out = binary.BigEndian.AppendUint16(out, v)
	}
	out = appendWindowsSystemTime(out, s.StandardDate)
	out = binary.BigEndian.AppendUint32(out, uint32(s.StandardBias))
	for _, v := range s.DaylightName {
		out = binary.BigEndian.AppendUint16(out, v)
	}
	out = appendWindowsSystemTime(out, s.DaylightDate)
	out = binary.BigEndian.AppendUint32(out, uint32(s.DaylightBias))
	return out
}
func appendWindowsSystemTime(out []byte, v WindowsSystemTime) []byte {
	for _, field := range [...]uint16{v.Year, v.Month, v.DayOfWeek, v.Day, v.Hour, v.Minute, v.Second, v.Milliseconds} {
		out = binary.BigEndian.AppendUint16(out, field)
	}
	return out
}
func decodeWindowsSystemTime(wire []byte) WindowsSystemTime {
	return WindowsSystemTime{binary.BigEndian.Uint16(wire), binary.BigEndian.Uint16(wire[2:]), binary.BigEndian.Uint16(wire[4:]), binary.BigEndian.Uint16(wire[6:]), binary.BigEndian.Uint16(wire[8:]), binary.BigEndian.Uint16(wire[10:]), binary.BigEndian.Uint16(wire[12:]), binary.BigEndian.Uint16(wire[14:])}
}

// WindowsTimezoneSourceFromBytes decodes owned input without touching platform APIs.
func WindowsTimezoneSourceFromBytes(wire []byte) (WindowsTimezoneSource, error) {
	if len(wire) != WindowsTimezoneSourceBytes || wire[0] != 1 {
		return WindowsTimezoneSource{}, errors.New("invalid native timezone source")
	}
	s := WindowsTimezoneSource{WordBits: wire[1], InitializationYear: int64(binary.BigEndian.Uint64(wire[2:10])), Bias: int32(binary.BigEndian.Uint32(wire[10:14]))}
	for i := range s.StandardName {
		s.StandardName[i] = binary.BigEndian.Uint16(wire[14+2*i:])
	}
	s.StandardDate = decodeWindowsSystemTime(wire[78:94])
	s.StandardBias = int32(binary.BigEndian.Uint32(wire[94:98]))
	for i := range s.DaylightName {
		s.DaylightName[i] = binary.BigEndian.Uint16(wire[98+2*i:])
	}
	s.DaylightDate = decodeWindowsSystemTime(wire[162:178])
	s.DaylightBias = int32(binary.BigEndian.Uint32(wire[178:182]))
	return s, nil
}
func nativeWindowsOffset(bias int32, bits uint8) int64 {
	if bits == 32 {
		return int64(-bias * int32(60))
	}
	return -int64(bias) * 60
}

// PrepareWindowsTimezone reproduces Go's native Local transition construction.
// It preserves emitted order even when extreme biases make transitions unsorted.
func PrepareWindowsTimezone(source WindowsTimezoneSource) (*PreparedTimezone, error) {
	if source.WordBits != 32 && source.WordBits != 64 {
		return nil, errors.New("invalid native timezone word width")
	}
	if source.InitializationYear < int64(math.MinInt)+101 || source.InitializationYear > int64(math.MaxInt)-101 {
		return nil, errors.New("unrepresentable native timezone initialization year")
	}
	z := &PreparedTimezone{codecVersion: 2}
	var transitions []preparedTZTransition
	if source.StandardDate.Month == 0 {
		z.first = nativeWindowsOffset(source.Bias, source.WordBits)
		z.cache = preparedTimezoneCache{present: true, start: preparedTZAlpha, end: preparedTZOmega, offset: z.first}
		transitions = []preparedTZTransition{{when: preparedTZAlpha, offset: z.first}}
	} else {
		var err error
		z.first, transitions, err = nativeWindowsTransitions(source)
		if err != nil {
			return nil, err
		}
	}
	z.count = len(transitions)
	hash := sha256.New()
	_, _ = hash.Write([]byte("scheduler2-prepared-windows-timezone-v2"))
	_, _ = hash.Write(source.Bytes())
	var pair [16]byte
	binary.BigEndian.PutUint64(pair[:8], uint64(z.first))
	_, _ = hash.Write(pair[:8])
	for _, tx := range transitions {
		binary.BigEndian.PutUint64(pair[:8], uint64(tx.when))
		binary.BigEndian.PutUint64(pair[8:], uint64(tx.offset))
		_, _ = hash.Write(pair[:])
	}
	copy(z.identity[:], hash.Sum(nil))
	z.setTransitions(transitions)
	return z, nil
}
func nativeWindowsTransitions(source WindowsTimezoneSource) (int64, []preparedTZTransition, error) {
	offsets := [2]int64{nativeWindowsOffset(source.Bias+source.StandardBias, source.WordBits), nativeWindowsOffset(source.Bias+source.DaylightBias, source.WordBits)}
	first, second := source.StandardDate, source.DaylightDate
	firstIndex, secondIndex := 0, 1
	if first.Month > second.Month {
		first, second = second, first
		firstIndex, secondIndex = secondIndex, firstIndex
	}
	transitions := make([]preparedTZTransition, 0, 400)
	year := int(source.InitializationYear)
	for y := year - 100; y < year+100; y++ {
		a, err := nativeWindowsPseudoUnix(y, first)
		if err != nil {
			return 0, nil, err
		}
		b, err := nativeWindowsPseudoUnix(y, second)
		if err != nil {
			return 0, nil, err
		}
		transitions = append(transitions, preparedTZTransition{when: a - offsets[secondIndex], offset: offsets[firstIndex]}, preparedTZTransition{when: b - offsets[firstIndex], offset: offsets[secondIndex]})
	}
	return offsets[0], transitions, nil
}
func nativeWindowsPseudoUnix(year int, date WindowsSystemTime) (int64, error) {
	t := time.Date(year, time.Month(date.Month), 1, int(date.Hour), int(date.Minute), int(date.Second), 0, time.UTC)
	day := 1
	weekday := int(date.DayOfWeek) - int(t.Weekday())
	if weekday < 0 {
		weekday += 7
	}
	day += weekday
	if week := int(date.Day) - 1; week < 4 {
		day += week * 7
	} else {
		// Go indexes daysIn by the original month in this branch. Reject only a
		// structurally unsafe injected month; valid OS records remain unchanged.
		if date.Month < 1 || date.Month > 12 {
			return 0, errors.New("invalid native timezone last-week month")
		}
		day += 28
		if day > preparedDaysInMonth(int(date.Month), year) {
			day -= 7
		}
	}
	return t.Unix() + int64(day-1)*86400, nil
}
func (z *PreparedTimezone) nativeHeader() []byte {
	wire := make([]byte, 0, PreparedNativeTimezoneHeaderBytes)
	wire = append(wire, 2)
	wire = append(wire, z.identity[:]...)
	wire = binary.BigEndian.AppendUint32(wire, uint32(z.count))
	wire = binary.BigEndian.AppendUint64(wire, uint64(z.first))
	present := byte(0)
	if z.cache.present {
		present = 1
	}
	wire = append(wire, present)
	wire = binary.BigEndian.AppendUint64(wire, uint64(z.cache.start))
	wire = binary.BigEndian.AppendUint64(wire, uint64(z.cache.end))
	wire = binary.BigEndian.AppendUint64(wire, uint64(z.cache.offset))
	return wire
}
func decodeNativeTimezoneHeader(wire []byte) (*PreparedTimezone, error) {
	bad := errors.New("invalid native timezone header")
	if len(wire) != PreparedNativeTimezoneHeaderBytes || wire[0] != 2 {
		return nil, bad
	}
	z := &PreparedTimezone{codecVersion: 2, count: int(binary.BigEndian.Uint32(wire[33:37])), first: int64(binary.BigEndian.Uint64(wire[37:45]))}
	copy(z.identity[:], wire[1:33])
	if z.identity == [32]byte{} || wire[45] > 1 {
		return nil, bad
	}
	z.cache = preparedTimezoneCache{present: wire[45] == 1, start: int64(binary.BigEndian.Uint64(wire[46:54])), end: int64(binary.BigEndian.Uint64(wire[54:62])), offset: int64(binary.BigEndian.Uint64(wire[62:70]))}
	switch z.count {
	case 1:
		if z.cache != (preparedTimezoneCache{present: true, start: preparedTZAlpha, end: preparedTZOmega, offset: z.first}) {
			return nil, bad
		}
	case 400:
		if z.cache != (preparedTimezoneCache{}) {
			return nil, bad
		}
	default:
		return nil, bad
	}
	return z, nil
}
