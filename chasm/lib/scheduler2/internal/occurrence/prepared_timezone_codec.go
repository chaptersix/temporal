package occurrence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// PreparedTimezoneHeaderBytes is the fixed encoded control size. It excludes
// transition pages and source bytes, which are stored separately.
const PreparedTimezoneHeaderBytes = 89

// PreparedTimezonePageBytes is the largest encoded sixteen-entry page.
const PreparedTimezonePageBytes = 230

// Header returns detached fixed-size control for persistence. Page readers and
// source data are never serialized into this header.
func (z *PreparedTimezone) Header() []byte {
	wire := make([]byte, 0, PreparedTimezoneHeaderBytes)
	wire = append(wire, 1)
	wire = append(wire, z.identity[:]...)
	wire = binary.BigEndian.AppendUint32(wire, uint32(z.count))
	wire = binary.BigEndian.AppendUint32(wire, uint32(z.first))
	present := byte(0)
	if z.cache.present {
		present = 1
	}
	wire = append(wire, present)
	wire = binary.BigEndian.AppendUint64(wire, uint64(z.cache.start))
	wire = binary.BigEndian.AppendUint64(wire, uint64(z.cache.end))
	wire = binary.BigEndian.AppendUint32(wire, uint32(z.cache.offset))
	mode := byte(0)
	if z.rules.valid {
		mode = 1
	}
	if z.rules.daylight {
		mode = 2
	}
	wire = append(wire, mode)
	wire = binary.BigEndian.AppendUint32(wire, uint32(z.rules.standardOffset))
	wire = binary.BigEndian.AppendUint32(wire, uint32(z.rules.daylightOffset))
	for _, rule := range []preparedTZRule{z.rules.start, z.rules.end} {
		wire = append(wire, byte(rule.kind))
		wire = binary.BigEndian.AppendUint16(wire, uint16(rule.day))
		wire = append(wire, byte(rule.week), byte(rule.mon))
		wire = binary.BigEndian.AppendUint32(wire, uint32(rule.time))
	}
	return wire
}

// PageCount is fixed-header metadata; it does not enumerate or load pages.
func (z *PreparedTimezone) PageCount() uint32 {
	return uint32((uint64(z.count) + preparedTimezonePageSize - 1) / preparedTimezonePageSize)
}

// Page returns one detached, bounded encoded page for incremental installation.
func (z *PreparedTimezone) Page(ordinal uint32) ([]byte, error) {
	if ordinal >= z.PageCount() {
		return nil, errors.New("prepared timezone page outside bounds")
	}
	var page preparedTimezonePage
	var err error
	if z.readPage != nil {
		page, err = z.readPage(int(ordinal))
	} else if int(ordinal) < len(z.pages) {
		page = z.pages[ordinal]
	} else {
		err = errors.New("prepared timezone page absent")
	}
	if err != nil {
		return nil, err
	}
	expected := min(preparedTimezonePageSize, z.count-int(ordinal)*preparedTimezonePageSize)
	if page.identity != z.identity || page.ordinal != int(ordinal) || page.length != expected {
		return nil, errors.New("prepared timezone page identity mismatch")
	}
	wire := make([]byte, 0, 38+12*page.length)
	wire = append(wire, 1)
	wire = append(wire, z.identity[:]...)
	wire = binary.BigEndian.AppendUint32(wire, ordinal)
	wire = append(wire, byte(page.length))
	for _, entry := range page.entries[:page.length] {
		wire = binary.BigEndian.AppendUint64(wire, uint64(entry.when))
		wire = binary.BigEndian.AppendUint32(wire, uint32(entry.offset))
	}
	return wire, nil
}

// RestorePreparedTimezone validates fixed control without reading source or
// transition pages. The supplied reader must address one immutable bounded page
// and remain valid for the enclosing transaction only.
func RestorePreparedTimezone(header []byte, reader func(uint32) ([]byte, error)) (*PreparedTimezone, error) {
	z, err := decodePreparedTimezoneHeader(header)
	if err != nil {
		return nil, err
	}
	if z.count > 0 && reader == nil {
		return nil, errors.New("prepared timezone page reader required")
	}
	z.readPage = func(ordinal int) (preparedTimezonePage, error) {
		if ordinal < 0 || uint64(ordinal) >= uint64(z.PageCount()) {
			return preparedTimezonePage{}, errors.New("prepared timezone page outside bounds")
		}
		wire, err := reader(uint32(ordinal))
		if err != nil {
			return preparedTimezonePage{}, err
		}
		return z.decodePage(uint32(ordinal), wire)
	}
	return z, nil
}

func decodePreparedTimezoneHeader(wire []byte) (*PreparedTimezone, error) {
	bad := errors.New("invalid prepared timezone header")
	if len(wire) != PreparedTimezoneHeaderBytes || wire[0] != 1 {
		return nil, bad
	}
	z := &PreparedTimezone{}
	copy(z.identity[:], wire[1:33])
	count := binary.BigEndian.Uint32(wire[33:37])
	if uint64(count) > uint64(math.MaxInt) || z.identity == [32]byte{} {
		return nil, bad
	}
	z.count = int(count)
	z.first = int(int32(binary.BigEndian.Uint32(wire[37:41])))
	if wire[41] > 1 {
		return nil, bad
	}
	z.cache = preparedTimezoneCache{present: wire[41] == 1, start: int64(binary.BigEndian.Uint64(wire[42:50])), end: int64(binary.BigEndian.Uint64(wire[50:58])), offset: int(int32(binary.BigEndian.Uint32(wire[58:62])))}
	if !z.cache.present && z.cache != (preparedTimezoneCache{}) {
		return nil, bad
	}
	mode := wire[62]
	if mode > 2 {
		return nil, bad
	}
	z.rules = preparedTZRules{valid: mode != 0, daylight: mode == 2, standardOffset: int(int32(binary.BigEndian.Uint32(wire[63:67]))), daylightOffset: int(int32(binary.BigEndian.Uint32(wire[67:71])))}
	rules := []*preparedTZRule{&z.rules.start, &z.rules.end}
	for i, rule := range rules {
		part := wire[71+i*9 : 80+i*9]
		*rule = preparedTZRule{kind: preparedTZRuleKind(part[0]), day: int(binary.BigEndian.Uint16(part[1:3])), week: int(part[3]), mon: int(part[4]), time: int(int32(binary.BigEndian.Uint32(part[5:9])))}
	}
	if !validPreparedTimezoneRules(z.rules) {
		return nil, bad
	}
	if z.count == 0 && !bytes.Equal(wire, PreparedUTC().Header()) {
		return nil, bad
	}
	return z, nil
}

func validPreparedTimezoneRules(r preparedTZRules) bool {
	if !r.valid {
		return r == (preparedTZRules{})
	}
	if r.standardOffset < -608399 || r.standardOffset > 608399 {
		return false
	}
	if !r.daylight {
		return r.daylightOffset == 0 && r.start == (preparedTZRule{}) && r.end == (preparedTZRule{})
	}
	if r.daylightOffset < -608399 || r.daylightOffset > 611999 {
		return false
	}
	return validPreparedTimezoneRule(r.start) && validPreparedTimezoneRule(r.end)
}

func validPreparedTimezoneRule(rule preparedTZRule) bool {
	if rule.time < -608399 || rule.time > 608399 {
		return false
	}
	switch rule.kind {
	case preparedTZRuleJulian:
		return rule.day >= 1 && rule.day <= 365 && rule.week == 0 && rule.mon == 0
	case preparedTZRuleDOY:
		return rule.day >= 0 && rule.day <= 365 && rule.week == 0 && rule.mon == 0
	case preparedTZRuleMonthWeekDay:
		return rule.day >= 0 && rule.day <= 6 && rule.week >= 1 && rule.week <= 5 && rule.mon >= 1 && rule.mon <= 12
	default:
		return false
	}
}

func (z *PreparedTimezone) decodePage(ordinal uint32, wire []byte) (preparedTimezonePage, error) {
	expected := min(preparedTimezonePageSize, z.count-int(ordinal)*preparedTimezonePageSize)
	if expected <= 0 || len(wire) != 38+12*expected || wire[0] != 1 || !bytes.Equal(wire[1:33], z.identity[:]) || binary.BigEndian.Uint32(wire[33:37]) != ordinal || int(wire[37]) != expected {
		return preparedTimezonePage{}, errors.New("invalid prepared timezone page")
	}
	page := preparedTimezonePage{identity: z.identity, ordinal: int(ordinal), length: expected}
	for i := range expected {
		entry := wire[38+12*i : 50+12*i]
		page.entries[i] = preparedTZTransition{when: int64(binary.BigEndian.Uint64(entry[:8])), offset: int(int32(binary.BigEndian.Uint32(entry[8:])))}
	}
	return page, nil
}
