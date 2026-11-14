package occurrence

import (
	"encoding/binary"
	"errors"
	"time"
)

// PreparedSpecStateBytes is the exact version-one continuation size. Time values
// use UTC seconds/nanoseconds with an explicit zero sentinel; no location pointer
// or monotonic clock reading is serialized.
const PreparedSpecStateBytes = 328

// EncodePreparedSpecState serializes a fixed continuation without reflection.
// Definition/zone/history integrity remains the enclosing owner's obligation.
func EncodePreparedSpecState(s PreparedSpecState) ([]byte, error) {
	if err := validateSpecStateShape(s); err != nil {
		return nil, err
	}
	out := make([]byte, 0, PreparedSpecStateBytes)
	out = append(out, 1)
	out = append(out, s.SourceID[:]...)
	out = append(out, s.ZoneID[:]...)
	for _, value := range [5]time.Time{s.Boundary, s.After, s.Nominal, s.Actual, s.Wall} {
		out = appendSpecTime(out, value)
	}
	out = append(out, byte(s.Phase))
	out = binary.BigEndian.AppendUint64(out, s.Index)
	out = binary.BigEndian.AppendUint64(out, uint64(s.Minimum))
	c := s.Calendar
	out = append(out, c.SourceID[:]...)
	out = append(out, c.ResolvedZoneID[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(c.AfterSeconds))
	out = binary.BigEndian.AppendUint32(out, uint32(c.AfterNanos))
	out = append(out, byte(c.Phase))
	for _, value := range [6]int{c.Year, c.Month, c.Day, c.Hour, c.Minute, c.Second} {
		out = binary.BigEndian.AppendUint64(out, uint64(int64(value)))
	}
	out = appendSpecBool(out, c.SecondCopy)
	out = binary.BigEndian.AppendUint64(out, uint64(c.CandidateSeconds))
	out = appendSpecBool(out, c.Found)
	out = appendSpecBool(out, s.Hash.Started)
	out = appendSpecBool(out, s.Hash.Done)
	out = binary.BigEndian.AppendUint64(out, s.Hash.Offset)
	for _, value := range [4]uint32{s.Hash.H, s.Hash.G, s.Hash.F, s.Hash.Hash} {
		out = binary.BigEndian.AppendUint32(out, value)
	}
	out = binary.BigEndian.AppendUint64(out, uint64(int64(s.Iterations)))
	for _, value := range [4]bool{s.Warned, s.Found, s.Excluded, s.Following} {
		out = appendSpecBool(out, value)
	}
	out = binary.BigEndian.AppendUint64(out, uint64(s.MaxJitter))
	return out, nil
}
func appendSpecBool(out []byte, value bool) []byte {
	if value {
		return append(out, 1)
	}
	return append(out, 0)
}
func appendSpecTime(out []byte, value time.Time) []byte {
	if value.IsZero() {
		return append(out, make([]byte, 13)...)
	}
	out = append(out, 1)
	out = binary.BigEndian.AppendUint64(out, uint64(value.Unix()))
	return binary.BigEndian.AppendUint32(out, uint32(value.Nanosecond()))
}

type specStateDecoder struct {
	data    []byte
	offset  int
	invalid bool
}

func (r *specStateDecoder) byte() byte { v := r.data[r.offset]; r.offset++; return v }
func (r *specStateDecoder) boolean() bool {
	v := r.byte()
	if v > 1 {
		r.invalid = true
	}
	return v == 1
}
func (r *specStateDecoder) u32() uint32 {
	v := binary.BigEndian.Uint32(r.data[r.offset : r.offset+4])
	r.offset += 4
	return v
}
func (r *specStateDecoder) u64() uint64 {
	v := binary.BigEndian.Uint64(r.data[r.offset : r.offset+8])
	r.offset += 8
	return v
}
func (r *specStateDecoder) integer() int {
	v := int64(r.u64())
	if int64(int(v)) != v {
		r.invalid = true
	}
	return int(v)
}
func (r *specStateDecoder) identity() [32]byte {
	var id [32]byte
	copy(id[:], r.data[r.offset:r.offset+32])
	r.offset += 32
	return id
}
func (r *specStateDecoder) instant() time.Time {
	present := r.boolean()
	seconds := int64(r.u64())
	nanos := r.u32()
	if nanos >= 1e9 {
		r.invalid = true
	}
	if !present {
		if seconds != 0 || nanos != 0 {
			r.invalid = true
		}
		return time.Time{}
	}
	value := time.Unix(seconds, int64(nanos)).UTC()
	if value.IsZero() {
		r.invalid = true
	}
	return value
}

// DecodePreparedSpecState validates the fixed representation, invocation identity,
// boundary and zone without reading any definition or timezone page. A malformed
// encoding returns no usable state. Internal wall times retain the full time.Time
// seconds domain; only the public boundary must be in the supported input domain.
func DecodePreparedSpecState(data []byte, h PreparedSpecHeader, z *PreparedLocation, boundary time.Time) (PreparedSpecState, error) {
	if len(data) != PreparedSpecStateBytes || data[0] != 1 {
		return PreparedSpecState{}, errors.New("invalid prepared spec state encoding")
	}
	r := specStateDecoder{data: data, offset: 1}
	s := PreparedSpecState{SourceID: r.identity(), ZoneID: r.identity()}
	s.Boundary = r.instant()
	s.After = r.instant()
	s.Nominal = r.instant()
	s.Actual = r.instant()
	s.Wall = r.instant()
	s.Phase = specPhase(r.byte())
	s.Index = r.u64()
	s.Minimum = int64(r.u64())
	c := CalendarNextState{SourceID: r.identity(), ResolvedZoneID: r.identity(), AfterSeconds: int64(r.u64()), AfterNanos: int32(r.u32()), Phase: CalendarNextPhase(r.byte())}
	c.Year = r.integer()
	c.Month = r.integer()
	c.Day = r.integer()
	c.Hour = r.integer()
	c.Minute = r.integer()
	c.Second = r.integer()
	c.SecondCopy = r.boolean()
	c.CandidateSeconds = int64(r.u64())
	c.Found = r.boolean()
	s.Calendar = c
	s.Hash = JitterHashState{Started: r.boolean(), Done: r.boolean(), Offset: r.u64(), H: r.u32(), G: r.u32(), F: r.u32(), Hash: r.u32()}
	s.Iterations = r.integer()
	s.Warned = r.boolean()
	s.Found = r.boolean()
	s.Excluded = r.boolean()
	s.Following = r.boolean()
	s.MaxJitter = time.Duration(r.u64())
	if r.invalid {
		return PreparedSpecState{}, errors.New("noncanonical prepared spec state encoding")
	}
	if err := validateSpecStateShape(s); err != nil {
		return PreparedSpecState{}, err
	}
	// These callbacks are capability placeholders; validation never invokes them.
	reader := PreparedSpecReader{Term: func(bool, uint64) (PreparedSpecTerm, error) { return PreparedSpecTerm{}, nil }, Seed: func(uint64) (SeedWindow, error) { return SeedWindow{}, nil }}
	if err := validatePreparedSpec(s, h, z, boundary, reader, 1); err != nil {
		return PreparedSpecState{}, err
	}
	if s.Hash.Started && (s.Hash.Offset > h.SeedBytes+15 || s.Hash.Offset%20 != 0 || (h.SeedBytes+15 <= 24 && (!s.Hash.Done || s.Hash.Offset != 0))) {
		return PreparedSpecState{}, errors.New("invalid prepared spec hash cursor")
	}
	return s, nil
}
func validateSpecStateShape(s PreparedSpecState) error {
	if s.Phase > specComplete || s.Iterations < 0 {
		return errors.New("invalid prepared spec state shape")
	}
	if s.Phase == specInitial && s != (PreparedSpecState{}) {
		return errors.New("invalid initial prepared spec state")
	}
	if s.Phase != specUnion && s.Calendar != (CalendarNextState{}) {
		return errors.New("calendar cursor outside prepared spec union")
	}
	if s.Calendar.Phase == CalendarNextInitial {
		if s.Calendar != (CalendarNextState{}) {
			return errors.New("invalid initial calendar cursor")
		}
	} else {
		if err := validateCalendarWallState(s.Calendar); err != nil {
			return err
		}
		if s.Calendar.SourceID == [32]byte{} || s.Calendar.ResolvedZoneID != s.ZoneID || s.Calendar.AfterSeconds != s.After.Unix() || s.Calendar.AfterNanos != int32(s.After.Nanosecond()) {
			return errors.New("invalid nested calendar identity")
		}
	}
	if !s.Hash.Started && s.Hash != (JitterHashState{}) {
		return errors.New("invalid initial prepared spec hash")
	}
	if s.Phase != specHash && s.Phase != specComplete && s.Hash != (JitterHashState{}) {
		return errors.New("hash cursor outside prepared spec jitter")
	}
	return validateSpecResultShape(s)
}

func validateSpecResultShape(s PreparedSpecState) error {
	if s.Phase != specComplete {
		if s.Found || s.Excluded || s.Hash.Done {
			return errors.New("result published before prepared spec completion")
		}
		return nil
	}
	if !s.Found {
		if !s.Nominal.IsZero() || !s.Actual.IsZero() || s.Excluded || s.Hash != (JitterHashState{}) {
			return errors.New("invalid exhausted prepared spec result")
		}
		return nil
	}
	if s.Nominal.IsZero() || s.Actual.IsZero() {
		return errors.New("missing completed prepared spec result")
	}
	if s.Excluded {
		if !s.Actual.Equal(s.Nominal) || s.Hash != (JitterHashState{}) {
			return errors.New("invalid excluded prepared spec result")
		}
	} else if !s.Hash.Done {
		return errors.New("incomplete prepared spec jitter result")
	}
	return nil
}
