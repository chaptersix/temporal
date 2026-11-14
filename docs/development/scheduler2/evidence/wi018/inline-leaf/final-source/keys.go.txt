// Package queueindex defines ordered keys for durable scheduler queue indexes.
package queueindex

import (
	"encoding/binary"
	"time"
)

// TimeKey orders timestamps chronologically, then by admission sequence. Its
// fixed width preserves the public timestamp domain without UnixNano overflow.
type TimeKey [20]byte

// NewTimeKey encodes normalized seconds and nanoseconds from an exact instant.
// The sign-bit transform makes signed seconds sort in unsigned byte order.
func NewTimeKey(at time.Time, sequence uint64) TimeKey {
	var key TimeKey
	binary.BigEndian.PutUint64(key[:8], uint64(at.Unix())^(uint64(1)<<63))
	binary.BigEndian.PutUint32(key[8:12], uint32(at.Nanosecond()))
	binary.BigEndian.PutUint64(key[12:], sequence)
	return key
}

// Time returns the exact instant represented by the key.
func (k TimeKey) Time() time.Time {
	seconds := int64(binary.BigEndian.Uint64(k[:8]) ^ (uint64(1) << 63))
	return time.Unix(seconds, int64(binary.BigEndian.Uint32(k[8:12]))).UTC()
}

// Sequence returns the admission sequence used to break timestamp ties.
func (k TimeKey) Sequence() uint64 { return binary.BigEndian.Uint64(k[12:]) }
