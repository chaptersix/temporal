package decision

import (
	"strconv"
	"time"
)

// Timestamp preserves a public nominal or actual time outside int64 Unix
// nanoseconds. Its zero value selects the existing nanosecond model field.
// It is a value so reducer snapshots cannot alias mutable protobuf timestamps.
type Timestamp struct {
	Present bool
	Seconds int64
	Nanos   int32
}

// Time returns the exact public time, or the original model representation.
func (t Timestamp) Time(nanoseconds int64) time.Time {
	if t.Present {
		return time.Unix(t.Seconds, int64(t.Nanos)).UTC()
	}
	return time.Unix(0, nanoseconds).UTC()
}

// Key preserves existing model identities and distinguishes every wide instant.
func (t Timestamp) Key(nanoseconds int64) string {
	if t.Present {
		return t.Time(0).Format(time.RFC3339Nano)
	}
	return strconv.FormatInt(nanoseconds, 10)
}
