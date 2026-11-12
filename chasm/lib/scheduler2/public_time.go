package scheduler2

import (
	"time"

	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// encodeTime keeps existing nanosecond records unchanged and uses an explicit
// seconds/nanoseconds representation when conversion would otherwise wrap.
func encodeTime(at time.Time) (int64, *timestamppb.Timestamp) {
	nano := at.UnixNano()
	if time.Unix(0, nano).Equal(at) {
		return nano, nil
	}
	return 0, timestamppb.New(at)
}
func decodeTime(nano int64, wide *timestamppb.Timestamp) time.Time {
	if wide != nil {
		return wide.AsTime()
	}
	return time.Unix(0, nano).UTC()
}
func decodeDecisionTime(wide *timestamppb.Timestamp) decision.Timestamp {
	if wide == nil {
		return decision.Timestamp{}
	}
	return decision.Timestamp{Present: true, Seconds: wide.Seconds, Nanos: wide.Nanos}
}
func encodeDecisionTime(at decision.Timestamp) *timestamppb.Timestamp {
	if !at.Present {
		return nil
	}
	return &timestamppb.Timestamp{Seconds: at.Seconds, Nanos: at.Nanos}
}
