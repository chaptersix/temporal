package scheduler

import (
	"crypto/sha256"
	"encoding/hex"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/encoding/protojson"
)

// ScheduleSpecLogInfo contains a JSON representation of a schedule spec that is safe to log.
// Embedded timezone data is omitted because it can be large; its size and digest are retained
// so operators can still identify the exact input.
type ScheduleSpecLogInfo struct {
	Spec               string
	TimezoneDataSize   int
	TimezoneDataSHA256 string
}

func NewScheduleSpecLogInfo(spec *schedulepb.ScheduleSpec) ScheduleSpecLogInfo {
	redacted := &schedulepb.ScheduleSpec{}
	if spec != nil {
		redacted = common.CloneProto(spec)
	}

	timezoneData := redacted.GetTimezoneData()
	redacted.TimezoneData = nil
	specJSON, _ := protojson.Marshal(redacted)

	info := ScheduleSpecLogInfo{
		Spec:             string(specJSON),
		TimezoneDataSize: len(timezoneData),
	}
	if len(timezoneData) > 0 {
		digest := sha256.Sum256(timezoneData)
		info.TimezoneDataSHA256 = hex.EncodeToString(digest[:])
	}
	return info
}
