package scheduler2

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
)

// StagedTimezoneSource is immutable input-proportional preparation. It binds a
// calendar definition to one separately versioned timezone owner. Construct it
// outside CHASM mutation callbacks and retain no live transaction context in it.
type StagedTimezoneSource struct {
	definitionID, ownerID, fingerprint [32]byte
	generation                         uint64
	header                             []byte
	pages                              [][]byte
	name                               string
	source                             []byte
	resolvedAt                         time.Time
	utc                                bool
}

// StageCalendarTimezone prepares explicit resolved TZif bytes. It does not load
// a named zone; named resolution and its refresh policy belong to the caller.
// Absent custom data is an error. Use StageCalendarUTC for an explicit UTC input.
func StageCalendarTimezone(calendar *StagedCalendarSource, name string, data []byte, resolvedAt time.Time, generation uint64) (*StagedTimezoneSource, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("resolved timezone data is required")
	}
	if err := calendar.validate(); err != nil {
		return nil, err
	}
	zone, err := occurrence.PrepareTimezone(data, resolvedAt)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	return stageCalendarTimezone(calendar, name, data, resolvedAt, generation, false, zone)
}

// StageCalendarUTC stages a canonical UTC snapshot without inventing TZif bytes.
func StageCalendarUTC(calendar *StagedCalendarSource, generation uint64) (*StagedTimezoneSource, error) {
	if err := calendar.validate(); err != nil {
		return nil, err
	}
	return stageCalendarTimezone(calendar, "UTC", nil, time.Unix(0, 0).UTC(), generation, true, occurrence.PreparedUTC())
}

func stageCalendarTimezone(calendar *StagedCalendarSource, name string, data []byte, resolvedAt time.Time, generation uint64, utc bool, zone *occurrence.PreparedTimezone) (*StagedTimezoneSource, error) {
	if generation == 0 {
		return nil, serviceerror.NewInvalidArgument("timezone generation must be positive")
	}
	stage := &StagedTimezoneSource{definitionID: calendar.sourceID, generation: generation, fingerprint: zone.Fingerprint(), header: zone.Header(), name: name, source: bytes.Clone(data), resolvedAt: resolvedAt.UTC(), utc: utc}
	hash := sha256.New()
	_, _ = hash.Write([]byte("scheduler2-timezone-owner-v1"))
	_, _ = hash.Write(stage.definitionID[:])
	_, _ = hash.Write(stage.fingerprint[:])
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], generation)
	_, _ = hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(len(name)))
	_, _ = hash.Write(number[:])
	_, _ = hash.Write([]byte(name))
	copy(stage.ownerID[:], hash.Sum(nil))
	for ordinal := uint32(0); ordinal < zone.PageCount(); ordinal++ {
		page, err := zone.Page(ordinal)
		if err != nil {
			return nil, err
		}
		stage.pages = append(stage.pages, page)
	}
	return stage, nil
}

func (stage *StagedTimezoneSource) validate() (*occurrence.PreparedTimezone, error) {
	if stage == nil || stage.generation == 0 || stage.definitionID == [32]byte{} || stage.ownerID == [32]byte{} || stage.fingerprint == [32]byte{} {
		return nil, serviceerror.NewInvalidArgument("timezone source is uninitialized")
	}
	zone, err := occurrence.RestorePreparedTimezone(stage.header, func(ordinal uint32) ([]byte, error) {
		if uint64(ordinal) >= uint64(len(stage.pages)) {
			return nil, serviceerror.NewInvalidArgument("timezone staged page is absent")
		}
		return stage.pages[ordinal], nil
	})
	if err != nil {
		return nil, err
	}
	if zone.Fingerprint() != stage.fingerprint || uint64(zone.PageCount()) != uint64(len(stage.pages)) {
		return nil, serviceerror.NewInvalidArgument("timezone staging header does not match")
	}
	return zone, nil
}
