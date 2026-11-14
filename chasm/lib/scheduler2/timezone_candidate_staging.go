package scheduler2

import (
	"bytes"
	"crypto/sha256"
	"math"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const timezoneSourcePageBytes = 1024

// TimezoneCandidateDescriptor is fixed metadata for an unselected immutable
// snapshot. Source identity includes the exact serialized preparation instant.
type TimezoneCandidateDescriptor struct {
	resolvedAtSeconds                              int64
	resolvedAtNanos                                int32
	sourceKind                                     timezonesource.Kind
	nativeYear                                     int64
	nativeWordBits                                 uint8
	definitionID, ownerID, fingerprint, sourceHash [32]byte
	generation                                     uint64
	header                                         [occurrence.PreparedTimezoneHeaderBytes]byte
	headerSize                                     uint8
	totalPages                                     uint32
	sourceBytes                                    uint64
}

func (d TimezoneCandidateDescriptor) OwnerID() [32]byte           { return d.ownerID }
func (d TimezoneCandidateDescriptor) DefinitionOwnerID() [32]byte { return d.definitionID }
func (d TimezoneCandidateDescriptor) Fingerprint() [32]byte       { return d.fingerprint }
func (d TimezoneCandidateDescriptor) Generation() uint64          { return d.generation }
func (d TimezoneCandidateDescriptor) SourcePageCount() uint64 {
	return definitionPageCount(d.sourceBytes, timezoneSourcePageBytes)
}

// StagedTimezoneCandidate owns source-proportional immutable bytes outside mutation.
type StagedTimezoneCandidate struct {
	descriptor TimezoneCandidateDescriptor
	source     []byte
	pages      [][]byte
}

func StageTimezoneCandidate(stage *StagedTimezoneSource) (*StagedTimezoneCandidate, error) {
	zone, err := stage.validate()
	if err != nil {
		return nil, err
	}
	source, err := proto.MarshalOptions{Deterministic: true}.Marshal(stage.sourceProto())
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(source)
	var ownerInput [64]byte
	copy(ownerInput[:32], stage.ownerID[:])
	copy(ownerInput[32:], hash[:])
	d := TimezoneCandidateDescriptor{definitionID: stage.definitionID, ownerID: sha256.Sum256(ownerInput[:]), fingerprint: stage.fingerprint, sourceHash: hash, generation: stage.generation, headerSize: uint8(len(stage.header)), totalPages: zone.PageCount(), sourceBytes: uint64(len(source))}
	copy(d.header[:], stage.header)
	d.resolvedAtSeconds = stage.resolvedAt.Unix()
	d.resolvedAtNanos = int32(stage.resolvedAt.Nanosecond())
	d.sourceKind = timezonesource.TZif
	if stage.utc {
		d.sourceKind = timezonesource.UTC
	} else if len(stage.native) > 0 {
		native, err := occurrence.WindowsTimezoneSourceFromBytes(stage.native)
		if err != nil {
			return nil, err
		}
		d.sourceKind = timezonesource.WindowsNative
		d.nativeYear = native.InitializationYear
		d.nativeWordBits = native.WordBits
	}

	candidate := &StagedTimezoneCandidate{descriptor: d, source: source, pages: stage.pages}
	if err := candidate.validate(); err != nil {
		return nil, err
	}
	return candidate, nil
}
func (s *StagedTimezoneCandidate) Descriptor() TimezoneCandidateDescriptor { return s.descriptor }
func (d TimezoneCandidateDescriptor) validate() (*occurrence.PreparedTimezone, error) {
	if err := d.validateRecovery(); err != nil {
		return nil, err
	}
	if d.definitionID == [32]byte{} || d.ownerID == [32]byte{} || d.fingerprint == [32]byte{} || d.sourceHash == [32]byte{} || d.generation == 0 || d.sourceBytes == 0 || d.sourceBytes > uint64(math.MaxInt) || d.headerSize > occurrence.PreparedTimezoneHeaderBytes {
		return nil, serviceerror.NewInvalidArgument("invalid pending timezone descriptor")
	}
	zone, err := occurrence.RestorePreparedTimezone(d.header[:d.headerSize], func(uint32) ([]byte, error) {
		return nil, serviceerror.NewInternal("timezone descriptor has no page reader")
	})
	if err != nil {
		return nil, err
	}
	if zone.Fingerprint() != d.fingerprint || zone.PageCount() != d.totalPages {
		return nil, serviceerror.NewInvalidArgument("pending timezone descriptor does not match header")
	}
	return zone, nil
}
func (s *StagedTimezoneCandidate) validate() error {
	if s == nil {
		return serviceerror.NewInvalidArgument("timezone candidate is missing")
	}
	if _, err := s.descriptor.validate(); err != nil {
		return err
	}
	if uint64(len(s.source)) != s.descriptor.sourceBytes || uint64(len(s.pages)) != uint64(s.descriptor.totalPages) {
		return serviceerror.NewInvalidArgument("timezone candidate source lengths do not match")
	}
	return nil
}

type timezoneCandidatePage struct {
	source  bool
	ordinal uint64
	encoded []byte
}

// TimezoneInstallBatch contains at most sixteen total raw and compiled records.
// Raw source precedes compiled pages so a durable full source permits exact cold
// reconstruction before the compiled prefix completes.
type TimezoneInstallBatch struct {
	descriptor         TimezoneCandidateDescriptor
	sourceFrontier     uint64
	transitionFrontier uint32
	count              uint8
	pages              [maxPreparationInstallPages]timezoneCandidatePage
}

func (b *TimezoneInstallBatch) OwnerID() [32]byte  { return b.descriptor.ownerID }
func (b *TimezoneInstallBatch) Generation() uint64 { return b.descriptor.generation }
func (s *StagedTimezoneCandidate) InstallBatch(sourceFrontier uint64, transitionFrontier uint32, limit int) (*TimezoneInstallBatch, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	d := s.descriptor
	if limit < 1 || limit > maxPreparationInstallPages || sourceFrontier > d.SourcePageCount() || transitionFrontier > d.totalPages || (sourceFrontier < d.SourcePageCount() && transitionFrontier != 0) {
		return nil, serviceerror.NewInvalidArgument("invalid pending timezone frontier or budget")
	}
	batch := &TimezoneInstallBatch{descriptor: d, sourceFrontier: sourceFrontier, transitionFrontier: transitionFrontier}
	for ordinal := sourceFrontier; ordinal < d.SourcePageCount() && int(batch.count) < limit; ordinal++ {
		begin := ordinal * timezoneSourcePageBytes
		end := min(begin+timezoneSourcePageBytes, d.sourceBytes)
		batch.pages[batch.count] = timezoneCandidatePage{source: true, ordinal: ordinal, encoded: bytes.Clone(s.source[begin:end])}
		batch.count++
	}
	for ordinal := transitionFrontier; ordinal < d.totalPages && int(batch.count) < limit; ordinal++ {
		batch.pages[batch.count] = timezoneCandidatePage{ordinal: uint64(ordinal), encoded: bytes.Clone(s.pages[ordinal])}
		batch.count++
	}
	return batch, nil
}

func (d TimezoneCandidateDescriptor) resolvedAt() time.Time {
	return time.Unix(d.resolvedAtSeconds, int64(d.resolvedAtNanos)).UTC()
}
func (d TimezoneCandidateDescriptor) validateRecovery() error {
	stamp := &timestamppb.Timestamp{Seconds: d.resolvedAtSeconds, Nanos: d.resolvedAtNanos}
	if stamp.CheckValid() != nil {
		return serviceerror.NewInvalidArgument("invalid pending timezone preparation instant")
	}
	switch d.sourceKind {
	case timezonesource.UTC, timezonesource.TZif:
		if d.nativeYear != 0 || d.nativeWordBits != 0 || d.header[0] != 1 {
			return serviceerror.NewInvalidArgument("invalid pending TZif source metadata")
		}
	case timezonesource.WindowsNative:
		if (d.nativeWordBits != 32 && d.nativeWordBits != 64) || d.header[0] != 2 {
			return serviceerror.NewInvalidArgument("invalid pending native source metadata")
		}
	default:
		return serviceerror.NewInvalidArgument("invalid pending timezone source kind")
	}
	return nil
}
