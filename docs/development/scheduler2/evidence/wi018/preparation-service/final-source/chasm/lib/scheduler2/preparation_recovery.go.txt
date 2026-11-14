package scheduler2

import (
	"go.temporal.io/api/serviceerror"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecoverTimezone re-reads a missing stage outside mutation, preserving its
// original preparation parameters. A false match means source bytes changed;
// the caller must supersede through a newer generation rather than blend pages.
func (s *PreparationService) RecoverTimezone(definition *StagedDefinitionSource, d TimezoneCandidateDescriptor) (*StagedTimezoneCandidate, bool, error) {
	if err := definition.validate(); err != nil {
		return nil, false, err
	}
	if _, err := d.validate(); err != nil {
		return nil, false, err
	}
	if definition.owner != d.definitionID {
		return nil, false, serviceerror.NewFailedPrecondition("timezone recovery definition owner does not match")
	}
	snapshot, err := s.ResolveDefinition(definition)
	if err != nil {
		return nil, false, err
	}
	if snapshot.Kind() != d.sourceKind {
		return nil, false, nil
	}
	source := &scheduler2pb.TimezonePreparationSource{Name: snapshot.Name(), ResolvedAt: timestamppb.New(d.resolvedAt()), Utc: d.sourceKind == timezonesource.UTC, TzifData: snapshot.Data(), WindowsNativeData: snapshot.NativeData()}
	if d.sourceKind == timezonesource.WindowsNative {
		native, err := occurrence.WindowsTimezoneSourceFromBytes(source.WindowsNativeData)
		if err != nil {
			return nil, false, err
		}
		native.InitializationYear = d.nativeYear
		native.WordBits = d.nativeWordBits
		source.WindowsNativeData = native.Bytes()
	}
	stage, err := StageDefinitionTimezoneSnapshot(definition, source, d.generation)
	if err != nil {
		return nil, false, err
	}
	candidate, err := StageTimezoneCandidate(stage)
	if err != nil {
		return nil, false, err
	}
	if candidate.descriptor != d {
		return nil, false, nil
	}
	s.stages.Put(preparationCacheKey('t', d.ownerID), candidate)
	return candidate, true, nil
}
