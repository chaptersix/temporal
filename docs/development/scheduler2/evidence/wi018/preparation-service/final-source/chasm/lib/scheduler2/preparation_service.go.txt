package scheduler2

import (
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
)

// PreparationService owns one process source history and bounded immutable stage
// cache. Invoke its preparation methods outside CHASM mutations. Offline readers
// use persisted capabilities and never construct a process provider.
type PreparationService struct {
	resolver *timezonesource.Resolver
	stages   cache.Cache
}

func NewPreparationService(environment *timezonesource.Environment, wallClock clock.TimeSource) (*PreparationService, error) {
	if environment == nil || wallClock == nil {
		return nil, serviceerror.NewInvalidArgument("preparation environment and wall clock are required")
	}
	return &PreparationService{resolver: timezonesource.NewResolver(environment), stages: cache.New(1000, &cache.Options{TTL: 24 * time.Hour, TimeSource: wallClock})}, nil
}
func preparationCacheKey(kind byte, owner [32]byte) string {
	return string(append([]byte{kind}, owner[:]...))
}

// PrepareDefinition performs source-proportional ingress preparation and resolves
// canonical timezone input through the same provider used by source tasks.
func (s *PreparationService) PrepareDefinition(spec *schedulepb.ScheduleSpec, seed string, revision int64) (*StagedDefinitionSource, *timezonesource.Snapshot, error) {
	stage, err := StageDefinitionSource(spec, seed, revision)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := s.ResolveDefinition(stage)
	if err != nil {
		return nil, nil, err
	}
	s.stages.Put(preparationCacheKey('d', stage.owner), stage)
	return stage, snapshot, nil
}
func (s *PreparationService) Definition(owner [32]byte) (*StagedDefinitionSource, bool) {
	value, ok := s.stages.Get(preparationCacheKey('d', owner)).(*StagedDefinitionSource)
	return value, ok
}

// RestoreDefinition reconstructs a detached source only after its caller's read
// fence. A replacement cannot populate the requested old owner with new bytes.
func (s *PreparationService) RestoreDefinition(source *scheduler2pb.DefinitionPreparationSource, revision int64, expectedOwner [32]byte) (*StagedDefinitionSource, error) {
	stage, err := StageDefinitionSnapshot(source, revision)
	if err != nil {
		return nil, err
	}
	if stage.owner != expectedOwner {
		return nil, serviceerror.NewFailedPrecondition("reconstructed definition owner does not match")
	}
	s.stages.Put(preparationCacheKey('d', stage.owner), stage)
	return stage, nil
}
func (s *PreparationService) ResolveDefinition(stage *StagedDefinitionSource) (*timezonesource.Snapshot, error) {
	spec, err := stage.CanonicalSpec()
	if err != nil {
		return nil, err
	}
	return s.resolver.Resolve(spec.TimezoneName, spec.TimezoneData)
}
func (s *PreparationService) DefinitionBatch(owner [32]byte, frontier uint64, limit int) (*DefinitionInstallBatch, bool, error) {
	stage, ok := s.Definition(owner)
	if !ok {
		return nil, false, nil
	}
	batch, err := stage.InstallBatch(frontier, limit)
	return batch, true, err
}

func (s *PreparationService) PrepareTimezone(definition *StagedDefinitionSource, snapshot *timezonesource.Snapshot, generation uint64) (*StagedTimezoneCandidate, error) {
	stage, err := StageDefinitionTimezone(definition, snapshot, generation)
	if err != nil {
		return nil, err
	}
	candidate, err := StageTimezoneCandidate(stage)
	if err != nil {
		return nil, err
	}
	s.stages.Put(preparationCacheKey('t', candidate.descriptor.ownerID), candidate)
	return candidate, nil
}
func (s *PreparationService) Timezone(owner [32]byte) (*StagedTimezoneCandidate, bool) {
	value, ok := s.stages.Get(preparationCacheKey('t', owner)).(*StagedTimezoneCandidate)
	return value, ok
}
func (s *PreparationService) TimezoneBatch(owner [32]byte, sourceFrontier uint64, transitionFrontier uint32, limit int) (*TimezoneInstallBatch, bool, error) {
	stage, ok := s.Timezone(owner)
	if !ok {
		return nil, false, nil
	}
	batch, err := stage.InstallBatch(sourceFrontier, transitionFrontier, limit)
	return batch, true, err
}

// RestoreTimezone uses only a complete detached original source. Re-resolution
// of an incomplete source must produce a separately fenced candidate instead.
func (s *PreparationService) RestoreTimezone(definition *StagedDefinitionSource, source *scheduler2pb.TimezonePreparationSource, generation uint64, expectedOwner [32]byte) (*StagedTimezoneCandidate, error) {
	stage, err := StageDefinitionTimezoneSnapshot(definition, source, generation)
	if err != nil {
		return nil, err
	}
	candidate, err := StageTimezoneCandidate(stage)
	if err != nil {
		return nil, err
	}
	if candidate.descriptor.ownerID != expectedOwner {
		return nil, serviceerror.NewFailedPrecondition("reconstructed timezone owner does not match")
	}
	s.stages.Put(preparationCacheKey('t', expectedOwner), candidate)
	return candidate, nil
}
