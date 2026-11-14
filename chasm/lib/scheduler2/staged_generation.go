package scheduler2

import (
	"bytes"
	"math"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
)

type generationCursor interface {
	Begin(time.Time) (occurrence.State, error)
	Update(occurrence.State, time.Time) (occurrence.State, *occurrence.Candidate, error)
}

type preparedGenerationPlan struct {
	stage       *StagedDefinitionSource
	previous    *DefinitionPreparation
	replacement *definitionReplacementPlan
	request     *pb.SourcePreparationRequest
}

// prepareGenerationCursor consumes immutable ingress preparation. It never
// resolves a timezone or rebuilds calendar masks inside the mutation callback.
func (s *Scheduler) prepareGenerationCursor(ctx chasm.Context, config GenerationConfig) (*Generator, generationCursor, *preparedGenerationPlan, error) {
	var g *Generator
	if s.HasGenerator {
		g = s.Generator.Get(ctx)
	}
	if config.prepared == nil {
		if g != nil && g.PreparedDefinition.IsSet() {
			return nil, nil, nil, serviceerror.NewFailedPrecondition("prepared definition replacement requires staged input")
		}
		cursor, err := s.newEnumerator(config.Spec, config.Seed, config.Revision)
		return g, cursor, nil, err
	}
	stage := config.prepared
	if err := stage.validate(); err != nil {
		return nil, nil, nil, err
	}
	if s.OperationStore == nil || stage.revision != config.Revision || string(stage.source.Seed) != config.Seed {
		return nil, nil, nil, serviceerror.NewFailedPrecondition("prepared generation binding does not match")
	}
	if g != nil && !g.PreparedDefinition.IsSet() {
		return nil, nil, nil, serviceerror.NewFailedPrecondition("prepared generation requires a fresh execution")
	}
	cursor, err := occurrence.NewPreparedCursor(stage.header.identity, stage.revision, stage.header.prepared(stage.owner, DefinitionEvaluationPolicy{ReturnExcluded: true}))
	if err != nil {
		return nil, nil, nil, err
	}
	plan := &preparedGenerationPlan{stage: stage}
	if g != nil {
		plan.previous = g.PreparedDefinition.Get(ctx)
		plan.replacement, err = plan.previous.prepareDefinitionReplacement(ctx, stage)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	token := uint64(1)
	if old, ok := s.SourceRequest.TryGet(ctx); ok {
		if old.Token == math.MaxUint64 {
			return nil, nil, nil, serviceerror.NewFailedPrecondition("source task token exhausted")
		}
		token = old.Token + 1
	}
	// Installation starts the first lookup of this definition. Its source
	// resolution grants that lookup, including any bounded continuation quanta.
	plan.request = &pb.SourcePreparationRequest{DefinitionOwnerId: bytes.Clone(stage.owner[:]), Token: token, Phase: sourceDefinition, Pending: true, CheckKind: sourceCheckAutomatic}
	if len(stage.pages) == 0 {
		plan.request.Phase = sourceResolve
	}
	return g, cursor, plan, nil
}

func (s *Scheduler) publishPreparedGeneration(ctx chasm.MutableContext, g *Generator, plan *preparedGenerationPlan) {
	if plan.previous != nil {
		plan.previous.publishDefinitionReplacement(ctx, plan.replacement)
	} else {
		g.PreparedDefinition = chasm.NewComponentField(ctx, publishNewDefinitionPreparation(ctx, plan.stage))
	}
	g.PreparedWork = chasm.NewEmptyField[*pb.Payload]()
	g.PreparedSourceRevision = 0
	s.publishSourceRequest(ctx, plan.request)
}
