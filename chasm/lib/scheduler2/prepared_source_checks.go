package scheduler2

import (
	"bytes"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
)

const (
	sourceCheckAutomatic uint32 = iota + 1
	sourceCheckBackfill
	sourceCheckPreview
)

type sourceGrant struct {
	generator *Generator
	backfill  *pb.BackfillState
	preview   *pb.PublicListPreview
	revision  int64
}

// requestPreparedSource starts one source check for a logical lookup. Other
// lookups wait on this fixed obligation and receive their own grant afterward.
func (s *Scheduler) requestPreparedSource(ctx chasm.MutableContext, kind uint32, backfill, preview uint64) (bool, error) {
	if !s.SourceRequest.IsSet() {
		return true, nil
	}
	old := s.SourceRequest.Get(ctx)
	if old.Pending {
		return false, nil
	}
	if old.Token == math.MaxUint64 {
		return false, serviceerror.NewFailedPrecondition("source task token exhausted")
	}
	p := s.Generator.Get(ctx).PreparedDefinition.Get(ctx)
	if _, err := p.header(); err != nil {
		return false, err
	}
	s.publishSourceRequest(ctx, &pb.SourcePreparationRequest{DefinitionOwnerId: bytes.Clone(p.OwnerId), Token: old.Token + 1, Phase: sourceResolve, Pending: true, CheckKind: kind, BackfillId: backfill, PreviewStamp: preview})
	return false, nil
}
func (s *Scheduler) ensureAutomaticSourceReady(ctx chasm.MutableContext, g *Generator) (bool, error) {
	if g.PreparedSourceRevision == g.Continuation.Revision {
		return true, nil
	}
	return s.requestPreparedSource(ctx, sourceCheckAutomatic, 0, 0)
}
func (s *Scheduler) ensureBackfillSourceReady(ctx chasm.MutableContext, g *Generator, b *pb.BackfillState) (bool, error) {
	if b.PreparedSourceRevision == g.Continuation.Revision {
		return true, nil
	}
	return s.requestPreparedSource(ctx, sourceCheckBackfill, b.Id, 0)
}
func (s *Scheduler) ensurePreviewSourceReady(ctx chasm.MutableContext, job *pb.PublicListPreview) (bool, error) {
	if job.PreparedSourceReady {
		return true, nil
	}
	return s.requestPreparedSource(ctx, sourceCheckPreview, 0, job.Stamp)
}

// prepareSourceGrant reads and validates bounded recipient metadata before a
// completing response publishes source pages or changes its task fence.
func (s *Scheduler) prepareSourceGrant(ctx chasm.Context, r *pb.SourcePreparationRequest, p *DefinitionPreparation) (sourceGrant, error) {
	grant := sourceGrant{revision: p.Revision}
	switch r.CheckKind {
	case 0:
	case sourceCheckAutomatic:
		grant.generator = s.Generator.Get(ctx)
	case sourceCheckBackfill:
		for _, b := range s.Backfills {
			if b.Id == r.BackfillId {
				grant.backfill = b
				break
			}
		}
	case sourceCheckPreview:
		if job, ok := s.ListPreview.TryGet(ctx); ok && job.Stamp == r.PreviewStamp && bytes.Equal(job.OwnerId, r.DefinitionOwnerId) {
			if err := validateListPreview(job); err != nil {
				return sourceGrant{}, err
			}
			grant.preview = common.CloneProto(job)
			grant.preview.PreparedSourceReady = true
		}
	default:
		return sourceGrant{}, serviceerror.NewInternal("invalid source check recipient")
	}
	return grant, nil
}
func (s *Scheduler) publishSourceGrant(ctx chasm.MutableContext, grant sourceGrant) {
	if grant.generator != nil {
		grant.generator.PreparedSourceRevision = grant.revision
	}
	if grant.backfill != nil {
		grant.backfill.PreparedSourceRevision = grant.revision
	}
	if grant.preview != nil {
		s.ListPreview = chasm.NewDataField(ctx, grant.preview)
	}
}
