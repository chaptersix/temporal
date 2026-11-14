package scheduler2

import (
	"errors"
	"strconv"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
)

func (s *Scheduler) preparedEnumerator(ctx chasm.Context, g *Generator) (*occurrence.PreparedEnumerator, occurrence.PreparedSpecHeader, *occurrence.PreparedLocation, error) {
	p := g.PreparedDefinition.Get(ctx)
	header, reader, err := p.Prepared(ctx, DefinitionEvaluationPolicy{ReturnExcluded: true})
	if err != nil {
		return nil, header, nil, err
	}
	timezone, err := p.PreparedTimezone(ctx)
	if err != nil {
		return nil, header, nil, err
	}
	zone, err := occurrence.NewResolvedLocation(timezone)
	if err != nil {
		return nil, header, nil, err
	}
	enumerator, err := occurrence.NewPreparedEnumerator([32]byte(p.SourceId), p.Revision, header, zone, reader)
	return enumerator, header, zone, err
}

func (s *Scheduler) scanPreparedAutomatic(ctx chasm.MutableContext, g *Generator) error {
	if ready, err := s.ensureAutomaticSourceReady(ctx, g); err != nil || !ready {
		return err
	}
	e, header, zone, err := s.preparedEnumerator(ctx, g)
	if err != nil {
		return err
	}
	state := decodeContinuation(g.Continuation)
	var work occurrence.PreparedSpecState
	if stored, ok := g.PreparedWork.TryGet(ctx); ok {
		work, err = occurrence.DecodePreparedSpecState(stored.Data, header, zone, state.SearchAfter)
		if errors.Is(err, occurrence.ErrPreparedZoneChanged) {
			work, err = occurrence.PreparedSpecState{}, nil
		}
		if err != nil {
			return err
		}
	}
	result, err := e.Peek(state, work, ctx.Now(s), occurrence.MaxPreparationSteps)
	if err != nil {
		return err
	}
	var nextWork *scheduler2pb.Payload
	if result.Work != (occurrence.PreparedSpecState{}) {
		encoded, err := occurrence.EncodePreparedSpecState(result.Work)
		if err != nil {
			return err
		}
		nextWork = &scheduler2pb.Payload{Data: encoded}
	}
	if err := s.publishAutomaticScan(ctx, g, result.Result, nextWork); err != nil {
		return err
	}
	if nextWork == nil {
		g.PreparedSourceRevision = 0
	}
	return nil
}

type automaticDisposer interface {
	DisposeRange(occurrence.State) (occurrence.State, error)
}

func (s *Scheduler) automaticDisposer(ctx chasm.Context, g *Generator) (automaticDisposer, error) {
	if g.PreparedDefinition.IsSet() {
		p := g.PreparedDefinition.Get(ctx)
		h, err := p.header()
		if err != nil {
			return nil, err
		}
		return occurrence.NewPreparedCursor([32]byte(p.SourceId), p.Revision, h.prepared([32]byte(p.OwnerId), DefinitionEvaluationPolicy{ReturnExcluded: true}))
	}
	return s.currentEnumerator(ctx)
}

func (s *Scheduler) scanPreparedBackfill(ctx chasm.MutableContext, g *Generator, b *scheduler2pb.BackfillState) error {
	if ready, err := s.ensureBackfillSourceReady(ctx, g, b); err != nil || !ready {
		return err
	}
	e, header, zone, err := s.preparedEnumerator(ctx, g)
	if err != nil {
		return err
	}
	var state occurrence.State
	rebased := false
	if b.Continuation == nil {
		state, err = e.BeginRange(decodeTime(b.Start, b.StartTime), decodeTime(b.End, b.EndTime))
	} else {
		state = decodeContinuation(b.Continuation)
		if b.Continuation.Revision != g.Continuation.Revision {
			state, _, err = e.RebaseRange(state)
			rebased = true
		}
	}
	if err != nil {
		return err
	}
	var work occurrence.PreparedSpecState
	if !rebased && b.Continuation != nil {
		if stored, ok := s.BackfillWork.Get(ctx, strconv.FormatUint(b.Id, 10)); ok {
			work, err = occurrence.DecodePreparedSpecState(stored.Data, header, zone, state.SearchAfter)
			if errors.Is(err, occurrence.ErrPreparedZoneChanged) {
				work, err = occurrence.PreparedSpecState{}, nil
			}
			if err != nil {
				return err
			}
		}
	}
	result, err := e.PeekRange(state, work, decodeTime(b.End, b.EndTime), occurrence.MaxPreparationSteps)
	if err != nil {
		return err
	}
	var nextWork *scheduler2pb.Payload
	if result.Work != (occurrence.PreparedSpecState{}) {
		data, err := occurrence.EncodePreparedSpecState(result.Work)
		if err != nil {
			return err
		}
		nextWork = &scheduler2pb.Payload{Data: data}
	}
	if err := s.publishBackfillScan(ctx, g, b, result.Result, nextWork, rebased); err != nil {
		return err
	}
	if nextWork == nil {
		b.PreparedSourceRevision = 0
	}
	return nil
}
