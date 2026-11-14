package scheduler2

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

// Backfill accepts a local range command with framework request-ID deduplication.
// Ranges are inclusive at both ends; jittered actual time must not exceed the end.
func Backfill(ctx context.Context, ref chasm.ComponentRef, requestID string, request *schedulepb.BackfillRequest) error {
	_, _, err := chasm.UpdateComponent(ctx, ref, func(s *Scheduler, cc chasm.MutableContext, request *schedulepb.BackfillRequest) (chasm.NoValue, error) {
		return nil, s.AcceptBackfill(cc, request)
	}, request, chasm.WithRequestID(requestID))
	if errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		return nil
	}
	return err
}

// AcceptBackfill stores bounded command metadata without preparing the definition
// or start payload. Enumeration and admission run through the shared coordinator.
func (s *Scheduler) AcceptBackfill(ctx chasm.MutableContext, request *schedulepb.BackfillRequest) error {
	if s.Closed {
		return serviceerror.NewNotFound("schedule is closed")
	}
	if request == nil || !s.HasGenerator || request.StartTime == nil || request.EndTime == nil || !decision.ValidPolicy(request.OverlapPolicy) {
		return serviceerror.NewInvalidArgument("backfill requires a range, supported policy and configured template")
	}
	if request.StartTime.CheckValid() != nil || request.EndTime.CheckValid() != nil {
		return serviceerror.NewInvalidArgument("invalid backfill timestamp")
	}
	start, end := request.StartTime.AsTime(), request.EndTime.AsTime()
	minimum, maximum := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC)
	if end.Before(start) || (s.PublicRevision == 0 && (start.Before(minimum) || !start.Before(maximum) || end.Before(minimum) || !end.Before(maximum))) {
		return serviceerror.NewInvalidArgument("backfill range is outside [2000, 2101) or reversed")
	}
	if err := s.checkPublicCommandCapacity(1); err != nil {
		return err
	}
	if s.PublicRevision == 0 && len(s.Backfills) >= 4 {
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "active backfill capacity exhausted")
	}
	if s.BackfillSequence == math.MaxUint64 {
		return serviceerror.NewFailedPrecondition("backfill sequence exhausted")
	}
	// Reconciliation may reject an exhausted coordinator token. Restore the
	// previous root on that error; reconcileWork checks before publishing a task.
	atNano, err := activityNano(ctx.Now(s))
	if err != nil {
		return err
	}
	previous := s.RootState
	next := common.CloneProto(previous)
	if !next.ActivitySet || atNano > next.ActivityAt {
		next.ActivityAt = atNano
	}
	next.ActivitySet = true
	next.BackfillSequence++
	startNano, startTime := encodeTime(start)
	endNano, endTime := encodeTime(end)
	next.Backfills = append(next.Backfills, &scheduler2pb.BackfillState{Id: next.BackfillSequence, Start: startNano, StartTime: startTime, End: endNano, EndTime: endTime, Policy: request.OverlapPolicy})
	s.RootState = next
	if err := s.reconcileWork(ctx); err != nil {
		s.RootState = previous
		return err
	}
	return nil
}

// selectedBackfill preserves a candidate's acquired turn across capacity waits.
// Otherwise requests rotate by sequence, independently of slice storage order.
func (s *Scheduler) selectedBackfill() *scheduler2pb.BackfillState {
	var first, after *scheduler2pb.BackfillState
	for _, backfill := range s.Backfills {
		if backfill.Id == s.BackfillFocus {
			return backfill
		}
		if first == nil || backfill.Id < first.Id {
			first = backfill
		}
		if backfill.Id > s.LastBackfill && (after == nil || backfill.Id < after.Id) {
			after = backfill
		}
	}
	if after != nil {
		return after
	}
	return first
}

func backfillNeedsScan(g *Generator, b *scheduler2pb.BackfillState) bool {
	return b.Continuation == nil || b.Continuation.Revision != g.Continuation.Revision || b.Continuation.Pending == nil
}

func backfillEnumerator(ctx chasm.Context, g *Generator) (*occurrence.Enumerator, error) {
	definition := g.Definition.Get(ctx)
	spec := &schedulepb.ScheduleSpec{}
	if err := proto.Unmarshal(definition.Spec, spec); err != nil {
		return nil, err
	}
	return g.Scheduler.Get(ctx).newEnumerator(spec, definition.Seed, g.Continuation.Revision)
}

// scanBackfill performs one bounded search quantum. An eligible candidate pins
// its request's turn until durable handoff; exclusion-only yields rotate fairly.
func (s *Scheduler) scanBackfill(ctx chasm.MutableContext) error {
	b := s.selectedBackfill()
	if b == nil {
		return nil
	}
	g := s.Generator.Get(ctx)
	if g.PreparedDefinition.IsSet() {
		return s.scanPreparedBackfill(ctx, g, b)
	}
	enumerator, err := backfillEnumerator(ctx, g)
	if err != nil {
		return err
	}
	var state occurrence.State
	if b.Continuation == nil {
		state, err = enumerator.BeginRange(decodeTime(b.Start, b.StartTime), decodeTime(b.End, b.EndTime))
	} else {
		state = decodeContinuation(b.Continuation)
		if b.Continuation.Revision != g.Continuation.Revision {
			state, _, err = enumerator.RebaseRange(state)
			b.SkipBatchRemaining = 0
		}
	}
	if err != nil {
		return err
	}
	result, err := enumerator.PeekRange(state, decodeTime(b.End, b.EndTime), 8)
	if err != nil {
		return err
	}
	return s.publishBackfillScan(ctx, g, b, result, nil, false)
}

func (s *Scheduler) publishBackfillScan(ctx chasm.MutableContext, g *Generator, b *scheduler2pb.BackfillState, result occurrence.Result, work *scheduler2pb.Payload, rebased bool) error {
	switch result.Status {
	case occurrence.Active:
		b.Continuation = encodeContinuation(result.State)
		s.BackfillFocus = b.Id
	case occurrence.Yield:
		b.Continuation = encodeContinuation(result.State)
		s.LastBackfill = b.Id
		s.BackfillFocus = 0
	case occurrence.Empty, occurrence.Impossible, occurrence.Exhausted:
		kept := make([]*scheduler2pb.BackfillState, 0, len(s.Backfills)-1)
		for _, other := range s.Backfills {
			if other.Id != b.Id {
				kept = append(kept, other)
			}
		}
		state, err := s.stateFor(ctx, "")
		if err != nil {
			return err
		}
		if _, _, err := s.checkIdlePlan(state, g.Continuation, true, len(kept) > 0); err != nil {
			return err
		}
		s.Backfills = kept
		s.LastBackfill = b.Id
		s.BackfillFocus = 0
	default:
		return serviceerror.NewInternal("unexpected backfill enumeration status")
	}
	if g.PreparedDefinition.IsSet() {
		key := strconv.FormatUint(b.Id, 10)
		if work == nil {
			s.BackfillWork.Delete(ctx, key)
		} else {
			s.BackfillWork.Put(ctx, key, work)
		}
		if rebased {
			b.SkipBatchRemaining = 0
		}
	}
	return nil
}

// finishBackfill advances only after the coordinator's durable handoff or overlap
// disposition. The next range quantum determines whether enumeration is complete.
func (s *Scheduler) finishBackfill(ctx chasm.MutableContext) error {
	b := s.selectedBackfill()
	if b == nil || b.Continuation == nil {
		return serviceerror.NewFailedPrecondition("backfill handoff has no selected continuation")
	}
	g := s.Generator.Get(ctx)
	enumerator, err := s.automaticDisposer(ctx, g)
	if err != nil {
		return err
	}
	state, err := enumerator.DisposeRange(decodeContinuation(b.Continuation))
	if err != nil {
		return err
	}
	b.Continuation = encodeContinuation(state)
	s.LastBackfill = b.Id
	s.BackfillFocus = 0
	return nil
}
