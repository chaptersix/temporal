package scheduler2

import (
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
)

type memoVersionKey struct{}
type memoVersionReservation struct {
	owner        *Scheduler
	before, next uint64
}

var _ chasm.VisibilityMemoVersionProvider = (*Scheduler)(nil)

// MemoVersion returns an independent revision without reading the memo or runtime
// configuration. Old records with an absent field have virtual baseline one.
func (s *Scheduler) MemoVersion(chasm.Context) uint64 { return max(uint64(1), s.MemoRevision) }

func (s *Scheduler) prepareMemoVersion() (uint64, error) {
	before := s.MemoVersion(nil)
	if before == math.MaxUint64 {
		return 0, serviceerror.NewFailedPrecondition("schedule memo revision exhausted")
	}
	return before + 1, nil
}

// reserveMemoVersion preflights one atomic memo revision for all nested writers
// on this root. The returned context must flow to every publication in the same
// CHASM transaction. ContextWithValue preserves the concrete CHASM context type.
func (s *Scheduler) reserveMemoVersion(ctx chasm.MutableContext) (chasm.MutableContext, error) {
	if reservation, ok := ctx.Value(memoVersionKey{}).(*memoVersionReservation); ok && reservation != nil && reservation.owner == s {
		if !reservation.matches(s) {
			return nil, serviceerror.NewInternal("stale memo revision reservation")
		}
		return ctx, nil
	}
	next, err := s.prepareMemoVersion()
	if err != nil {
		return nil, err
	}
	return chasm.ContextWithValue(ctx, memoVersionKey{}, &memoVersionReservation{owner: s, before: s.MemoVersion(nil), next: next}), nil
}
func (r *memoVersionReservation) matches(s *Scheduler) bool {
	current := s.MemoVersion(nil)
	return r.owner == s && (current == r.before || current == r.next)
}

// publishMemoVersion consumes the prepared revision without another increment.
// Multiple memo replacements in one transaction share this revision. A missing
// reservation is a programming invariant failure: discard the enclosing lease.
func (s *Scheduler) publishMemoVersion(ctx chasm.MutableContext) {
	reservation, ok := ctx.Value(memoVersionKey{}).(*memoVersionReservation)
	if !ok || reservation == nil || !reservation.matches(s) {
		panic(serviceerror.NewInternal("memo publication requires prepared revision")) //nolint:forbidigo // The framework must discard a partially published transaction on an invalid publication plan.
	}
	s.MemoRevision = reservation.next
}
