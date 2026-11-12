package scheduler2

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	schedulerconfig "go.temporal.io/server/chasm/lib/scheduler"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

type backfillBatchConfigKey struct{}

func (s *Scheduler) publicSkipBatch(b *scheduler2pb.BackfillState) bool {
	return s.PublicRevision != 0 && resolvedPolicy(b.Policy, s.Policy) == enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
}

func (s *Scheduler) suppressBackfill(b *scheduler2pb.BackfillState) bool {
	return s.publicSkipBatch(b) && b.SkipBatchRemaining > 0 && b.SkipBatchRevision == s.Revision
}

// backfillBatchCapacity preserves the reference batch cardinality independently
// of per-transaction work. Retained tracked records stand in for BufferedStarts;
// prototype caps bound this metadata scan to four records and four queue entries.
func (s *Scheduler) backfillBatchCapacity(ctx chasm.Context) int64 {
	getter, ok := ctx.Value(backfillBatchConfigKey{}).(func(string) schedulerconfig.Tweakables)
	if !ok || getter == nil {
		return 0
	}
	tweaks := getter(ctx.NamespaceEntry().Name().String())
	buffered := int64(len(s.Queue))
	for _, field := range s.Operations {
		if field.Get(ctx).Phase != scheduler2pb.PHASE_START_ONLY {
			buffered++
		}
	}
	available := max(int64(0), int64(tweaks.MaxBufferSize)/2-max(int64(0), buffered-10)-int64(tweaks.GeneratorBufferReserveSize))
	return available / int64(max(1, len(s.Backfills)))
}
