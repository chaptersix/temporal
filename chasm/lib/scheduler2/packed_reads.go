package scheduler2

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/packedstore"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

func (s *Scheduler) actionRecord(ctx chasm.Context, id string) (*pb.PublicActionRecord, bool, error) {
	if s.PackedStorage == nil {
		value, ok := s.ActionRecords.Get(ctx, id)
		return value, ok, nil
	}
	v, err := s.newPackedView(ctx)
	if err != nil {
		return nil, false, err
	}
	defer v.close()
	return v.action(id)
}
func (s *Scheduler) readyRecord(ctx chasm.Context, sequence uint64) (*pb.ReadyRecord, bool, error) {
	if s.PackedStorage == nil {
		value, ok := s.ReadyRecords.Get(ctx, readyRecordKey(sequence))
		return value, ok, nil
	}
	v, err := s.newPackedView(ctx)
	if err != nil {
		return nil, false, err
	}
	defer v.close()
	return v.ready(sequence)
}
func (s *Scheduler) runningRecord(ctx chasm.Context, sequence uint64) (*pb.RunningPublicAction, bool, error) {
	if s.PackedStorage == nil {
		value, ok := s.RunningRecords.Get(ctx, runningRecordKey(sequence))
		return value, ok, nil
	}
	v, err := s.newPackedView(ctx)
	if err != nil {
		return nil, false, err
	}
	defer v.close()
	return v.running(sequence)
}
func (s *Scheduler) queuedRecord(ctx chasm.Context, sequence uint64) (*pb.QueuedOccurrence, bool, error) {
	if s.PackedStorage == nil {
		value, ok := s.QueueRecords.Get(ctx, queueRecordKey(sequence))
		return value, ok, nil
	}
	v, err := s.newPackedView(ctx)
	if err != nil {
		return nil, false, err
	}
	defer v.close()
	return v.queued(sequence)
}

type packedIndexReader struct {
	batch *packedstore.Batch
	kind  byte
	err   error
}

func (b *packedIndexReader) Read(key string) ([]byte, bool, error) {
	if b.err != nil {
		return nil, false, b.err
	}
	return b.batch.Get(packedstore.ID{b.kind}, []byte(key))
}
func (*packedIndexReader) Apply([]queueindex.IndexChange) error {
	return serviceerror.NewInternal("packed index read view cannot publish")
}
func (s *Scheduler) packedIndex(ctx chasm.Context, kind byte) queueindex.IndexBackend {
	return &packedIndexReader{batch: packedstore.NewBatch(&packedPageBackend{ctx: ctx, pages: s.PackedPages}), kind: kind, err: s.validatePackedStorage()}
}
func (s *Scheduler) operationIndexReader(ctx chasm.Context, retirement bool) queueindex.IndexBackend {
	if s.PackedStorage == nil {
		return &indexBackend{ctx: ctx, pages: s.OperationPages}
	}
	kind := packedReadyIndexKind
	if retirement {
		kind = packedRetirementIndexKind
	}
	return s.packedIndex(ctx, kind)
}
func (s *Scheduler) projectionIndexReader(ctx chasm.Context) queueindex.IndexBackend {
	if s.PackedStorage == nil {
		return &indexBackend{ctx: ctx, pages: s.ProjectionPages}
	}
	return s.packedIndex(ctx, packedRunningIndexKind)
}
func (s *Scheduler) queueIndexReader(ctx chasm.Context) queueindex.IndexBackend {
	if s.PackedStorage == nil {
		return &indexBackend{ctx: ctx, pages: s.QueuePages}
	}
	return s.packedIndex(ctx, packedQueueIndexKind)
}
