package scheduler2

import (
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

func (s *Scheduler) queueMutationBatch(ctx chasm.Context) (*queueindex.CountBatch, *operationIndexCapture) {
	if s.PackedStorage == nil {
		return s.queueBatch(ctx), nil
	}
	capture := &operationIndexCapture{reader: s.queueIndexReader(ctx)}
	return queueindex.NewCountStoreWithBackend(capture).Begin(), capture
}
func (s *Scheduler) stagePackedQueueMutation(ctx chasm.MutableContext, capture *operationIndexCapture, changeRecord bool, sequence uint64, record *pb.QueuedOccurrence) error {
	if capture == nil {
		return nil
	}
	e, err := s.newPackedEditor(ctx)
	if err != nil {
		return err
	}
	defer e.abort()
	if err = e.applyIndex(packedQueueIndexKind, capture.changes); err != nil {
		return err
	}
	if changeRecord {
		if err = e.setQueued(sequence, record); err != nil {
			return err
		}
	}
	plan, err := e.finish(s.OperationStore, s.ProjectionStore)
	if err != nil {
		return err
	}
	// Queue writes never allocate admission locators. The detached operation plan
	// may advance that control later in this same semantic transition.
	s.publishPackedMutation(ctx, plan)
	return nil
}
