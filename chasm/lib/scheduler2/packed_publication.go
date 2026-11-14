package scheduler2

import (
	"slices"
	"strconv"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

// preparePackedPublication captures one physical batch before any existing queue,
// record, blob, root or task publication. Legacy addressed/finite profiles do not
// participate. The same lease must publish the returned detached mutation.
func (s *Scheduler) preparePackedPublication(ctx chasm.Context, records operationRecordPlan, public publicTransitionPlan) (packedMutation, error) {
	if s.PackedStorage == nil {
		return packedMutation{}, nil
	}
	e, err := s.newPackedEditor(ctx)
	if err != nil {
		return packedMutation{}, err
	}
	defer e.abort()
	if err = e.applyIndex(packedRetirementIndexKind, records.indexes.changes); err != nil {
		return packedMutation{}, err
	}
	for _, change := range records.changes {
		// Release request metadata while its last operation identity is still owned.
		if change.changeRequest && change.request == nil {
			if err = e.setRequest(change.id, nil); err != nil {
				return packedMutation{}, err
			}
		}
		if err = e.setOperation(change.id, change.operation); err != nil {
			return packedMutation{}, err
		}
		if change.changeRequest && change.request != nil {
			if err = e.setRequest(change.id, change.request); err != nil {
				return packedMutation{}, err
			}
		}
	}
	if err = e.applyReadiness(records.readiness); err != nil {
		return packedMutation{}, err
	}
	projection := s.ProjectionStore
	if public.projection != nil {
		if err = e.applyProjection(public.projection); err != nil {
			return packedMutation{}, err
		}
		projection = public.projection.control
	}
	return e.finish(records.control, projection)
}
func (s *Scheduler) preparePackedReadiness(ctx chasm.Context, plan operationReadinessPlan, control *pb.OperationStoreControl) (packedMutation, error) {
	if s.PackedStorage == nil {
		return packedMutation{}, nil
	}
	e, err := s.newPackedEditor(ctx)
	if err != nil {
		return packedMutation{}, err
	}
	defer e.abort()
	if err = e.applyReadiness(plan); err != nil {
		return packedMutation{}, err
	}
	return e.finish(control, s.ProjectionStore)
}
func (e *packedEditor) applyReadiness(plan operationReadinessPlan) error {
	if err := e.applyIndex(packedReadyIndexKind, plan.indexes.changes); err != nil {
		return err
	}
	for _, record := range plan.recovered {
		if err := e.setOperation(record.Id, record); err != nil {
			return err
		}
	}
	for _, change := range plan.records {
		if err := e.setReady(change.sequence, change.record); err != nil {
			return err
		}
	}
	return nil
}
func (e *packedEditor) applyProjection(plan *projectionMutation) error {
	if err := e.applyIndex(packedRunningIndexKind, plan.indexes); err != nil {
		return err
	}
	ids := make([]string, 0, len(plan.actions))
	for id := range plan.actions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if err := e.setAction(id, plan.actions[id]); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(plan.running))
	for key := range plan.running {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		sequence, err := strconv.ParseUint(key, 16, 64)
		if err != nil || runningRecordKey(sequence) != key {
			return serviceerror.NewInternal("invalid packed running address")
		}
		if err = e.setRunning(sequence, plan.running[key]); err != nil {
			return err
		}
	}
	return nil
}
