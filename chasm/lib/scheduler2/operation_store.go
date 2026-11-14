package scheduler2

import (
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/common"
)

const operationStoreVersion = 1

func (s *Scheduler) completeOperationProfile() bool {
	return s.PublicRevision != 0 && s.IndexedQueue != nil && s.PayloadStore != nil && s.ProjectionStore != nil && s.OperationStore != nil && s.OperationStore.StartDispatch != nil
}

func (s *Scheduler) validateOperationStore() error {
	if s.OperationStore == nil {
		return nil
	}
	c := s.OperationStore
	if c.FormatVersion != operationStoreVersion || c.OperationCount < 0 || c.RequestCount < 0 || c.RequestCount > c.OperationCount || c.RetirementCount < 0 || c.RetirementCount > c.OperationCount {
		return serviceerror.NewInternal("invalid operation store summary")
	}
	if dispatch := c.StartDispatch; dispatch != nil {
		if c.PendingCount < 0 || c.PendingCount > c.OperationCount || c.NextReadySequence == 0 || dispatch.Token == 0 {
			return serviceerror.NewInternal("invalid readiness summary")
		}
		if len(s.PendingStarts) != 0 {
			return serviceerror.NewInternal("mixed start scheduling representations")
		}
	}
	if len(s.Operations) != 0 || len(s.Requests) != 0 {
		return serviceerror.NewInternal("mixed operation storage representations")
	}
	return nil
}

// operationRecord reads one detached metadata value without loading issuance bytes.
func (s *Scheduler) operationRecord(ctx chasm.Context, id string) (*pb.OperationState, bool, error) {
	if s.PackedStorage != nil {
		v, err := s.newPackedView(ctx)
		if err != nil {
			return nil, false, err
		}
		defer v.close()
		return v.operation(id)
	}
	if s.OperationStore != nil {
		value, ok := s.OperationRecords.Get(ctx, id)
		return value, ok, nil
	}
	field, ok := s.Operations[id]
	if !ok {
		return nil, false, nil
	}
	return common.CloneProto(field.Get(ctx)), true, nil
}

func (s *Scheduler) requestRecord(ctx chasm.Context, id string) (*pb.Payload, bool, error) {
	if s.PackedStorage != nil {
		v, err := s.newPackedView(ctx)
		if err != nil {
			return nil, false, err
		}
		defer v.close()
		return v.request(id)
	}
	if s.OperationStore != nil {
		value, ok := s.RequestRecords.Get(ctx, id)
		return value, ok, nil
	}
	field, ok := s.Requests[id]
	if !ok {
		return nil, false, nil
	}
	return common.CloneProto(field.Get(ctx)), true, nil
}

func (s *Scheduler) requiredRequestRecord(ctx chasm.Context, id string) (*pb.Payload, error) {
	if err := s.validateOperationStore(); err != nil {
		return nil, err
	}
	record, ok, err := s.requestRecord(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, serviceerror.NewInternal("operation issuance request is missing")
	}
	return record, nil
}

// operationRecordChange is a fixed addressed owner mutation. Nil values delete.
// Request replacement is forbidden: retries retain the exact admitted snapshot.
type operationRecordChange struct {
	id            string
	operation     *pb.OperationState
	request       *pb.Payload
	changeRequest bool
}
type operationRecordPlan struct {
	changes         []operationRecordChange
	control         *pb.OperationStoreControl
	owners          []blobOwnerChange
	indexes         operationIndexPlan
	readiness       operationReadinessPlan
	dispatchChanged bool
}

func (s *Scheduler) prepareOperationRecords(ctx chasm.Context, changes []operationRecordChange) (operationRecordPlan, error) {
	if err := s.validateOperationStore(); err != nil {
		return operationRecordPlan{}, err
	}
	if len(changes) > 3 {
		return operationRecordPlan{}, serviceerror.NewInternal("operation record mutation budget exceeded")
	}
	plan := operationRecordPlan{control: common.CloneProto(s.OperationStore)}
	seen := make(map[string]bool, len(changes))
	for _, change := range changes {
		if change.id == "" || seen[change.id] || change.operation != nil && change.operation.Id != change.id {
			return operationRecordPlan{}, serviceerror.NewInternal("invalid operation record identity")
		}
		seen[change.id] = true
		oldRefs, err := s.prepareOperationRecord(ctx, change, plan.control)
		if err != nil {
			return operationRecordPlan{}, err
		}
		if change.changeRequest {
			plan.owners = append(plan.owners, blobOwnerChange{owner: "request/" + change.id, before: oldRefs, after: payloadRefs(change.request)})
		}
		change.operation = common.CloneProto(change.operation)
		change.request = common.CloneProto(change.request)
		plan.changes = append(plan.changes, change)
	}
	if plan.control != nil && plan.control.RequestCount > plan.control.OperationCount {
		return operationRecordPlan{}, serviceerror.NewInternal("invalid request owner count")
	}
	indexes, err := s.prepareOperationIndexes(ctx, changes, plan.control)
	if err != nil {
		return operationRecordPlan{}, err
	}
	plan.indexes = indexes
	if err := s.prepareOperationDispatch(ctx, &plan); err != nil {
		return operationRecordPlan{}, err
	}
	return plan, nil
}

func (s *Scheduler) prepareOperationDispatch(ctx chasm.Context, plan *operationRecordPlan) error {
	if plan.control == nil || plan.control.StartDispatch == nil {
		return nil
	}
	editor, err := s.newOperationReadinessEditor(ctx, plan.control)
	if err != nil {
		return err
	}
	defer editor.abort()
	for _, change := range plan.changes {
		if err := editor.change(change); err != nil {
			return err
		}
	}
	plan.dispatchChanged, err = prepareStartDispatch(editor, false)
	if err != nil {
		return err
	}
	plan.readiness, err = editor.finish()
	if err != nil {
		return err
	}
	return nil
}

func changedRecordCount(count int64, before, after bool) (int64, error) {
	if before == after {
		return count, nil
	}
	if after {
		if count == math.MaxInt64 {
			return 0, serviceerror.NewInternal("operation record count exhausted")
		}
		return count + 1, nil
	}
	if count == 0 {
		return 0, serviceerror.NewInternal("operation record count underflow")
	}
	return count - 1, nil
}

// publishOperationRecords is nonrejecting after addressed preflight under the same lease.
func (s *Scheduler) publishOperationRecords(ctx chasm.MutableContext, plan operationRecordPlan) {
	if s.PackedStorage != nil {
		s.OperationStore = plan.control
		s.publishStartDispatch(ctx, plan.dispatchChanged)
		return
	}
	s.publishOperationIndexes(ctx, plan.indexes)
	for _, c := range plan.changes {
		if s.OperationStore != nil {
			if c.operation == nil {
				s.OperationRecords.Delete(ctx, c.id)
			} else {
				s.OperationRecords.Put(ctx, c.id, c.operation)
			}
			if c.changeRequest {
				if c.request == nil {
					s.RequestRecords.Delete(ctx, c.id)
				} else {
					s.RequestRecords.Put(ctx, c.id, c.request)
				}
			}
		} else {
			if c.operation == nil {
				delete(s.Operations, c.id)
			} else {
				s.Operations[c.id] = chasm.NewDataField(ctx, c.operation)
			}
			if c.changeRequest {
				if c.request == nil {
					delete(s.Requests, c.id)
				} else {
					s.Requests[c.id] = chasm.NewDataField(ctx, c.request)
				}
			}
		}
	}
	s.publishOperationReadiness(ctx, plan.readiness)
	s.OperationStore = plan.control
	s.publishStartDispatch(ctx, plan.dispatchChanged)
}

func (s *Scheduler) prepareTransitionRecords(ctx chasm.Context, next decision.State, admitted bool, eviction, completedEviction string, request *pb.Payload) (operationRecordPlan, error) {
	var changes []operationRecordChange
	if eviction != "" {
		changes = append(changes, operationRecordChange{id: eviction, changeRequest: true})
	}
	if op := next.Operation; op != nil {
		metadata := &pb.OperationState{
			StartObserved: op.StartObserved, StartObservedAt: op.StartObservedAt,
			OccurrenceId: op.OccurrenceID, ActionId: op.ActionID, Id: op.ID, ChainId: op.ChainID,
			Nominal: op.Nominal, NominalTime: encodeDecisionTime(op.NominalTime), WorkflowId: op.WorkflowID, Deadline: op.Deadline, RetryAt: op.RetryAt, Phase: pb.Phase(op.Phase), Policy: op.Policy, ManualId: op.ManualID, BackfillId: op.BackfillID,
		}
		change := operationRecordChange{id: op.ID, operation: metadata, changeRequest: admitted || decision.Terminal(op.Phase)}
		if admitted && !decision.Terminal(op.Phase) {
			change.request = request
		}
		changes = append(changes, change)
	}
	if completedEviction != "" {
		found := false
		for i := range changes {
			if changes[i].id == completedEviction {
				changes[i] = operationRecordChange{id: completedEviction, changeRequest: true}
				found = true
			}
		}
		if !found {
			changes = append(changes, operationRecordChange{id: completedEviction, changeRequest: true})
		}
	}
	return s.prepareOperationRecords(ctx, changes)
}

func (s *Scheduler) prepareOperationRecord(ctx chasm.Context, change operationRecordChange, control *pb.OperationStoreControl) (refs [3]string, err error) {
	_, existed, err := s.operationRecord(ctx, change.id)
	if err != nil {
		return refs, err
	}
	hadRequest := false
	if change.changeRequest || change.operation == nil {
		refs, hadRequest, err = s.operationRequestOwnership(ctx, change.id)
		if err != nil {
			return refs, err
		}
	}
	if change.changeRequest && change.request != nil && hadRequest {
		return refs, serviceerror.NewInternal("immutable operation request cannot be replaced")
	}
	if change.operation == nil && (!change.changeRequest && hadRequest || change.request != nil) {
		return refs, serviceerror.NewInternal("request owner requires operation metadata")
	}
	if control != nil {
		if existed && control.OperationCount == 0 || hadRequest && control.RequestCount == 0 {
			return refs, serviceerror.NewInternal("operation record membership exceeds summary")
		}
		var err error
		control.OperationCount, err = changedRecordCount(control.OperationCount, existed, change.operation != nil)
		if err != nil {
			return refs, err
		}
		if change.changeRequest {
			control.RequestCount, err = changedRecordCount(control.RequestCount, hadRequest, change.request != nil)
			if err != nil {
				return refs, err
			}
		}
	}
	return refs, nil
}

func (s *Scheduler) operationRequestOwnership(ctx chasm.Context, id string) ([3]string, bool, error) {
	if s.PayloadStore != nil {
		old, ok, err := s.requestEnvelopeRecord(ctx, id)
		return payloadRefs(old), ok, err
	}
	if s.OperationStore == nil {
		_, ok := s.Requests[id]
		return [3]string{}, ok, nil
	}
	_, ok := s.RequestRecords.Get(ctx, id)
	return [3]string{}, ok, nil
}

// Cleanup reads references without materializing an immutable issuance snapshot.
func (s *Scheduler) requestEnvelopeRecord(ctx chasm.Context, id string) (*pb.Payload, bool, error) {
	if s.PackedStorage == nil {
		return s.requestRecord(ctx, id)
	}
	v, err := s.newPackedView(ctx)
	if err != nil {
		return nil, false, err
	}
	defer v.close()
	record, _, ok, err := v.requestEnvelope(id)
	return record, ok, err
}
