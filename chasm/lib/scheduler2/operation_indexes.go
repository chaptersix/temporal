package scheduler2

import (
	"fmt"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/decision"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/queueindex"
)

// The owner namespace is disjoint from queue generations and future operation
// ready/running indexes. Retirement pages are never shared across executions.
var operationRetirementIndex = queueindex.RetirementIndex{1, 'r', 'e', 't', 'i', 'r', 'e'}

type operationIndexPlan struct{ changes []queueindex.IndexChange }

// operationIndexCapture runs the complete index commit preflight without DATA
// publication. Its detached result can publish with the operation record plan.
type operationIndexCapture struct {
	reader  queueindex.IndexBackend
	changes []queueindex.IndexChange
}

func (c *operationIndexCapture) Read(key string) ([]byte, bool, error) { return c.reader.Read(key) }
func (c *operationIndexCapture) Apply(changes []queueindex.IndexChange) error {
	c.changes = make([]queueindex.IndexChange, len(changes))
	for i, change := range changes {
		c.changes[i] = queueindex.IndexChange{Key: change.Key, Value: slices.Clone(change.Value), Delete: change.Delete}
	}
	slices.SortFunc(c.changes, func(a, b queueindex.IndexChange) int { return strings.Compare(a.Key, b.Key) })
	return nil
}

// operationRetirementKey reconstructs identity from the admitted metadata. The
// legacy nominal remains the sort value even when NominalTime is present.
func (s *Scheduler) operationRetirementKey(op *pb.OperationState) (queueindex.RetirementKey, error) {
	var empty queueindex.RetirementKey
	if op == nil || s.Incarnation == "" {
		return empty, serviceerror.NewInternal("invalid retirement operation identity")
	}
	if op.NominalTime != nil {
		if err := op.NominalTime.CheckValid(); err != nil {
			return empty, serviceerror.NewInternal("invalid retirement nominal timestamp")
		}
	}
	occurrence := fmt.Sprintf("automatic/%d", op.Nominal)
	if op.ManualId != 0 {
		occurrence = fmt.Sprintf("manual/%d", op.ManualId)
	}
	if op.BackfillId != 0 {
		occurrence = fmt.Sprintf("backfill/%d/%s", op.BackfillId, decodeDecisionTime(op.NominalTime).Key(op.Nominal))
	}
	action := s.Incarnation + "/" + occurrence + "/action"
	if op.OccurrenceId != occurrence || op.ActionId != action || op.Id != action+"/start" {
		return empty, serviceerror.NewInternal("retirement operation identity mismatch")
	}
	key, err := queueindex.NewRetirementKey(op.Nominal, occurrence+"/action/start")
	if err != nil {
		return empty, serviceerror.NewInternal("invalid retirement operation suffix")
	}
	return key, nil
}

func (s *Scheduler) prepareOperationIndexes(ctx chasm.Context, changes []operationRecordChange, control *pb.OperationStoreControl) (operationIndexPlan, error) {
	var plan operationIndexPlan
	if err := s.validateOperationStore(); err != nil {
		return plan, err
	}
	if s.OperationStore == nil || s.PublicRevision == 0 {
		return plan, nil
	}
	if len(changes) > 3 {
		return plan, serviceerror.NewInternal("operation index mutation budget exceeded")
	}
	capture := &operationIndexCapture{reader: &indexBackend{ctx: ctx, pages: s.OperationPages}}
	batch := queueindex.NewCountStoreWithBackend(capture).Begin()
	defer batch.Abort()
	if err := validateRetirementCount(batch, s.OperationStore); err != nil {
		return plan, err
	}
	if control == nil {
		return plan, serviceerror.NewInternal("missing prospective operation control")
	}
	for _, change := range changes {
		if err := s.prepareRetirementChange(ctx, batch, change, control); err != nil {
			return plan, err
		}
	}

	if err := validateRetirementCount(batch, control); err != nil {
		return plan, err
	}
	if err := batch.Commit(); err != nil {
		return plan, err
	}
	plan.changes = capture.changes
	return plan, nil
}

// publishOperationIndexes follows every rejecting preflight under the same lease.
// A framework panic requires discarding the enclosing CHASM transaction.
func (s *Scheduler) publishOperationIndexes(ctx chasm.MutableContext, plan operationIndexPlan) {
	for _, change := range plan.changes {
		if change.Delete {
			s.OperationPages.Delete(ctx, change.Key)
		} else {
			s.OperationPages.Put(ctx, change.Key, &commonpb.Payload{Data: change.Value})
		}
	}
}

func (s *Scheduler) indexedRetentionEviction(ctx chasm.Context, before, next decision.State) (string, error) {
	if err := s.validateOperationStore(); err != nil {
		return "", err
	}
	if s.OperationStore == nil || s.PublicRevision == 0 {
		return "", serviceerror.NewInternal("retirement index requires public operation storage")
	}
	if next.Operation == nil || before.Operation != nil && before.Operation.ID == next.Operation.ID || s.OperationStore.OperationCount < 4 {
		return "", nil
	}
	batch := queueindex.NewCountStoreWithBackend(&indexBackend{ctx: ctx, pages: s.OperationPages}).Begin()
	defer batch.Abort()
	if err := validateRetirementCount(batch, s.OperationStore); err != nil {
		return "", err
	}
	key, ok, err := batch.RetirementMinimum(operationRetirementIndex)
	if err != nil || !ok {
		return "", err
	}
	id := s.Incarnation + "/" + key.Suffix()
	op, ok := s.operationRecord(ctx, id)
	if !ok || op == nil || op.Phase != pb.PHASE_START_ONLY {
		return "", serviceerror.NewInternal("retirement index references missing or ineligible operation")
	}
	actual, err := s.operationRetirementKey(op)
	if err != nil {
		return "", err
	}
	if actual != key {
		return "", serviceerror.NewInternal("retirement index key disagrees with operation")
	}
	return id, nil
}

func validateRetirementCount(batch *queueindex.CountBatch, control *pb.OperationStoreControl) error {
	if control == nil || control.RetirementCount < 0 || control.RetirementCount > control.OperationCount {
		return serviceerror.NewInternal("invalid retirement summary")
	}
	count, err := batch.RetirementCount(operationRetirementIndex)
	if err != nil {
		return err
	}
	if count != uint64(control.RetirementCount) {
		return serviceerror.NewInternal("retirement index count disagrees with summary")
	}
	return nil
}

func (s *Scheduler) prepareRetirementChange(ctx chasm.Context, batch *queueindex.CountBatch, change operationRecordChange, control *pb.OperationStoreControl) error {
	old, exists := s.operationRecord(ctx, change.id)
	if exists && (old == nil || old.Id != change.id) {
		return serviceerror.NewInternal("retirement record identity mismatch")
	}
	if change.operation != nil && change.operation.Id != change.id {
		return serviceerror.NewInternal("retirement change identity mismatch")
	}
	before := exists && old.Phase == pb.PHASE_START_ONLY
	after := change.operation != nil && change.operation.Phase == pb.PHASE_START_ONLY
	var err error
	control.RetirementCount, err = changedRecordCount(control.RetirementCount, before, after)
	if err != nil {
		return err
	}
	if before {
		if err = s.changeRetirementMembership(batch, old, false); err != nil {
			return err
		}
	}
	if after {
		return s.changeRetirementMembership(batch, change.operation, true)
	}
	return nil
}
func (s *Scheduler) changeRetirementMembership(batch *queueindex.CountBatch, op *pb.OperationState, add bool) error {
	key, err := s.operationRetirementKey(op)
	if err != nil {
		return err
	}
	if add {
		return batch.InsertRetirement(operationRetirementIndex, key)
	}
	return batch.DeleteRetirement(operationRetirementIndex, key)
}
